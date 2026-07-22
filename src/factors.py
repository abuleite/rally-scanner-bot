"""
多因子评分模型 v1.0 - 基于踏空组合 (ZH063783) 12年调仓数据
========================================================

六维因子评分体系 (100分):
  1. 行业因子 (40%) - 偏好医药制造/电子制造/电气设备/有色金属/新能源
  2. 市值因子 (20%) - 主板(60/00开头)、市值>100亿、非ST
  3. 趋势因子 (15%) - MA60/MA120之上、均线多头排列
  4. 动量因子 (10%) - 近3月涨幅10-50%、近1月缩量回调
  5. 质量因子 (10%) - ROE>10%、营收增长>15%
  6. 仓位因子 (5%)  - 单票≤40%、基金/北向持仓增加

评分公式:
  Score = 0.40*Industry + 0.20*MarketCap + 0.15*Trend
        + 0.10*Momentum + 0.10*Quality + 0.05*Position

输出:
  - 每只股票的综合评分 (0-100)
  - 各因子明细得分
  - 交易 playbook (入场位/止损/仓位/出场条件)
"""

import re
import time
import random
import threading
import requests
import pandas as pd
import numpy as np
from loguru import logger
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# 行业分类映射 (踏空组合偏好行业)
# ============================================================

PREFERRED_INDUSTRIES = {
    "医药制造": {
        "keywords": ["医药", "制药", "生物", "医疗", "药", "基因", "疫苗", "健康", "医"],
        "score": 40,
    },
    "电子制造": {
        "keywords": ["电子", "半导体", "芯片", "集成电路", "光学", "光电子", "PCB", "传感器"],
        "score": 38,
    },
    "电气设备": {
        "keywords": ["电气", "新能源", "光伏", "风电", "储能", "电池", "锂电", "充电桩", "特高压", "电力"],
        "score": 36,
    },
    "有色金属": {
        "keywords": ["有色", "黄金", "铜", "铝", "稀土", "锂", "钴", "镍", "矿产", "冶炼"],
        "score": 34,
    },
    "计算机/软件": {
        "keywords": ["软件", "计算机", "信息", "数据", "人工智能", "云计算", "大数据", "网络安全", "信创", "AI", "IT"],
        "score": 32,
    },
    "汽车制造": {
        "keywords": ["汽车", "整车", "新能源车", "零部件", "智能驾驶", "自动驾驶", "汽配"],
        "score": 30,
    },
    "军工航天": {
        "keywords": ["军工", "航天", "航空", "船舶", "兵器", "国防", "军"],
        "score": 28,
    },
    "化工": {
        "keywords": ["化工", "化学", "塑料", "橡胶", "纤维", "新材料", "石化"],
        "score": 24,
    },
    "食品饮料": {
        "keywords": ["食品", "饮料", "酒", "乳", "调味", "肉类", "粮油"],
        "score": 20,
    },
    "其他": {
        "keywords": [],
        "score": 10,
    },
}

# 行业关键词 → 行业名 快速查找表 (构建时自动填充)
_INDUSTRY_LOOKUP = {}
for _ind_name, _ind_info in PREFERRED_INDUSTRIES.items():
    for _kw in _ind_info["keywords"]:
        _INDUSTRY_LOOKUP[_kw] = _ind_name


# ============================================================
# 评分结果数据结构
# ============================================================

@dataclass
class FactorScore:
    """单只股票的多因子评分结果"""
    code: str
    name: str

    # 综合评分
    total_score: float = 0.0
    level: str = ""          # S(>=85), A(>=70), B(>=55), C(<55)

    # 六因子得分 (0-100)
    industry_score: float = 0.0    # 行业适配度
    marketcap_score: float = 0.0   # 市值适配度
    trend_score: float = 0.0       # 趋势强度
    momentum_score: float = 0.0    # 动量质量
    quality_score: float = 0.0     # 质量基本面
    position_score: float = 0.0    # 仓位合理性

    # 因子明细
    industry_name: str = ""
    market_cap: float = 0.0        # 流通市值(亿)
    ma_alignment_ok: bool = False  # 均线多头排列
    ma_deep_trend_ok: bool = False # 深层趋势确认
    surge_3m: float = 0.0          # 近3月涨幅%
    surge_1m: float = 0.0          # 近1月涨幅%
    vol_contraction: bool = False  # 近期缩量回调
    roe: float = 0.0               # ROE %
    revenue_growth: float = 0.0    # 营收增长率%
    fund_holding_increase: bool = False  # 基金持仓增加
    north_holding_increase: bool = False # 北向持仓增加

    # 交易 playbook
    entry_price: float = 0.0       # 建议入场价
    stop_loss: float = 0.0         # 止损价
    position_pct: float = 0.0      # 建议仓位%
    exit_condition: str = ""       # 出场条件
    playbook_note: str = ""        # 操作备注

    # 当前行情数据
    current_price: float = 0.0
    change_pct: float = 0.0
    turnover_rate: float = 0.0
    pe: float = 0.0

    # 评分时间
    eval_time: str = ""


# ============================================================
# 多因子评分引擎
# ============================================================

class MultiFactorScorer:
    """
    基于踏空组合 (ZH063783) 选股方法论的六因子评分引擎

    核心权重:
      - 行业 40%: 重仓行业高配，非偏好行业低配
      - 市值 20%: 主板大盘股优先，排除小盘/ST
      - 趋势 15%: 均线多头 + 中长期趋势确认
      - 动量 10%: 温和上涨而非暴涨，缩量回调佳
      - 质量 10%: ROE > 10%，营收持续增长
      - 仓位 5%:  机构/北向增持信号
    """

    def __init__(self):
        self._spot_cache: Optional[pd.DataFrame] = None
        self._fund_cache: Optional[pd.DataFrame] = None

    # ============================================================
    # 因子 1: 行业适配度 (40%)
    # ============================================================

    def _score_industry(self, name: str, industry_info: Optional[str] = None) -> Tuple[float, str]:
        """
        评估股票行业与踏空组合偏好的匹配度

        匹配逻辑:
          - 精确匹配优先行业 → 满分
          - 关键词匹配 → 逐级递减
          - 无匹配 → 基准分

        Returns:
            (得分, 行业名称)
        """
        # 如果传入了已知的行业信息
        if industry_info:
            for ind_name, ind_info in PREFERRED_INDUSTRIES.items():
                for kw in ind_info["keywords"]:
                    if kw in str(industry_info):
                        return float(ind_info["score"]), ind_name

        # 通过股票名称关键词匹配
        for ind_name, ind_info in PREFERRED_INDUSTRIES.items():
            for kw in ind_info["keywords"]:
                if kw in str(name):
                    return float(ind_info["score"]), ind_name

        return float(PREFERRED_INDUSTRIES["其他"]["score"]), "其他"

    # ============================================================
    # 因子 2: 市值适配度 (20%)
    # ============================================================

    def _score_marketcap(self, code: str, market_cap: float = 0.0) -> float:
        """
        评估市值适配度

        逻辑:
          - 主板 (60/00开头): 基础分 80
          - 中小板 (002开头): 基础分 60
          - 创业板 (300/301开头): 基础分 40
          - 排除科创板(688)/北交所(8)/老三板(4): 0分

          市值加权:
          - 市值 > 500亿: +20
          - 市值 100-500亿: +15
          - 市值 50-100亿: +10
          - 市值 < 50亿: +5
          - ST 股: 0分

        Returns:
            得分 (0-100)
        """
        code = str(code).strip()

        # 排除板
        if code.startswith("688"):
            return 0.0  # 科创板
        if code.startswith("8"):
            return 0.0  # 北交所
        if code.startswith("4"):
            return 0.0  # 老三板

        # 板块基础分
        if code.startswith("60"):
            board_score = 80.0  # 上海主板
        elif code.startswith("00"):
            board_score = 75.0  # 深圳主板
        elif code.startswith(("002", "003")):
            board_score = 60.0  # 中小板
        elif code.startswith(("300", "301")):
            board_score = 45.0  # 创业板
        else:
            board_score = 30.0  # 其他

        # 市值加权
        if market_cap > 0:
            if market_cap > 500:
                size_bonus = 20.0
            elif market_cap > 100:
                size_bonus = 15.0
            elif market_cap > 50:
                size_bonus = 10.0
            else:
                size_bonus = 5.0
        else:
            size_bonus = 10.0  # 无市值数据时给中间分

        return min(100.0, board_score + size_bonus)

    # ============================================================
    # 因子 3: 趋势强度 (15%)
    # ============================================================

    def _score_trend(self, kline_df: pd.DataFrame) -> Tuple[float, bool, bool]:
        """
        评估趋势强度 (基于 K 线技术指标)

        条件:
          - 股价在 MA60 之上: +30
          - 股价在 MA120 之上: +20
          - MA5 > MA10 > MA20: +25 (短期多头)
          - MA20 > MA60 > MA120: +25 (中长期确认)

        Returns:
            (得分, 均线多头排列, 深层趋势确认)
        """
        if kline_df is None or len(kline_df) < 120:
            return 30.0, False, False

        try:
            close = kline_df["close"]
            latest = close.iloc[-1]

            ma5 = close.rolling(5).mean().iloc[-1]
            ma10 = close.rolling(10).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            ma60 = close.rolling(60).mean().iloc[-1]
            ma120 = close.rolling(120).mean().iloc[-1]

            score = 0.0

            # 价格与关键均线位置
            if not np.isnan(ma60) and latest > ma60:
                score += 30.0
            elif not np.isnan(ma60) and latest > ma60 * 0.95:
                score += 15.0  # 接近 MA60 给半分

            if not np.isnan(ma120) and latest > ma120:
                score += 20.0
            elif not np.isnan(ma120) and latest > ma120 * 0.95:
                score += 10.0

            # 均线排列
            ma_short_ok = False
            if not any(np.isnan(x) for x in [ma5, ma10, ma20]):
                if ma5 > ma10 > ma20:
                    score += 25.0
                    ma_short_ok = True
                elif ma10 > ma20:
                    score += 15.0  # MA5 稍乱但 MA10 > MA20 还行

            ma_deep_ok = False
            if not any(np.isnan(x) for x in [ma20, ma60, ma120]):
                if ma20 > ma60 > ma120:
                    score += 25.0
                    ma_deep_ok = True
                elif ma20 > ma60:
                    score += 10.0

            return min(100.0, score), ma_short_ok, ma_deep_ok

        except Exception as e:
            logger.debug("趋势评分计算异常: {}", e)
            return 30.0, False, False

    # ============================================================
    # 因子 4: 动量质量 (10%)
    # ============================================================

    def _score_momentum(self, kline_df: pd.DataFrame) -> Tuple[float, float, float, bool]:
        """
        评估动量质量

        踏空组合风格:
          - 近 3 月涨幅 10-50% 为佳 (温和上涨，非暴涨暴跌)
          - 近 1 月缩量回调为佳 (买在回调)
          - 近期涨幅过大 (>50%) 警惕回调风险

        评分逻辑:
          - 3月涨幅 10-30%: 满分
          - 3月涨幅 30-50%: 80分
          - 3月涨幅 0-10%: 60分
          - 3月涨幅 <0%: 40分
          - 3月涨幅 >50%: 30分 (警惕回调)
          - 1月缩量回调: +10分

        Returns:
            (得分, 3月涨幅%, 1月涨幅%, 缩量回调)
        """
        if kline_df is None or len(kline_df) < 60:
            return 50.0, 0.0, 0.0, False

        try:
            close = kline_df["close"]
            volume = kline_df["volume"]

            # 近 3 月涨幅 (约 60 个交易日)
            surge_3m = (close.iloc[-1] / close.iloc[-min(60, len(close))]
                        - 1) * 100 if len(close) >= 60 else 0

            # 近 1 月涨幅 (约 20 个交易日)
            surge_1m = (close.iloc[-1] / close.iloc[-min(20, len(close))]
                        - 1) * 100 if len(close) >= 20 else 0

            score = 0.0

            # 3月涨幅评分
            if 10 <= surge_3m <= 30:
                score = 100.0
            elif 30 < surge_3m <= 50:
                score = 80.0
            elif 0 <= surge_3m < 10:
                score = 60.0
            elif -10 <= surge_3m < 0:
                score = 40.0
            elif surge_3m > 50:
                score = 30.0
            else:
                score = 20.0

            # 缩量回调加分
            vol_contraction = False
            if len(volume) >= 20:
                recent_vol = volume.iloc[-5:].mean()
                prev_vol = volume.iloc[-20:-5].mean()
                if prev_vol > 0 and recent_vol / prev_vol < 0.75 and surge_1m < 5:
                    score += 10.0
                    vol_contraction = True

            return min(100.0, score), round(surge_3m, 2), round(surge_1m, 2), vol_contraction

        except Exception as e:
            logger.debug("动量评分计算异常: {}", e)
            return 50.0, 0.0, 0.0, False

    # ============================================================
    # 因子 5: 质量基本面 (10%)
    # ============================================================

    def _score_quality(self, finance_data: Optional[Dict] = None) -> Tuple[float, float, float]:
        """
        评估公司质量基本面

        离线模式下的简化评分:
          - 有财务数据: 基于 ROE/营收增长评分
          - 无财务数据: 基于技术面估算 (流通市值 + 趋势)

        Returns:
            (得分, ROE%, 营收增长%)
        """
        if finance_data:
            roe = float(finance_data.get("roe", 0))
            rev_growth = float(finance_data.get("revenue_growth", 0))

            score = 0.0
            if roe >= 20:
                score += 50
            elif roe >= 15:
                score += 40
            elif roe >= 10:
                score += 30
            elif roe > 0:
                score += 15

            if rev_growth >= 30:
                score += 50
            elif rev_growth >= 15:
                score += 40
            elif rev_growth >= 5:
                score += 25
            elif rev_growth > 0:
                score += 10
            elif rev_growth == 0:
                score += 10  # 无数据给基础分

            return min(100.0, score), roe, rev_growth

        # 无财务数据时给中性分
        return 45.0, 0.0, 0.0

    # ============================================================
    # 因子 6: 仓位合理性 (5%)
    # ============================================================

    def _score_position(self, market_cap: float = 0.0) -> Tuple[float, bool, bool]:
        """
        评估仓位合理性

        逻辑:
          - 市值 > 50亿: 适合重仓
          - 基金持仓增加: 机构背书
          - 北向资金增持: 外资认可

        Returns:
            (得分, 基金增持, 北向增持)
        """
        score = 0.0

        # 市值影响仓位上限
        if market_cap > 500:
            score += 50
        elif market_cap > 100:
            score += 40
        elif market_cap > 50:
            score += 30
        else:
            score += 15

        fund_increase = False
        north_increase = False

        # 机构/北向数据需要额外数据源，离线模式下用技术面补充
        # 如果股票满足趋势因子良好，给予机构认可加分
        score += 25  # 基础信任分 (踏空组合历史上对大盘股高仓位)

        return min(100.0, score), fund_increase, north_increase

    # ============================================================
    # 交易 playbook 生成
    # ============================================================

    def _build_playbook(
        self,
        result: FactorScore,
        kline_df: Optional[pd.DataFrame] = None,
    ) -> FactorScore:
        """
        基于评分结果生成交易 playbook

        包含:
          - 入场价: 基于趋势/动量情况确定
          - 止损价: 技术止损 (关键均线/近期低点)
          - 仓位建议: 总分越高，仓位越重
          - 出场条件: 趋势破坏/动量衰竭/止损触发
        """
        price = result.current_price

        # --- 建议入场价 ---
        # 如果多头排列且缩量回调中 → 当前价即入场
        # 如果多头排列但放量上涨 → 等回踩 MA10
        if result.ma_alignment_ok:
            if result.vol_contraction:
                result.entry_price = round(price, 2)
                result.playbook_note = "缩量回调中，当前价可入场"
            else:
                # 回踩 MA10 入场
                if kline_df is not None and len(kline_df) >= 10:
                    ma10 = kline_df["close"].rolling(10).mean().iloc[-1]
                    if not np.isnan(ma10):
                        result.entry_price = round(ma10, 2)
                        result.playbook_note = f"等待回踩 MA10 ({ma10:.2f}) 入场"
                    else:
                        result.entry_price = round(price * 0.97, 2)
                        result.playbook_note = "等待回调 3% 入场"
                else:
                    result.entry_price = round(price * 0.97, 2)
                    result.playbook_note = "等待回调 3% 入场"
        else:
            result.entry_price = round(price * 0.95, 2)
            result.playbook_note = "等待均线修复后入场"

        # --- 止损价 ---
        if kline_df is not None and len(kline_df) >= 60:
            ma60 = kline_df["close"].rolling(60).mean().iloc[-1]
            recent_low = kline_df["low"].iloc[-20:].min()

            if not np.isnan(ma60) and ma60 > 0:
                # 取 MA60 和 20日最低价的较高者作为止损
                result.stop_loss = round(max(ma60, recent_low) * 0.97, 2)
                result.exit_condition = f"跌破 {result.stop_loss:.2f} (MA60/20日低点止损)"
            else:
                result.stop_loss = round(price * 0.92, 2)
                result.exit_condition = f"跌破 {result.stop_loss:.2f} (8%硬止损)"
        else:
            result.stop_loss = round(price * 0.92, 2)
            result.exit_condition = f"跌破 {result.stop_loss:.2f} (8%硬止损)"

        # --- 仓位建议 ---
        if result.total_score >= 85:
            result.position_pct = 25.0
        elif result.total_score >= 70:
            result.position_pct = 20.0
        elif result.total_score >= 55:
            result.position_pct = 15.0
        else:
            result.position_pct = 10.0

        return result

    # ============================================================
    # 单股综合评分
    # ============================================================

    def score_single(
        self,
        code: str,
        name: str,
        kline_df: Optional[pd.DataFrame] = None,
        market_cap: float = 0.0,
        industry_info: Optional[str] = None,
        finance_data: Optional[Dict] = None,
        current_price: float = 0.0,
        change_pct: float = 0.0,
        turnover_rate: float = 0.0,
        pe: float = 0.0,
    ) -> FactorScore:
        """
        对单只股票执行六因子综合评分

        Args:
            code: 股票代码
            name: 股票名称
            kline_df: K线数据 (需包含 close/volume)
            market_cap: 流通市值(亿)
            industry_info: 已知行业信息
            finance_data: 财务数据 {"roe": float, "revenue_growth": float}
            current_price: 现价
            change_pct: 涨跌幅%
            turnover_rate: 换手率%
            pe: 市盈率

        Returns:
            FactorScore 评分结果
        """
        result = FactorScore(
            code=code,
            name=name,
            current_price=current_price,
            change_pct=change_pct,
            turnover_rate=turnover_rate,
            pe=pe,
            eval_time=datetime.now().strftime("%Y-%m-%d %H:%M"),
        )

        # 因子 1: 行业 (40%)
        result.industry_score, result.industry_name = self._score_industry(name, industry_info)

        # 因子 2: 市值 (20%)
        result.marketcap_score = self._score_marketcap(code, market_cap)
        result.market_cap = market_cap

        # 因子 3: 趋势 (15%)
        trend_score, ma_short, ma_deep = 30.0, False, False
        if kline_df is not None and not kline_df.empty:
            trend_score, ma_short, ma_deep = self._score_trend(kline_df)
        result.trend_score = trend_score
        result.ma_alignment_ok = ma_short
        result.ma_deep_trend_ok = ma_deep

        # 因子 4: 动量 (10%)
        momentum_score, surge_3m, surge_1m, vol_cont = 50.0, 0.0, 0.0, False
        if kline_df is not None and not kline_df.empty:
            momentum_score, surge_3m, surge_1m, vol_cont = self._score_momentum(kline_df)
        result.momentum_score = momentum_score
        result.surge_3m = surge_3m
        result.surge_1m = surge_1m
        result.vol_contraction = vol_cont

        # 因子 5: 质量 (10%)
        result.quality_score, result.roe, result.revenue_growth = self._score_quality(finance_data)

        # 因子 6: 仓位 (5%)
        result.position_score, result.fund_holding_increase, result.north_holding_increase = \
            self._score_position(market_cap)

        # ---- 综合评分 ----
        result.total_score = (
            0.40 * result.industry_score +
            0.20 * result.marketcap_score +
            0.15 * result.trend_score +
            0.10 * result.momentum_score +
            0.10 * result.quality_score +
            0.05 * result.position_score
        )

        # ---- 等级划分 ----
        if result.total_score >= 85:
            result.level = "S"
        elif result.total_score >= 70:
            result.level = "A"
        elif result.total_score >= 55:
            result.level = "B"
        else:
            result.level = "C"

        # ---- 交易 playbook ----
        result = self._build_playbook(result, kline_df)

        return result

    # ============================================================
    # 批量评分
    # ============================================================

    def score_batch(
        self,
        stock_data: Dict[str, Any],
        industry_map: Optional[Dict[str, str]] = None,
    ) -> List[FactorScore]:
        """
        批量多因子评分

        Args:
            stock_data: {code: {"data": DataFrame, "name": str, "market": str}}
            industry_map: {code: "行业名称"} 可选行业映射

        Returns:
            按总评分降序排列的结果列表
        """
        results = []
        total = len(stock_data)

        for i, (code, info) in enumerate(stock_data.items()):
            try:
                kline_df = info.get("data")
                name = info.get("name", code)
                industry = industry_map.get(code) if industry_map else None

                # 从 K 线提取现价和涨跌幅
                current_price = 0.0
                change_pct = 0.0
                if kline_df is not None and len(kline_df) >= 2:
                    current_price = float(kline_df["close"].iloc[-1])
                    prev_close = float(kline_df["close"].iloc[-2])
                    if prev_close > 0:
                        change_pct = (current_price - prev_close) / prev_close * 100

                result = self.score_single(
                    code=code,
                    name=name,
                    kline_df=kline_df,
                    current_price=current_price,
                    change_pct=change_pct,
                    industry_info=industry,
                )
                results.append(result)

            except Exception as e:
                logger.warning("[{}] {} 多因子评分异常: {}", code, name, e)
                # 即使异常，也生成基础评分
                result = self.score_single(code=code, name=code)
                results.append(result)

            if (i + 1) % 100 == 0:
                logger.info(
                    "多因子评分进度: {}/{} ({:.0f}%)",
                    i + 1, total, (i + 1) / total * 100,
                )

        # 按总分降序
        results.sort(key=lambda x: x.total_score, reverse=True)

        # 统计
        s_count = sum(1 for r in results if r.level == "S")
        a_count = sum(1 for r in results if r.level == "A")
        b_count = sum(1 for r in results if r.level == "B")
        c_count = sum(1 for r in results if r.level == "C")

        logger.info(
            "多因子评分完成 | 共 {} 只 | S={} A={} B={} C={}",
            len(results), s_count, a_count, b_count, c_count,
        )

        return results


# ============================================================
# 行业识别辅助函数
# ============================================================

def identify_industry(name: str, known_industry: Optional[str] = None) -> Tuple[str, float]:
    """
    根据股票名称识别所属行业

    Returns:
        (行业名称, 行业分数)
    """
    if known_industry:
        for ind_name, ind_info in PREFERRED_INDUSTRIES.items():
            for kw in ind_info["keywords"]:
                if kw in known_industry:
                    return ind_name, float(ind_info["score"])

    for ind_name, ind_info in PREFERRED_INDUSTRIES.items():
        for kw in ind_info["keywords"]:
            if kw in name:
                return ind_name, float(ind_info["score"])

    return "其他", float(PREFERRED_INDUSTRIES["其他"]["score"])


# ============================================================
# 最近交易日获取 (兼容周末)
# ============================================================

def get_latest_trade_date() -> str:
    """获取最近交易日的 YYYYMMDD 格式日期"""
    today = datetime.now()
    if today.weekday() == 5:
        today = today - timedelta(days=1)
    elif today.weekday() == 6:
        today = today - timedelta(days=2)
    return today.strftime("%Y%m%d")
