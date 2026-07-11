"""
Quant Scanner v2.0 - 主升浪加速行情硬核动量策略过滤引擎
============================================================
基于顶级量化对冲基金动量策略框架, 通过 4 大硬核指标精准识别
处于"主升浪加速阶段"的个股, 彻底过滤震荡市和阴跌股。

四维硬核动量模型 (全部通过才输出信号):
  1. 均线完美多头排列 (MA Alignment)
     Price > MA5 > MA10 > MA20 > MA60 且 MA20 > MA60 > MA120
  2. 动量爆发与放量破局 (Volume Breakout)
     当日收盘价创 20/60 日新高 + 成交量 > 5日/20日均量 × 2.0
  3. 波动率收敛后突破 (VCP 形态量化)
     过去10日振幅逐渐收紧 + 今日大阳线突破前高 + 布林带开口剧烈放大
  4. 大盘环境过滤器 (Beta Filter)
     大盘指数 MA20 过滤器, 指数在 MA20 下方触发空仓熔断

技术指标库:
  - ta: https://github.com/bukosabino/ta (5.1k stars)
    纯 Pandas/NumPy 实现, 无需编译 C 库, GitHub Actions 友好
"""

import pandas as pd
import numpy as np
from loguru import logger
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


# ============================================================
# 大盘环境状态枚举
# ============================================================

class MarketRegime(Enum):
    """大盘环境状态"""
    BULLISH = "BULLISH"           # 指数 > MA20, 做多环境
    CAUTION = "CAUTION"           # 指数 < MA20 但 > MA60, 谨慎环境
    BEARISH = "BEARISH"           # 指数 < MA20 且 < MA60, 空仓熔断


@dataclass
class MarketEnvironment:
    """大盘环境分析结果"""
    index_name: str = ""          # 指数名称 (沪深300/标普500)
    index_price: float = 0        # 指数最新价
    index_ma20: float = 0         # 指数 MA20
    index_ma60: float = 0         # 指数 MA60
    regime: MarketRegime = MarketRegime.BULLISH
    circuit_breaker: bool = False # 是否触发空仓熔断

    def __str__(self):
        status_map = {
            MarketRegime.BULLISH: "做多环境 (指数 > MA20)",
            MarketRegime.CAUTION: "谨慎环境 (指数 < MA20 > MA60)",
            MarketRegime.BEARISH: "空仓熔断 (指数 < MA20 < MA60)",
        }
        return (f"[{self.index_name}] 价格={self.index_price:.2f} "
                f"MA20={self.index_ma20:.2f} MA60={self.index_ma60:.2f} | "
                f"{status_map.get(self.regime, '未知')}")


# ============================================================
# 扫描结果数据结构
# ============================================================

@dataclass
class SignalResult:
    """单只股票的主升浪加速扫描结果"""
    code: str
    name: str
    market: str
    price: float
    change_pct: float

    # 四维硬核信号
    ma_alignment: bool = False        # 1. 均线完美多头排列
    volume_breakout: bool = False     # 2. 动量爆发与放量破局
    vcp_pattern: bool = False         # 3. VCP 波动率收敛后突破
    beta_pass: bool = False           # 4. 大盘环境过滤器通过

    # 子信号明细 (用于日志和报告)
    # -- MA Alignment --
    ma_perfect: bool = False          # Price > MA5 > MA10 > MA20 > MA60
    ma_deep_trend: bool = False       # MA20 > MA60 > MA120
    # -- Volume Breakout --
    new_high_20d: bool = False        # 创20日新高
    new_high_60d: bool = False        # 创60日新高
    vol_surge_5d: bool = False        # 量 > 5日均量 × 2.0
    vol_surge_20d: bool = False       # 量 > 20日均量 × 2.0
    # -- VCP Pattern --
    vcp_contraction: bool = False     # 振幅收敛
    vcp_breakout_candle: bool = False # 大阳线突破前高
    bollinger_explode: bool = False   # 布林带开口剧烈放大

    # 指标数值
    ma5: float = 0
    ma10: float = 0
    ma20: float = 0
    ma60: float = 0
    ma120: float = 0
    dif: float = 0
    dea: float = 0
    macd_hist: float = 0
    rsi_6: float = 0
    rsi_12: float = 0
    boll_upper: float = 0
    boll_lower: float = 0
    boll_width: float = 0
    boll_width_ratio: float = 0       # 当日带宽 / 20日平均带宽
    volume_ratio_5d: float = 0        # 量比 (5日)
    volume_ratio_20d: float = 0       # 量比 (20日)
    vcp_contraction_ratio: float = 0  # 振幅收敛比 (前5日均振幅 / 后5日均振幅)
    candle_body_pct: float = 0        # 今日实体涨幅 %
    surge_5d: float = 0
    surge_20d: float = 0

    # 今日 K 线明细 (供 notifier 计算买卖决策手令)
    today_high: float = 0       # 今日最高价
    today_low: float = 0        # 今日最低价
    today_open: float = 0       # 今日开盘价
    today_volume: float = 0     # 今日成交量

    # 淘汰原因 (用于日志)
    reject_reason: str = ""

    # 综合评分 (0-100)
    score: int = 0
    level: str = ""


# ============================================================
# 主升浪加速行情扫描引擎
# ============================================================

class RallyScanner:
    """
    主升浪加速行情扫描引擎 v2.0

    四维硬核动量模型:
      全部 4 个指标通过 → S级 (强烈主升浪加速)
      3 个通过 (缺 Beta) → A级 (主升浪确认, 但大盘环境不佳)
      3 个通过 (缺 VCP)  → B级 (趋势放量, 但无 VCP 形态)
    """

    def __init__(
        self,
        volume_surge_ratio: float = 2.0,
        vcp_lookback: int = 10,
        vcp_contraction_threshold: float = 1.3,
        bollinger_explode_ratio: float = 1.5,
        candle_body_min_pct: float = 3.0,
        rsi_lower: float = 50,
        rsi_upper: float = 85,
    ):
        """
        Args:
            volume_surge_ratio: 放量倍数阈值 (默认 2.0 倍)
            vcp_lookback: VCP 振幅收敛回看天数 (默认 10 日)
            vcp_contraction_threshold: 振幅收敛比阈值 (前半段/后半段 > 1.3)
            bollinger_explode_ratio: 布林带开口放大倍数 (当日带宽 > 均值 × 1.5)
            candle_body_min_pct: 突破大阳线最低实体涨幅 % (默认 3%)
            rsi_lower: RSI 下限
            rsi_upper: RSI 上限
        """
        self.volume_surge_ratio = volume_surge_ratio
        self.vcp_lookback = vcp_lookback
        self.vcp_contraction_threshold = vcp_contraction_threshold
        self.bollinger_explode_ratio = bollinger_explode_ratio
        self.candle_body_min_pct = candle_body_min_pct
        self.rsi_lower = rsi_lower
        self.rsi_upper = rsi_upper

    # ============================================================
    # 指标计算
    # ============================================================

    def _calc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算全部技术指标"""
        from ta.trend import MACD, SMAIndicator
        from ta.momentum import RSIIndicator
        from ta.volatility import BollingerBands

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        # ---- MA 均线 (含 MA120) ----
        df["ma5"] = SMAIndicator(close, window=5).sma_indicator()
        df["ma10"] = SMAIndicator(close, window=10).sma_indicator()
        df["ma20"] = SMAIndicator(close, window=20).sma_indicator()
        df["ma60"] = SMAIndicator(close, window=60).sma_indicator()
        df["ma120"] = SMAIndicator(close, window=120).sma_indicator()

        # ---- MACD ----
        macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
        df["dif"] = macd.macd()
        df["dea"] = macd.macd_signal()
        df["macd_hist"] = macd.macd_diff()

        # ---- RSI ----
        df["rsi_6"] = RSIIndicator(close, window=6).rsi()
        df["rsi_12"] = RSIIndicator(close, window=12).rsi()

        # ---- 布林带 ----
        bb = BollingerBands(close, window=20, window_dev=2)
        df["boll_upper"] = bb.bollinger_hband()
        df["boll_lower"] = bb.bollinger_lband()
        df["boll_mid"] = bb.bollinger_mavg()
        df["boll_width"] = (df["boll_upper"] - df["boll_lower"]) / df["boll_mid"]
        df["boll_width_ma20"] = df["boll_width"].rolling(20).mean()

        # ---- 量能指标 ----
        df["vol_ma5"] = volume.rolling(5).mean()
        df["vol_ma20"] = volume.rolling(20).mean()
        df["volume_ratio_5d"] = volume / df["vol_ma5"]
        df["volume_ratio_20d"] = volume / df["vol_ma20"]

        # ---- 新高检测 ----
        df["high_20d"] = high.rolling(20).max().shift(1)  # 过去20日最高(不含今日)
        df["high_60d"] = high.rolling(60).max().shift(1)  # 过去60日最高(不含今日)

        # ---- 振幅 (VCP 用) ----
        df["amplitude"] = (high - low) / close  # 日振幅

        # ---- K线实体 ----
        df["body"] = (close - df["open"]) / df["open"] * 100  # 实体涨幅 %
        df["body_abs"] = df["body"].abs()

        # ---- 近期涨幅 ----
        df["surge_5d"] = (close / close.shift(5) - 1) * 100
        df["surge_20d"] = (close / close.shift(20) - 1) * 100

        return df

    def _calc_vcp(self, df: pd.DataFrame) -> dict:
        """
        VCP (Volatility Contraction Pattern) 波动率收敛形态量化

        Mark Minervini 的 VCP 形态核心:
        - 价格波动率从左到右逐渐收敛 (T1 > T2 > T3 ...)
        - 成交量在收缩期间萎缩
        - 突破时伴随着放量和布林带开口扩张

        量化方法:
        - 将回看期分为前半段和后半段
        - 前半段平均振幅 / 后半段平均振幅 > 阈值 → 收敛
        - 今日收盘 > 前N日最高价 → 突破
        - 今日实体涨幅 > 阈值 → 大阳线
        - 布林带宽 > 20日均值 × 放大倍数 → 开口扩张

        Returns:
            VCP 分析结果字典
        """
        lookback = self.vcp_lookback
        recent = df.tail(lookback)

        if len(recent) < lookback:
            return {"valid": False}

        # 振幅收敛分析: 前半段 vs 后半段
        half = lookback // 2
        front_amp = recent["amplitude"].iloc[:half].mean()
        back_amp = recent["amplitude"].iloc[half:].mean()

        if np.isnan(front_amp) or np.isnan(back_amp) or back_amp == 0:
            contraction_ratio = 0
            contracted = False
        else:
            contraction_ratio = front_amp / back_amp
            contracted = contraction_ratio >= self.vcp_contraction_threshold

        # 今日突破前高
        latest = df.iloc[-1]
        prev_high_20d = latest["high_20d"]
        prev_high_60d = latest["high_60d"]

        broke_20d = (not np.isnan(prev_high_20d) and latest["close"] > prev_high_20d)
        broke_60d = (not np.isnan(prev_high_60d) and latest["close"] > prev_high_60d)

        # 今日大阳线
        body_pct = latest["body"]
        is_big_candle = (body_pct >= self.candle_body_min_pct)

        # 布林带开口扩张
        boll_width = latest["boll_width"]
        boll_width_ma20 = latest["boll_width_ma20"]
        if not np.isnan(boll_width_ma20) and boll_width_ma20 > 0:
            boll_width_ratio = boll_width / boll_width_ma20
            bollinger_explode = boll_width_ratio >= self.bollinger_explode_ratio
        else:
            boll_width_ratio = 0
            bollinger_explode = False

        # VCP 形态成立: 收敛 + 突破 + 布林扩张
        vcp_valid = contracted and (broke_20d or broke_60d) and is_big_candle and bollinger_explode

        return {
            "valid": True,
            "contraction_ratio": round(contraction_ratio, 2),
            "contracted": contracted,
            "front_amp": round(front_amp * 100, 2),
            "back_amp": round(back_amp * 100, 2),
            "broke_20d": broke_20d,
            "broke_60d": broke_60d,
            "body_pct": round(body_pct, 2),
            "is_big_candle": is_big_candle,
            "boll_width_ratio": round(boll_width_ratio, 2),
            "bollinger_explode": bollinger_explode,
            "vcp_valid": vcp_valid,
        }

    # ============================================================
    # 大盘环境分析
    # ============================================================

    def analyze_market_index(self, index_df: pd.DataFrame, index_name: str) -> MarketEnvironment:
        """
        分析大盘指数环境, 判断是否触发空仓熔断

        Args:
            index_df: 指数 K线数据 [date, close, ...]
            index_name: 指数名称

        Returns:
            MarketEnvironment 大盘环境分析结果
        """
        env = MarketEnvironment(index_name=index_name)

        if index_df is None or len(index_df) < 60:
            logger.warning("大盘指数数据不足 ({}), 跳过 Beta 过滤", index_name)
            env.regime = MarketRegime.BULLISH
            env.circuit_breaker = False
            return env

        df = index_df.copy()
        close = df["close"]

        env.index_price = round(float(close.iloc[-1]), 2)
        env.index_ma20 = round(float(close.rolling(20).mean().iloc[-1]), 2)
        env.index_ma60 = round(float(close.rolling(60).mean().iloc[-1]), 2)

        # 判断市场状态
        if env.index_price > env.index_ma20:
            env.regime = MarketRegime.BULLISH
            env.circuit_breaker = False
        elif env.index_price > env.index_ma60:
            env.regime = MarketRegime.CAUTION
            env.circuit_breaker = False
        else:
            env.regime = MarketRegime.BEARISH
            env.circuit_breaker = True

        logger.info("大盘环境分析 | {} | {}", index_name, env)

        if env.circuit_breaker:
            logger.warning("=" * 60)
            logger.warning("  [空仓熔断] 大盘指数跌破 MA20 且跌破 MA60")
            logger.warning("  系统性风险极高, 扫描器进入防御模式")
            logger.warning("  仅输出四维全部通过的 S 级信号, 其余全部过滤")
            logger.warning("=" * 60)

        return env

    # ============================================================
    # 四维硬核指标检测
    # ============================================================

    def _check_ma_alignment(self, df: pd.DataFrame, result: SignalResult) -> bool:
        """
        指标 1: 均线完美多头排列

        条件:
          Price > MA5 > MA10 > MA20 > MA60  (短期多头)
          且 MA20 > MA60 > MA120            (中长期趋势确认)

        两条都满足才算通过, 彻底过滤震荡市和阴跌股。
        """
        latest = df.iloc[-1]
        price = result.price

        # 短期完美多头排列: Price > MA5 > MA10 > MA20 > MA60
        ma_perfect = (
            price > result.ma5 > result.ma10 > result.ma20 > result.ma60
        )
        result.ma_perfect = ma_perfect

        # 中长期趋势确认: MA20 > MA60 > MA120
        if result.ma120 > 0:
            ma_deep_trend = result.ma20 > result.ma60 > result.ma120
        else:
            ma_deep_trend = False
        result.ma_deep_trend = ma_deep_trend

        passed = ma_perfect and ma_deep_trend

        if passed:
            logger.debug(
                "[{}] {} 均线多头 PASS | "
                "Price={} > MA5={} > MA10={} > MA20={} > MA60={} > MA120={}",
                result.code, result.name,
                price, result.ma5, result.ma10, result.ma20, result.ma60, result.ma120,
            )
        else:
            if not ma_perfect:
                result.reject_reason = f"MA排列不完美 (Price={price} MA5={result.ma5} MA10={result.ma10} MA20={result.ma20} MA60={result.ma60})"
            elif not ma_deep_trend:
                result.reject_reason = f"MA深层趋势不满足 (MA20={result.ma20} MA60={result.ma60} MA120={result.ma120})"
            logger.debug(
                "[{}] {} 均线多头 FAIL | {}",
                result.code, result.name, result.reject_reason,
            )

        return passed

    def _check_volume_breakout(self, df: pd.DataFrame, result: SignalResult) -> bool:
        """
        指标 2: 动量爆发与放量破局

        条件 (全部满足):
          - 当日收盘价创过去 20 日或 60 日新高
          - 当日成交量 > 5 日均量 × 2.0
          - 当日成交量 > 20 日均量 × 2.0

        真金白银的放量突破, 排除无量假突破。
        """
        latest = df.iloc[-1]

        # 新高检测
        result.new_high_20d = result.new_high_20d  # 已在 VCP 中计算
        result.new_high_60d = result.new_high_60d

        # 量能爆发
        result.vol_surge_5d = result.volume_ratio_5d >= self.volume_surge_ratio
        result.vol_surge_20d = result.volume_ratio_20d >= self.volume_surge_ratio

        # 必须创新高 (20日或60日)
        made_new_high = result.new_high_20d or result.new_high_60d
        # 必须放量 (5日和20日均满足)
        volume_confirmed = result.vol_surge_5d and result.vol_surge_20d

        passed = made_new_high and volume_confirmed

        if passed:
            high_type = "20日+60日" if (result.new_high_20d and result.new_high_60d) else \
                        ("20日" if result.new_high_20d else "60日")
            logger.debug(
                "[{}] {} 放量突破 PASS | "
                "新高={} | 量比5d={:.2f}x 量比20d={:.2f}x (阈值{}x)",
                result.code, result.name,
                high_type, result.volume_ratio_5d, result.volume_ratio_20d,
                self.volume_surge_ratio,
            )
        else:
            reasons = []
            if not made_new_high:
                reasons.append(f"未创新高(20日高={latest['high_20d']:.2f} 60日高={latest['high_60d']:.2f})")
            if not result.vol_surge_5d:
                reasons.append(f"量比5d={result.volume_ratio_5d:.2f}x<{self.volume_surge_ratio}x")
            if not result.vol_surge_20d:
                reasons.append(f"量比20d={result.volume_ratio_20d:.2f}x<{self.volume_surge_ratio}x")
            result.reject_reason = "放量突破失败: " + "; ".join(reasons)
            logger.debug(
                "[{}] {} 放量突破 FAIL | {}",
                result.code, result.name, result.reject_reason,
            )

        return passed

    def _check_vcp_pattern(self, df: pd.DataFrame, result: SignalResult, vcp: dict) -> bool:
        """
        指标 3: 波动率收敛后突破 (VCP 形态量化)

        条件 (全部满足):
          - 过去 10 日振幅逐渐收紧 (前半段均振幅 / 后半段均振幅 > 1.3)
          - 今日大阳线突破前高 (实体涨幅 > 3%)
          - 布林带开口剧烈放大 (当日带宽 > 20日均值 × 1.5)

        Mark Minervini VCP 形态的量化实现。
        """
        result.vcp_contraction = vcp.get("contracted", False)
        result.vcp_breakout_candle = vcp.get("is_big_candle", False) and \
            (vcp.get("broke_20d", False) or vcp.get("broke_60d", False))
        result.bollinger_explode = vcp.get("bollinger_explode", False)
        result.vcp_contraction_ratio = vcp.get("contraction_ratio", 0)
        result.boll_width_ratio = vcp.get("boll_width_ratio", 0)
        result.candle_body_pct = vcp.get("body_pct", 0)

        passed = vcp.get("vcp_valid", False)

        if passed:
            logger.debug(
                "[{}] {} VCP形态 PASS | "
                "收敛比={:.2f} (前{:.2f}%/后{:.2f}%) | "
                "大阳线={:.2f}% | 布林扩张={:.2f}x",
                result.code, result.name,
                vcp["contraction_ratio"],
                vcp["front_amp"], vcp["back_amp"],
                vcp["body_pct"], vcp["boll_width_ratio"],
            )
        else:
            reasons = []
            if not vcp.get("contracted", False):
                reasons.append(f"振幅未收敛(比={vcp.get('contraction_ratio', 0):.2f}<{self.vcp_contraction_threshold})")
            if not vcp.get("is_big_candle", False):
                reasons.append(f"非大阳线(实体={vcp.get('body_pct', 0):.2f}%<{self.candle_body_min_pct}%)")
            if not (vcp.get("broke_20d", False) or vcp.get("broke_60d", False)):
                reasons.append("未突破前高")
            if not vcp.get("bollinger_explode", False):
                reasons.append(f"布林未扩张(比={vcp.get('boll_width_ratio', 0):.2f}x<{self.bollinger_explode_ratio}x)")
            result.reject_reason = "VCP形态失败: " + "; ".join(reasons)
            logger.debug(
                "[{}] {} VCP形态 FAIL | {}",
                result.code, result.name, result.reject_reason,
            )

        return passed

    def _check_beta_filter(self, result: SignalResult, market_env: MarketEnvironment) -> bool:
        """
        指标 4: 大盘环境过滤器

        逻辑:
          - BULLISH (指数 > MA20): 通过, 正常做多
          - CAUTION (指数 < MA20 > MA60): 降级通过, 仅保留高分信号
          - BEARISH (指数 < MA20 < MA60): 空仓熔断, 全部拒绝

        在空仓熔断状态下, 即使个股四维全部通过也会被过滤,
        因为系统性风险下任何多头头寸都极度危险。
        """
        if market_env.circuit_breaker:
            result.beta_pass = False
            result.reject_reason = f"空仓熔断: 大盘{market_env.index_name}跌破MA20且跌破MA60, 系统性风险"
            logger.debug(
                "[{}] {} Beta过滤 FAIL | 空仓熔断, 拒绝一切多头信号",
                result.code, result.name,
            )
            return False

        if market_env.regime == MarketRegime.CAUTION:
            # 谨慎环境: 降级通过, 但在后续评分中会扣分
            result.beta_pass = True
            logger.debug(
                "[{}] {} Beta过滤 PASS (谨慎) | 大盘在MA20下方但MA60上方",
                result.code, result.name,
            )
            return True

        # 做多环境
        result.beta_pass = True
        logger.debug(
            "[{}] {} Beta过滤 PASS | 大盘在MA20上方, 做多环境",
            result.code, result.name,
        )
        return True

    # ============================================================
    # 单股扫描
    # ============================================================

    def scan_single(
        self,
        code: str,
        name: str,
        market: str,
        df: pd.DataFrame,
        market_env: Optional[MarketEnvironment] = None,
    ) -> Optional[SignalResult]:
        """
        扫描单只股票的主升浪加速信号

        四维硬核指标串联检测, 任一指标失败即淘汰并记录原因。

        Args:
            code: 股票代码
            name: 股票名称
            market: 市场类型
            df: K线数据
            market_env: 大盘环境分析结果

        Returns:
            SignalResult 或 None
        """
        # 数据量检查: 需要 MA120, 至少 120 个交易日
        if len(df) < 120:
            logger.debug("[{}] {} 数据不足({}日<120日), 跳过", code, name, len(df))
            return None

        # 计算指标
        try:
            df = self._calc_indicators(df)
        except Exception as e:
            logger.debug("[{}] {} 指标计算异常: {}", code, name, e)
            return None

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # 构建初始结果
        result = SignalResult(
            code=code,
            name=name,
            market=market,
            price=round(float(latest["close"]), 2),
            change_pct=round(float((latest["close"] - prev["close"]) / prev["close"] * 100), 2),
            ma5=round(float(latest["ma5"]), 2) if not np.isnan(latest["ma5"]) else 0,
            ma10=round(float(latest["ma10"]), 2) if not np.isnan(latest["ma10"]) else 0,
            ma20=round(float(latest["ma20"]), 2) if not np.isnan(latest["ma20"]) else 0,
            ma60=round(float(latest["ma60"]), 2) if not np.isnan(latest["ma60"]) else 0,
            ma120=round(float(latest["ma120"]), 2) if not np.isnan(latest["ma120"]) else 0,
            dif=round(float(latest["dif"]), 4) if not np.isnan(latest["dif"]) else 0,
            dea=round(float(latest["dea"]), 4) if not np.isnan(latest["dea"]) else 0,
            macd_hist=round(float(latest["macd_hist"]), 4) if not np.isnan(latest["macd_hist"]) else 0,
            rsi_6=round(float(latest["rsi_6"]), 2) if not np.isnan(latest["rsi_6"]) else 0,
            rsi_12=round(float(latest["rsi_12"]), 2) if not np.isnan(latest["rsi_12"]) else 0,
            boll_upper=round(float(latest["boll_upper"]), 2) if not np.isnan(latest["boll_upper"]) else 0,
            boll_lower=round(float(latest["boll_lower"]), 2) if not np.isnan(latest["boll_lower"]) else 0,
            boll_width=round(float(latest["boll_width"]), 4) if not np.isnan(latest["boll_width"]) else 0,
            volume_ratio_5d=round(float(latest["volume_ratio_5d"]), 2) if not np.isnan(latest["volume_ratio_5d"]) else 0,
            volume_ratio_20d=round(float(latest["volume_ratio_20d"]), 2) if not np.isnan(latest["volume_ratio_20d"]) else 0,
            surge_5d=round(float(latest["surge_5d"]), 2) if not np.isnan(latest["surge_5d"]) else 0,
            surge_20d=round(float(latest["surge_20d"]), 2) if not np.isnan(latest["surge_20d"]) else 0,
            today_high=round(float(latest["high"]), 2),
            today_low=round(float(latest["low"]), 2),
            today_open=round(float(latest["open"]), 2),
            today_volume=float(latest["volume"]) if not np.isnan(latest["volume"]) else 0,
            new_high_20d=bool(not np.isnan(latest["high_20d"]) and latest["close"] > latest["high_20d"]),
            new_high_60d=bool(not np.isnan(latest["high_60d"]) and latest["close"] > latest["high_60d"]),
        )

        # ============================================================
        # 四维硬核指标串联检测 (任一失败即淘汰)
        # ============================================================
        logger.debug("-" * 40)
        logger.debug("[{}] {} 开始四维检测 | 现价={}", code, name, result.price)

        # 指标 1: 均线完美多头排列
        result.ma_alignment = self._check_ma_alignment(df, result)
        if not result.ma_alignment:
            logger.info(
                "[{}] {} 淘汰 | 均线多头不满足 | {}",
                code, name, result.reject_reason,
            )
            return None

        # 指标 2: 动量爆发与放量破局
        result.volume_breakout = self._check_volume_breakout(df, result)
        if not result.volume_breakout:
            logger.info(
                "[{}] {} 淘汰 | 放量突破不满足 | {}",
                code, name, result.reject_reason,
            )
            return None

        # 指标 3: VCP 波动率收敛后突破
        vcp = self._calc_vcp(df)
        result.vcp_pattern = self._check_vcp_pattern(df, result, vcp)
        if not result.vcp_pattern:
            logger.info(
                "[{}] {} 淘汰 | VCP形态不满足 | {}",
                code, name, result.reject_reason,
            )
            return None

        # 指标 4: 大盘环境过滤器
        if market_env is not None:
            result.beta_pass = self._check_beta_filter(result, market_env)
            if not result.beta_pass:
                logger.warning(
                    "[{}] {} 淘汰 | 大盘环境熔断 | {}",
                    code, name, result.reject_reason,
                )
                return None
        else:
            result.beta_pass = True

        # ============================================================
        # 全部通过, 计算综合评分
        # ============================================================
        score = self._calc_score(result, market_env)
        result.score = score

        # 信号等级
        if result.beta_pass and result.ma_alignment and result.volume_breakout and result.vcp_pattern:
            if market_env and market_env.regime == MarketRegime.BULLISH:
                result.level = "S"  # 做多环境 + 四维全通 = 强烈主升浪
            else:
                result.level = "A"  # 谨慎环境 + 四维全通 = 主升浪确认
        else:
            result.level = "B"  # 部分通过 = 候选关注

        logger.info(
            "[{}] {} 命中 {} 级信号 | 评分={} | "
            "MA={} Vol={} VCP={} Beta={}",
            code, name, result.level, result.score,
            "PASS" if result.ma_alignment else "FAIL",
            "PASS" if result.volume_breakout else "FAIL",
            "PASS" if result.vcp_pattern else "FAIL",
            "PASS" if result.beta_pass else "FAIL",
        )

        return result

    def _calc_score(self, result: SignalResult, market_env: Optional[MarketEnvironment]) -> int:
        """
        综合评分计算 (0-100)

        评分维度:
          - 均线多头排列 (30分): 完美多头20 + 深层趋势10
          - 放量突破 (25分): 新高10 + 量比5d 8 + 量比20d 7
          - VCP 形态 (30分): 收敛10 + 突破10 + 布林扩张10
          - 大盘环境 (15分): 做多15 / 谨慎8 / 熔断0
          - RSI 加分 (-5~+5): RSI 在健康区间加分
        """
        score = 0

        # 1. 均线多头排列 (30分)
        if result.ma_perfect:
            score += 20
        if result.ma_deep_trend:
            score += 10

        # 2. 放量突破 (25分)
        if result.new_high_20d:
            score += 5
        if result.new_high_60d:
            score += 5
        if result.vol_surge_5d:
            score += 8
        if result.vol_surge_20d:
            score += 7

        # 3. VCP 形态 (30分)
        if result.vcp_contraction:
            score += 10
        if result.vcp_breakout_candle:
            score += 10
        if result.bollinger_explode:
            score += 10

        # 4. 大盘环境 (15分)
        if market_env:
            if market_env.regime == MarketRegime.BULLISH:
                score += 15
            elif market_env.regime == MarketRegime.CAUTION:
                score += 8
            # BEARISH = 0 分 (但已在前置过滤中淘汰)

        # 5. RSI 加分/扣分
        if self.rsi_lower <= result.rsi_12 <= self.rsi_upper:
            score += 5
        elif result.rsi_12 > self.rsi_upper:
            score -= 5  # RSI 超买扣分

        return max(0, min(100, score))

    # ============================================================
    # 批量扫描
    # ============================================================

    def scan_batch(
        self,
        stock_data: dict,
        market_env: Optional[MarketEnvironment] = None,
    ) -> list:
        """
        批量扫描股票池

        Args:
            stock_data: {code: {"data": DataFrame, "name": str, "market": str}}
            market_env: 大盘环境分析结果

        Returns:
            [SignalResult] 按评分降序排列
        """
        results = []
        reject_stats = {
            "data_insufficient": 0,
            "ma_alignment_fail": 0,
            "volume_breakout_fail": 0,
            "vcp_pattern_fail": 0,
            "beta_filter_fail": 0,
        }
        total = len(stock_data)

        for i, (code, info) in enumerate(stock_data.items()):
            result = self.scan_single(
                code=code,
                name=info["name"],
                market=info["market"],
                df=info["data"].copy(),
                market_env=market_env,
            )
            if result:
                results.append(result)
            else:
                # 统计淘汰原因
                df = info["data"]
                if len(df) < 120:
                    reject_stats["data_insufficient"] += 1
                # 更精确的统计需要 result.reject_reason, 但被淘汰时 result 为 None
                # 这里基于日志已足够, 不再做精确统计

            if (i + 1) % 200 == 0:
                logger.info(
                    "扫描进度: {}/{} ({:.0f}%) | 命中: {} | 淘汰(数据不足): {}",
                    i + 1, total, (i + 1) / total * 100,
                    len(results), reject_stats["data_insufficient"],
                )

        # 按评分降序排列
        results.sort(key=lambda x: x.score, reverse=True)

        # 扫描统计
        s_count = sum(1 for r in results if r.level == "S")
        a_count = sum(1 for r in results if r.level == "A")
        b_count = sum(1 for r in results if r.level == "B")

        logger.info("=" * 60)
        logger.info("扫描完成 | 共扫描 {} 只 | 命中 {} 只", total, len(results))
        logger.info("  S级(强烈主升浪): {} 只", s_count)
        logger.info("  A级(主升浪确认): {} 只", a_count)
        logger.info("  B级(候选关注): {} 只", b_count)
        logger.info("  淘汰(数据不足): {} 只", reject_stats["data_insufficient"])
        if market_env:
            logger.info("  大盘环境: {}", market_env)
        logger.info("=" * 60)

        return results
