"""
Notification Bot v3.0 - 消息通知与决策模块
================================================
将 scanner.py 筛选出的主升浪牛股进行数据聚合, 通过 Webhook 自动
推送到用户终端, 并附带前瞻估值锚定和明日决策手令。

功能:
  1. 决策卡片生成 -- 每只股票一张结构化卡片
     - 当前状态 (现价 / 涨跌幅 / 换手率)
     - 触发核心信号 (放量突破 / 均线多头 / VCP 形态 / 布林扩张)
     - 前瞻估值锚定 (Forward PE / PEG / 估值合理突破 vs 纯情绪投机破局)
     - 明日决策手令 (追击买点 / 回踩买点 / 硬性止损线)
  2. 多渠道 Webhook 推送 (带指数退避重试 + 死信队列)
     - Telegram Bot
     - 钉钉机器人
     - 企业微信群机器人
     - Discord Webhook
     - Server酱 / PushPlus (微信)
  3. 防漏报机制
     - 指数退避重试 (3 次, 1s/2s/4s)
     - 失败消息写入死信队列 (logs/dead_letter/)
     - 多渠道并行投递, 任一渠道成功即视为送达

推送渠道文档:
  - Telegram Bot:    https://core.telegram.org/bots/api#sendmessage
  - 钉钉机器人:       https://open.dingtalk.com/document/robots/custom-robot-access
  - 企业微信群机器人:  https://developer.work.weixin.qq.com/document/path/91770
  - Discord Webhook: https://discord.com/developers/docs/resources/webhook
  - Server酱:        https://sct.ftqq.com/
  - PushPlus:        https://www.pushplus.plus/
"""

import os
import time
import json
import hmac
import base64
import hashlib
import urllib.parse
import requests
import numpy as np
from loguru import logger
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# 决策卡片数据结构
# ============================================================

@dataclass
class DecisionCard:
    """
    单只股票的完整决策卡片

    由 SignalResult 富化而来, 新增:
      - 估值数据 (Forward PE / PEG / 估值类型)
      - 决策手令 (追击买点 / 回踩买点 / 止损线)
      - 触发信号摘要
    """
    # ---- 基本信息 ----
    code: str = ""
    name: str = ""
    market: str = ""
    price: float = 0
    change_pct: float = 0
    level: str = ""
    score: int = 0

    # ---- 今日 K 线 ----
    today_high: float = 0
    today_low: float = 0
    today_open: float = 0
    ma5: float = 0
    ma10: float = 0
    ma20: float = 0
    ma60: float = 0

    # ---- 信号指标 ----
    volume_ratio_5d: float = 0
    volume_ratio_20d: float = 0
    new_high_20d: bool = False
    new_high_60d: bool = False
    vcp_contraction_ratio: float = 0
    boll_width_ratio: float = 0
    candle_body_pct: float = 0
    surge_5d: float = 0

    # ---- 估值数据 (enrichment 获取) ----
    turnover_rate: float = 0          # 换手率 %
    forward_pe: float = 0             # 动态/前瞻市盈率
    peg_ratio: float = 0              # PEG = PE / 盈利增速
    valuation_type: str = ""          # 估值分类标签
    valuation_emoji: str = ""         # 估值分类 emoji

    # ---- 明日决策手令 (计算得出) ----
    breakout_buy: float = 0           # 最佳追击买点 (突破今日最高价)
    pullback_buy_ma5: float = 0       # 左侧回踩买点 (MA5 支撑)
    pullback_buy_ma10: float = 0      # 左侧回踩买点 (MA10 支撑)
    stop_loss: float = 0              # 硬性防守止损线
    stop_loss_type: str = ""          # 止损线类型 ("MA20" / "今日大阳线底部")

    # ---- 触发信号摘要 ----
    trigger_summary: str = ""

    # ---- 潜在收益风险比 ----
    risk_reward_ratio: float = 0      # (追击买点目标涨幅 - 止损跌幅) / 止损跌幅


# ============================================================
# 通知推送引擎
# ============================================================

class Notifier:
    """
    多渠道通知推送引擎 v3.0

    工作流:
      SignalResult 列表
        -> enrich_signals()   富化为 DecisionCard (补 PE/PEG/换手率/买卖点)
        -> format_messages()  生成各渠道格式的消息
        -> send_all()         多渠道并行投递 (带重试)
        -> 死信队列兜底       发送失败的消息持久化保存
    """

    # 每条消息推送的最大卡片数 (防止超长消息被截断)
    MAX_CARDS_PER_MESSAGE = 5
    # 消息发送超时 (秒)
    REQUEST_TIMEOUT = 15

    def __init__(self):
        # ---- 通知渠道配置 ----
        self.sc_key = os.getenv("SC_KEY", "")
        self.pushplus_token = os.getenv("PUSHPLUS_TOKEN", "")
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.dingtalk_webhook = os.getenv("DINGTALK_WEBHOOK", "")
        self.dingtalk_secret = os.getenv("DINGTALK_SECRET", "")
        self.wecom_webhook = os.getenv("WECOM_WEBHOOK", "")
        self.discord_webhook = os.getenv("DISCORD_WEBHOOK", "")

        # ---- 重试配置 ----
        self.max_retries = int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))
        self.retry_base_delay = float(os.getenv("WEBHOOK_RETRY_DELAY", "1.0"))

        # ---- A 股 spot 数据缓存 (批量获取 PE/换手率) ----
        self._a_share_spot_cache: Optional[Any] = None

        # ---- 死信队列目录 ----
        self._dlq_dir = os.path.join(
            os.path.dirname(__file__), "..", "logs", "dead_letter"
        )

        logger.info(
            "Notifier v3.0 初始化 | "
            "Telegram={} 钉钉={} 企微={} Discord={} Server酱={} PushPlus={}",
            "ON" if self.tg_token else "OFF",
            "ON" if self.dingtalk_webhook else "OFF",
            "ON" if self.wecom_webhook else "OFF",
            "ON" if self.discord_webhook else "OFF",
            "ON" if self.sc_key else "OFF",
            "ON" if self.pushplus_token else "OFF",
        )

    # ============================================================
    # Part 1: 信号富化 -- SignalResult -> DecisionCard
    # ============================================================

    def enrich_signals(
        self,
        results: list,
        market: str = "a_share",
    ) -> List[DecisionCard]:
        """
        将扫描结果富化为决策卡片

        为每只命中股票:
          1. 获取估值数据 (Forward PE / PEG / 换手率)
          2. 计算明日决策手令 (追击买点 / 回踩买点 / 止损线)
          3. 生成触发信号摘要
          4. 计算收益风险比

        Args:
            results: [SignalResult] 扫描结果列表
            market: 市场类型 (a_share / us_stock / all)

        Returns:
            [DecisionCard] 决策卡片列表
        """
        if not results:
            return []

        # 预加载 A 股 spot 数据 (一次性批量获取, 后续查表)
        if market in ("a_share", "all") and self._a_share_spot_cache is None:
            self._preload_a_share_spot()

        cards: List[DecisionCard] = []

        for signal in results:
            try:
                card = self._enrich_single(signal, market)
                cards.append(card)
            except Exception as e:
                logger.error("[{}] {} 决策卡片富化失败: {}", signal.code, signal.name, e)
                # 即使富化失败, 也要生成基础卡片 (不含估值数据)
                card = self._build_basic_card(signal)
                cards.append(card)

        # 按评分降序排列
        cards.sort(key=lambda c: c.score, reverse=True)

        logger.info("决策卡片富化完成 | 共 {} 张", len(cards))
        return cards

    def _enrich_single(self, signal, market: str) -> DecisionCard:
        """富化单只股票的信号"""
        # 构建基础卡片
        card = self._build_basic_card(signal)

        # 获取估值数据
        if signal.market == "a_share":
            self._enrich_a_share_fundamentals(card)
        else:
            self._enrich_us_fundamentals(card)

        # 估值分类
        self._classify_valuation(card)

        # 计算明日决策手令
        self._calc_decision_points(card)

        # 生成触发信号摘要
        card.trigger_summary = self._build_trigger_summary(card)

        # 计算收益风险比
        if card.stop_loss > 0 and card.breakout_buy > 0:
            potential_gain = (card.breakout_buy * 1.05 - card.price) / card.price * 100
            potential_loss = (card.price - card.stop_loss) / card.price * 100
            if potential_loss > 0:
                card.risk_reward_ratio = round(potential_gain / potential_loss, 2)

        logger.debug(
            "[{}] {} 决策卡片 | PE={} PEG={} 估值={} | "
            "追击={} 回踩MA5={} 止损={}({}) | 风险比={}",
            card.code, card.name,
            card.forward_pe if card.forward_pe > 0 else "N/A",
            card.peg_ratio if card.peg_ratio > 0 else "N/A",
            card.valuation_type,
            card.breakout_buy, card.pullback_buy_ma5,
            card.stop_loss, card.stop_loss_type,
            card.risk_reward_ratio,
        )

        return card

    def _build_basic_card(self, signal) -> DecisionCard:
        """从 SignalResult 构建基础决策卡片 (不含估值/决策手令)"""
        return DecisionCard(
            code=signal.code,
            name=signal.name,
            market=signal.market,
            price=signal.price,
            change_pct=signal.change_pct,
            level=signal.level,
            score=signal.score,
            today_high=signal.today_high,
            today_low=signal.today_low,
            today_open=signal.today_open,
            ma5=signal.ma5,
            ma10=signal.ma10,
            ma20=signal.ma20,
            ma60=signal.ma60,
            volume_ratio_5d=signal.volume_ratio_5d,
            volume_ratio_20d=signal.volume_ratio_20d,
            new_high_20d=signal.new_high_20d,
            new_high_60d=signal.new_high_60d,
            vcp_contraction_ratio=signal.vcp_contraction_ratio,
            boll_width_ratio=signal.boll_width_ratio,
            candle_body_pct=signal.candle_body_pct,
            surge_5d=signal.surge_5d,
        )

    # ============================================================
    # Part 1a: 估值数据获取
    # ============================================================

    def _preload_a_share_spot(self):
        """预加载 A 股全市场 spot 数据 (批量获取 PE / 换手率)"""
        try:
            import akshare as ak
            logger.info("预加载 A 股 spot 数据 (PE/换手率)...")
            df = ak.stock_zh_a_spot_em()
            self._a_share_spot_cache = df
            logger.info("A 股 spot 数据加载成功 | {} 只股票", len(df))
        except Exception as e:
            logger.warning("A 股 spot 数据加载失败, 估值数据将缺失: {}", e)
            self._a_share_spot_cache = None

    def _enrich_a_share_fundamentals(self, card: DecisionCard):
        """从 A 股 spot 缓存中获取 PE 和换手率"""
        if self._a_share_spot_cache is None:
            return

        try:
            df = self._a_share_spot_cache
            row = df[df["代码"] == card.code]
            if row.empty:
                return

            row = row.iloc[0]

            # 换手率
            if "换手率" in row.index:
                val = row["换手率"]
                if not np.isnan(val):
                    card.turnover_rate = round(float(val), 2)

            # 动态市盈率
            if "市盈率-动态" in row.index:
                val = row["市盈率-动态"]
                if not np.isnan(val):
                    card.forward_pe = round(float(val), 2)

        except Exception as e:
            logger.debug("[{}] A 股估值数据获取失败: {}", card.code, e)

    def _enrich_us_fundamentals(self, card: DecisionCard):
        """通过 yfinance 获取美股 Forward PE 和 PEG"""
        try:
            import yfinance as yf
            ticker = yf.Ticker(card.code)
            info = ticker.info or {}

            card.forward_pe = round(float(info.get("forwardPE", 0)), 2) if info.get("forwardPE") else 0
            card.peg_ratio = round(float(info.get("pegRatio", 0)), 2) if info.get("pegRatio") else 0

            # 换手率 (yfinance 不直接提供, 用成交量/流通股本近似)
            shares = info.get("floatShares", 0)
            if shares and card.today_volume:
                card.turnover_rate = round(card.today_volume / shares * 100, 2)

        except Exception as e:
            logger.debug("[{}] 美股估值数据获取失败: {}", card.code, e)

    # ============================================================
    # Part 1b: 估值分类
    # ============================================================

    def _classify_valuation(self, card: DecisionCard):
        """
        根据 Forward PE 和 PEG 对突破进行估值分类

        分类逻辑:
          - PEG < 1.5 且 PE > 0    -> "估值合理突破" (基本面支撑的突破)
          - PEG > 2.0 或 PE > 80   -> "纯情绪投机破局" (情绪驱动, 高风险)
          - 其余                    -> "估值中性突破"
          - PE <= 0 (亏损)          -> "基本面缺失"
        """
        pe = card.forward_pe
        peg = card.peg_ratio

        if pe <= 0 and peg <= 0:
            card.valuation_type = "基本面缺失(亏损)"
            card.valuation_emoji = "[!]"
            return

        # 优先用 PEG 判断
        if peg > 0:
            if peg < 1.0:
                card.valuation_type = "估值合理突破"
                card.valuation_emoji = "[V]"
            elif peg < 1.5:
                card.valuation_type = "估值合理突破"
                card.valuation_emoji = "[V]"
            elif peg < 2.0:
                card.valuation_type = "估值中性突破"
                card.valuation_emoji = "[~]"
            else:
                card.valuation_type = "纯情绪投机破局"
                card.valuation_emoji = "[!]"
            return

        # PEG 缺失时用 PE 判断
        if pe > 0:
            if pe < 30:
                card.valuation_type = "估值合理突破"
                card.valuation_emoji = "[V]"
            elif pe < 60:
                card.valuation_type = "估值中性突破"
                card.valuation_emoji = "[~]"
            elif pe < 80:
                card.valuation_type = "估值中性突破"
                card.valuation_emoji = "[~]"
            else:
                card.valuation_type = "纯情绪投机破局"
                card.valuation_emoji = "[!]"
            return

        card.valuation_type = "估值数据缺失"
        card.valuation_emoji = "[?]"

    # ============================================================
    # Part 1c: 明日决策手令计算
    # ============================================================

    def _calc_decision_points(self, card: DecisionCard):
        """
        计算明日决策手令:

        1. 最佳追击买点: 突破今日最高价的右侧买点
           breakout_buy = today_high (明日盘中突破此价即追击)

        2. 左侧回踩买点: 回踩 MA5 或 MA10 均线的支撑位
           pullback_buy_ma5  = MA5  (激进回踩位)
           pullback_buy_ma10 = MA10 (稳健回踩位)

        3. 硬性防守止损线: 跌破 MA20 或今日大阳线底部, 取较高者
           - 若今日为大阳线 (实体 > 5%), 止损设在大阳线底部 (今日最低价)
           - 否则止损设在 MA20
           - 若大阳线底部 > MA20, 取大阳线底部 (更紧的止损)
           - 若 MA20 > 大阳线底部, 取 MA20 (保护性止损)
        """
        # 1. 追击买点
        card.breakout_buy = card.today_high

        # 2. 回踩买点
        card.pullback_buy_ma5 = card.ma5
        card.pullback_buy_ma10 = card.ma10

        # 3. 止损线
        is_big_candle = card.candle_body_pct >= 5.0
        stop_candidates = []

        if card.ma20 > 0:
            stop_candidates.append(("MA20", card.ma20))

        if is_big_candle and card.today_low > 0:
            stop_candidates.append(("今日大阳线底部", card.today_low))

        if stop_candidates:
            # 取较高者作为止损 (先被触发的那条)
            card.stop_loss_type, card.stop_loss = max(stop_candidates, key=lambda x: x[1])
        else:
            card.stop_loss = 0
            card.stop_loss_type = "N/A"

    # ============================================================
    # Part 1d: 触发信号摘要
    # ============================================================

    def _build_trigger_summary(self, card: DecisionCard) -> str:
        """生成人类可读的触发核心信号摘要"""
        signals = []

        # 放量突破
        if card.new_high_60d and card.volume_ratio_5d >= 2.0:
            signals.append(
                f"放量 {card.volume_ratio_5d:.1f}x 突破 60 日平台"
            )
        elif card.new_high_20d and card.volume_ratio_5d >= 2.0:
            signals.append(
                f"放量 {card.volume_ratio_5d:.1f}x 突破 20 日平台"
            )
        elif card.volume_ratio_5d >= 2.0:
            signals.append(
                f"量比 {card.volume_ratio_5d:.1f}x 放量"
            )

        # 均线多头
        if card.ma5 > card.ma10 > card.ma20 > card.ma60 > 0:
            signals.append("均线 MA5>MA10>MA20>MA60 完美多头加速")

        # VCP 形态
        if card.vcp_contraction_ratio >= 1.3:
            signals.append(
                f"VCP 收敛比 {card.vcp_contraction_ratio:.1f}x + 大阳线 {card.candle_body_pct:.1f}%"
            )

        # 布林带
        if card.boll_width_ratio >= 1.5:
            signals.append(f"布林带开口扩张 {card.boll_width_ratio:.1f}x")

        # 5 日涨幅
        if card.surge_5d >= 8:
            signals.append(f"5 日涨幅 {card.surge_5d:.1f}%")

        return " / ".join(signals) if signals else "多维动量信号共振"

    # ============================================================
    # Part 2: 消息格式化
    # ============================================================

    def _format_summary_text(self, cards: List[DecisionCard], market_env=None) -> str:
        """
        生成汇总消息 (纯文本, 用于 Telegram)
        包含所有命中股票的概览表
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        s_count = sum(1 for c in cards if c.level == "S")
        a_count = sum(1 for c in cards if c.level == "A")
        b_count = sum(1 for c in cards if c.level == "B")

        lines = [
            f"=== 主升浪加速警报 ===",
            f"时间: {now}",
        ]

        if market_env:
            regime_map = {
                "BULLISH": "做多环境",
                "CAUTION": "谨慎环境",
                "BEARISH": "空仓熔断",
            }
            regime_text = regime_map.get(market_env.regime.value, "未知")
            lines.append(
                f"大盘: {market_env.index_name} {regime_text} "
                f"({market_env.index_price} vs MA20 {market_env.index_ma20})"
            )
            if market_env.circuit_breaker:
                lines.append("[!] 空仓熔断生效, 系统性风险极高")

        lines.append(f"命中: S级 {s_count} | A级 {a_count} | B级 {b_count}")
        lines.append("")

        for i, card in enumerate(cards[:20], 1):
            market_tag = "A股" if card.market == "a_share" else "美股"
            pe_str = f"PE={card.forward_pe}" if card.forward_pe > 0 else "PE=N/A"
            peg_str = f"PEG={card.peg_ratio}" if card.peg_ratio > 0 else ""
            lines.append(
                f"{i}. [{card.level}] {card.code} {card.name}({market_tag}) "
                f"| {card.price} ({card.change_pct:+.2f}%) "
                f"| 评分{card.score} | {pe_str} {peg_str} {card.valuation_type}"
            )

        lines.append("")
        lines.append("> 不构成投资建议, 请结合基本面自行判断")

        return "\n".join(lines)

    def _format_card_markdown(self, card: DecisionCard, index: int = 1) -> str:
        """
        生成单只股票的 Markdown 决策卡片

        用于: Server酱 / PushPlus / 钉钉 / 企业微信
        """
        market_tag = "A股" if card.market == "a_share" else "美股"
        level_emoji = {"S": "🔥", "A": "🚀", "B": "📊"}.get(card.level, "📈")

        # 换手率
        turnover_str = f"{card.turnover_rate:.2f}%" if card.turnover_rate > 0 else "N/A"

        # 估值
        pe_str = f"{card.forward_pe:.1f}" if card.forward_pe > 0 else "N/A"
        peg_str = f"{card.peg_ratio:.2f}" if card.peg_ratio > 0 else "N/A"

        # 风险比
        rr_str = f"{card.risk_reward_ratio:.1f}:1" if card.risk_reward_ratio > 0 else "N/A"

        lines = [
            f"### {level_emoji}【主升浪加速警报】{card.code} {card.name}",
            f"",
            f"**当前状态** ({market_tag} | {card.level}级 | 评分{card.score})",
            f"- 现价: **{card.price}** | 涨跌: **{card.change_pct:+.2f}%** | 换手率: **{turnover_str}**",
            f"",
            f"**触发核心信号**",
            f"- {card.trigger_summary}",
            f"",
            f"**前瞻估值锚定**",
            f"- Forward PE: **{pe_str}** | PEG: **{peg_str}**",
            f"- {card.valuation_emoji} {card.valuation_type}",
            f"",
            f"**明日决策手令**",
            f"- 🎯 最佳追击买点: **{card.breakout_buy:.2f}** (突破今日最高价右侧买点)",
            f"- 📉 左侧回踩买点: **{card.pullback_buy_ma5:.2f}** (MA5) / **{card.pullback_buy_ma10:.2f}** (MA10)",
            f"- 🛑 硬性防守止损: **{card.stop_loss:.2f}** (跌破{card.stop_loss_type}坚决止损)",
            f"- 📐 收益风险比: **{rr_str}**",
            f"",
            f"---",
        ]

        return "\n".join(lines)

    def _format_card_telegram(self, card: DecisionCard, index: int = 1) -> str:
        """
        生成 Telegram 格式的决策卡片 (Markdown)
        Telegram 消息上限 4096 字符
        """
        market_tag = "A股" if card.market == "a_share" else "美股"
        level_emoji = {"S": "🔥", "A": "🚀", "B": "📊"}.get(card.level, "📈")

        turnover_str = f"{card.turnover_rate:.2f}%" if card.turnover_rate > 0 else "N/A"
        pe_str = f"{card.forward_pe:.1f}" if card.forward_pe > 0 else "N/A"
        peg_str = f"{card.peg_ratio:.2f}" if card.peg_ratio > 0 else "N/A"
        rr_str = f"{card.risk_reward_ratio:.1f}:1" if card.risk_reward_ratio > 0 else "N/A"

        # Telegram Markdown (使用粗体和代码块)
        lines = [
            f"{level_emoji} *【主升浪加速警报】* `{card.code}` {card.name}",
            f"",
            f"*当前状态* ({market_tag} | {card.level}级 | 评分{card.score})",
            f"  现价: *{card.price}* | 涨跌: *{card.change_pct:+.2f}%* | 换手率: *{turnover_str}*",
            f"",
            f"*触发核心信号*",
            f"  {card.trigger_summary}",
            f"",
            f"*前瞻估值锚定*",
            f"  Forward PE: *{pe_str}* | PEG: *{peg_str}*",
            f"  {card.valuation_emoji} {card.valuation_type}",
            f"",
            f"*明日决策手令*",
            f"  🎯 追击买点: *{card.breakout_buy:.2f}* (突破今日最高价)",
            f"  📉 回踩买点: *{card.pullback_buy_ma5:.2f}* (MA5) / *{card.pullback_buy_ma10:.2f}* (MA10)",
            f"  🛑 止损线: *{card.stop_loss:.2f}* ({card.stop_loss_type})",
            f"  📐 风险比: *{rr_str}*",
            f"",
            f"`------------------------------`",
        ]

        return "\n".join(lines)

    def _format_card_discord(self, card: DecisionCard) -> dict:
        """
        生成 Discord Embed 格式的决策卡片

        Returns:
            Discord embed 对象 (dict)
        """
        market_tag = "A股" if card.market == "a_share" else "美股"

        # Embed 颜色 (按等级)
        color_map = {"S": 0xFF4500, "A": 0xFF8C00, "B": 0x4169E1}
        color = color_map.get(card.level, 0x808080)

        # 估值 emoji
        val_emoji_map = {
            "估值合理突破": "🟢",
            "估值中性突破": "🟡",
            "纯情绪投机破局": "🔴",
            "基本面缺失(亏损)": "⚫",
            "估值数据缺失": "⚪",
        }
        val_emoji = val_emoji_map.get(card.valuation_type, "⚪")

        level_emoji = {"S": "🔥", "A": "🚀", "B": "📊"}.get(card.level, "📈")

        turnover_str = f"{card.turnover_rate:.2f}%" if card.turnover_rate > 0 else "N/A"
        pe_str = f"{card.forward_pe:.1f}" if card.forward_pe > 0 else "N/A"
        peg_str = f"{card.peg_ratio:.2f}" if card.peg_ratio > 0 else "N/A"
        rr_str = f"{card.risk_reward_ratio:.1f}:1" if card.risk_reward_ratio > 0 else "N/A"

        embed = {
            "title": f"{level_emoji} 主升浪加速警报 | {card.code} {card.name}",
            "color": color,
            "fields": [
                {
                    "name": "📊 当前状态",
                    "value": (
                        f"**{market_tag}** | {card.level}级 | 评分 {card.score}\n"
                        f"现价: **{card.price}** | 涨跌: **{card.change_pct:+.2f}%** | "
                        f"换手率: **{turnover_str}**"
                    ),
                    "inline": False,
                },
                {
                    "name": "🎯 触发核心信号",
                    "value": card.trigger_summary,
                    "inline": False,
                },
                {
                    "name": f"{val_emoji} 前瞻估值锚定",
                    "value": (
                        f"Forward PE: **{pe_str}** | PEG: **{peg_str}**\n"
                        f"{card.valuation_type}"
                    ),
                    "inline": False,
                },
                {
                    "name": "📋 明日决策手令",
                    "value": (
                        f"🎯 追击买点: **{card.breakout_buy:.2f}** (突破今日最高价)\n"
                        f"📉 回踩买点: **{card.pullback_buy_ma5:.2f}** (MA5) / "
                        f"**{card.pullback_buy_ma10:.2f}** (MA10)\n"
                        f"🛑 止损线: **{card.stop_loss:.2f}** ({card.stop_loss_type})\n"
                        f"📐 风险比: **{rr_str}**"
                    ),
                    "inline": False,
                },
            ],
            "footer": {"text": "量化信号扫描结果, 不构成投资建议"},
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        return embed

    # ============================================================
    # Part 3: Webhook 发送 (带指数退避重试)
    # ============================================================

    def _send_with_retry(
        self,
        method: str,
        url: str,
        payload: Optional[dict] = None,
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        channel_name: str = "unknown",
    ) -> bool:
        """
        带指数退避重试的 HTTP 发送

        重试策略:
          - 最多重试 max_retries 次 (默认 3 次)
          - 退避延迟: 1s, 2s, 4s (指数增长)
          - 重试条件: 网络错误 / 超时 / 5xx 服务器错误
          - 不重试: 4xx 客户端错误 (重试无意义)
          - 失败后写入死信队列

        Returns:
            是否最终发送成功
        """
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                if method.upper() == "POST":
                    if payload is not None:
                        resp = requests.post(
                            url, json=payload, headers=headers,
                            timeout=self.REQUEST_TIMEOUT,
                        )
                    elif data is not None:
                        resp = requests.post(
                            url, data=data, headers=headers,
                            timeout=self.REQUEST_TIMEOUT,
                        )
                    else:
                        resp = requests.post(
                            url, headers=headers,
                            timeout=self.REQUEST_TIMEOUT,
                        )
                else:
                    resp = requests.get(url, timeout=self.REQUEST_TIMEOUT)

                # 成功条件: 2xx 状态码
                if 200 <= resp.status_code < 300:
                    # 部分渠道返回 200 但 body 中有错误码, 需要检查
                    try:
                        body = resp.json()
                        # Telegram: ok=true
                        # 钉钉: errcode=0
                        # 企微: errcode=0
                        # PushPlus: code=200
                        # Server酱: code=0
                        if isinstance(body, dict):
                            errcode = body.get("errcode", body.get("code", 0))
                            if errcode not in (0, 200, None):
                                if body.get("ok") is True:
                                    pass  # Telegram ok=true
                                else:
                                    raise requests.RequestException(
                                        f"{channel_name} API 返回错误: {body}"
                                    )
                    except ValueError:
                        pass  # 非 JSON 响应, 忽略

                    if attempt > 1:
                        logger.info(
                            "{} 第 {} 次重试发送成功",
                            channel_name, attempt,
                        )
                    else:
                        logger.info("{} 发送成功", channel_name)
                    return True

                # 4xx 不重试
                if 400 <= resp.status_code < 500:
                    logger.error(
                        "{} 客户端错误 {} (不重试): {}",
                        channel_name, resp.status_code, resp.text[:200],
                    )
                    self._save_to_dead_letter(
                        channel_name,
                        {"url": url, "payload": payload or data, "response": resp.text[:500]},
                        f"HTTP {resp.status_code}",
                    )
                    return False

                # 5xx 重试
                last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"

            except (requests.ConnectionError, requests.Timeout) as e:
                last_error = f"网络错误: {e}"
            except requests.RequestException as e:
                last_error = f"请求异常: {e}"

            # 重试前等待 (指数退避)
            if attempt < self.max_retries:
                delay = self.retry_base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "{} 第 {}/{} 次发送失败, {} 后重试 | 错误: {}",
                    channel_name, attempt, self.max_retries,
                    f"{delay:.1f}s", last_error,
                )
                time.sleep(delay)

        # 全部重试失败
        logger.error(
            "{} 发送失败 (重试 {} 次均失败) | 最后错误: {}",
            channel_name, self.max_retries, last_error,
        )
        self._save_to_dead_letter(
            channel_name,
            {"url": url, "payload": payload or data},
            str(last_error),
        )
        return False

    def _save_to_dead_letter(self, channel: str, payload: dict, error: str):
        """
        将发送失败的消息保存到死信队列

        文件路径: logs/dead_letter/dlq_{channel}_{timestamp}.json
        可用于后续手动重发或排查
        """
        try:
            os.makedirs(self._dlq_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"dlq_{channel}_{ts}.json"
            filepath = os.path.join(self._dlq_dir, filename)

            dead_letter = {
                "channel": channel,
                "error": error,
                "timestamp": datetime.now().isoformat(),
                "payload": payload,
            }

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(dead_letter, f, ensure_ascii=False, indent=2)

            logger.error("消息已写入死信队列: {}", filepath)
        except Exception as e:
            logger.error("死信队列写入失败: {}", e)

    # ============================================================
    # Part 3a: 各渠道发送方法
    # ============================================================

    def _send_telegram(self, text: str) -> bool:
        """通过 Telegram Bot 发送消息"""
        if not self.tg_token or not self.tg_chat_id:
            return False

        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        return self._send_with_retry(
            "POST", url, payload=payload, channel_name="Telegram",
        )

    def _send_dingtalk(self, title: str, markdown: str) -> bool:
        """通过钉钉机器人发送 Markdown 消息"""
        if not self.dingtalk_webhook:
            return False

        url = self.dingtalk_webhook

        # 加签认证
        if self.dingtalk_secret:
            timestamp, sign = self._sign_dingtalk(self.dingtalk_secret)
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}timestamp={timestamp}&sign={sign}"

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": markdown,
            },
        }
        return self._send_with_retry(
            "POST", url, payload=payload, channel_name="DingTalk",
        )

    def _send_wecom(self, markdown: str) -> bool:
        """通过企业微信群机器人发送 Markdown 消息"""
        if not self.wecom_webhook:
            return False

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": markdown,
            },
        }
        return self._send_with_retry(
            "POST", self.wecom_webhook, payload=payload, channel_name="WeCom",
        )

    def _send_discord(self, embeds: list) -> bool:
        """通过 Discord Webhook 发送 Embed 消息"""
        if not self.discord_webhook:
            return False

        payload = {"embeds": embeds}
        return self._send_with_retry(
            "POST", self.discord_webhook, payload=payload, channel_name="Discord",
        )

    def _send_serverchan(self, title: str, markdown: str) -> bool:
        """通过 Server酱 推送到微信"""
        if not self.sc_key:
            return False

        url = f"https://sctapi.ftqq.com/{self.sc_key}.send"
        data = {"title": title, "desp": markdown}
        return self._send_with_retry(
            "POST", url, data=data, channel_name="ServerChan",
        )

    def _send_pushplus(self, title: str, markdown: str) -> bool:
        """通过 PushPlus 推送到微信"""
        if not self.pushplus_token:
            return False

        url = "http://www.pushplus.plus/send"
        payload = {
            "token": self.pushplus_token,
            "title": title,
            "content": markdown,
            "template": "markdown",
        }
        return self._send_with_retry(
            "POST", url, payload=payload, channel_name="PushPlus",
        )

    @staticmethod
    def _sign_dingtalk(secret: str) -> tuple:
        """
        钉钉机器人加签认证

        算法: HMAC-SHA256(timestamp + "\n" + secret, secret) -> base64 -> urlencode
        """
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(
            secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    # ============================================================
    # Part 4: 多渠道并行投递
    # ============================================================

    def _deliver_all_channels(
        self,
        cards: List[DecisionCard],
        market_env=None,
    ) -> bool:
        """
        将决策卡片通过所有已配置的渠道并行投递

        投递策略:
          1. 先发送汇总消息 (所有渠道)
          2. 再发送详细卡片 (每渠道分批发送, 每批 MAX_CARDS_PER_MESSAGE 张)
          3. 多渠道并行, 任一渠道成功即视为送达

        Returns:
            是否至少有一个渠道成功送达
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        title = f"主升浪加速警报 | {now} | 命中{len(cards)}只"

        # ---- 汇总消息 ----
        summary_text = self._format_summary_text(cards, market_env)

        # 汇总消息的 Markdown 版本 (含表格)
        summary_md = self._build_summary_markdown(cards, market_env)

        # ---- 详细卡片 (按批次) ----
        card_batches = []
        for i in range(0, len(cards), self.MAX_CARDS_PER_MESSAGE):
            batch = cards[i:i + self.MAX_CARDS_PER_MESSAGE]
            batch_md = "\n".join(
                self._format_card_markdown(c, j + 1)
                for j, c in enumerate(batch)
            )
            batch_tg = "\n".join(
                self._format_card_telegram(c, j + 1)
                for j, c in enumerate(batch)
            )
            card_batches.append({
                "markdown": batch_md,
                "telegram": batch_tg,
                "discord_embeds": [self._format_card_discord(c) for c in batch],
            })

        # ---- 多渠道并行发送 ----
        channels = []

        # Telegram
        if self.tg_token and self.tg_chat_id:
            def _tg_send():
                success = self._send_telegram(summary_text)
                for batch in card_batches:
                    success = self._send_telegram(batch["telegram"]) or success
                    time.sleep(0.5)  # 避免触发 Telegram 限速 (30 msg/s)
                return success
            channels.append(("Telegram", _tg_send))

        # 钉钉
        if self.dingtalk_webhook:
            def _dingtalk_send():
                success = self._send_dingtalk(title, summary_md)
                for i, batch in enumerate(card_batches):
                    batch_title = f"{title} ({i + 1}/{len(card_batches)})"
                    success = self._send_dingtalk(batch_title, batch["markdown"]) or success
                    time.sleep(1)  # 钉钉限速: 20 条/分钟
                return success
            channels.append(("DingTalk", _dingtalk_send))

        # 企业微信
        if self.wecom_webhook:
            def _wecom_send():
                success = self._send_wecom(summary_md)
                for batch in card_batches:
                    success = self._send_wecom(batch["markdown"]) or success
                    time.sleep(1)  # 企微限速: 20 条/分钟
                return success
            channels.append(("WeCom", _wecom_send))

        # Discord
        if self.discord_webhook:
            def _discord_send():
                # Discord embeds 最多 10 个 per request
                all_embeds = [self._format_card_discord(c) for c in cards]
                success = True
                for i in range(0, len(all_embeds), 10):
                    batch = all_embeds[i:i + 10]
                    success = self._send_discord(batch) and success
                    time.sleep(0.5)
                return success
            channels.append(("Discord", _discord_send))

        # Server酱
        if self.sc_key:
            def _sc_send():
                full_md = summary_md + "\n\n" + "\n".join(
                    self._format_card_markdown(c) for c in cards[:5]
                )
                return self._send_serverchan(title, full_md)
            channels.append(("ServerChan", _sc_send))

        # PushPlus
        if self.pushplus_token:
            def _pp_send():
                full_md = summary_md + "\n\n" + "\n".join(
                    self._format_card_markdown(c) for c in cards[:5]
                )
                return self._send_pushplus(title, full_md)
            channels.append(("PushPlus", _pp_send))

        if not channels:
            logger.warning("未配置任何通知渠道")
            return False

        # 并行发送
        logger.info("开始多渠道投递 | 渠道数: {}", len(channels))
        any_success = False

        with ThreadPoolExecutor(max_workers=len(channels)) as executor:
            futures = {
                executor.submit(fn): name
                for name, fn in channels
            }
            for future in as_completed(futures):
                channel_name = futures[future]
                try:
                    success = future.result()
                    if success:
                        logger.info("渠道 {} 投递成功", channel_name)
                        any_success = True
                    else:
                        logger.warning("渠道 {} 投递失败", channel_name)
                except Exception as e:
                    logger.error("渠道 {} 投递异常: {}", channel_name, e)

        if any_success:
            logger.info("多渠道投递完成 | 至少一个渠道成功送达")
        else:
            logger.error("多渠道投递完成 | 所有渠道均失败, 请检查死信队列")

        return any_success

    def _build_summary_markdown(
        self,
        cards: List[DecisionCard],
        market_env=None,
    ) -> str:
        """构建汇总消息的 Markdown 版本 (含表格)"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        s_count = sum(1 for c in cards if c.level == "S")
        a_count = sum(1 for c in cards if c.level == "A")
        b_count = sum(1 for c in cards if c.level == "B")

        lines = [
            f"# 主升浪加速警报",
            f"",
            f"> 时间: {now}",
            f"> 命中: S级 **{s_count}** | A级 **{a_count}** | B级 **{b_count}**",
        ]

        if market_env:
            regime_map = {
                "BULLISH": "做多环境",
                "CAUTION": "谨慎环境",
                "BEARISH": "空仓熔断",
            }
            regime_text = regime_map.get(market_env.regime.value, "未知")
            lines.append(
                f"> 大盘: {market_env.index_name} {regime_text} "
                f"({market_env.index_price} vs MA20 {market_env.index_ma20})"
            )
            if market_env.circuit_breaker:
                lines.append(f"> **[空仓熔断生效]** 系统性风险极高")

        lines.append(f"")
        lines.append(
            f"| # | 代码 | 名称 | 市场 | 现价 | 涨跌% | 评分 | "
            f"PE | PEG | 估值 | 追击买点 | 止损 |"
        )
        lines.append(
            f"|---|------|------|------|------|-------|------|"
            f"----|-----|------|---------|------|"
        )

        for i, card in enumerate(cards[:20], 1):
            market_tag = "A股" if card.market == "a_share" else "美股"
            pe_str = f"{card.forward_pe:.1f}" if card.forward_pe > 0 else "-"
            peg_str = f"{card.peg_ratio:.2f}" if card.peg_ratio > 0 else "-"
            lines.append(
                f"| {i} | {card.code} | {card.name} | {market_tag} | "
                f"{card.price} | {card.change_pct:+.2f}% | **{card.score}** | "
                f"{pe_str} | {peg_str} | {card.valuation_type} | "
                f"{card.breakout_buy:.2f} | {card.stop_loss:.2f} |"
            )

        lines.append(f"")
        lines.append(f"> 不构成投资建议, 请结合基本面自行判断")

        return "\n".join(lines)

    # ============================================================
    # Part 5: 统一通知入口
    # ============================================================

    def notify(
        self,
        results: list,
        dry_run: bool = False,
        market_env=None,
        market: str = "a_share",
    ) -> bool:
        """
        统一通知接口: 富化 -> 格式化 -> 多渠道投递

        Args:
            results: [SignalResult] 扫描结果列表
            dry_run: True=仅控制台输出不推送
            market_env: 大盘环境分析结果
            market: 市场类型 (a_share / us_stock / all)

        Returns:
            是否至少有一个渠道推送成功
        """
        # 空仓熔断特殊处理
        if not results:
            logger.info("扫描结果为空, 无信号需要推送")
            if market_env and market_env.circuit_breaker:
                logger.warning("空仓熔断状态: 建议全部空仓观望")
                # 即使没有命中信号, 也推送一条空仓警告
                if not dry_run:
                    self._send_circuit_breaker_warning(market_env)
            return False

        # Step 1: 信号富化
        logger.info("Stage 3.1: 信号富化 (估值锚定 + 决策手令计算)")
        cards = self.enrich_signals(results, market=market)

        if not cards:
            logger.warning("决策卡片生成失败")
            return False

        # Step 2: 控制台输出 (始终执行)
        logger.info("Stage 3.2: 控制台输出决策卡片")
        self._print_to_console(cards, market_env)

        if dry_run:
            logger.info("DRY RUN 模式: 仅控制台输出, 不推送通知")
            return True

        # Step 3: 多渠道投递
        logger.info("Stage 3.3: 多渠道 Webhook 投递")
        success = self._deliver_all_channels(cards, market_env)

        if not success:
            logger.warning("所有通知渠道均未配置或推送失败, 请检查 .env 配置和死信队列")

        return success

    def _send_circuit_breaker_warning(self, market_env):
        """发送空仓熔断警告消息"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        warning = (
            f"**[空仓熔断警报]**\n\n"
            f"时间: {now}\n"
            f"大盘: {market_env.index_name}\n"
            f"指数: {market_env.index_price}\n"
            f"MA20: {market_env.index_ma20}\n"
            f"MA60: {market_env.index_ma60}\n\n"
            f"大盘跌破 MA20 且跌破 MA60, 系统性风险极高。\n"
            f"建议全部空仓观望, 等待大盘企稳。"
        )

        # 发送到所有已配置渠道
        if self.tg_token and self.tg_chat_id:
            self._send_telegram(warning)
        if self.dingtalk_webhook:
            self._send_dingtalk("空仓熔断警报", warning)
        if self.wecom_webhook:
            self._send_wecom(warning)
        if self.discord_webhook:
            self._send_discord([{
                "title": "🛑 空仓熔断警报",
                "color": 0xFF0000,
                "description": warning,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }])
        if self.sc_key:
            self._send_serverchan("空仓熔断警报", warning)
        if self.pushplus_token:
            self._send_pushplus("空仓熔断警报", warning)

    def _print_to_console(self, cards: List[DecisionCard], market_env=None):
        """控制台打印决策卡片"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        sep = "=" * 70

        print(f"\n{sep}")
        print(f"  主升浪加速警报 | {now} | 命中 {len(cards)} 只")
        print(sep)

        if market_env:
            regime_map = {
                "BULLISH": "做多环境",
                "CAUTION": "谨慎环境",
                "BEARISH": "空仓熔断",
            }
            regime_text = regime_map.get(market_env.regime.value, "未知")
            print(f"  大盘: {market_env.index_name} {regime_text}")
            print(f"  指数={market_env.index_price} MA20={market_env.index_ma20} MA60={market_env.index_ma60}")
            if market_env.circuit_breaker:
                print(f"  [!] 空仓熔断生效")

        print(sep)

        for i, card in enumerate(cards, 1):
            market_tag = "A股" if card.market == "a_share" else "美股"
            level_emoji = {"S": "[S]", "A": "[A]", "B": "[B]"}.get(card.level, "[?]")

            print(f"\n  {level_emoji} {card.code} {card.name} ({market_tag})")
            print(f"  {'-' * 66}")
            print(f"  现价: {card.price} | 涨跌: {card.change_pct:+.2f}% | "
                  f"换手率: {card.turnover_rate:.2f}%" if card.turnover_rate > 0 else
                  f"  现价: {card.price} | 涨跌: {card.change_pct:+.2f}% | 换手率: N/A")
            print(f"  评分: {card.score} | 信号: {card.trigger_summary}")
            pe_str = f"PE={card.forward_pe:.1f}" if card.forward_pe > 0 else "PE=N/A"
            peg_str = f"PEG={card.peg_ratio:.2f}" if card.peg_ratio > 0 else "PEG=N/A"
            print(f"  估值: {pe_str} {peg_str} -> {card.valuation_type}")
            print(f"  -----------------------------------------------")
            print(f"  [追击买点] {card.breakout_buy:.2f} (突破今日最高价)")
            print(f"  [回踩买点] {card.pullback_buy_ma5:.2f} (MA5) / {card.pullback_buy_ma10:.2f} (MA10)")
            print(f"  [止损线]   {card.stop_loss:.2f} ({card.stop_loss_type})")
            if card.risk_reward_ratio > 0:
                print(f"  [风险比]   {card.risk_reward_ratio:.1f}:1")

        print(f"\n{sep}")
        print(f"  > 不构成投资建议, 请结合基本面自行判断")
        print(f"{sep}\n")
