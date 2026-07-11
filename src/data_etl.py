"""
Data ETL 模块 - 数据抓取与清洗 (v3.0 东财HTTP版)
================================================
负责从 AkShare (A股) / yfinance (美股) / 东财HTTP (A股兜底) 获取实时和历史K线数据，
进行清洗、标准化、缓存，为下游扫描引擎提供统一格式数据。

v3.0 重大变更 -- 东财 HTTP 接口作为 A 股第三数据源:
  - AkShare 失败 → 自动切换 efinance 备用源
  - efinance 失败 → 自动切换东财 HTTP 纯接口 (GitHub Actions 海外环境最稳定)
  - 美股仍使用 yfinance (海外环境原生支持)

依赖开源库:
  - AkShare:   https://github.com/akfamily/akshare   (21.2k stars)
  - yfinance:  https://github.com/ranaroussi/yfinance (24.6k stars)
  - efinance:  https://github.com/Micro-sheep/efinance (2.3k stars, 备用)

关键 API:
  AkShare A股日线:
    ak.stock_zh_a_spot_em()          -> 全市场实时行情快照
    ak.stock_zh_a_hist(symbol, period, start_date, end_date, adjust)
  efinance A股日线 (备用):
    ef.stock.get_quote_history(stock_codes, klt=101, fqt=1)
  东财 HTTP A股日线 (兜底, 纯 requests, 无需额外库):
    push2his.eastmoney.com/api/qt/stock/kline/get
  yfinance 美股日线:
    yf.download(tickers, start, end, interval)
"""

import time
import random
import requests
import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime, timedelta
from typing import Optional


# ============================================================
# 全局 User-Agent 注入 (防止海外 IP 被国内数据源封锁)
# ============================================================

# 国内常见浏览器 User-Agent 列表
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Monkey-patch requests.Session.request, 为所有 HTTP 请求注入浏览器 User-Agent
# AkShare / efinance / 东财HTTP 都使用 requests, 此补丁确保所有请求都带上正常浏览器 UA
if not getattr(requests.Session.request, '_patched_with_ua', False):
    _original_session_request = requests.Session.request

    def _patched_session_request(self, method, url, **kwargs):
        headers = kwargs.get('headers') or {}
        if not headers.get('User-Agent'):
            headers['User-Agent'] = random.choice(_USER_AGENTS)
        kwargs['headers'] = headers
        return _original_session_request(self, method, url, **kwargs)

    _patched_session_request._patched_with_ua = True
    requests.Session.request = _patched_session_request
    logger.debug("User-Agent 全局注入补丁已生效")


class DataETL:
    """数据抓取与清洗引擎 (v3.0 东财HTTP版)"""

    # 数据源健康状态
    SOURCE_AKSHARE = True
    SOURCE_EFINANCE = True
    SOURCE_YFINANCE = True
    SOURCE_EASTMONEY = True

    # 请求超时 (秒) -- 超过此时间判定为数据源不可用
    REQUEST_TIMEOUT = 30

    # 东财 HTTP 接口超时
    EASTMONEY_TIMEOUT = 15

    def __init__(self, market: str = "a_share"):
        self.market = market
        self._ak = None
        self._yf = None
        self._ef = None
        # 数据源故障计数器 (连续失败 N 次后暂时跳过)
        self._fail_counts = {"akshare": 0, "efinance": 0, "yfinance": 0, "eastmoney": 0}
        self._FAIL_THRESHOLD = 3

    @property
    def ak(self):
        """懒加载 AkShare"""
        if self._ak is None:
            import akshare as ak
            self._ak = ak
            logger.info("AkShare 加载成功 | 版本: {}", ak.__version__)
        return self._ak

    @property
    def yf(self):
        """懒加载 yfinance"""
        if self._yf is None:
            import yfinance as yf
            self._yf = yf
            logger.info("yfinance 加载成功")
        return self._yf

    @property
    def ef(self):
        """懒加载 efinance (备用数据源)"""
        if self._ef is None:
            import efinance as ef
            self._ef = ef
            logger.info("efinance 加载成功 (备用数据源)")
        return self._ef

    # ============================================================
    # 数据源健康管理
    # ============================================================

    def _is_source_available(self, source: str) -> bool:
        """检查数据源是否可用 (连续失败次数未超阈值)"""
        return self._fail_counts.get(source, 0) < self._FAIL_THRESHOLD

    def _mark_source_fail(self, source: str):
        """标记数据源一次失败"""
        self._fail_counts[source] = self._fail_counts.get(source, 0) + 1
        count = self._fail_counts[source]
        if count >= self._FAIL_THRESHOLD:
            logger.warning(
                "数据源 {} 连续失败 {} 次, 暂时禁用",
                source, count,
            )

    def _mark_source_success(self, source: str):
        """标记数据源成功, 重置失败计数"""
        if self._fail_counts.get(source, 0) > 0:
            self._fail_counts[source] = 0

    # ============================================================
    # 重试配置
    # ============================================================

    MAX_RETRIES = 3
    RETRY_DELAY_MIN = 2.0   # 重试间隔最小秒数
    RETRY_DELAY_MAX = 5.0   # 重试间隔最大秒数

    def _retry_delay(self) -> float:
        """随机延迟, 避免被数据源限流"""
        return random.uniform(self.RETRY_DELAY_MIN, self.RETRY_DELAY_MAX)

    # ============================================================
    # 东财 HTTP 接口 (纯 requests, 无需额外库, GitHub Actions 友好)
    # ============================================================

    @staticmethod
    def _secid(code: str) -> str:
        """根据股票代码判断东财 secid (1=上海, 0=深圳)"""
        # 上海: 600xxx, 601xxx, 603xxx, 605xxx, 688xxx(科创板)
        # 深圳: 000xxx, 002xxx, 003xxx, 300xxx(创业板), 301xxx(创业板)
        if code.startswith("6"):
            return f"1.{code}"
        return f"0.{code}"

    def _get_a_share_stock_list_eastmoney(self) -> pd.DataFrame:
        """
        通过东方财富 HTTP 接口获取 A 股全市场股票列表
        返回: DataFrame[代码, 名称, 最新价, 涨跌幅]
        """
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 5000,          # 每页 5000 条, 足够覆盖全市场
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",  # 沪深A股
            "fields": "f12,f14,f2,f3",  # 代码, 名称, 最新价, 涨跌幅
        }
        try:
            resp = requests.get(
                url, params=params,
                timeout=self.EASTMONEY_TIMEOUT,
                headers={"User-Agent": random.choice(_USER_AGENTS)},
            )
            resp.raise_for_status()
            data = resp.json()

            diff = data.get("data", {}).get("diff")
            if not diff:
                logger.warning("东财 HTTP 返回 diff 为空")
                return pd.DataFrame()

            rows = []
            for item in diff.values():
                code = str(item.get("f12", "")).strip()
                name = item.get("f14", "")
                if not code or not name:
                    continue
                rows.append({
                    "代码": code,
                    "名称": name,
                    "最新价": item.get("f2", 0),
                    "涨跌幅": item.get("f3", 0),
                })

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            # 过滤 ST 和退市股
            df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]
            logger.info(
                "A股股票列表获取成功 (东财HTTP) | 共 {} 只股票",
                len(df),
            )
            self._mark_source_success("eastmoney")
            return df

        except Exception as e:
            logger.warning("东财 HTTP 获取股票列表失败: {}", e)
            self._mark_source_fail("eastmoney")
            return pd.DataFrame()

    def _get_a_share_kline_eastmoney(
        self, symbol: str, days: int, adjust: str
    ) -> pd.DataFrame:
        """通过东方财富 HTTP 接口获取 A 股 K 线数据"""
        secid = self._secid(symbol)
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
        fqt_map = {"qfq": 1, "hfq": 2, "": 0}
        fqt = fqt_map.get(adjust, 1)

        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57",
            "klt": 101,          # 日线
            "fqt": fqt,
            "beg": start_date,
            "end": end_date,
            "lmt": 500,          # 最多返回 500 条
        }
        try:
            resp = requests.get(
                url, params=params,
                timeout=self.EASTMONEY_TIMEOUT,
                headers={"User-Agent": random.choice(_USER_AGENTS)},
            )
            resp.raise_for_status()
            data = resp.json()

            klines = data.get("data", {}).get("klines", [])
            if not klines:
                logger.debug("东财 HTTP {} K线返回空", symbol)
                return pd.DataFrame()

            rows = []
            for kline in klines:
                parts = kline.split(",")
                if len(parts) < 6:
                    continue
                rows.append({
                    "date": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "low": float(parts[4]),
                    "high": float(parts[3]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]) if len(parts) > 6 else 0,
                })

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            logger.debug("东财 HTTP 获取 {} K线成功 | {} 条", symbol, len(df))
            self._mark_source_success("eastmoney")
            return df

        except Exception as e:
            logger.debug("东财 HTTP 获取 {} K线失败: {}", symbol, e)
            self._mark_source_fail("eastmoney")
            return pd.DataFrame()

    def _get_a_share_index_eastmoney(self, days: int) -> pd.DataFrame:
        """通过东方财富 HTTP 接口获取沪深300指数"""
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")

        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "1.000300",   # 沪深300
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57",
            "klt": 101,
            "fqt": 0,
            "beg": start_date,
            "end": end_date,
            "lmt": 500,
        }
        try:
            resp = requests.get(
                url, params=params,
                timeout=self.EASTMONEY_TIMEOUT,
                headers={"User-Agent": random.choice(_USER_AGENTS)},
            )
            resp.raise_for_status()
            data = resp.json()

            klines = data.get("data", {}).get("klines", [])
            if not klines:
                logger.warning("东财 HTTP 沪深300指数返回空")
                return pd.DataFrame()

            rows = []
            for kline in klines:
                parts = kline.split(",")
                if len(parts) < 6:
                    continue
                rows.append({
                    "date": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "low": float(parts[4]),
                    "high": float(parts[3]),
                    "volume": float(parts[5]),
                })

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            logger.info(
                "沪深300指数数据获取成功 (东财HTTP) | {} 日",
                len(df),
            )
            self._mark_source_success("eastmoney")
            return df

        except Exception as e:
            logger.warning("东财 HTTP 获取沪深300指数失败: {}", e)
            self._mark_source_fail("eastmoney")
            return pd.DataFrame()

    # ============================================================
    # A股数据
    # ============================================================

    def get_a_share_stock_list(self) -> pd.DataFrame:
        """
        获取A股全市场股票列表
        返回: DataFrame[代码, 名称, 最新价, 涨跌幅, ...]

        故障切换: AkShare → efinance → 东财HTTP
        每个数据源最多重试 MAX_RETRIES 次, 重试间隔随机延迟 2-5 秒
        所有数据源彻底失败时返回空 DataFrame
        """
        # 主源: AkShare (带重试)
        if self._is_source_available("akshare"):
            for attempt in range(self.MAX_RETRIES):
                try:
                    df = self.ak.stock_zh_a_spot_em()
                    if df is not None and not df.empty:
                        # 过滤 ST 和退市股
                        df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]
                        logger.info(
                            "A股股票列表获取成功 (AkShare) | 共 {} 只股票 (第{}次尝试)",
                            len(df), attempt + 1,
                        )
                        self._mark_source_success("akshare")
                        return df
                    else:
                        logger.warning(
                            "AkShare 返回空数据 (第{}/{}次)",
                            attempt + 1, self.MAX_RETRIES,
                        )
                except Exception as e:
                    logger.warning(
                        "AkShare 获取股票列表失败 (第{}/{}次): {}",
                        attempt + 1, self.MAX_RETRIES, e,
                    )

                if attempt < self.MAX_RETRIES - 1:
                    delay = self._retry_delay()
                    logger.info("等待 {:.1f}s 后重试 AkShare...", delay)
                    time.sleep(delay)

            logger.warning(
                "AkShare 连续 {} 次重试均失败, 切换备用源",
                self.MAX_RETRIES,
            )
            self._mark_source_fail("akshare")

        # 备用源: efinance (带重试)
        if self._is_source_available("efinance"):
            for attempt in range(self.MAX_RETRIES):
                try:
                    df = self.ef.stock.get_realtime_quotes()
                    if df is not None and not df.empty:
                        col_map = {}
                        if "股票代码" in df.columns:
                            col_map["股票代码"] = "代码"
                        if "股票名称" in df.columns:
                            col_map["股票名称"] = "名称"
                        df = df.rename(columns=col_map)
                        if "名称" in df.columns:
                            df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]
                        logger.info(
                            "A股股票列表获取成功 (efinance) | 共 {} 只股票 (第{}次尝试)",
                            len(df), attempt + 1,
                        )
                        self._mark_source_success("efinance")
                        return df
                    else:
                        logger.warning(
                            "efinance 返回空数据 (第{}/{}次)",
                            attempt + 1, self.MAX_RETRIES,
                        )
                except Exception as e:
                    logger.warning(
                        "efinance 获取股票列表失败 (第{}/{}次): {}",
                        attempt + 1, self.MAX_RETRIES, e,
                    )

                if attempt < self.MAX_RETRIES - 1:
                    delay = self._retry_delay()
                    logger.info("等待 {:.1f}s 后重试 efinance...", delay)
                    time.sleep(delay)

            logger.error(
                "efinance 连续 {} 次重试均失败, 切换东财HTTP",
                self.MAX_RETRIES,
            )
            self._mark_source_fail("efinance")

        # 兜底源: 东财 HTTP (纯 requests, GitHub Actions 最稳定)
        if self._is_source_available("eastmoney"):
            logger.info("尝试东财 HTTP 接口获取股票列表...")
            df = self._get_a_share_stock_list_eastmoney()
            if not df.empty:
                return df

        logger.error("所有数据源获取A股股票列表均失败 (已重试 {} 次/源)", self.MAX_RETRIES)
        return pd.DataFrame()

    def get_a_share_kline(
        self,
        symbol: str,
        days: int = 120,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """
        获取A股个股历史K线数据 (前复权)

        故障切换: AkShare → efinance → 东财HTTP

        Args:
            symbol: 股票代码, 如 "000001"
            days: 获取最近N个交易日数据
            adjust: 复权类型 qfq=前复权 hfq=后复权 ""=不复权

        Returns:
            标准化 DataFrame: [date, open, high, low, close, volume, amount]
        """
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")

        # 主源: AkShare
        if self._is_source_available("akshare"):
            df = self._fetch_a_share_kline_akshare(symbol, days, start_date, end_date, adjust)
            if not df.empty:
                self._mark_source_success("akshare")
                return df
            if df is not None and df.empty:
                return pd.DataFrame()

        # 备用源: efinance
        if self._is_source_available("efinance"):
            df = self._fetch_a_share_kline_efinance(symbol, days, adjust)
            if not df.empty:
                self._mark_source_success("efinance")
                return df

        # 兜底源: 东财 HTTP
        if self._is_source_available("eastmoney"):
            df = self._get_a_share_kline_eastmoney(symbol, days, adjust)
            if not df.empty:
                return df

        return pd.DataFrame()

    def _fetch_a_share_kline_akshare(
        self, symbol: str, days: int, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame:
        """通过 AkShare 获取A股K线"""
        try:
            df = self.ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
            if df.empty:
                return pd.DataFrame()

            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
            })
            cols = ["date", "open", "high", "low", "close", "volume", "amount"]
            df = df[[c for c in cols if c in df.columns]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            return df

        except Exception as e:
            logger.debug("AkShare 获取 {} K线失败: {}", symbol, e)
            self._mark_source_fail("akshare")
            return pd.DataFrame()

    def _fetch_a_share_kline_efinance(
        self, symbol: str, days: int, adjust: str
    ) -> pd.DataFrame:
        """通过 efinance 获取A股K线 (备用源)"""
        try:
            fqt_map = {"qfq": 1, "hfq": 2, "": 0}
            fqt = fqt_map.get(adjust, 1)

            df = self.ef.stock.get_quote_history(
                stock_codes=symbol,
                klt=101,
                fqt=fqt,
            )
            if df is None or df.empty:
                return pd.DataFrame()

            col_map = {
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
            }
            df = df.rename(columns=col_map)
            cols = ["date", "open", "high", "low", "close", "volume", "amount"]
            df = df[[c for c in cols if c in df.columns]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)

            logger.debug("efinance 获取 {} K线成功 (备用源)", symbol)
            return df

        except Exception as e:
            logger.debug("efinance 获取 {} K线失败: {}", symbol, e)
            self._mark_source_fail("efinance")
            return pd.DataFrame()

    # ============================================================
    # 美股数据
    # ============================================================

    def get_us_stock_list(self) -> list:
        """
        获取美股热门股票列表
        默认使用内置的大盘股+热门股列表
        """
        us_stocks = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
            "AMD", "NFLX", "JPM", "V", "MA", "DIS", "BABA", "INTC",
            "CRM", "ADBE", "PYPL", "UBER", "SHOP", "SQ", "COIN",
            "PLTR", "SNOW", "DDOG", "NET", "CRWD", "ZM", "DOCU",
        ]
        logger.info("美股股票列表 (内置): 共 {} 只", len(us_stocks))
        return us_stocks

    def get_us_kline(
        self,
        symbol: str,
        days: int = 120,
    ) -> pd.DataFrame:
        """
        获取美股个股历史K线数据

        故障切换: yfinance → AkShare 美股接口

        Args:
            symbol: 股票代码, 如 "AAPL"
            days: 获取最近N个交易日数据

        Returns:
            标准化 DataFrame: [date, open, high, low, close, volume]
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days * 2)

        # 主源: yfinance
        if self._is_source_available("yfinance"):
            df = self._fetch_us_kline_yfinance(symbol, start_date, end_date, days)
            if not df.empty:
                self._mark_source_success("yfinance")
                return df

        # 备用源: AkShare 美股接口
        if self._is_source_available("akshare"):
            df = self._fetch_us_kline_akshare(symbol, days)
            if not df.empty:
                self._mark_source_success("akshare")
                return df

        return pd.DataFrame()

    def _fetch_us_kline_yfinance(
        self, symbol: str, start_date: datetime, end_date: datetime, days: int
    ) -> pd.DataFrame:
        """通过 yfinance 获取美股K线"""
        try:
            df = self.yf.download(
                symbol,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                return pd.DataFrame()

            df = df.reset_index()
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            cols = ["date", "open", "high", "low", "close", "volume"]
            df = df[[c for c in cols if c in df.columns]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            return df

        except Exception as e:
            logger.debug("yfinance 获取 {} K线失败: {}", symbol, e)
            self._mark_source_fail("yfinance")
            return pd.DataFrame()

    def _fetch_us_kline_akshare(self, symbol: str, days: int) -> pd.DataFrame:
        """通过 AkShare 美股接口获取K线 (备用源)"""
        try:
            df = self.ak.stock_us_hist(
                symbol=symbol,
                period="daily",
                adjust="qfq",
            )
            if df is None or df.empty:
                return pd.DataFrame()

            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            })
            cols = ["date", "open", "high", "low", "close", "volume"]
            df = df[[c for c in cols if c in df.columns]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)

            logger.debug("AkShare 获取 {} 美股K线成功 (备用源)", symbol)
            return df

        except Exception as e:
            logger.debug("AkShare 美股接口获取 {} 失败: {}", symbol, e)
            self._mark_source_fail("akshare")
            return pd.DataFrame()

    # ============================================================
    # 大盘指数数据 (Beta Filter 用)
    # ============================================================

    def get_a_share_index(self, days: int = 200) -> pd.DataFrame:
        """
        获取沪深300指数历史K线数据 (用于 Beta Filter)

        故障切换: AkShare → efinance → 东财HTTP
        每个数据源最多重试 MAX_RETRIES 次

        Returns:
            标准化 DataFrame: [date, close, open, high, low, volume]
        """
        # 主源: AkShare (带重试)
        if self._is_source_available("akshare"):
            for attempt in range(self.MAX_RETRIES):
                try:
                    df = self.ak.stock_zh_index_daily(symbol="sh000300")
                    if df is not None and not df.empty:
                        df = df.rename(columns={
                            "date": "date", "close": "close", "open": "open",
                            "high": "high", "low": "low", "volume": "volume",
                        })
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values("date").reset_index(drop=True)
                        df = df.tail(days).reset_index(drop=True)
                        logger.info(
                            "沪深300指数数据获取成功 (AkShare) | {} 日 (第{}次尝试)",
                            len(df), attempt + 1,
                        )
                        self._mark_source_success("akshare")
                        return df
                except Exception as e:
                    logger.warning(
                        "AkShare 获取沪深300指数失败 (第{}/{}次): {}",
                        attempt + 1, self.MAX_RETRIES, e,
                    )

                if attempt < self.MAX_RETRIES - 1:
                    delay = self._retry_delay()
                    logger.info("等待 {:.1f}s 后重试 AkShare 指数...", delay)
                    time.sleep(delay)

            logger.warning("AkShare 指数连续 {} 次失败, 切换 efinance", self.MAX_RETRIES)
            self._mark_source_fail("akshare")

        # 备用源: efinance (带重试)
        if self._is_source_available("efinance"):
            for attempt in range(self.MAX_RETRIES):
                try:
                    df = self.ef.stock.get_quote_history(
                        stock_codes="000300",
                        klt=101,
                        fqt=0,
                    )
                    if df is not None and not df.empty:
                        col_map = {
                            "日期": "date", "开盘": "open", "收盘": "close",
                            "最高": "high", "最低": "low", "成交量": "volume",
                        }
                        df = df.rename(columns=col_map)
                        cols = ["date", "open", "high", "low", "close", "volume"]
                        df = df[[c for c in cols if c in df.columns]].copy()
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values("date").reset_index(drop=True)
                        df = df.tail(days).reset_index(drop=True)
                        logger.info(
                            "沪深300指数数据获取成功 (efinance) | {} 日 (第{}次尝试)",
                            len(df), attempt + 1,
                        )
                        self._mark_source_success("efinance")
                        return df
                except Exception as e:
                    logger.warning(
                        "efinance 获取沪深300指数失败 (第{}/{}次): {}",
                        attempt + 1, self.MAX_RETRIES, e,
                    )

                if attempt < self.MAX_RETRIES - 1:
                    delay = self._retry_delay()
                    logger.info("等待 {:.1f}s 后重试 efinance 指数...", delay)
                    time.sleep(delay)

            logger.error("efinance 指数连续 {} 次失败, 切换东财HTTP", self.MAX_RETRIES)
            self._mark_source_fail("efinance")

        # 兜底源: 东财 HTTP
        if self._is_source_available("eastmoney"):
            logger.info("尝试东财 HTTP 接口获取沪深300指数...")
            df = self._get_a_share_index_eastmoney(days)
            if not df.empty:
                return df

        logger.error("所有数据源获取沪深300指数均失败")
        return pd.DataFrame()

    def get_us_index(self, days: int = 200) -> pd.DataFrame:
        """
        获取标普500指数历史K线数据 (用于 Beta Filter)

        yfinance API:
            yf.download("^GSPC", ...)

        Returns:
            标准化 DataFrame: [date, close, open, high, low, volume]
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days * 2)
            df = self.yf.download(
                "^GSPC",
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                logger.warning("标普500指数数据为空")
                return pd.DataFrame()

            df = df.reset_index()
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            cols = ["date", "open", "high", "low", "close", "volume"]
            df = df[[c for c in cols if c in df.columns]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            logger.info("标普500指数数据获取成功 | {} 日", len(df))
            return df

        except Exception as e:
            logger.error("获取标普500指数数据失败: {}", e)
            return pd.DataFrame()

    def get_market_index(self, days: int = 200) -> pd.DataFrame:
        """获取大盘指数数据 (根据市场自动路由)"""
        if self.market in ("a_share", "all"):
            return self.get_a_share_index(days)
        else:
            return self.get_us_index(days)

    # ============================================================
    # 统一数据接口
    # ============================================================

    def get_stock_list(self) -> list:
        """
        获取股票列表 (根据市场自动路由)

        安全处理: 数据源失败时返回空列表, 不抛出 KeyError
        """
        if self.market in ("a_share", "all"):
            df = self.get_a_share_stock_list()

            # 安全检查: 数据源彻底失败时 df 为空 DataFrame (无列)
            if df is None or df.empty or "代码" not in df.columns:
                logger.error("A股股票列表获取失败 (数据源不可用), 返回空列表")
                if self.market == "a_share":
                    return []
                us_codes = self.get_us_stock_list()
                return [("us_stock", code, code) for code in us_codes]

            if self.market == "a_share":
                return [
                    ("a_share", code, name)
                    for code, name in zip(df["代码"], df["名称"])
                ]

            us_codes = self.get_us_stock_list()
            return (
                [("a_share", code, name) for code, name in zip(df["代码"], df["名称"])]
                + [("us_stock", code, code) for code in us_codes]
            )
        else:
            us_codes = self.get_us_stock_list()
            return [("us_stock", code, code) for code in us_codes]

    def get_kline(self, market_type: str, symbol: str, days: int = 120) -> pd.DataFrame:
        """获取K线数据 (根据市场自动路由)"""
        if market_type == "a_share":
            return self.get_a_share_kline(symbol, days)
        else:
            return self.get_us_kline(symbol, days)

    def batch_fetch(
        self,
        stock_list: list,
        days: int = 120,
        rate_limit: float = 0.3,
    ) -> dict:
        """
        批量获取K线数据

        Args:
            stock_list: [(market_type, code, name), ...]
            days: K线天数
            rate_limit: 每次请求间隔(秒), 避免被封

        Returns:
            {symbol: DataFrame}
        """
        results = {}
        total = len(stock_list)

        for i, (mtype, code, name) in enumerate(stock_list):
            df = self.get_kline(mtype, code, days)
            if not df.empty and len(df) >= 60:
                results[code] = {"data": df, "name": name, "market": mtype}

            if (i + 1) % 100 == 0:
                logger.info("数据抓取进度: {}/{} ({:.0f}%)", i + 1, total, (i + 1) / total * 100)
            time.sleep(rate_limit)

        logger.info("数据抓取完成 | 成功: {}/{}", len(results), total)
        return results
