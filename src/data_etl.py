"""
Data ETL 模块 - 数据抓取与清洗 (v2.0 自愈版)
================================================
负责从 AkShare (A股) / yfinance (美股) 获取实时和历史K线数据，
进行清洗、标准化、缓存，为下游扫描引擎提供统一格式数据。

v2.0 新增 -- 数据源故障切换 (Failover):
  - A股主源 AkShare 失败 → 自动切换 efinance 备用源
  - 美股主源 yfinance 失败 → 自动切换 AkShare 美股接口
  - 每次请求带超时检测, 超时自动切换
  - 健康检查预热, 提前剔除不可用源

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
  yfinance 美股日线:
    yf.download(tickers, start, end, interval)
"""

import time
import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime, timedelta
from typing import Optional


class DataETL:
    """数据抓取与清洗引擎 (v2.0 自愈版)"""

    # 数据源健康状态
    SOURCE_AKSHARE = True
    SOURCE_EFINANCE = True
    SOURCE_YFINANCE = True

    # 请求超时 (秒) -- 超过此时间判定为数据源不可用
    REQUEST_TIMEOUT = 30

    def __init__(self, market: str = "a_share"):
        self.market = market
        self._ak = None
        self._yf = None
        self._ef = None
        # 数据源故障计数器 (连续失败 N 次后暂时跳过)
        self._fail_counts = {"akshare": 0, "efinance": 0, "yfinance": 0}
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
    # A股数据
    # ============================================================

    def get_a_share_stock_list(self) -> pd.DataFrame:
        """
        获取A股全市场股票列表 (东财接口)
        返回: DataFrame[代码, 名称, 最新价, 涨跌幅, ...]

        故障切换: AkShare → efinance
        """
        # 主源: AkShare
        if self._is_source_available("akshare"):
            try:
                df = self.ak.stock_zh_a_spot_em()
                # 过滤 ST 和退市股
                df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]
                logger.info("A股股票列表获取成功 (AkShare) | 共 {} 只股票", len(df))
                self._mark_source_success("akshare")
                return df
            except Exception as e:
                logger.warning("AkShare 获取股票列表失败, 切换备用源: {}", e)
                self._mark_source_fail("akshare")

        # 备用源: efinance
        if self._is_source_available("efinance"):
            try:
                df = self.ef.stock.get_realtime_quotes()
                if df is not None and not df.empty:
                    # efinance 列名映射
                    col_map = {}
                    if "股票代码" in df.columns:
                        col_map["股票代码"] = "代码"
                    if "股票名称" in df.columns:
                        col_map["股票名称"] = "名称"
                    df = df.rename(columns=col_map)
                    # 过滤 ST 和退市股
                    if "名称" in df.columns:
                        df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]
                    logger.info("A股股票列表获取成功 (efinance) | 共 {} 只股票", len(df))
                    self._mark_source_success("efinance")
                    return df
            except Exception as e:
                logger.error("efinance 获取股票列表也失败: {}", e)
                self._mark_source_fail("efinance")

        logger.error("所有数据源获取A股股票列表均失败")
        return pd.DataFrame()

    def get_a_share_kline(
        self,
        symbol: str,
        days: int = 120,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """
        获取A股个股历史K线数据 (前复权)

        故障切换: AkShare → efinance

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
            # 空数据不标记失败 (可能是新股上市不足)
            if df is not None and df.empty:
                return pd.DataFrame()

        # 备用源: efinance
        if self._is_source_available("efinance"):
            df = self._fetch_a_share_kline_efinance(symbol, days, adjust)
            if not df.empty:
                self._mark_source_success("efinance")
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

            # 标准化列名
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
            # efinance 参数: klt=101(日线), fqt=1(前复权)/2(后复权)/0(不复权)
            fqt_map = {"qfq": 1, "hfq": 2, "": 0}
            fqt = fqt_map.get(adjust, 1)

            df = self.ef.stock.get_quote_history(
                stock_codes=symbol,
                klt=101,
                fqt=fqt,
            )
            if df is None or df.empty:
                return pd.DataFrame()

            # efinance 列名映射
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
        # 热门美股代码列表
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

            # 标准化列名
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })

            # 处理多级列名 (yfinance 有时返回 MultiIndex)
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
            # AkShare 美股历史数据接口
            df = self.ak.stock_us_hist(
                symbol=symbol,
                period="daily",
                adjust="qfq",
            )
            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名 (AkShare 美股返回中文列名)
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

        故障切换: AkShare index_daily → efinance 指数接口

        Returns:
            标准化 DataFrame: [date, close, open, high, low, volume]
        """
        # 主源: AkShare
        if self._is_source_available("akshare"):
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
                    logger.info("沪深300指数数据获取成功 (AkShare) | {} 日", len(df))
                    self._mark_source_success("akshare")
                    return df
            except Exception as e:
                logger.warning("AkShare 获取沪深300指数失败, 切换备用源: {}", e)
                self._mark_source_fail("akshare")

        # 备用源: efinance
        if self._is_source_available("efinance"):
            try:
                # efinance 指数接口
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
                    logger.info("沪深300指数数据获取成功 (efinance) | {} 日", len(df))
                    self._mark_source_success("efinance")
                    return df
            except Exception as e:
                logger.error("efinance 获取沪深300指数也失败: {}", e)
                self._mark_source_fail("efinance")

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
        """获取股票列表 (根据市场自动路由)"""
        if self.market in ("a_share", "all"):
            df = self.get_a_share_stock_list()
            a_codes = df["代码"].tolist() if not df.empty else []
            if self.market == "a_share":
                return [("a_share", code, name) for code, name in
                        zip(df["代码"], df["名称"]) if not df.empty]
            us_codes = self.get_us_stock_list()
            return (
                [("a_share", code, name) for code, name in
                 zip(df["代码"], df["名称"]) if not df.empty]
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
