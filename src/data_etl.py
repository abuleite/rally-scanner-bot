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
import re
import random
import threading
import requests
import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


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


# ============================================================
# 线程本地 Baostock 会话 (多线程并发查询用, 每个 worker 独立 TCP 连接)
# ============================================================
_bs_local = threading.local()


def _get_thread_bs_session():
    """获取线程本地的 Baostock 会话 (每个线程独立 login, 线程安全)"""
    if not getattr(_bs_local, "bs", None):
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == "0" or lg.error_code == 0:
                _bs_local.bs = bs
                return bs
            return None
        except Exception:
            return None
    return _bs_local.bs


class DataETL:
    """数据抓取与清洗引擎 (v3.0 东财HTTP版)"""

    # 数据源健康状态
    SOURCE_AKSHARE = True
    SOURCE_EFINANCE = True
    SOURCE_YFINANCE = True
    SOURCE_EASTMONEY = True
    SOURCE_BAOSTOCK = True

    # 请求超时 (秒) -- 超过此时间判定为数据源不可用
    REQUEST_TIMEOUT = 30

    # 东财 HTTP 接口超时
    EASTMONEY_TIMEOUT = 15

    def __init__(self, market: str = "a_share"):
        self.market = market
        self._ak = None
        self._yf = None
        self._ef = None
        self._bs = None
        self._bs_logged_in = False
        # 数据源故障计数器 (连续失败 N 次后暂时跳过)
        self._fail_counts = {"baostock": 0, "akshare": 0, "efinance": 0, "yfinance": 0, "eastmoney": 0, "tencent": 0, "sina": 0}
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
    # Baostock 数据源 (TCP socket 直连, 海外 IP 不被风控, GitHub Actions 首选)
    # ============================================================

    def _baostock_login(self):
        """懒加载并登录 Baostock (整个生命周期只登录一次)"""
        if self._bs_logged_in:
            return True
        try:
            import baostock as bs
            self._bs = bs
            lg = bs.login()
            if lg.error_code == '0' or lg.error_code == 0:
                self._bs_logged_in = True
                logger.info("Baostock 登录成功 (TCP socket, 海外可用)")
                return True
            else:
                logger.warning("Baostock 登录失败: {}", lg.error_msg)
                self._mark_source_fail("baostock")
                return False
        except ImportError:
            logger.warning("baostock 未安装, 跳过此数据源 (pip install baostock)")
            self._mark_source_fail("baostock")
            return False
        except Exception as e:
            logger.warning("Baostock 登录异常: {}", e)
            self._mark_source_fail("baostock")
            return False

    def _baostock_logout(self):
        """登出 Baostock"""
        if self._bs_logged_in and self._bs:
            try:
                self._bs.logout()
                self._bs_logged_in = False
            except Exception:
                pass

    def _get_bs_session(self):
        """
        获取 Baostock 会话 (线程安全)
        主线程使用 self._bs, worker 线程使用线程本地会话
        """
        if threading.current_thread() is threading.main_thread():
            if self._baostock_login():
                return self._bs
            return None
        else:
            return _get_thread_bs_session()

    @staticmethod
    def _baostock_code(symbol: str) -> str:
        """将纯数字代码转换为 Baostock 格式 (sh.600519 / sz.000001)"""
        symbol = symbol.strip()
        if symbol.startswith(("sh.", "sz.")):
            return symbol
        if symbol.startswith("6"):
            return f"sh.{symbol}"
        return f"sz.{symbol}"

    @staticmethod
    def _is_excluded_code(code: str) -> bool:
        """
        判断股票代码是否应被排除:
        - 688 开头: 科创板
        - 8 开头: 北交所
        - 4 开头: 老三板
        """
        code = str(code).strip()
        if code.startswith("688"):
            return True
        if code.startswith("8"):
            return True
        if code.startswith("4"):
            return True
        return False

    def _get_a_share_stock_list_baostock(self) -> pd.DataFrame:
        """
        通过 Baostock 获取 A 股全市场股票列表
        返回: DataFrame[代码, 名称]
        """
        if not self._baostock_login():
            return pd.DataFrame()
        try:
            rs = self._bs.query_stock_basic()
            if rs.error_code != '0' and rs.error_code != 0:
                logger.warning("Baostock 获取股票列表失败: {}", rs.error_msg)
                self._mark_source_fail("baostock")
                return pd.DataFrame()

            data_list = []
            while (rs.error_code == '0' or rs.error_code == 0) and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return pd.DataFrame()

            df = pd.DataFrame(data_list, columns=rs.fields)
            # type=1 是股票, type=2 是指数
            df = df[df["type"] == "1"].copy()
            # 只保留上市状态 (status=1)
            df = df[df["status"] == "1"].copy()
            # 代码格式: sh.600519 -> 600519
            df["代码"] = df["code"].str.split(".").str[1]
            df["名称"] = df["code_name"]
            # 过滤 ST 和退市
            df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]

            logger.info("A股股票列表获取成功 (Baostock) | 共 {} 只股票", len(df))
            self._mark_source_success("baostock")
            return df[["代码", "名称"]].reset_index(drop=True)

        except Exception as e:
            logger.warning("Baostock 获取股票列表异常: {}", e)
            self._mark_source_fail("baostock")
            return pd.DataFrame()

    def _get_a_share_kline_baostock(
        self, symbol: str, days: int, adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        通过 Baostock 获取 A 股个股历史 K 线
        TCP socket 连接, 海外 IP 不被风控
        线程安全: 主线程和 worker 线程各有独立会话
        """
        bs = self._get_bs_session()
        if bs is None:
            return pd.DataFrame()
        try:
            bs_code = self._baostock_code(symbol)
            # adjustflag: 1=后复权, 2=前复权, 3=不复权
            adjust_map = {"qfq": "2", "hfq": "1", "": "3"}
            adjustflag = adjust_map.get(adjust, "2")

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            rs = bs.query_history_k_data_plus(
                code=bs_code,
                fields="date,code,open,high,low,close,volume,amount,turn,pctChg,preclose",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag=adjustflag,
            )
            if rs.error_code != '0' and rs.error_code != 0:
                logger.debug("Baostock 获取 {} K线失败: {}", symbol, rs.error_msg)
                self._mark_source_fail("baostock")
                return pd.DataFrame()

            data_list = []
            while (rs.error_code == '0' or rs.error_code == 0) and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return pd.DataFrame()

            df = pd.DataFrame(data_list, columns=rs.fields)
            # 转换数据类型
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)

            logger.debug("Baostock K线 {} | {} 条", symbol, len(df))
            self._mark_source_success("baostock")
            return df[["date", "open", "high", "low", "close", "volume", "amount"]]

        except Exception as e:
            logger.debug("Baostock 获取 {} K线异常: {}", symbol, e)
            self._mark_source_fail("baostock")
            return pd.DataFrame()

    def _get_a_share_index_baostock(self, days: int = 200) -> pd.DataFrame:
        """
        通过 Baostock 获取沪深300指数历史 K 线
        """
        if not self._baostock_login():
            return pd.DataFrame()
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            rs = self._bs.query_history_k_data_plus(
                code="sh.000300",
                fields="date,code,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
            )
            if rs.error_code != '0' and rs.error_code != 0:
                logger.warning("Baostock 获取沪深300指数失败: {}", rs.error_msg)
                self._mark_source_fail("baostock")
                return pd.DataFrame()

            data_list = []
            while (rs.error_code == '0' or rs.error_code == 0) and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return pd.DataFrame()

            df = pd.DataFrame(data_list, columns=rs.fields)
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)

            logger.info("沪深300指数数据获取成功 (Baostock) | {} 日", len(df))
            self._mark_source_success("baostock")
            return df[["date", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            logger.warning("Baostock 获取沪深300指数异常: {}", e)
            self._mark_source_fail("baostock")
            return pd.DataFrame()

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
        东财接口每次最多返回 100 条, 需分页获取全部 ~5500 只
        返回: DataFrame[代码, 名称, 最新价, 涨跌幅]
        """
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        base_params = {
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",  # 沪深A股
            "fields": "f12,f14,f2,f3",  # 代码, 名称, 最新价, 涨跌幅
        }
        try:
            all_rows = []
            page = 1
            total = 0
            while True:
                try:
                    params = {**base_params, "pn": page, "pz": 100}
                    resp = requests.get(
                        url, params=params,
                        timeout=self.EASTMONEY_TIMEOUT,
                        headers={"User-Agent": random.choice(_USER_AGENTS)},
                        proxies={"http": None, "https": None},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as page_e:
                    logger.warning("东财 HTTP 第{}页获取失败: {}, 已获取{}条", page, page_e, len(all_rows))
                    break

                if page == 1:
                    total = data.get("data", {}).get("total", 0)
                    logger.info("东财 HTTP 股票列表 total={} 开始分页获取", total)

                diff = data.get("data", {}).get("diff")
                if not diff:
                    break

                items = diff if isinstance(diff, list) else diff.values()
                page_count = 0
                for item in items:
                    code = str(item.get("f12", "")).strip()
                    name = item.get("f14", "")
                    if not code or not name:
                        continue
                    all_rows.append({
                        "代码": code,
                        "名称": name,
                        "最新价": item.get("f2", 0),
                        "涨跌幅": item.get("f3", 0),
                    })
                    page_count += 1

                if page_count == 0 or len(all_rows) >= total:
                    break
                page += 1

            if not all_rows:
                return pd.DataFrame()

            df = pd.DataFrame(all_rows)
            # 去重 (分页可能有边界重复)
            df = df.drop_duplicates(subset=["代码"]).reset_index(drop=True)
            # 过滤 ST 和退市股
            df = df[~df["名称"].str.contains(r"ST|退|B股", na=False)]
            logger.info(
                "A股股票列表获取成功 (东财HTTP) | 共 {} 只股票 ({} 页)",
                len(df), page,
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
                proxies={"http": None, "https": None},
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
                proxies={"http": None, "https": None},
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
    # 腾讯财经 HTTP 接口 (纯 requests, 额外 K 线备选)
    # ============================================================

    @staticmethod
    def _tencent_symbol(code: str) -> str:
        """A股代码转腾讯格式: 6开头=sh, 其余=sz"""
        prefix = "sh" if code.startswith("6") else "sz"
        return f"{prefix}{code}"

    def _get_a_share_kline_tencent(
        self, symbol: str, days: int, adjust: str
    ) -> pd.DataFrame:
        """通过腾讯财经 HTTP 接口获取 A 股 K 线数据"""
        tcode = self._tencent_symbol(symbol)
        fqt = "qfq" if adjust == "qfq" else ("hfq" if adjust == "hfq" else "")
        # 腾讯接口 param 格式: {code},day,,, {count}, {fqt}
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {"param": f"{tcode},day,,,{days * 2},{fqt}"}
        try:
            resp = requests.get(
                url, params=params,
                timeout=self.EASTMONEY_TIMEOUT,
                headers={"User-Agent": random.choice(_USER_AGENTS)},
                proxies={"http": None, "https": None},
            )
            resp.raise_for_status()
            data = resp.json()

            stock_data = data.get("data", {}).get(tcode, {})
            # 优先取前复权/后复权数据, 没有则取原始数据
            klines = stock_data.get(fqt) or stock_data.get("day", [])
            if not klines:
                logger.debug("腾讯 HTTP {} K线返回空", symbol)
                return pd.DataFrame()

            # 腾讯格式: [date, open, close, high, low, volume]
            rows = []
            for kline in klines:
                if len(kline) < 6:
                    continue
                rows.append({
                    "date": kline[0],
                    "open": float(kline[1]),
                    "close": float(kline[2]),
                    "high": float(kline[3]),
                    "low": float(kline[4]),
                    "volume": float(kline[5]),
                    "amount": float(kline[6]) if len(kline) > 6 else 0,
                })

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            logger.debug("腾讯 HTTP 获取 {} K线成功 | {} 条", symbol, len(df))
            self._mark_source_success("tencent")
            return df

        except Exception as e:
            logger.debug("腾讯 HTTP 获取 {} K线失败: {}", symbol, e)
            self._mark_source_fail("tencent")
            return pd.DataFrame()

    # ============================================================
    # 新浪财经 HTTP 接口 (纯 requests, 额外 K 线备选)
    # ============================================================

    def _get_a_share_kline_sina(
        self, symbol: str, days: int
    ) -> pd.DataFrame:
        """通过新浪财经 HTTP 接口获取 A 股 K 线数据 (仅日线不复权)"""
        scode = self._tencent_symbol(symbol)  # 新浪格式与腾讯相同
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            "symbol": scode,
            "scale": "240",       # 日线
            "datalen": str(days * 2),
            "ma": "no",
        }
        try:
            resp = requests.get(
                url, params=params,
                timeout=self.EASTMONEY_TIMEOUT,
                headers={"User-Agent": random.choice(_USER_AGENTS)},
                proxies={"http": None, "https": None},
            )
            resp.raise_for_status()
            import json as _json
            klines = _json.loads(resp.text)
            if not klines:
                logger.debug("新浪 HTTP {} K线返回空", symbol)
                return pd.DataFrame()

            # 新浪格式: [{day, open, high, low, close, volume}, ...]
            rows = []
            for item in klines:
                rows.append({
                    "date": item.get("day", ""),
                    "open": float(item.get("open", 0)),
                    "high": float(item.get("high", 0)),
                    "low": float(item.get("low", 0)),
                    "close": float(item.get("close", 0)),
                    "volume": float(item.get("volume", 0)),
                    "amount": 0,
                })

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            logger.debug("新浪 HTTP 获取 {} K线成功 | {} 条", symbol, len(df))
            self._mark_source_success("sina")
            return df

        except Exception as e:
            logger.debug("新浪 HTTP 获取 {} K线失败: {}", symbol, e)
            self._mark_source_fail("sina")
            return pd.DataFrame()

    # ============================================================
    # A股数据
    # ============================================================

    def _get_a_share_hot_stocks_eastmoney(self, min_change_pct: float = 5.0) -> pd.DataFrame:
        """
        通过东方财富 HTTP 接口获取涨幅 > min_change_pct 的股票
        利用东财接口排序功能 (fid=f3 按涨跌幅降序), 涨幅 < 阈值时提前终止分页
        通常只需 1-3 页即可获取全部涨幅 > 5% 的股票

        Returns: DataFrame[代码, 名称, 涨跌幅]
        """
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        base_params = {
            "po": 1,           # 降序 (涨幅最高的在前)
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",       # 按涨跌幅排序
            "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14,f3",  # 代码, 名称, 涨跌幅
        }
        try:
            all_rows = []
            page = 1
            while True:
                try:
                    params = {**base_params, "pn": page, "pz": 100}
                    resp = requests.get(
                        url, params=params,
                        timeout=self.EASTMONEY_TIMEOUT,
                        headers={"User-Agent": random.choice(_USER_AGENTS)},
                        proxies={"http": None, "https": None},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as page_e:
                    logger.warning("东财 HTTP 涨幅榜第{}页失败: {}", page, page_e)
                    break

                diff = data.get("data", {}).get("diff")
                if not diff:
                    break

                items = diff if isinstance(diff, list) else diff.values()
                page_count = 0
                stop = False
                for item in items:
                    code = str(item.get("f12", "")).strip()
                    name = item.get("f14", "")
                    change_pct = item.get("f3", 0)

                    if not code or not name:
                        continue

                    # 涨幅 < 阈值, 后面都是更低的, 提前终止
                    try:
                        pct = float(change_pct)
                    except (ValueError, TypeError):
                        continue
                    if pct < min_change_pct:
                        stop = True
                        break

                    # 过滤科创板/北交所/ST
                    if self._is_excluded_code(code):
                        continue
                    if re.search(r"ST|退|B股", str(name)):
                        continue

                    all_rows.append({"代码": code, "名称": name, "涨跌幅": pct})
                    page_count += 1

                if stop or page_count == 0:
                    break
                page += 1

            if not all_rows:
                logger.info("东财涨幅榜: 无涨幅 > {}% 的股票", min_change_pct)
                return pd.DataFrame()

            df = pd.DataFrame(all_rows)
            df = df.drop_duplicates(subset=["代码"]).reset_index(drop=True)
            logger.info(
                "东财涨幅榜获取成功 | 涨幅 > {}%: {} 只 ({} 页)",
                min_change_pct, len(df), page,
            )
            self._mark_source_success("eastmoney")
            return df

        except Exception as e:
            logger.warning("东财 HTTP 涨幅榜获取失败: {}", e)
            self._mark_source_fail("eastmoney")
            return pd.DataFrame()

    def _get_a_share_hot_stocks_baostock(self, min_change_pct: float = 5.0) -> pd.DataFrame:
        """
        通过 Baostock 获取涨幅 > min_change_pct 的股票
        1. 获取全市场股票列表
        2. 过滤科创板/北交所/ST
        3. 多线程查询每只股票最近交易日涨跌幅 (pctChg)
        4. 筛选涨幅 > 阈值的股票

        线程安全: 每个 worker 线程独立 Baostock login
        """
        # 1. 获取全市场列表
        df_list = self._get_a_share_stock_list_baostock()
        if df_list.empty:
            return pd.DataFrame()

        # 2. 过滤科创板/北交所
        df_list = df_list[~df_list["代码"].apply(self._is_excluded_code)].reset_index(drop=True)
        logger.info("Baostock 涨幅筛选: 过滤后 {} 只待查询涨跌幅", len(df_list))

        # 3. 多线程查询涨跌幅
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        def _query_pct(code_name):
            """查询单只股票最近交易日涨跌幅 (worker 线程)"""
            code, name = code_name
            bs = _get_thread_bs_session()
            if bs is None:
                return (code, name, None)
            try:
                bs_code = self._baostock_code(code)
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields="date,pctChg",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                )
                if rs.error_code != "0" and rs.error_code != 0:
                    return (code, name, None)
                last_pct = None
                while rs.next():
                    row = rs.get_row_data()
                    try:
                        last_pct = float(row[1])
                    except (ValueError, TypeError):
                        pass
                return (code, name, last_pct)
            except Exception:
                return (code, name, None)

        hot_rows = []
        total = len(df_list)
        completed = 0

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(_query_pct, (row["代码"], row["名称"])): idx
                for idx, row in df_list.iterrows()
            }
            for future in as_completed(futures):
                completed += 1
                code, name, pct = future.result()
                if pct is not None and pct >= min_change_pct:
                    hot_rows.append({"代码": code, "名称": name, "涨跌幅": pct})
                if completed % 500 == 0:
                    logger.info(
                        "Baostock 涨幅查询进度: {}/{} ({:.0f}%) | 已筛选出 {} 只",
                        completed, total, completed / total * 100, len(hot_rows),
                    )

        if not hot_rows:
            logger.info("Baostock 涨幅筛选: 无涨幅 > {}% 的股票", min_change_pct)
            return pd.DataFrame()

        df = pd.DataFrame(hot_rows)
        df = df.sort_values("涨跌幅", ascending=False).reset_index(drop=True)
        logger.info(
            "Baostock 涨幅筛选完成 | 涨幅 > {}%: {} 只 (共查询 {} 只)",
            min_change_pct, len(df), total,
        )
        self._mark_source_success("baostock")
        return df

    def get_hot_stock_list(self, min_change_pct: float = 5.0) -> list:
        """
        获取涨幅 > min_change_pct 的热门股票列表
        故障切换: 东财HTTP (快速, 按涨幅排序提前终止) → Baostock (多线程, 可靠)

        Returns: [("a_share", code, name), ...]
        """
        # 首选: 东财HTTP (按涨幅排序, 只需1-3页)
        if self._is_source_available("eastmoney"):
            df = self._get_a_share_hot_stocks_eastmoney(min_change_pct)
            if not df.empty:
                return [("a_share", row["代码"], row["名称"]) for _, row in df.iterrows()]

        # 备用: Baostock 多线程查询
        if self._is_source_available("baostock"):
            df = self._get_a_share_hot_stocks_baostock(min_change_pct)
            if not df.empty:
                return [("a_share", row["代码"], row["名称"]) for _, row in df.iterrows()]

        logger.error("获取涨幅 > {}% 股票列表失败 (所有数据源均不可用)", min_change_pct)
        return []
        """
        获取A股全市场股票列表
        返回: DataFrame[代码, 名称, 最新价, 涨跌幅, ...]

        故障切换: Baostock → AkShare → efinance → 东财HTTP
        Baostock 为第一优先级 (TCP socket, 海外 IP 不被风控)
        每个数据源最多重试 MAX_RETRIES 次, 重试间隔随机延迟 2-5 秒
        所有数据源彻底失败时返回空 DataFrame
        """
        # 首选源: Baostock (TCP socket, 海外可用, 无需注册)
        if self._is_source_available("baostock"):
            df = self._get_a_share_stock_list_baostock()
            if not df.empty:
                return df

        # 备用源1: AkShare (带重试)
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

        故障切换: Baostock → AkShare → efinance → 东财HTTP → 腾讯HTTP → 新浪HTTP
        Baostock 为第一优先级 (TCP socket, 海外 IP 不被风控)

        Args:
            symbol: 股票代码, 如 "000001"
            days: 获取最近N个交易日数据
            adjust: 复权类型 qfq=前复权 hfq=后复权 ""=不复权

        Returns:
            标准化 DataFrame: [date, open, high, low, close, volume, amount]
        """
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")

        # 首选源: Baostock (TCP socket, 海外可用)
        if self._is_source_available("baostock"):
            df = self._get_a_share_kline_baostock(symbol, days, adjust)
            if not df.empty:
                return df

        # 备用源1: AkShare
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

        # 兜底源1: 东财 HTTP
        if self._is_source_available("eastmoney"):
            df = self._get_a_share_kline_eastmoney(symbol, days, adjust)
            if not df.empty:
                return df

        # 兜底源2: 腾讯财经 HTTP
        if self._is_source_available("tencent"):
            df = self._get_a_share_kline_tencent(symbol, days, adjust)
            if not df.empty:
                return df

        # 兜底源3: 新浪财经 HTTP (仅不复权日线)
        if self._is_source_available("sina"):
            df = self._get_a_share_kline_sina(symbol, days)
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

        故障切换: Baostock → AkShare → efinance → 东财HTTP
        Baostock 为第一优先级 (TCP socket, 海外 IP 不被风控)
        每个数据源最多重试 MAX_RETRIES 次

        Returns:
            标准化 DataFrame: [date, close, open, high, low, volume]
        """
        # 首选源: Baostock (TCP socket, 海外可用)
        if self._is_source_available("baostock"):
            df = self._get_a_share_index_baostock(days)
            if not df.empty:
                return df

        # 备用源1: AkShare (带重试)
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

    def get_stock_list(self, min_change_pct: float = 0.0) -> list:
        """
        获取股票列表 (根据市场自动路由)

        Args:
            min_change_pct: 最小涨幅过滤 (0=不过滤, >0=只取涨幅超过此值的股票)
                           涨幅过滤可大幅减少扫描数量 (5000+ → 几十~两百家)

        安全处理: 数据源失败时返回空列表, 不抛出 KeyError
        """
        if self.market in ("a_share", "all"):
            # 涨幅过滤模式: 只扫描涨幅 > min_change_pct 的股票
            if min_change_pct > 0:
                hot_list = self.get_hot_stock_list(min_change_pct)
                if self.market == "a_share":
                    return hot_list
                else:
                    us_codes = self.get_us_stock_list()
                    return hot_list + [("us_stock", code, code) for code in us_codes]

            # 全量模式 (不过滤涨幅)
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
        rate_limit: float = 0.05,
        max_workers: int = 10,
    ) -> dict:
        """
        批量获取K线数据 (多线程并发)

        Args:
            stock_list: [(market_type, code, name), ...]
            days: K线天数
            rate_limit: 每次请求间隔(秒), 多线程模式下自动降低
            max_workers: 最大并发线程数 (Baostock 每个 worker 独立 TCP 连接)

        Returns:
            {symbol: {"data": DataFrame, "name": str, "market": str}}
        """
        results = {}
        total = len(stock_list)

        if total == 0:
            return results

        def _fetch_one(item):
            """单只股票 K 线获取 (worker 线程, 线程安全)"""
            mtype, code, name = item
            try:
                df = self.get_kline(mtype, code, days)
                if not df.empty and len(df) >= 60:
                    return (code, {"data": df, "name": name, "market": mtype})
            except Exception as e:
                logger.debug("获取 {} K线异常: {}", code, e)
            return (code, None)

        # 多线程并发获取
        actual_workers = min(max_workers, total)
        logger.info(
            "开始多线程抓取K线 | {} 只股票, {} 线程, {} 天",
            total, actual_workers, days,
        )

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = {executor.submit(_fetch_one, item): item for item in stock_list}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                code, result = future.result()
                if result is not None:
                    results[code] = result
                if completed % 50 == 0 or completed == total:
                    logger.info(
                        "数据抓取进度: {}/{} ({:.0f}%) | 成功: {}",
                        completed, total, completed / total * 100, len(results),
                    )

        logger.info("数据抓取完成 | 成功: {}/{}", len(results), total)
        return results
