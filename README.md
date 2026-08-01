"""
Data ETL - 涨停股专用数据抓取与清洗 (v4.1)
================================================
专为 GitHub Actions 海外环境设计，全部使用纯 HTTP 接口：
  1. 东财涨停板池 (push2ex) - 秒级获取当日涨停股
  2. 腾讯财经 K 线 (ifzq.gtimg) - 历史日线，海外稳定
  3. 新浪财经 K 线备用
  4. 沪深300指数 (腾讯)

设计原则：
  - 绝不依赖需要登录/API Key 的库
  - 任何数据源失败都优雅降级，返回空结果而不是崩溃
  - 只扫涨停股，数量少，速度快 (通常 10-25 分钟)
"""

import time
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests
from loguru import logger

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

EASTMONEY_ZT_UT = "7eea3edcaed734bea9cbfc24409ed989"
REQUEST_TIMEOUT = 15


def _headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Referer": "https://quote.eastmoney.com/",
        "Accept": "*/*",
    }


def _is_excluded(code: str, name: str = "") -> bool:
    code = str(code).strip()
    if code.startswith(("688", "8", "4")):
        return True
    if re.search(r"ST|退|B股|\*ST", str(name), re.IGNORECASE):
        return True
    return False


def _to_tencent_code(code: str) -> str:
    code = str(code).strip()
    if code.startswith(("sh", "sz")):
        return code
    if code.startswith("6"):
        return f"sh{code}"
    return f"sz{code}"


def _trade_date_str() -> str:
    today = datetime.now()
    if today.weekday() == 5:
        today -= timedelta(days=1)
    elif today.weekday() == 6:
        today -= timedelta(days=2)
    return today.strftime("%Y%m%d")


class DataETL:
    def __init__(self, market: str = "a_share"):
        self.market = market
        self._fail_counts = {"eastmoney": 0, "tencent": 0, "sina": 0}
        self._FAIL_THRESHOLD = 3

    def _mark_fail(self, source: str):
        self._fail_counts[source] = self._fail_counts.get(source, 0) + 1
        if self._fail_counts[source] >= self._FAIL_THRESHOLD:
            logger.warning("数据源 {} 连续失败 {} 次, 暂时降级", source, self._fail_counts[source])

    def _mark_ok(self, source: str):
        self._fail_counts[source] = 0

    def _available(self, source: str) -> bool:
        return self._fail_counts.get(source, 0) < self._FAIL_THRESHOLD

    def _get_zt_pool_eastmoney(self) -> List[Tuple[str, str, str]]:
        date_str = _trade_date_str()
        url = "https://push2ex.eastmoney.com/getTopicZTPool"
        params = {
            "ut": EASTMONEY_ZT_UT,
            "dpt": "wz.ztzt",
            "Pageindex": "0",
            "pagesize": "500",
            "sort": "fbt:asc",
            "date": date_str,
        }
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            pool = (data.get("data") or {}).get("pool") or []
            if not pool:
                logger.info("东财涨停板: {} 无数据 (可能非交易日)", date_str)
                return []

            results = []
            max_lb = 0
            for item in pool:
                code = str(item.get("c", "")).strip()
                name = str(item.get("n", "")).strip()
                if not code or not name or _is_excluded(code, name):
                    continue
                lb = int(item.get("lbc") or 0)
                max_lb = max(max_lb, lb)
                results.append(("a_share", code, name))

            logger.info("东财涨停板获取成功 | {} | 涨停股 {} 只 (最高连板 {} 板)", date_str, len(results), max_lb)
            self._mark_ok("eastmoney")
            return results
        except Exception as e:
            logger.warning("东财涨停板失败: {}", e)
            self._mark_fail("eastmoney")
            return []

    def get_zt_stock_list(self) -> List[Tuple[str, str, str]]:
        if self._available("eastmoney"):
            lst = self._get_zt_pool_eastmoney()
            if lst:
                return lst
        logger.error("获取涨停股列表失败 (所有数据源均不可用)")
        return []

    def get_stock_list(self, min_change_pct: float = 5.0, zt_only: bool = True) -> List[Tuple[str, str, str]]:
        if zt_only:
            return self.get_zt_stock_list()
        logger.info("当前仅支持涨停股模式，自动切换为 zt_only")
        return self.get_zt_stock_list()

    def get_market_index(self, days: int = 200) -> pd.DataFrame:
        try:
            url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            params = {"param": f"sh000300,day,,,{days * 2},"}
            resp = requests.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            stock_data = data.get("data", {}).get("sh000300", {})
            klines = stock_data.get("day") or stock_data.get("qfqday") or []
            if not klines:
                logger.warning("腾讯沪深300返回空")
                return pd.DataFrame()

            rows = []
            for k in klines:
                if len(k) < 6:
                    continue
                rows.append({
                    "date": k[0],
                    "open": float(k[1]),
                    "close": float(k[2]),
                    "high": float(k[3]),
                    "low": float(k[4]),
                    "volume": float(k[5]),
                })
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True).tail(days)
            logger.info("沪深300指数获取成功 (腾讯) | {} 日", len(df))
            self._mark_ok("tencent")
            return df
        except Exception as e:
            logger.warning("获取大盘指数失败: {}", e)
            self._mark_fail("tencent")
            return pd.DataFrame()

    def _kline_tencent(self, code: str, days: int = 120) -> pd.DataFrame:
        tcode = _to_tencent_code(code)
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {"param": f"{tcode},day,,,{days * 2},qfq"}
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            stock_data = data.get("data", {}).get(tcode, {})
            klines = stock_data.get("qfqday") or stock_data.get("day") or []
            if not klines:
                return pd.DataFrame()

            rows = []
            for k in klines:
                if len(k) < 6:
                    continue
                rows.append({
                    "date": k[0],
                    "open": float(k[1]),
                    "close": float(k[2]),
                    "high": float(k[3]),
                    "low": float(k[4]),
                    "volume": float(k[5]),
                    "amount": float(k[6]) if len(k) > 6 else 0.0,
                })
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True).tail(days)
            self._mark_ok("tencent")
            return df
        except Exception as e:
            logger.debug("腾讯K线 {} 失败: {}", code, e)
            self._mark_fail("tencent")
            return pd.DataFrame()

    def _kline_sina(self, code: str, days: int = 120) -> pd.DataFrame:
        scode = _to_tencent_code(code)
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {"symbol": scode, "scale": "240", "datalen": str(days * 2), "ma": "no"}
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            klines = resp.json()
            if not klines:
                return pd.DataFrame()
            rows = []
            for item in klines:
                rows.append({
                    "date": item.get("day", ""),
                    "open": float(item.get("open", 0)),
                    "high": float(item.get("high", 0)),
                    "low": float(item.get("low", 0)),
                    "close": float(item.get("close", 0)),
                    "volume": float(item.get("volume", 0)),
                    "amount": 0.0,
                })
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True).tail(days)
            self._mark_ok("sina")
            return df
        except Exception as e:
            logger.debug("新浪K线 {} 失败: {}", code, e)
            self._mark_fail("sina")
            return pd.DataFrame()

    def get_kline(self, mtype: str, code: str, days: int = 120) -> pd.DataFrame:
        if mtype != "a_share":
            return pd.DataFrame()
        if self._available("tencent"):
            df = self._kline_tencent(code, days)
            if not df.empty:
                return df
        if self._available("sina"):
            df = self._kline_sina(code, days)
            if not df.empty:
                return df
        return pd.DataFrame()

    def get_a_share_kline(self, symbol: str, days: int = 120, adjust: str = "qfq") -> pd.DataFrame:
        return self.get_kline("a_share", symbol, days)

    def batch_fetch(
        self,
        stock_list: List[Tuple[str, str, str]],
        days: int = 120,
        rate_limit: float = 0.1,
        max_workers: int = 4,
    ) -> Dict[str, Dict]:
        results: Dict[str, Dict] = {}
        total = len(stock_list)
        if total == 0:
            return results

        actual_workers = min(max_workers, total)
        logger.info("开始多线程抓取K线 | {} 只股票, {} 线程, {} 天", total, actual_workers, days)

        def _fetch_one(item):
            mtype, code, name = item
            try:
                time.sleep(random.uniform(0.15, 0.45))
                df = self.get_kline(mtype, code, days)
                if not df.empty and len(df) >= 60:
                    return code, {"data": df, "name": name, "market": mtype}
            except Exception as e:
                logger.debug("抓取 {} 异常: {}", code, e)
            return code, None

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = {executor.submit(_fetch_one, item): item for item in stock_list}
            completed = 0
            for fut in as_completed(futures):
                completed += 1
                try:
                    code, result = fut.result(timeout=40)
                    if result is not None:
                        results[code] = result
                except Exception as e:
                    logger.warning("任务异常: {}", e)

                if completed % 20 == 0 or completed == total:
                    logger.info(
                        "数据抓取进度: {}/{} ({:.0f}%) | 成功: {}",
                        completed, total, completed / total * 100, len(results),
                    )

        logger.info("数据抓取完成 | 成功: {}/{}", len(results), total)
        return results


def health_check(market: str = "a_share") -> Dict[str, Dict]:
    out = {}
    for s in ["eastmoney", "tencent", "sina"]:
        out[s] = {"available": True, "latency_ms": 0, "error": ""}
    try:
        r = requests.get(
            "https://push2ex.eastmoney.com/getTopicZTPool",
            params={"ut": EASTMONEY_ZT_UT, "dpt": "wz.ztzt", "Pageindex": 0, "pagesize": 1, "date": _trade_date_str()},
            headers=_headers(),
            timeout=8,
        )
        out["eastmoney"]["available"] = r.status_code == 200
    except Exception as e:
        out["eastmoney"] = {"available": False, "latency_ms": 0, "error": str(e)[:80]}
    return out
