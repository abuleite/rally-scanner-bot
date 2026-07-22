"""
Data ETL v4.0 - 纯 HTTP 版 (海外可用)
======================================
彻底移除 AkShare / Baostock / efinance / 东财 依赖。
全部使用腾讯财经 HTTP API + 新浪财经备胎, 确保 GitHub Actions 海外环境 100% 可用。

v4.0 重大变更:
  - 删除 AkShare (海外连接超时)
  - 删除 Baostock (TCP 端口被封)
  - 删除 efinance (海外被封)
  - 删除 东财 HTTP (海外 IP 风控)
  - 主力: 腾讯财经 HTTP API (qt.gtimg.cn + web.ifzq.gtimg.cn)
  - 备用: 新浪财经 HTTP API (money.finance.sina.com.cn)

数据源对比:
  腾讯 qt.gtimg.cn      → 行情快照 (不封IP, 80只/批) ✨主力
  腾讯 web.ifzq.gtimg.cn → K线 + 指数 (不封IP, 前复权) ✨主力
  新浪 money.finance.sina → 股票列表 + K线备胎 (不封IP) 🔄备用

依赖: requests, pandas, numpy, loguru (零第三方数据封装库)
"""

import time
import re
import random
import requests
import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote


# ============================================================
# 全局 User-Agent
# ============================================================

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


# ============================================================
# 工具函数
# ============================================================

def _secid(code: str) -> str:
    """股票代码转东财格式 secid (仅指数查询备用)"""
    return f"1.{code}" if code.startswith("6") else f"0.{code}"


def _tencent_code(code: str) -> str:
    """A股代码转腾讯格式: sh600519 / sz000001"""
    return f"sh{code}" if code.startswith("6") else f"sz{code}"


def _is_excluded(code: str) -> bool:
    """排除科创板(688)/北交所(920+8)/老三板(4)"""
    code = str(code).strip()
    return code.startswith(("688", "920", "8", "4"))


# ============================================================
# DataETL v4.0
# ============================================================

class DataETL:
    """数据抓取与清洗引擎 v4.0 (纯 HTTP, 海外可用)"""

    REQUEST_TIMEOUT = 15

    def __init__(self, market: str = "a_share"):
        self.market = market
        self._fail_counts: dict = {}
        self._FAIL_THRESHOLD = 3
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": random.choice(_USER_AGENTS)})
        s.proxies = {"http": None, "https": None}
        return s

    def _ok(self, source: str) -> bool:
        return self._fail_counts.get(source, 0) < self._FAIL_THRESHOLD

    def _mark_ok(self, source: str):
        self._fail_counts[source] = 0

    def _mark_fail(self, source: str):
        c = self._fail_counts.get(source, 0) + 1
        self._fail_counts[source] = c
        if c >= self._FAIL_THRESHOLD:
            logger.warning("数据源 {} 连续失败 {} 次, 暂时禁用", source, c)

    # ============================================================
    # 新浪 - 全市场股票列表
    # ============================================================

    def _get_stock_list_sina(self) -> pd.DataFrame:
        """
        通过新浪财经 API 获取全市场 A 股代码 + 名称
        API: vip.stock.finance.sina.com.cn (不封IP, 每页最多100条)
        Returns: DataFrame[代码, 名称]
        
        注意: 第1-3页主要是北交所(920xxx), 会被过滤但不会提前终止
        """
        all_rows = []
        for page in range(1, 60):
            url = (
                "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                "Market_Center.getHQNodeData"
            )
            params = {
                "page": page,
                "num": 80,       # Sina 最大支持约 80-100 条
                "sort": "symbol",
                "asc": 1,
                "node": "hs_a",
                "symbol": "",
                "_s_r_a": "auto",
            }
            try:
                resp = self._session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
                resp.encoding = "gb2312"
                text = resp.text.strip()
                if not text or text.startswith("null"):
                    break
                try:
                    data = resp.json()
                except Exception:
                    import json as _json
                    match = re.search(r'\[.*\]', text, re.DOTALL)
                    if not match:
                        break
                    try:
                        data = _json.loads(match.group())
                    except Exception:
                        break

                if not isinstance(data, list) or len(data) == 0:
                    break

                for item in data:
                    code = str(item.get("code", "")).strip()
                    name = item.get("name", "")
                    if not code or not name:
                        continue
                    if _is_excluded(code):
                        continue
                    if re.search(r"ST|退|B股", name):
                        continue
                    all_rows.append({"代码": code, "名称": name})

                # 如果返回数 < 80, 说明已到末尾
                if len(data) < 80:
                    break

            except Exception as e:
                logger.warning("新浪股票列表第{}页失败: {}", page, e)
                break

        if not all_rows:
            self._mark_fail("sina_list")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows).drop_duplicates(subset=["代码"]).reset_index(drop=True)
        logger.info("新浪股票列表获取成功 | {} 只 A 股 ({} 页)", len(df), page)
        self._mark_ok("sina_list")
        return df

    # ============================================================
    # 腾讯 - 批量行情快照 (用于涨停检测)
    # ============================================================

    def _get_spot_quotes_tencent(self, codes: List[str]) -> pd.DataFrame:
        """
        通过腾讯 qt.gtimg.cn 批量获取行情快照 (不封IP)
        API: https://qt.gtimg.cn/q={codes}
        每批最多约 80 只, 返回格式: v_sh600519="1~平安银行~..."
        Returns: DataFrame[代码, 名称, 最新价, 涨跌幅, 涨停价]
        """
        rows = []
        batch_size = 60
        total = len(codes)

        for i in range(0, total, batch_size):
            batch = codes[i:i + batch_size]
            tencent_codes = ",".join([_tencent_code(c) for c in batch])
            url = f"https://qt.gtimg.cn/q={tencent_codes}"

            try:
                resp = self._session.get(url, timeout=self.REQUEST_TIMEOUT)
                if resp.status_code != 200:
                    continue
                # 腾讯返回 gbk 编码
                raw = resp.content.decode("gbk", errors="replace")

                for line in raw.strip().split("\n"):
                    line = line.strip()
                    if not line or "=" not in line:
                        continue
                    try:
                        # 格式: v_sh600519="1~贵州茅台~..."
                        key_part = line.split("=")[0].strip()
                        val_part = line.split('"')[1] if '"' in line else ""
                        if not val_part:
                            continue
                        fields = val_part.split("~")
                        if len(fields) < 48:
                            continue

                        code = key_part.replace("v_", "").replace("sh", "").replace("sz", "")
                        name = fields[1]
                        price = float(fields[3]) if fields[3] else 0.0
                        change_pct = float(fields[32]) if fields[32] else 0.0
                        limit_up = float(fields[47]) if len(fields) > 47 and fields[47] else 0.0
                        high = float(fields[33]) if len(fields) > 33 and fields[33] else 0.0
                        low = float(fields[34]) if len(fields) > 34 and fields[34] else 0.0
                        open_ = float(fields[5]) if len(fields) > 5 and fields[5] else 0.0
                        volume = float(fields[6]) if len(fields) > 6 and fields[6] else 0.0
                        amount = float(fields[37]) if len(fields) > 37 and fields[37] else 0.0
                        turnover = float(fields[38]) if len(fields) > 38 and fields[38] else 0.0
                        pe = float(fields[39]) if len(fields) > 39 and fields[39] else 0.0
                        market_cap = float(fields[45]) if len(fields) > 45 and fields[45] else 0.0

                        rows.append({
                            "代码": code,
                            "名称": name,
                            "最新价": price,
                            "涨跌幅": change_pct,
                            "涨停价": limit_up,
                            "最高": high,
                            "最低": low,
                            "开盘": open_,
                            "成交量": volume,
                            "成交额": amount,
                            "换手率": turnover if turnover else 0,
                            "市盈率": pe if pe else 0,
                            "总市值": market_cap if market_cap else 0,
                        })
                    except (ValueError, IndexError) as e:
                        continue

            except Exception as e:
                logger.debug("腾讯行情快照批次失败 (offset={}): {}", i, e)
                continue

            # 批次间短暂休眠, 避免触发限流
            if i + batch_size < total:
                time.sleep(0.15)

        if not rows:
            self._mark_fail("tencent_spot")
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        logger.info("腾讯行情快照获取成功 | {} 只", len(df))
        self._mark_ok("tencent_spot")
        return df

    # ============================================================
    # 腾讯 - K 线数据
    # ============================================================

    def _get_kline_tencent(self, code: str, days: int) -> pd.DataFrame:
        """
        通过腾讯财经获取 A 股 K 线 (前复权)
        API: web.ifzq.gtimg.cn/appstock/app/fqkline/get
        Returns: DataFrame[date, open, high, low, close, volume, amount]
        """
        tcode = _tencent_code(code)
        # 多取一些, 然后 tail 截取
        fetch_days = max(days * 2, 250)
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {"param": f"{tcode},day,,,{fetch_days},qfq"}

        try:
            resp = self._session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            stock = data.get("data", {}).get(tcode, {})
            # 优先取 qfqday (前复权日线), 其次 day (不复权)
            klines = stock.get("qfqday") or stock.get("day") or []
            if not klines:
                logger.debug("腾讯 K 线 {} 返回空", code)
                return pd.DataFrame()

            rows = []
            for k in klines:
                # 标准格式: 6 元素 [date, open, close, high, low, volume]
                # 除权除息日: 7 元素, k[6] = dict (分红信息), 忽略即可
                if len(k) < 6:
                    continue
                try:
                    rows.append({
                        "date": str(k[0]),
                        "open": float(k[1]),
                        "close": float(k[2]),
                        "high": float(k[3]),
                        "low": float(k[4]),
                        "volume": float(k[5]),
                        "amount": 0.0,  # 腾讯 K 线不含成交额
                    })
                except (ValueError, TypeError):
                    continue

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            return df[["date", "open", "high", "low", "close", "volume", "amount"]]

        except Exception as e:
            logger.debug("腾讯 K 线 {} 失败: {}", code, e)
            self._mark_fail("tencent_kline")
            return pd.DataFrame()

    # ============================================================
    # 新浪 - K 线数据 (备用)
    # ============================================================

    def _get_kline_sina(self, code: str, days: int) -> pd.DataFrame:
        """
        通过新浪财经获取 A 股 K 线 (不复权, 备用)
        API: money.finance.sina.com.cn
        Returns: DataFrame[date, open, high, low, close, volume]
        """
        scode = _tencent_code(code)
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            "symbol": scode,
            "scale": "240",
            "datalen": str(days * 2),
            "ma": "no",
        }

        try:
            resp = self._session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
            resp.raise_for_status()
            import json as _json
            klines = _json.loads(resp.text)

            if not klines or not isinstance(klines, list):
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
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            return df[["date", "open", "high", "low", "close", "volume", "amount"]]

        except Exception as e:
            logger.debug("新浪 K 线 {} 失败: {}", code, e)
            self._mark_fail("sina_kline")
            return pd.DataFrame()

    # ============================================================
    # 腾讯 - 指数数据
    # ============================================================

    def _get_index_tencent(self, code: str, days: int) -> pd.DataFrame:
        """
        通过腾讯财经获取指数 K 线 (如 sh000300 沪深300)
        Returns: DataFrame[date, open, high, low, close, volume]
        """
        fetch_days = max(days * 2, 300)
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {"param": f"{code},day,,,{fetch_days},"}

        try:
            resp = self._session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            stock = data.get("data", {}).get(code, {})
            klines = stock.get("day") or []
            if not klines:
                logger.warning("腾讯指数 {} 返回空", code)
                return pd.DataFrame()

            rows = []
            for k in klines:
                if len(k) < 6:
                    continue
                rows.append({
                    "date": str(k[0]),
                    "open": float(k[1]),
                    "close": float(k[2]),
                    "high": float(k[3]),
                    "low": float(k[4]),
                    "volume": float(k[5]),
                })

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(days).reset_index(drop=True)
            return df[["date", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            logger.warning("腾讯指数 {} 失败: {}", code, e)
            self._mark_fail("tencent_kline")
            return pd.DataFrame()

    # ============================================================
    # 同花顺 - 涨停揭秘 API (海外优先, 零鉴权)
    # ============================================================

    def _get_zt_list_ths(self) -> pd.DataFrame:
        """
        同花顺涨停揭秘 API (海外可用, 零鉴权, 单次返回全量涨停股)
        比批量扫描腾讯快 100 倍, 是 v4.0 涨停检测的首选源
        
        Returns: DataFrame[代码, 名称, 涨跌幅, 涨停原因]
        """
        now = datetime.now()
        # 同花顺日期格式: YYYYMMDD
        date_str = now.strftime("%Y%m%d")
        # 如果当前是凌晨(还没到开盘时间), 尝试用昨天日期
        if now.hour < 9:
            date_str = (now - timedelta(days=3)).strftime("%Y%m%d")

        url = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"
        params = {
            "page": 1,
            "limit": 200,
            "field": "199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004",
            "filter": "HS,GEM2STAR",
            "order_field": "330324",
            "order_type": "0",
            "date": date_str,
        }

        try:
            resp = self._session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            info = (data.get("data") or {}).get("info", [])

            if not info:
                logger.info("同花顺涨停揭秘: {} 无涨停股", date_str)
                return pd.DataFrame()

            rows = []
            for item in info:
                code = str(item.get("code", "")).strip()
                name = item.get("name", "")
                if not code or not name:
                    continue
                if _is_excluded(code):
                    continue
                if re.search(r"ST|退|B股", name):
                    continue

                change_rate = item.get("change_rate", 0)
                high_days = item.get("high_days", "")
                reason = item.get("reason_type", "")

                # 解析连板数
                lbc = 0
                if high_days and "板" in str(high_days):
                    try:
                        lbc = int(re.search(r"(\d+)板", str(high_days)).group(1))
                    except (AttributeError, ValueError):
                        pass

                rows.append({
                    "代码": code,
                    "名称": name,
                    "涨跌幅": float(change_rate) if change_rate else 0,
                    "连板数": lbc,
                    "涨停原因": reason,
                })

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            logger.info("同花顺涨停揭秘 | {} 涨停: {} 只 (连板最高 {} 板)", date_str, len(df), df["连板数"].max() if len(df) > 0 else 0)
            return df

        except Exception as e:
            logger.warning("同花顺涨停揭秘失败: {}", e)
            return pd.DataFrame()

    # ============================================================
    # 涨停股 - 腾讯批量扫描 (同花顺失败时的备选方案)
    # ============================================================

    def _get_zt_list_tencent_scan(self) -> pd.DataFrame:
        """
        批量扫描腾讯行情快照, 检测涨停股 (备选方案)
        
        流程:
          1. 新浪获取全市场代码
          2. 腾讯批量查询行情快照 (60只/批)
          3. 筛选涨跌幅 >= 9.7 或 最新价 >= 涨停价*0.99 的股票
        
        Returns: DataFrame[代码, 名称, 涨跌幅]
        """
        # Step 1: 获取所有代码
        stock_list = self._get_stock_list_sina()
        if stock_list.empty:
            return pd.DataFrame()

        codes = stock_list["代码"].tolist()
        logger.info("腾讯涨停扫描: {} 只待查询 (60只/批)", len(codes))

        # Step 2: 批量查行情
        spot_df = self._get_spot_quotes_tencent(codes)
        if spot_df.empty:
            return pd.DataFrame()

        # Step 3: 筛选涨停
        zt_mask = (
            (spot_df["涨跌幅"] >= 9.7) |
            ((spot_df["涨停价"] > 0) & (spot_df["最新价"] >= spot_df["涨停价"] * 0.99))
        )
        zt_df = spot_df[zt_mask].copy()

        if zt_df.empty:
            logger.info("腾讯扫描: 无涨停股")
            return pd.DataFrame()

        logger.info("腾讯扫描发现涨停股: {} 只", len(zt_df))
        return zt_df[["代码", "名称", "涨跌幅"]].reset_index(drop=True)

    # ============================================================
    # 公共接口
    # ============================================================

    def get_market_index(self, days: int = 200) -> pd.DataFrame:
        """获取沪深300指数 K 线"""
        df = self._get_index_tencent("sh000300", days)
        if df.empty:
            # 备用: 中证500
            df = self._get_index_tencent("sh000905", days)
        if not df.empty:
            logger.info("沪深300指数数据 | {} 日", len(df))
            self._mark_ok("tencent_kline")
        else:
            logger.warning("指数数据获取失败 (所有源不可用)")
        return df

    def get_stock_list(self, min_change_pct: float = 0.0, zt_only: bool = False) -> List[Tuple]:
        """
        获取股票列表

        Args:
            min_change_pct: 最小涨幅 (0=不过滤)
            zt_only: 只取涨停股

        Returns: [("a_share", code, name), ...]
        """
        if zt_only:
            # 首选: 同花顺涨停揭秘 (最快, 零鉴权)
            df = self._get_zt_list_ths()
            if not df.empty:
                return [("a_share", r["代码"], r["名称"]) for _, r in df.iterrows()]

            # 备用: 腾讯批量扫描
            df = self._get_zt_list_tencent_scan()
            if not df.empty:
                return [("a_share", r["代码"], r["名称"]) for _, r in df.iterrows()]

            logger.error("涨停股列表获取失败 (所有数据源不可用)")
            return []

        # 涨幅过滤模式
        if min_change_pct > 0:
            stock_list = self._get_stock_list_sina()
            if stock_list.empty:
                return []
            codes = stock_list["代码"].tolist()
            spot_df = self._get_spot_quotes_tencent(codes)
            if spot_df.empty:
                return []
            hot = spot_df[spot_df["涨跌幅"] >= min_change_pct]
            return [("a_share", r["代码"], r["名称"]) for _, r in hot.iterrows()]

        # 全量模式
        df = self._get_stock_list_sina()
        if df.empty:
            return []
        return [("a_share", r["代码"], r["名称"]) for _, r in df.iterrows()]

    def get_kline(self, market_type: str, symbol: str, days: int = 120) -> pd.DataFrame:
        """获取个股 K 线 (自动路由)"""
        if market_type != "a_share":
            return pd.DataFrame()

        # 首选: 腾讯 K 线
        df = self._get_kline_tencent(symbol, days)
        if not df.empty:
            self._mark_ok("tencent_kline")
            return df

        # 备用: 新浪 K 线
        df = self._get_kline_sina(symbol, days)
        if not df.empty:
            self._mark_ok("sina_kline")
            return df

        return pd.DataFrame()

    def batch_fetch(
        self,
        stock_list: List[Tuple],
        days: int = 120,
        max_workers: int = 5,  # v4.0 降低并发: 纯 HTTP, 避免触发限流
    ) -> dict:
        """
        批量获取 K 线数据 (多线程并发)

        Args:
            stock_list: [("a_share", code, name), ...]
            days: 需要的 K 线天数
            max_workers: 并发线程数

        Returns:
            {code: {"data": DataFrame, "name": str, "market": str}}
        """
        results = {}
        total = len(stock_list)
        if total == 0:
            return results

        actual_workers = min(max_workers, total)
        logger.info("批量抓取 K 线 | {} 只, {} 线程, {} 天", total, actual_workers, days)

        def _fetch_one(item):
            mtype, code, name = item
            try:
                df = self.get_kline(mtype, code, days)
                if not df.empty and len(df) >= 60:
                    return (code, {"data": df, "name": name, "market": mtype})
            except Exception as e:
                logger.debug("{} K线异常: {}", code, e)
            return (code, None)

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = {executor.submit(_fetch_one, item): item for item in stock_list}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                code, result = future.result()
                if result is not None:
                    results[code] = result
                if completed % 20 == 0 or completed == total:
                    logger.info("K线进度: {}/{} ({:.0f}%) | 成功: {}", completed, total, completed / total * 100, len(results))

        logger.info("K线抓取完成 | 成功: {}/{}", len(results), total)
        return results
