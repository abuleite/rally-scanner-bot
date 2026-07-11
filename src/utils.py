"""
Self-Healing Utils - 环境自愈与运维工具模块
================================================
为 24/7 无人值守运行提供以下保障:

1. 交易日历判断 (TradingCalendar)
   - 自动跳过周末和中国法定节假日
   - 内置简易节假日表, 支持手动追加

2. 临时文件自动清理 (cleanup_temp_files)
   - 扫描工作目录下的 *.csv / *.json / *.log 临时文件
   - 按文件年龄清理, 防止磁盘溢出
   - 保留最近 N 天的日志和死信队列

3. 数据源健康检查 (health_check)
   - 快速 ping 各数据源 API, 返回可用性报告

4. 全局异常兜底 (safe_run)
   - 装饰器/上下文管理器, 捕获未处理异常并记录
   - 防止单股异常导致整条流水线崩溃
"""

import os
import gc
import glob
import time
import json
import random
import shutil
import requests
import traceback
from datetime import datetime, timedelta, date
from typing import Optional, Callable, Any
from loguru import logger
from functools import wraps


# ============================================================
# 全局 User-Agent 注入 (防止海外 IP 被国内数据源封锁)
# ============================================================

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# Monkey-patch requests.Session.request (与 data_etl.py 共享, 幂等不重复打补丁)
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


# ============================================================
# 1. 交易日历
# ============================================================

class TradingCalendar:
    """
    A股交易日历判断器

    判断逻辑:
      1. 周六/周日 → 非交易日
      2. 法定节假日 → 非交易日 (内置简易表)
      3. 其余 → 交易日

    注意: 内置节假日表为硬编码, 每年需更新。
          如需精确判断, 可接入 AkShare 交易日历接口。
    """

    # 2024-2026 A股主要节假日 (格式: YYYY-MM-DD)
    # 每年春节/国庆等假期会有微调, 此表为近似值
    HOLIDAYS = {
        # 2024
        "2024-01-01", "2024-02-09", "2024-02-12", "2024-02-13", "2024-02-14",
        "2024-02-15", "2024-02-16", "2024-04-04", "2024-04-05", "2024-05-01",
        "2024-05-02", "2024-05-03", "2024-06-10", "2024-09-16", "2024-09-17",
        "2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-07",
        # 2025
        "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31",
        "2025-02-03", "2025-04-04", "2025-05-01", "2025-05-02", "2025-05-05",
        "2025-06-02", "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-06",
        "2025-10-07", "2025-10-08",
        # 2026
        "2026-01-01", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
        "2026-02-20", "2026-02-23", "2026-04-06", "2026-05-01", "2026-06-19",
        "2026-09-25", "2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06",
        "2026-10-07", "2026-10-08",
    }

    @classmethod
    def is_trading_day(cls, check_date: Optional[date] = None) -> bool:
        """
        判断给定日期是否为 A股交易日

        Args:
            check_date: 日期对象, 默认今天

        Returns:
            True=交易日, False=非交易日
        """
        if check_date is None:
            check_date = date.today()

        # 周末
        if check_date.weekday() >= 5:
            return False

        # 节假日
        date_str = check_date.strftime("%Y-%m-%d")
        if date_str in cls.HOLIDAYS:
            return False

        return True

    @classmethod
    def is_us_trading_day(cls, check_date: Optional[date] = None) -> bool:
        """
        判断给定日期是否为美股交易日

        美股节假日与A股不同, 这里做简化处理:
        仅排除周末, 美股节假日近似忽略 (yfinance 会自动返回空数据)
        """
        if check_date is None:
            check_date = date.today()
        return check_date.weekday() < 5

    @classmethod
    def should_run(cls, market: str = "a_share") -> bool:
        """
        根据市场判断今天是否应该运行扫描

        Args:
            market: a_share / us_stock / all

        Returns:
            True=应该运行, False=今天无需运行
        """
        today = date.today()

        if market == "us_stock":
            return cls.is_us_trading_day(today)
        elif market == "all":
            return cls.is_trading_day(today) or cls.is_us_trading_day(today)
        else:
            return cls.is_trading_day(today)


# ============================================================
# 2. 临时文件自动清理
# ============================================================

def cleanup_temp_files(
    base_dir: str,
    max_age_days: int = 7,
    keep_patterns: Optional[list] = None,
    dry_run: bool = False,
) -> dict:
    """
    自动清理工作目录下的临时文件, 防止磁盘溢出

    清理规则:
      - 扫描 *.csv / *.tmp / *.json 临时文件 (排除 configs/ 和 logs/dead_letter/)
      - 文件年龄超过 max_age_days 天的删除
      - 日志文件 (*.log) 保留最近 max_age_days 天
      - 死信队列 (logs/dead_letter/) 保留最近 max_age_days 天
      - keep_patterns 中的文件路径不清理

    Args:
        base_dir: 项目根目录
        max_age_days: 文件最大保留天数
        keep_patterns: 保留文件路径模式列表 (glob)
        dry_run: True=仅报告不删除

    Returns:
        清理统计 {"scanned": N, "deleted": N, "freed_mb": M, "details": [...]}
    """
    if keep_patterns is None:
        keep_patterns = []

    # 展开 keep_patterns
    keep_files = set()
    for pattern in keep_patterns:
        for f in glob.glob(os.path.join(base_dir, pattern), recursive=True):
            keep_files.add(os.path.abspath(f))

    now = time.time()
    cutoff = now - max_age_days * 86400

    stats = {
        "scanned": 0,
        "deleted": 0,
        "freed_mb": 0,
        "details": [],
    }

    # 需要清理的临时文件模式
    temp_patterns = [
        "**/*.csv",
        "**/*.tmp",
        "**/*.json",
        "**/*.log",
        "**/*.pkl",
        "**/*.parquet",
    ]

    # 排除目录 (不清理)
    exclude_dirs = {
        os.path.abspath(os.path.join(base_dir, ".git")),
        os.path.abspath(os.path.join(base_dir, "configs")),
    }

    for pattern in temp_patterns:
        for filepath in glob.glob(
            os.path.join(base_dir, pattern), recursive=True
        ):
            abs_path = os.path.abspath(filepath)

            # 排除 keep_files
            if abs_path in keep_files:
                continue

            # 排除 configs 和 .git
            if any(abs_path.startswith(d) for d in exclude_dirs):
                continue

            # 排除 requirements.txt, package.json 等配置文件
            basename = os.path.basename(abs_path)
            if basename in ("requirements.txt", "package.json", ".env"):
                continue

            # 排除死信队列中最近 max_age_days 天的文件
            if "dead_letter" in abs_path:
                mtime = os.path.getmtime(abs_path)
                if mtime > cutoff:
                    continue

            stats["scanned"] += 1

            try:
                mtime = os.path.getmtime(abs_path)
                if mtime > cutoff:
                    continue  # 文件未过期, 跳过

                file_size = os.path.getsize(abs_path)

                if dry_run:
                    stats["details"].append({
                        "file": abs_path,
                        "size_mb": round(file_size / 1024 / 1024, 2),
                        "age_days": round((now - mtime) / 86400, 1),
                        "action": "DRY_RUN",
                    })
                else:
                    os.remove(abs_path)
                    stats["deleted"] += 1
                    stats["freed_mb"] += file_size / 1024 / 1024
                    stats["details"].append({
                        "file": abs_path,
                        "size_mb": round(file_size / 1024 / 1024, 2),
                        "age_days": round((now - mtime) / 86400, 1),
                        "action": "DELETED",
                    })

            except Exception as e:
                logger.debug("清理文件失败 {}: {}", abs_path, e)

    stats["freed_mb"] = round(stats["freed_mb"], 2)

    logger.info(
        "临时文件清理完成 | 扫描={} 删除={} 释放={}MB",
        stats["scanned"], stats["deleted"], stats["freed_mb"],
    )

    return stats


# ============================================================
# 3. 数据源健康检查
# ============================================================

def health_check(market: str = "a_share") -> dict:
    """
    快速检查各数据源的可用性

    Returns:
        {
            "akshare": {"available": bool, "latency_ms": int, "error": str},
            "yfinance": {"available": bool, "latency_ms": int, "error": str},
            "efinance": {"available": bool, "latency_ms": int, "error": str},
            "eastmoney": {"available": bool, "latency_ms": int, "error": str},
        }
    """
    results = {}

    # AkShare
    try:
        start = time.time()
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        latency = int((time.time() - start) * 1000)
        results["akshare"] = {
            "available": not df.empty,
            "latency_ms": latency,
            "rows": len(df),
            "error": "",
        }
        logger.info("AkShare 健康检查 PASS | {}ms | {} 行", latency, len(df))
    except Exception as e:
        results["akshare"] = {"available": False, "latency_ms": 0, "error": str(e)}
        logger.warning("AkShare 健康检查 FAIL | {}", e)

    # yfinance (仅美股市场需要)
    if market in ("us_stock", "all"):
        try:
            start = time.time()
            import yfinance as yf
            df = yf.download("AAPL", period="5d", progress=False, auto_adjust=True)
            latency = int((time.time() - start) * 1000)
            results["yfinance"] = {
                "available": not df.empty,
                "latency_ms": latency,
                "rows": len(df),
                "error": "",
            }
            logger.info("yfinance 健康检查 PASS | {}ms | {} 行", latency, len(df))
        except Exception as e:
            results["yfinance"] = {"available": False, "latency_ms": 0, "error": str(e)}
            logger.warning("yfinance 健康检查 FAIL | {}", e)
    else:
        results["yfinance"] = {"available": True, "latency_ms": 0, "error": "skipped (a_share only)"}

    # efinance (备用数据源)
    try:
        start = time.time()
        import efinance as ef
        df = ef.stock.get_realtime_quotes()
        latency = int((time.time() - start) * 1000)
        results["efinance"] = {
            "available": df is not None and len(df) > 0,
            "latency_ms": latency,
            "rows": len(df) if df is not None else 0,
            "error": "",
        }
        logger.info("efinance 健康检查 PASS | {}ms | {} 行", latency, len(df) if df is not None else 0)
    except Exception as e:
        results["efinance"] = {"available": False, "latency_ms": 0, "error": str(e)}
        logger.warning("efinance 健康检查 FAIL | {}", e)

    # 东财 HTTP (兜底数据源, 纯 requests, GitHub Actions 友好)
    try:
        start = time.time()
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 10, "po": 1, "np": 1,
            "fltt": 2, "invt": 2, "fid": "f12",
            "fs": "m:1+t:2", "fields": "f12,f14",
        }
        resp = requests.get(url, params=params, timeout=15, headers={"User-Agent": random.choice(_USER_AGENTS)})
        resp.raise_for_status()
        data = resp.json()
        diff = data.get("data", {}).get("diff", {})
        latency = int((time.time() - start) * 1000)
        has_data = bool(diff)
        results["eastmoney"] = {
            "available": has_data,
            "latency_ms": latency,
            "rows": len(diff) if diff else 0,
            "error": "",
        }
        logger.info("东财HTTP 健康检查 PASS | {}ms | {} 行", latency, len(diff) if diff else 0)
    except Exception as e:
        results["eastmoney"] = {"available": False, "latency_ms": 0, "error": str(e)}
        logger.warning("东财HTTP 健康检查 FAIL | {}", e)

    return results


# ============================================================
# 4. 全局异常兜底
# ============================================================

def safe_run(func: Callable) -> Callable:
    """
    装饰器: 捕获函数内所有未处理异常, 防止流水线崩溃

    用法:
        @safe_run
        def some_risky_operation():
            ...

    异常时记录完整堆栈并返回 None, 调用方需处理 None 返回值。
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                "函数 {} 异常被安全捕获: {}\n{}",
                func.__name__, e, traceback.format_exc(),
            )
            return None
    return wrapper


def memory_cleanup():
    """
    主动触发垃圾回收, 释放内存

    在批量处理大量股票数据后调用, 防止内存泄漏累积。
    """
    collected = gc.collect()
    if collected > 0:
        logger.debug("GC 回收 {} 个对象", collected)


# ============================================================
# 5. 磁盘空间检查
# ============================================================

def check_disk_space(path: str = ".", min_mb: int = 100) -> bool:
    """
    检查磁盘剩余空间是否充足

    Args:
        path: 检查路径
        min_mb: 最低剩余空间 (MB)

    Returns:
        True=空间充足, False=空间不足
    """
    try:
        usage = shutil.disk_usage(path)
        free_mb = usage.free / 1024 / 1024
        if free_mb < min_mb:
            logger.warning(
                "磁盘空间不足 | 剩余 {:.0f}MB < 最低要求 {}MB",
                free_mb, min_mb,
            )
            return False
        return True
    except Exception as e:
        logger.debug("磁盘空间检查失败: {}", e)
        return True  # 检查失败不阻塞运行
