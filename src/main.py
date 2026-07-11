"""
主升浪加速行情自动扫描与决策 Bot - 主流水线编排器 v3.0
================================================
三段式流水线: Data ETL (自愈) → Quant Scanner (四维硬核动量) → Notification Bot

v3.0 新增 -- 24/7 无人值守自愈:
  1. 交易日历判断 -- 非交易日自动跳过, 不浪费资源
  2. 数据源故障切换 -- 主源失败自动切换备用源
  3. 临时文件自动清理 -- 每次运行后清理过期 CSV/JSON/日志
  4. 全局异常兜底 -- 单股异常不崩溃整条流水线
  5. 内存自动回收 -- 批量处理后主动 GC
  6. 磁盘空间检查 -- 空间不足时强制清理

部署方案:
  A. GitHub Actions Cron (零成本推荐)
     → .github/workflows/run_scanner.yml 自动触发
  B. Docker + crontab (轻量云服务器)
     → docker-compose up -d 一键启动
  C. 本地 crontab
     → crontab -e 添加定时任务

使用方式:
  python src/main.py                    # 正常运行
  python src/main.py --force            # 强制运行 (忽略交易日判断)
  python src/main.py --dry-run          # 仅扫描不推送
  python src/main.py --cleanup-only     # 仅清理临时文件
  python src/main.py --health-check     # 数据源健康检查
"""

import os
import sys
import time
import argparse
from loguru import logger
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "configs", ".env"))

# 添加 src 到路径
sys.path.insert(0, os.path.dirname(__file__))

from data_etl import DataETL
from scanner import RallyScanner, MarketEnvironment
from notifier import Notifier
from utils import (
    TradingCalendar,
    cleanup_temp_files,
    health_check,
    memory_cleanup,
    check_disk_space,
    safe_run,
)


def setup_logging():
    """配置日志: 同时输出到控制台和文件"""
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "scanner_{time:YYYY-MM-DD}.log")

    logger.remove()  # 移除默认 handler
    logger.add(
        sys.stderr,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    )
    logger.add(
        log_file,
        level="DEBUG",
        rotation="00:00",      # 每天 0 点轮转
        retention="14 days",   # 保留 14 天
        compression="zip",     # 旧日志压缩
        encoding="utf-8",
    )


@safe_run
def run_pipeline(force: bool = False, dry_run: bool = False):
    """
    执行完整的三段式流水线

    Args:
        force: True=强制运行 (忽略交易日判断)
        dry_run: True=仅扫描不推送通知
    """

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("主升浪加速行情自动扫描系统 v3.0 启动")
    logger.info("四维硬核动量 + 数据源自愈 + 自动清理")
    logger.info("=" * 60)

    # 读取配置
    market = os.getenv("MARKET", "a_share")
    scan_scope = os.getenv("SCAN_SCOPE", "all")
    custom_stocks = os.getenv("CUSTOM_STOCKS", "").split(",")
    custom_stocks = [s.strip() for s in custom_stocks if s.strip()]
    run_mode = os.getenv("RUN_MODE", "dry_run")
    if dry_run:
        run_mode = "dry_run"
    dry_run = run_mode == "dry_run"

    # 策略参数
    volume_surge_ratio = float(os.getenv("VOLUME_SURGE_RATIO", "2.0"))
    vcp_contraction_threshold = float(os.getenv("VCP_CONTRACTION_THRESHOLD", "1.3"))
    bollinger_explode_ratio = float(os.getenv("BOLLINGER_EXPLODE_RATIO", "1.5"))
    candle_body_min_pct = float(os.getenv("CANDLE_BODY_MIN_PCT", "3.0"))

    logger.info("配置加载 | 市场={} 范围={} 模式={} 强制={}", market, scan_scope, run_mode, force)

    # ============================================================
    # Pre-Flight: 交易日判断 + 磁盘空间检查
    # ============================================================
    if not force:
        if not TradingCalendar.should_run(market):
            logger.info("今天非交易日 (market={}), 自动跳过", market)
            return None
    else:
        logger.info("强制运行模式, 跳过交易日判断")

    project_root = os.path.dirname(__file__)
    if not check_disk_space(project_root, min_mb=200):
        logger.warning("磁盘空间不足, 执行强制清理...")
        cleanup_temp_files(project_root, max_age_days=3)

    # ============================================================
    # Stage 1: Data ETL - 数据抓取与清洗 (带故障切换)
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 1: Data ETL - 数据抓取与清洗 (自愈模式)")
    logger.info("-" * 60)

    etl = DataETL(market=market)

    # 1a. 获取大盘指数数据 (Beta Filter 用, 需要至少 200 日用于 MA120)
    logger.info("获取大盘指数数据 (Beta Filter)...")
    index_df = etl.get_market_index(days=200)
    if index_df is None or index_df.empty:
        logger.warning("大盘指数数据获取失败 (所有数据源均不可用), 将跳过 Beta 过滤")
        market_env = None
        scanner = RallyScanner(
            volume_surge_ratio=volume_surge_ratio,
            vcp_contraction_threshold=vcp_contraction_threshold,
            bollinger_explode_ratio=bollinger_explode_ratio,
            candle_body_min_pct=candle_body_min_pct,
        )
    else:
        scanner = RallyScanner(
            volume_surge_ratio=volume_surge_ratio,
            vcp_contraction_threshold=vcp_contraction_threshold,
            bollinger_explode_ratio=bollinger_explode_ratio,
            candle_body_min_pct=candle_body_min_pct,
        )
        index_name = "沪深300" if market in ("a_share", "all") else "标普500"
        market_env = scanner.analyze_market_index(index_df, index_name)

    # 1b. 获取个股数据
    if scan_scope == "custom" and custom_stocks:
        stock_list = []
        for code in custom_stocks:
            if market in ("a_share", "all"):
                stock_list.append(("a_share", code, code))
            else:
                stock_list.append(("us_stock", code, code))
        logger.info("使用自定义股票池: {} 只", len(stock_list))
    else:
        try:
            stock_list = etl.get_stock_list()
        except Exception as e:
            logger.error("获取股票列表时发生异常: {}", e)
            stock_list = []

    # 数据源连接失败时优雅退出, 不崩溃
    if not stock_list:
        logger.error("数据源连接失败，本次扫描跳过")
        if not dry_run:
            _send_data_failure_alert(market)
        return None

    logger.info("全市场扫描: {} 只股票", len(stock_list))

    # 获取 200 日数据 (MA120 需要 120 日 + 余量)
    stock_data = etl.batch_fetch(stock_list, days=200, rate_limit=0.15)

    if not stock_data:
        logger.error("数据源连接失败，本次扫描跳过")
        if not dry_run:
            _send_data_failure_alert(market)
        return None

    # ============================================================
    # Stage 2: Quant Scanner - 四维硬核动量扫描
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 2: Quant Scanner - 四维硬核动量扫描")
    logger.info("-" * 60)

    results = scanner.scan_batch(stock_data, market_env=market_env)

    # 统计
    s_count = sum(1 for r in results if r.level == "S")
    a_count = sum(1 for r in results if r.level == "A")
    b_count = sum(1 for r in results if r.level == "B")
    logger.info(
        "扫描结果 | S级={} A级={} B级={} 总命中={}",
        s_count, a_count, b_count, len(results),
    )

    # 主动释放内存
    del stock_data
    memory_cleanup()

    # ============================================================
    # Stage 3: Notification Bot - 消息决策流发送
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 3: Notification Bot - 消息推送")
    logger.info("-" * 60)

    notifier = Notifier()
    notifier.notify(results, dry_run=dry_run, market_env=market_env, market=market)

    # ============================================================
    # Post-Run: 临时文件清理
    # ============================================================
    logger.info("-" * 60)
    logger.info("Post-Run: 临时文件自动清理")
    logger.info("-" * 60)

    cleanup_temp_files(
        base_dir=project_root,
        max_age_days=7,
        keep_patterns=["configs/*", "logs/dead_letter/**"],
    )

    # ============================================================
    # 流水线完成
    # ============================================================
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("流水线执行完成 | 耗时 {:.1f}s | 命中 {} 只", elapsed, len(results))
    logger.info("=" * 60)

    return results


def _send_data_failure_alert(market: str):
    """数据源全部不可用时发送告警"""
    try:
        notifier = Notifier()
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        alert = (
            f"**[数据源故障告警]**\n\n"
            f"时间: {now}\n"
            f"市场: {market}\n\n"
            f"所有数据源 (AkShare + efinance) 均不可用, "
            f"已重试 3 次仍失败。\n"
            f"本次扫描已跳过, 等待下次定时触发自动恢复。\n"
            f"请检查网络连接或数据源 API 状态。"
        )
        if notifier.tg_token and notifier.tg_chat_id:
            notifier._send_telegram(alert)
        if notifier.dingtalk_webhook:
            notifier._send_dingtalk("数据源故障告警", alert)
        if notifier.wecom_webhook:
            notifier._send_wecom(alert)
        if notifier.discord_webhook:
            notifier._send_discord([{
                "title": "⚠️ 数据源故障告警",
                "color": 0xFF0000,
                "description": alert,
            }])
    except Exception as e:
        logger.error("发送数据源故障告警失败: {}", e)


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(
        description="主升浪加速行情自动扫描与决策 Bot v3.0",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="强制运行 (忽略交易日判断)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅扫描不推送通知",
    )
    parser.add_argument(
        "--cleanup-only", action="store_true",
        help="仅执行临时文件清理, 不运行扫描",
    )
    parser.add_argument(
        "--health-check", action="store_true",
        help="执行数据源健康检查, 不运行扫描",
    )
    args = parser.parse_args()

    setup_logging()

    if args.health_check:
        market = os.getenv("MARKET", "a_share")
        logger.info("执行数据源健康检查 (market={})...", market)
        results = health_check(market)
        print("\n=== 数据源健康检查结果 ===")
        for source, status in results.items():
            status_str = "PASS" if status.get("available") else "FAIL"
            latency = status.get("latency_ms", 0)
            print(f"  {source:12s} | {status_str} | {latency}ms | {status.get('error', '')}")
        return

    if args.cleanup_only:
        project_root = os.path.dirname(__file__)
        logger.info("执行临时文件清理...")
        stats = cleanup_temp_files(project_root, max_age_days=7, dry_run=False)
        print(f"\n清理完成: 扫描 {stats['scanned']} 文件, 删除 {stats['deleted']} 文件, 释放 {stats['freed_mb']}MB")
        return

    run_pipeline(force=args.force, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
