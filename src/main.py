"""
主升浪加速行情自动扫描与决策 Bot - 主流水线编排器 v4.0
================================================
四段式流水线:
  1. Data ETL (自愈 + 多数据源故障切换)
  2. Momentum Scanner (四维硬核动量检测)
  3. Multi-Factor Scorer (六因子评分 + 交易 Playbook)
  4. Report Generator + Notification Bot (报告生成 + 多渠道推送)

v4.0 新增:
  - 基于踏空组合(ZH063783)的六因子评分模型 (行业40%+市值20%+趋势15%+动量10%+质量10%+仓位5%)
  - HTML/Excel 双格式报告自动生成
  - 交易 Playbook (入场/止损/仓位/出场条件)
  - 因子雷达图可视化
  - 四维动量 + 六因子双评分交叉验证

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
from factors import MultiFactorScorer
from report import (
    generate_html_report,
    generate_excel_report,
    generate_markdown_summary,
    print_to_console,
)
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
    执行完整的四段式流水线

    Args:
        force: True=强制运行 (忽略交易日判断)
        dry_run: True=仅扫描不推送通知
    """

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("主升浪加速行情自动扫描系统 v4.0 启动")
    logger.info("四维动量扫描 + 六因子评分 + 双模型交叉验证 + 报告生成")
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
    min_change_pct = float(os.getenv("MIN_CHANGE_PCT", "5.0"))
    scan_zt_only = os.getenv("SCAN_ZT_ONLY", "1").lower() in ("1", "true", "yes", "on")

    scan_mode = "涨停股" if scan_zt_only else f"涨幅>{min_change_pct}%"
    logger.info("配置加载 | 市场={} 范围={} 模式={} 强制={} 扫描模式={}", market, scan_scope, run_mode, force, scan_mode)

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
            stock_list = etl.get_stock_list(min_change_pct=min_change_pct, zt_only=scan_zt_only)
        except Exception as e:
            logger.error("获取股票列表时发生异常: {}", e)
            stock_list = []

    # 数据源连接失败时优雅退出, 不崩溃
    if not stock_list:
        logger.error("数据源连接失败或无达标股票，本次扫描跳过")
        if not dry_run:
            _send_data_failure_alert(market)
        return None

    logger.info("{}股票: {} 只 (已剔除科创板/北交所/ST)", scan_mode, len(stock_list))

    # 获取 120 日数据 (MA60 需要 60 日 + VCP 需要约 50 日窗口 + 余量)
    stock_data = etl.batch_fetch(stock_list, days=120)

    if not stock_data:
        logger.error("数据源连接失败，本次扫描跳过")
        if not dry_run:
            _send_data_failure_alert(market)
        return None

    # ============================================================
    # Stage 2a: Momentum Scanner - 四维硬核动量扫描
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 2a: Momentum Scanner - 四维硬核动量扫描")
    logger.info("-" * 60)

    momentum_results = scanner.scan_batch(stock_data, market_env=market_env)

    # 统计
    ms_count = sum(1 for r in momentum_results if r.level == "S")
    ma_count = sum(1 for r in momentum_results if r.level == "A")
    mb_count = sum(1 for r in momentum_results if r.level == "B")
    logger.info(
        "动量扫描结果 | S级={} A级={} B级={} 总命中={}",
        ms_count, ma_count, mb_count, len(momentum_results),
    )

    # ============================================================
    # Stage 2b: Multi-Factor Scorer - 六因子评分模型
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 2b: Multi-Factor Scorer - 六因子评分模型")
    logger.info("-" * 60)

    factor_scorer = MultiFactorScorer()

    # 判断扫描范围: 动量命中的优先评分, 其余股票量太大则只在涨停股基础上评分
    if scan_zt_only or len(stock_data) <= 200:
        # 涨停股模式或数量不多: 对所有已获取 K 线的股票评分
        factor_results = factor_scorer.score_batch(stock_data)
    else:
        # 数量多时: 只对动量命中和涨幅 > 5% 的股票评分
        target_codes = set()
        for r in momentum_results:
            target_codes.add(r.code)
        # 如果动量命中太少，至少取 Top100 涨幅最大的
        if len(target_codes) < 50:
            # 补充一些涨幅较大的股票
            sorted_codes = sorted(
                stock_data.keys(),
                key=lambda c: float(stock_data[c]["data"]["close"].iloc[-1])
                / float(stock_data[c]["data"]["close"].iloc[-2]) - 1
                if len(stock_data[c]["data"]) >= 2 else 0,
                reverse=True,
            )
            for code in sorted_codes:
                if len(target_codes) >= 100:
                    break
                target_codes.add(code)

        target_data = {c: stock_data[c] for c in target_codes if c in stock_data}
        logger.info("精准评分: 动量命中+涨幅Top股票共 {} 只", len(target_data))
        factor_results = factor_scorer.score_batch(target_data)

    # 统计
    fs_count = sum(1 for r in factor_results if r.level == "S")
    fa_count = sum(1 for r in factor_results if r.level == "A")
    fb_count = sum(1 for r in factor_results if r.level == "B")
    fc_count = sum(1 for r in factor_results if r.level == "C")
    logger.info(
        "因子评分结果 | S级={} A级={} B级={} C级={} 总评估={}",
        fs_count, fa_count, fb_count, fc_count, len(factor_results),
    )

    # 主动释放内存
    del stock_data
    memory_cleanup()

    # ============================================================
    # Stage 3: Report Generator - HTML + Excel 报告
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 3: Report Generator - 报告生成")
    logger.info("-" * 60)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(project_root, "outputs")

    html_path = generate_html_report(
        factor_results, momentum_results, market_env, output_dir
    )
    excel_path = generate_excel_report(factor_results, market_env, output_dir)
    markdown_summary = generate_markdown_summary(factor_results, market_env)

    if html_path:
        logger.info("HTML 报告: {}", html_path)
    if excel_path:
        logger.info("Excel 报表: {}", excel_path)

    # 控制台输出
    print_to_console(factor_results, market_env)

    # ============================================================
    # Stage 4: Notification Bot - 多渠道推送
    # ============================================================
    logger.info("-" * 60)
    logger.info("Stage 4: Notification Bot - 消息推送")
    logger.info("-" * 60)

    notifier = Notifier()

    # 推送动量扫描结果 (带决策卡片)
    notifier.notify(momentum_results, dry_run=dry_run, market_env=market_env, market=market)

    # 如果 PushPlus 可用，额外推送因子评分摘要
    if not dry_run and notifier.pushplus_token:
        try:
            title = f"选股日报 {fa_count}S{fa_count}A{fb_count}B"
            notifier._send_pushplus(title, markdown_summary)
            logger.info("因子评分摘要已推送至 PushPlus")
        except Exception as e:
            logger.warning("因子评分摘要推送失败: {}", e)

    # ============================================================
    # Post-Run: 临时文件清理
    # ============================================================
    logger.info("-" * 60)
    logger.info("Post-Run: 临时文件自动清理")
    logger.info("-" * 60)

    cleanup_temp_files(
        base_dir=project_root,
        max_age_days=7,
        keep_patterns=["configs/*", "logs/dead_letter/**", "outputs/**"],
    )

    # ============================================================
    # 流水线完成
    # ============================================================
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(
        "流水线执行完成 | 耗时 {:.1f}s | 动量命中 {} 只 | 因子评估 {} 只",
        elapsed, len(momentum_results), len(factor_results),
    )
    logger.info("=" * 60)

    return {"momentum": momentum_results, "factor": factor_results}


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
        description="主升浪加速行情自动扫描与决策 Bot v4.0",
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
