#!/bin/bash
# ============================================================
# Docker Entrypoint - 主升浪扫描 Bot 容器入口
# ============================================================
# 功能:
#   1. 如果参数是 python 命令 → 直接执行单次扫描
#   2. 如果参数是 cron (默认) → 启动 24/7 定时任务守护进程
#   3. 启动前自动清理 7 天前的临时文件
#   4. 启动前检查磁盘空间
# ============================================================

set -e

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Rally Scanner Bot 容器启动..."
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 时区: $(cat /etc/timezone 2>/dev/null || echo 'UTC')"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 工作目录: /app"

# 启动前清理临时文件 (保留最近 7 天)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 启动前清理临时文件..."
find /app -maxdepth 3 \( -name "*.csv" -o -name "*.tmp" -o -name "*.pkl" -o -name "*.parquet" \) -mtime +7 -delete 2>/dev/null || true
find /app/logs -name "*.log" -mtime +14 -delete 2>/dev/null || true

# 磁盘空间检查
FREE_MB=$(df -m /app | awk 'NR==2 {print $4}')
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 磁盘剩余空间: ${FREE_MB}MB"

if [ "$FREE_MB" -lt 200 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 磁盘空间不足 200MB, 执行强制清理..."
    find /app -maxdepth 3 \( -name "*.csv" -o -name "*.tmp" -o -name "*.pkl" \) -mtime +1 -delete 2>/dev/null || true
fi

# 判断运行模式
if [ "$1" = "cron" ]; then
    # ============================================================
    # 24/7 守护进程模式: 启动 cron
    # ============================================================
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 启动 24/7 定时任务守护进程"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 定时计划: 每个交易日 16:00 (北京时间) 自动扫描"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cron 配置:"
    cat /etc/cron.d/scanner-cron
    echo ""

    # 打印 cron 日志到 stdout (便于 docker logs 查看)
    print_cron_log() {
        if [ -f /app/logs/cron.log ]; then
            tail -f /app/logs/cron.log
        else
            # 等待日志文件创建
            touch /app/logs/cron.log
            tail -f /app/logs/cron.log
        fi
    }

    # 启动 cron 守护进程 (前台运行, 尾随日志)
    cron -f &
    CRON_PID=$!

    # 等待一秒让 cron 启动, 然后尾随日志
    sleep 1
    print_cron_log &

    # 等待 cron 进程
    wait $CRON_PID

elif [ "$1" = "python" ] || [ "$1" = "python3" ]; then
    # ============================================================
    # 单次运行模式: 直接执行 Python 命令
    # ============================================================
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 单次运行模式: $@"
    exec "$@"

else
    # ============================================================
    # 其他命令: 直接执行
    # ============================================================
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 执行命令: $@"
    exec "$@"
fi
