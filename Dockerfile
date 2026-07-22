# ============================================================
# 主升浪加速行情自动扫描 Bot - Docker 单文件部署 v2.0
# ============================================================
# 方案 B: 轻量云服务器 24/7 部署
#
# 特性:
#   - python:3.10-slim 精简基础镜像 (~120MB)
#   - 容器内置 cron 定时任务
#   - 日志自动轮转 + 临时文件自动清理
#   - TZ=Asia/Shanghai 时区正确
#
# 使用方式:
#   docker-compose up -d          # 一键启动 (推荐)
#   docker build -t rally-scanner .
#   docker run --rm --env-file configs/.env rally-scanner
#
# 手动运行单次扫描:
#   docker-compose run --rm scanner python src/main.py --force
#
# 查看日志:
#   docker-compose logs -f scanner
# ============================================================

FROM python:3.10-slim

# 设置时区
ENV TZ=Asia/Shanghai
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# 安装系统依赖
# - cron: 容器内定时任务
# - curl: 健康检查
# - tzdata: 时区支持
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ cron curl tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装 (利用 Docker 层缓存)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY src/ ./src/
COPY configs/ ./configs/

# 创建日志和临时文件目录
RUN mkdir -p /app/logs /app/logs/dead_letter /app/data

# ============================================================
# 容器内 Cron 定时任务配置
# ============================================================
# A股盘后扫描: 每个交易日 20:00 (北京时间)
# 0 12 * * 1-5 = UTC 12:00 = 北京时间 20:00
RUN echo '0 12 * * 1-5 cd /app && /usr/local/bin/python src/main.py >> /app/logs/cron.log 2>&1' > /etc/cron.d/scanner-cron && \
    chmod 0644 /etc/cron.d/scanner-cron && \
    crontab /etc/cron.d/scanner-cron && \
    touch /app/logs/cron.log

# 健康检查: 每 6 小时检查一次磁盘空间
RUN echo '0 */6 * * * df -h /app | tail -1 >> /app/logs/disk_check.log 2>&1' >> /etc/cron.d/scanner-cron

# ============================================================
# Entrypoint: cron 前台运行 + 单次扫描选项
# ============================================================
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

# 默认: 启动 cron 守护进程 (24/7 模式)
# 可通过 docker run 参数覆盖: python src/main.py --force
CMD ["cron"]
