# 主升浪加速行情自动扫描与决策 Bot

> 轻量级 · 零成本 · 24/7 无人值守的 A股/美股主升浪加速行情自动扫描系统

## 简介

基于 GitHub 开源生态构建的三段式流水线量化扫描系统，通过四维硬核动量模型精准识别主升浪加速行情，并通过多渠道 Webhook 推送含估值锚定和买卖决策手令的结构化卡片。

### 核心特性

- **四维硬核动量模型** — 均线多头排列 + 放量突破 + VCP形态 + 大盘环境过滤
- **数据源自愈** — AkShare↔efinance 自动故障切换，yfinance↔AkShare 美股备用
- **决策卡片推送** — 每只牛股附带 Forward PE/PEG 估值 + 追击买点/回踩买点/止损线
- **五渠道投递** — Telegram / 钉钉 / 企业微信 / Discord / Server酱(微信) 并行推送
- **防漏报** — 指数退避重试(3次) + 死信队列持久化 + 多渠道并行任一成功即送达
- **24/7 无人值守** — 交易日自动运行、非交易日自动跳过、临时文件自动清理
- **零成本** — GitHub Actions 每月 2000 分钟免费额度，月消耗约 150 分钟

## 架构

```
Data ETL (自愈) → Quant Scanner (四维硬核动量) → Notification Bot (决策卡片)
```

详见 [架构文档](docs/ARCHITECTURE.md)

## 快速开始

### 1. 安装依赖

```bash
cd rally-scanner-bot
pip install -r requirements.txt
```

### 2. 配置通知 Token

```bash
cp configs/config.example.env configs/.env
# 编辑 .env, 填入至少一个通知渠道的 Token
```

### 3. 运行扫描

```bash
cd src

# 正常运行 (交易日自动判断)
python main.py

# 强制运行 (忽略交易日判断)
python main.py --force

# 仅扫描不推送 (测试用)
python main.py --dry-run

# 数据源健康检查
python main.py --health-check

# 仅清理临时文件
python main.py --cleanup-only
```

## 部署方案

### 方案 A: GitHub Actions (零成本推荐)

1. **Fork 或推送** 本项目到你的 GitHub 仓库

2. **配置 Secrets**: 仓库 Settings → Secrets and variables → Actions → New repository secret

   | Secret 名称 | 说明 | 必填 |
   |-------------|------|------|
   | `TELEGRAM_BOT_TOKEN` | Telegram Bot Token | 至少配一个 |
   | `TELEGRAM_CHAT_ID` | Telegram Chat ID | |
   | `DINGTALK_WEBHOOK` | 钉钉机器人 Webhook URL | |
   | `DINGTALK_SECRET` | 钉钉加签密钥 | |
   | `WECOM_WEBHOOK` | 企业微信群机器人 Webhook | |
   | `DISCORD_WEBHOOK` | Discord Webhook URL | |
   | `SC_KEY` | Server酱 Key (微信) | |
   | `PUSHPLUS_TOKEN` | PushPlus Token (微信) | |

3. **自动运行**: 每个交易日 16:00 (北京时间) 自动扫描并推送

4. **手动触发**: Actions → 选择工作流 → Run workflow → 可选市场/模式/强制运行

### 方案 B: Docker 部署 (轻量云服务器)

```bash
# 1. 准备配置
cp configs/config.example.env configs/.env
# 编辑 .env 填入 Token

# 2. 一键启动 (24/7 守护进程)
docker-compose up -d

# 3. 查看日志
docker-compose logs -f scanner

# 4. 手动触发单次扫描
docker-compose run --rm scanner python src/main.py --force

# 5. 停止
docker-compose down
```

容器内 cron 定时: 每个交易日 16:00 (北京时间) 自动运行。

### 方案 C: 本地 crontab

```bash
# 编辑 crontab
crontab -e

# 添加: 每个交易日 16:00 运行
0 16 * * 1-5 cd /path/to/rally-scanner-bot && /usr/bin/python3 src/main.py >> logs/cron.log 2>&1
```

## 环境变量

### 必须配置 (至少一个通知渠道)

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `MARKET` | `a_share` | 扫描市场: `a_share` / `us_stock` / `all` |
| `SCAN_SCOPE` | `all` | 扫描范围: `all`(全市场) / `custom`(自定义池) |
| `CUSTOM_STOCKS` | `000001,600519` | 自定义股票池 (逗号分隔) |
| `RUN_MODE` | `notify` | 运行模式: `dry_run` / `notify` |

### 通知渠道 (至少配置一个)

| 变量名 | 说明 |
|--------|------|
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token |
| `TELEGRAM_CHAT_ID` | Telegram Chat ID |
| `DINGTALK_WEBHOOK` | 钉钉机器人 Webhook URL |
| `DINGTALK_SECRET` | 钉钉加签密钥 (可选) |
| `WECOM_WEBHOOK` | 企业微信群机器人 Webhook |
| `DISCORD_WEBHOOK` | Discord Webhook URL |
| `SC_KEY` | Server酱 Key |
| `PUSHPLUS_TOKEN` | PushPlus Token |

### 策略参数 (可选调整)

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `VOLUME_SURGE_RATIO` | `2.0` | 放量倍数阈值 |
| `VCP_CONTRACTION_THRESHOLD` | `1.3` | VCP振幅收敛比 |
| `BOLLINGER_EXPLODE_RATIO` | `1.5` | 布林带开口放大倍数 |
| `CANDLE_BODY_MIN_PCT` | `3.0` | 突破大阳线最低涨幅% |
| `WEBHOOK_MAX_RETRIES` | `3` | Webhook重试次数 |
| `WEBHOOK_RETRY_DELAY` | `1.0` | 重试基础延迟(秒) |

## 四维硬核动量模型

| 维度 | 条件 | 检测目标 |
|------|------|---------|
| ① 均线完美多头排列 | Price > MA5 > MA10 > MA20 > MA60 且 MA20 > MA60 > MA120 | 趋势确认 |
| ② 动量爆发与放量破局 | 创20/60日新高 + 量比5d>2.0x 且 量比20d>2.0x | 真金白银突破 |
| ③ VCP波动率收敛后突破 | 10日振幅收敛 + 大阳线(>3%) + 布林扩张(>1.5x) | 形态确认 |
| ④ 大盘环境过滤器 | 指数>MA20做多 / <MA20>MA60谨慎 / <MA20<MA60空仓熔断 | 系统性风险防御 |

信号等级: S级(做多+四维全通) / A级(谨慎+四维全通) / B级(部分通过)

> ⚠️ 量化信号扫描结果，不构成投资建议。

## 项目结构

```
rally-scanner-bot/
├── src/
│   ├── data_etl.py          # Stage 1: 数据抓取 (自愈故障切换)
│   ├── scanner.py           # Stage 2: 四维硬核动量扫描引擎
│   ├── notifier.py          # Stage 3: 决策卡片 + 多渠道推送
│   ├── main.py              # 主流水线编排器 (24/7 自愈)
│   └── utils.py             # 自愈工具: 交易日历/清理/健康检查
├── configs/
│   └── config.example.env   # 配置模板
├── .github/workflows/
│   └── run_scanner.yml      # GitHub Actions 定时任务
├── docs/
│   └── ARCHITECTURE.md      # 架构文档
├── docker-compose.yml       # Docker 一键部署
├── docker-entrypoint.sh     # 容器入口脚本
├── Dockerfile               # Docker 构建
├── requirements.txt         # Python 依赖
├── .gitignore
└── README.md
```

## 核心开源依赖

| 库 | Stars | 用途 |
|----|-------|------|
| [akshare](https://github.com/akfamily/akshare) | 21.2k | A股数据 (主源) |
| [efinance](https://github.com/Micro-sheep/efinance) | 2.3k | A股数据 (备用源) |
| [yfinance](https://github.com/ranaroussi/yfinance) | 24.6k | 美股数据 (主源) |
| [ta](https://github.com/bukosabino/ta) | 5.1k | 技术指标计算 |
