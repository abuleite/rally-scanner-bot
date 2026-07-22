"""
报告生成模块 v1.1 - 选股结果的结构化输出
===========================================

功能:
  1. HTML 报告: 含评分总览表、因子明细、雷达图 (纯 SVG)、行业×因子热度矩阵、交易 playbook
  2. Excel 报表: 多 sheet (评分明细/交易计划/行业分布/市场环境)
  3. Markdown 摘要: 适合微信/钉钉推送
  4. 控制台美化输出

输出目录: outputs/{date}/
"""

import os
import json
from datetime import datetime
from typing import List, Optional
from loguru import logger

# 延迟导入 pandas，仅在需要生成 Excel 时加载
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ============================================================
# 颜色与样式常量
# ============================================================

LEVEL_COLORS = {
    "S": ("#FF4500", "#FFF0E8", "🔥 重磅推荐"),
    "A": ("#FF8C00", "#FFF3E0", "🚀 强烈关注"),
    "B": ("#4169E1", "#E8F0FF", "📊 候选观察"),
    "C": ("#808080", "#F5F5F5", "📉 暂时观望"),
}

FACTOR_NAMES = {
    "industry_score": "行业适配",
    "marketcap_score": "市值适配",
    "trend_score": "趋势强度",
    "momentum_score": "动量质量",
    "quality_score": "质量基本面",
    "position_score": "仓位合理",
}

FACTOR_WEIGHTS = {
    "industry_score": 0.40,
    "marketcap_score": 0.20,
    "trend_score": 0.15,
    "momentum_score": 0.10,
    "quality_score": 0.10,
    "position_score": 0.05,
}


# ============================================================
# 雷达图 (纯 SVG 实现，零依赖)
# ============================================================

def _draw_radar_chart_svg(scores: dict, size: int = 280) -> str:
    """
    绘制六因子雷达图 (纯 SVG)

    Args:
        scores: {"industry_score": 85, "marketcap_score": 72, ...}
        size: SVG 尺寸

    Returns:
        SVG 字符串
    """
    import math

    factors = ["industry_score", "marketcap_score", "trend_score",
               "momentum_score", "quality_score", "position_score"]
    labels = [FACTOR_NAMES[f] for f in factors]
    n = len(factors)
    cx = size // 2
    cy = size // 2
    r_max = size // 2 - 40

    # 计算六边形顶点
    angles = [math.pi / 2 + 2 * math.pi * i / n for i in range(n)]
    vertices = [(cx + r * math.cos(a), cy - r * math.sin(a)) for a in angles]

    svg_parts = [f'<svg width="{size}" height="{size}" xmlns="http://www.w3.org/2000/svg">']

    # 背景网格 (3 层: 33%, 66%, 100%)
    for level in [0.33, 0.66, 1.0]:
        r = r_max * level
        points = []
        for a in angles:
            x = cx + r * math.cos(a)
            y = cy - r * math.sin(a)
            points.append(f"{x:.1f},{y:.1f}")
        svg_parts.append(
            f'<polygon points="{" ".join(points)}" '
            f'fill="none" stroke="#e0e0e0" stroke-width="1" opacity="0.5"/>'
        )

    # 轴线
    for a in angles:
        x = cx + r_max * math.cos(a)
        y = cy - r_max * math.sin(a)
        svg_parts.append(
            f'<line x1="{cx}" y1="{cy}" x2="{x:.1f}" y2="{y:.1f}" '
            f'stroke="#ddd" stroke-width="0.5"/>'
        )

    # 数据多边形
    data_points = []
    for i, f in enumerate(factors):
        val = scores.get(f, 0) / 100.0
        r = r_max * max(0, min(1, val))
        x = cx + r * math.cos(angles[i])
        y = cy - r * math.sin(angles[i])
        data_points.append(f"{x:.1f},{y:.1f}")

    svg_parts.append(
        f'<polygon points="{" ".join(data_points)}" '
        f'fill="#FF6B35" fill-opacity="0.25" stroke="#FF4500" stroke-width="2"/>'
    )

    # 数据点
    for i, f in enumerate(factors):
        val = scores.get(f, 0) / 100.0
        r = r_max * max(0, min(1, val))
        x = cx + r * math.cos(angles[i])
        y = cy - r * math.sin(angles[i])
        svg_parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#FF4500"/>'
        )

    # 标签
    for i, (label, a) in enumerate(zip(labels, angles)):
        r_label = r_max + 18
        x = cx + r_label * math.cos(a)
        y = cy - r_label * math.sin(a)
        score_val = int(scores.get(factors[i], 0))
        svg_parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="middle" '
            f'dominant-baseline="middle" font-size="11" fill="#333" '
            f'font-family="sans-serif">{label}<tspan fill="#FF4500" '
            f'font-weight="bold">({score_val})</tspan></text>'
        )

    svg_parts.append('</svg>')
    return "\n".join(svg_parts)


# ============================================================
# 热力图 (纯 SVG 实现，零依赖)
# ============================================================

def _draw_heatmap_svg(
    industry_scores: dict,
    width: int = 680,
    cell_w: int = 78,
    cell_h: int = 36,
) -> str:
    """
    绘制 行业 × 因子得分 热力图矩阵

    Args:
        industry_scores: {"医药制造": {"行业适配": 85, "市值适配": 72, ...}, ...}
        width: SVG 总宽度
        cell_w: 每个单元格宽度
        cell_h: 每个单元格高度

    Returns:
        SVG 字符串
    """
    factor_keys = ["行业适配", "市值适配", "趋势强度", "动量质量", "质量基本面", "仓位合理"]
    industries = list(industry_scores.keys())

    if not industries:
        return '<svg width="400" height="80"><text x="20" y="40" fill="#999">无数据</text></svg>'

    n_factors = len(factor_keys)
    n_industry = len(industries)

    # 布局参数
    left_margin = 70
    top_margin = 50
    gap = 2
    colorbar_w = 20
    colorbar_x = left_margin + n_factors * (cell_w + gap) + 30
    total_w = colorbar_x + colorbar_w + 40
    total_h = top_margin + n_industry * (cell_h + gap) + 40

    # 颜色映射: 低分→浅灰, 高分→深红 (中国红涨绿跌惯例, 高分=好=红色)
    def score_color(score: float) -> str:
        t = max(0, min(100, score)) / 100.0
        r = int(245 - t * 155)
        g = int(245 - t * 200)
        b = int(245 - t * 210)
        return f"rgb({r},{g},{b})"

    def score_text_color(score: float) -> str:
        return "#fff" if score >= 55 else "#333"

    svg = [
        f'<svg width="{total_w}" height="{total_h}" '
        f'xmlns="http://www.w3.org/2000/svg">',
    ]

    # 因子列标签
    for fi, fname in enumerate(factor_keys):
        x = left_margin + fi * (cell_w + gap) + cell_w / 2
        svg.append(
            f'<text x="{x:.0f}" y="28" text-anchor="middle" '
            f'font-size="11" fill="#4a5568" font-weight="600" '
            f'font-family="sans-serif">{fname}</text>'
        )

    # 行业行 + 热度格
    for ii, ind_name in enumerate(industries):
        y = top_margin + ii * (cell_h + gap)
        # 行业名标签
        svg.append(
            f'<text x="{left_margin - 8}" y="{y + cell_h / 2 + 4}" '
            f'text-anchor="end" font-size="12" fill="#2d3748" '
            f'font-family="sans-serif">{ind_name}</text>'
        )

        # 因子得分格
        for fi, fname in enumerate(factor_keys):
            score = industry_scores.get(ind_name, {}).get(fname, 0)
            x = left_margin + fi * (cell_w + gap)
            color = score_color(score)

            svg.append(
                f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" '
                f'rx="4" fill="{color}"/>'
            )
            svg.append(
                f'<text x="{x + cell_w / 2:.0f}" y="{y + cell_h / 2 + 4:.0f}" '
                f'text-anchor="middle" font-size="12" font-weight="600" '
                f'fill="{score_text_color(score)}" '
                f'font-family="sans-serif">{score:.0f}</text>'
            )

    # 颜色条 (Colorbar)
    bar_h = total_h - top_margin - 30
    bar_y = top_margin
    steps = 20
    step_h = bar_h / steps
    for s in range(steps):
        val = 100 - s * 5  # 100 → 0
        c = score_color(val)
        y = bar_y + s * step_h
        svg.append(
            f'<rect x="{colorbar_x}" y="{y:.1f}" width="{colorbar_w}" '
            f'height="{step_h:.1f}" fill="{c}"/>'
        )

    # 颜色条标签
    for val in [100, 75, 50, 25, 0]:
        frac = (100 - val) / 100
        y = bar_y + frac * bar_h
        svg.append(
            f'<text x="{colorbar_x + colorbar_w + 6}" y="{y + 4:.1f}" '
            f'font-size="10" fill="#718096" font-family="sans-serif">{val}</text>'
        )

    # 颜色条标题
    svg.append(
        f'<text x="{colorbar_x + colorbar_w / 2:.0f}" y="{bar_y - 8}" '
        f'text-anchor="middle" font-size="10" fill="#a0aec0" '
        f'font-family="sans-serif">分</text>'
    )

    svg.append('</svg>')
    return "\n".join(svg)


# ============================================================
# HTML 报告生成
# ============================================================

def generate_html_report(
    factor_results: list,
    scanner_results: Optional[list] = None,
    market_env=None,
    output_dir: str = "outputs",
) -> str:
    """
    生成 HTML 选股报告

    内容:
      1. 报告头: 时间/市场环境/统计
      2. 评分总览表: 综合排名
      3. 因子明细表: 六因子得分
      4. 行业 × 因子热度矩阵: 纯 SVG 热力图
      5. 交易 playbook 表
      6. 行业分布统计
      7. Top 3 雷达图

    Args:
        factor_results: [FactorScore] 多因子评分结果
        scanner_results: [SignalResult] 动量扫描结果 (可选)
        market_env: 大盘环境分析结果
        output_dir: 输出目录

    Returns:
        HTML 文件路径
    """
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    # 创建输出目录
    out_path = os.path.join(output_dir, date_str)
    os.makedirs(out_path, exist_ok=True)

    # 统计
    s_count = sum(1 for r in factor_results if r.level == "S")
    a_count = sum(1 for r in factor_results if r.level == "A")
    b_count = sum(1 for r in factor_results if r.level == "B")
    c_count = sum(1 for r in factor_results if r.level == "C")

    # 市场环境
    market_info = ""
    if market_env:
        regime_map = {"BULLISH": "做多环境", "CAUTION": "谨慎环境", "BEARISH": "空仓熔断"}
        regime_text = regime_map.get(market_env.regime.value if hasattr(market_env.regime, 'value') else str(market_env.regime), "未知")
        market_info = f"""
        <div class="market-info">
            <span>大盘: {market_env.index_name}</span>
            <span>指数: {market_env.index_price}</span>
            <span>MA20: {market_env.index_ma20}</span>
            <span>MA60: {market_env.index_ma60}</span>
            <span class="regime">{regime_text}</span>
            {'<span class="warning">[空仓熔断生效]</span>' if market_env.circuit_breaker else ''}
        </div>"""

    # ---- 评分总览表 ----
    overview_rows = []
    for i, r in enumerate(factor_results[:30], 1):
        level_color = LEVEL_COLORS.get(r.level, ("#808080", "#F5F5F5", "?"))
        entry_str = f"{r.entry_price:.2f}" if r.entry_price > 0 else "-"
        stop_str = f"{r.stop_loss:.2f}" if r.stop_loss > 0 else "-"
        pe_str = f"{r.pe:.1f}" if r.pe > 0 else "-"
        cap_str = f"{r.market_cap:.0f}亿" if r.market_cap > 0 else "-"

        overview_rows.append(f"""
        <tr style="background-color: {level_color[1]}">
            <td>{i}</td>
            <td><span class="level-badge" style="background:{level_color[0]}">{r.level}</span></td>
            <td class="code">{r.code}</td>
            <td>{r.name}</td>
            <td class="num">{r.total_score:.0f}</td>
            <td class="num">{r.current_price:.2f}</td>
            <td class="num">{r.change_pct:+.2f}%</td>
            <td class="num">{r.industry_score:.0f}</td>
            <td class="num">{r.marketcap_score:.0f}</td>
            <td class="num">{r.trend_score:.0f}</td>
            <td class="num">{r.momentum_score:.0f}</td>
            <td class="num">{r.quality_score:.0f}</td>
            <td class="num">{r.position_score:.0f}</td>
            <td>{r.industry_name}</td>
            <td>{pe_str}</td>
            <td>{cap_str}</td>
        </tr>""")

    # ---- 交易 playbook 表 ----
    playbook_rows = []
    for i, r in enumerate(factor_results[:30], 1):
        if r.level in ("S", "A", "B"):
            playbook_rows.append(f"""
            <tr>
                <td>{i}</td>
                <td><span class="level-badge" style="background:{LEVEL_COLORS.get(r.level,('#808080','','?'))[0]}">{r.level}</span></td>
                <td class="code">{r.code}</td>
                <td>{r.name}</td>
                <td class="num buy">{r.entry_price:.2f}</td>
                <td class="num stop">{r.stop_loss:.2f}</td>
                <td class="num">{r.position_pct:.0f}%</td>
                <td>{r.playbook_note}</td>
                <td>{r.exit_condition}</td>
            </tr>""")

    # ---- 行业 × 因子热力图 ----
    industry_factor_scores = {}
    for r in factor_results:
        ind = r.industry_name or "其他"
        if ind not in industry_factor_scores:
            industry_factor_scores[ind] = {
                "行业适配": [], "市值适配": [], "趋势强度": [],
                "动量质量": [], "质量基本面": [], "仓位合理": [],
            }
        industry_factor_scores[ind]["行业适配"].append(r.industry_score)
        industry_factor_scores[ind]["市值适配"].append(r.marketcap_score)
        industry_factor_scores[ind]["趋势强度"].append(r.trend_score)
        industry_factor_scores[ind]["动量质量"].append(r.momentum_score)
        industry_factor_scores[ind]["质量基本面"].append(r.quality_score)
        industry_factor_scores[ind]["仓位合理"].append(r.position_score)

    # 计算每个行业各因子平均分
    industry_avg_scores = {}
    for ind, scores in industry_factor_scores.items():
        industry_avg_scores[ind] = {
            k: sum(v) / len(v) if v else 0 for k, v in scores.items()
        }

    # 按命中数量排序，取 Top 10 行业
    industry_sorted = sorted(
        industry_avg_scores.items(),
        key=lambda x: len(industry_factor_scores.get(x[0], {}).get("行业适配", [])),
        reverse=True,
    )[:10]
    heatmap_data = dict(industry_sorted)
    heatmap_svg = _draw_heatmap_svg(heatmap_data)

    # ---- 行业分布 ----
    industry_counts = {}
    for r in factor_results:
        ind = r.industry_name or "其他"
        industry_counts[ind] = industry_counts.get(ind, 0) + 1
    industry_rows = ""
    for ind, count in sorted(industry_counts.items(), key=lambda x: x[1], reverse=True):
        industry_rows += f"<tr><td>{ind}</td><td class='num'>{count}</td></tr>"

    # ---- Top 3 雷达图 ----
    radar_sections = ""
    for i, r in enumerate(factor_results[:3]):
        if r.total_score >= 55:
            scores = {
                "industry_score": r.industry_score,
                "marketcap_score": r.marketcap_score,
                "trend_score": r.trend_score,
                "momentum_score": r.momentum_score,
                "quality_score": r.quality_score,
                "position_score": r.position_score,
            }
            radar_svg = _draw_radar_chart_svg(scores)
            radar_sections += f"""
            <div class="radar-card">
                <h4>#{i+1} {r.code} {r.name} <span class="level-badge" style="background:{LEVEL_COLORS.get(r.level,('#808080','','?'))[0]}">{r.level}级 {r.total_score:.0f}分</span></h4>
                {radar_svg}
            </div>"""

    # ---- 组装 HTML ----
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>选股报告 | {date_str} | 多因子评分</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
            background: #f5f7fa; color: #333; line-height: 1.6;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .header {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #fff; padding: 30px 40px; border-radius: 12px; margin-bottom: 24px;
        }}
        .header h1 {{ font-size: 28px; margin-bottom: 8px; }}
        .header .subtitle {{ color: #a0aec0; font-size: 14px; }}
        .market-info {{
            display: flex; gap: 20px; flex-wrap: wrap; margin-top: 16px;
            font-size: 14px; color: #cbd5e0;
        }}
        .market-info span {{ background: rgba(255,255,255,0.1); padding: 4px 12px; border-radius: 4px; }}
        .market-info .warning {{ background: #e53e3e; color: #fff; }}
        .market-info .regime {{ color: #68d391; font-weight: bold; }}

        .stats-row {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 16px; margin-bottom: 24px;
        }}
        .stat-card {{
            background: #fff; border-radius: 10px; padding: 20px;
            text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }}
        .stat-card .label {{ color: #718096; font-size: 13px; margin-bottom: 8px; }}
        .stat-card .value {{ font-size: 32px; font-weight: 700; }}
        .stat-card.s .value {{ color: #FF4500; }}
        .stat-card.a .value {{ color: #FF8C00; }}
        .stat-card.b .value {{ color: #4169E1; }}
        .stat-card.c .value {{ color: #808080; }}

        .section {{
            background: #fff; border-radius: 10px; padding: 24px;
            margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }}
        .section h2 {{
            font-size: 20px; margin-bottom: 16px; padding-bottom: 12px;
            border-bottom: 2px solid #e2e8f0; color: #2d3748;
        }}

        table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        th {{
            background: #f7fafc; padding: 10px 12px; text-align: left;
            font-weight: 600; color: #4a5568; border-bottom: 2px solid #e2e8f0;
            white-space: nowrap;
        }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #edf2f7; }}
        tr:hover {{ background: #f7fafc !important; }}
        .code {{ font-family: "SF Mono", "Menlo", monospace; font-weight: 600; color: #2b6cb0; }}
        .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .level-badge {{
            display: inline-block; color: #fff; padding: 2px 8px;
            border-radius: 4px; font-size: 12px; font-weight: 700;
        }}
        .buy {{ color: #e53e3e; font-weight: 700; }}
        .stop {{ color: #38a169; font-weight: 700; }}

        .radar-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 16px;
        }}
        .radar-card {{
            background: #fff; border-radius: 10px; padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06); text-align: center;
        }}
        .radar-card h4 {{ margin-bottom: 12px; color: #2d3748; }}

        .footer {{
            text-align: center; padding: 20px; color: #a0aec0;
            font-size: 12px; margin-top: 20px;
        }}
        .footer .disclaimer {{
            color: #e53e3e; font-weight: 600; margin-top: 8px;
        }}

        .tabs {{ display: flex; gap: 8px; margin-bottom: 16px; }}
        .tab {{
            padding: 8px 20px; border: none; border-radius: 8px;
            cursor: pointer; font-size: 14px; background: #edf2f7; color: #4a5568;
        }}
        .tab.active {{ background: #2b6cb0; color: #fff; }}
    </style>
</head>
<body>
<div class="container">

    <!-- 报告头 -->
    <div class="header">
        <h1>📊 智能选股决策报告</h1>
        <div class="subtitle">
            基于踏空组合 (ZH063783) 12年选股方法论的六因子评分模型 |
            报告时间: {date_str} {time_str}
        </div>
        {market_info}
    </div>

    <!-- 统计卡片 -->
    <div class="stats-row">
        <div class="stat-card s">
            <div class="label">S级 (重磅推荐)</div>
            <div class="value">{s_count}</div>
        </div>
        <div class="stat-card a">
            <div class="label">A级 (强烈关注)</div>
            <div class="value">{a_count}</div>
        </div>
        <div class="stat-card b">
            <div class="label">B级 (候选观察)</div>
            <div class="value">{b_count}</div>
        </div>
        <div class="stat-card c">
            <div class="label">C级 (暂时观望)</div>
            <div class="value">{c_count}</div>
        </div>
        <div class="stat-card" style="grid-column: span 2;">
            <div class="label">评估股票总数</div>
            <div class="value" style="color: #2b6cb0;">{len(factor_results)}</div>
        </div>
    </div>

    <!-- 行业分布 -->
    <div class="section">
        <h2>📈 行业分布</h2>
        <table>
            <thead><tr><th>行业</th><th>命中数量</th></tr></thead>
            <tbody>{industry_rows}</tbody>
        </table>
    </div>

    <!-- 行业 × 因子热力图 -->
    <div class="section">
        <h2>🔥 行业 × 因子热度矩阵</h2>
        <p style="color: #718096; font-size: 13px; margin-bottom: 16px;">
            行=行业，列=六因子。颜色越深（红）代表该行业在该因子维度上得分越高。
        </p>
        <div style="overflow-x: auto; text-align: center;">
            {heatmap_svg}
        </div>
    </div>

    <!-- Top 3 雷达图 -->
    <div class="section">
        <h2>🎯 Top 选股因子雷达图</h2>
        <div class="radar-grid">{radar_sections}</div>
    </div>

    <!-- 综合评分排名 -->
    <div class="section">
        <h2>📋 综合评分排名 (Top 30)</h2>
        <div style="overflow-x: auto;">
        <table>
            <thead><tr>
                <th>#</th><th>等级</th><th>代码</th><th>名称</th>
                <th>总分</th><th>现价</th><th>涨跌</th>
                <th>行业</th><th>市值</th><th>趋势</th>
                <th>动量</th><th>质量</th><th>仓位</th>
                <th>行业分类</th><th>PE</th><th>市值</th>
            </tr></thead>
            <tbody>{''.join(overview_rows)}</tbody>
        </table>
        </div>
    </div>

    <!-- 交易 plan -->
    <div class="section">
        <h2>🎯 交易 Playbook (S/A/B 级)</h2>
        <div style="overflow-x: auto;">
        <table>
            <thead><tr>
                <th>#</th><th>等级</th><th>代码</th><th>名称</th>
                <th>建议入场</th><th>止损价</th>
                <th>建议仓位</th><th>操作备注</th><th>出场条件</th>
            </tr></thead>
            <tbody>{''.join(playbook_rows)}</tbody>
        </table>
        </div>
    </div>

    <!-- 页脚 -->
    <div class="footer">
        <p>由 Rally Scanner Bot v4.0 自动生成 | 数据源: Baostock/同花顺/东方财富/腾讯财经</p>
        <p class="disclaimer">⚠️ 本报告仅为量化信号扫描结果，不构成投资建议。请结合基本面自行判断，股市有风险，投资需谨慎。</p>
    </div>

</div>
</body>
</html>"""

    # 写入文件
    filepath = os.path.join(out_path, f"stock_report_{date_str}.html")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("HTML 报告已生成: {}", filepath)
    return filepath


# ============================================================
# Excel 报表生成
# ============================================================

def generate_excel_report(
    factor_results: list,
    market_env=None,
    output_dir: str = "outputs",
) -> Optional[str]:
    """
    生成 Excel 选股报表

    Sheets:
      1. 评分明细: 全部股票的六因子得分
      2. 交易计划: S/A/B 级股票的交易 playbook
      3. 行业分布: 行业命中统计

    Args:
        factor_results: [FactorScore]
        market_env: 大盘环境
        output_dir: 输出目录

    Returns:
        Excel 文件路径，pandas 不可用时返回 None
    """
    if not HAS_PANDAS:
        logger.warning("pandas 未安装，跳过 Excel 生成")
        return None

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    out_path = os.path.join(output_dir, date_str)
    os.makedirs(out_path, exist_ok=True)

    # Sheet 1: 评分明细
    detail_rows = []
    for r in factor_results:
        detail_rows.append({
            "代码": r.code,
            "名称": r.name,
            "等级": r.level,
            "总分": round(r.total_score, 1),
            "行业(40%)": round(r.industry_score, 1),
            "市值(20%)": round(r.marketcap_score, 1),
            "趋势(15%)": round(r.trend_score, 1),
            "动量(10%)": round(r.momentum_score, 1),
            "质量(10%)": round(r.quality_score, 1),
            "仓位(5%)": round(r.position_score, 1),
            "行业分类": r.industry_name,
            "现价": r.current_price,
            "涨跌幅%": round(r.change_pct, 2),
            "PE": r.pe if r.pe > 0 else "",
            "流通市值(亿)": r.market_cap if r.market_cap > 0 else "",
            "近3月涨幅%": r.surge_3m,
            "近1月涨幅%": r.surge_1m,
            "均线多头": "是" if r.ma_alignment_ok else "否",
            "深层趋势": "是" if r.ma_deep_trend_ok else "否",
            "缩量回调": "是" if r.vol_contraction else "否",
        })
    df_detail = pd.DataFrame(detail_rows)

    # Sheet 2: 交易计划
    plan_rows = []
    for r in factor_results:
        if r.level in ("S", "A", "B"):
            plan_rows.append({
                "代码": r.code,
                "名称": r.name,
                "等级": r.level,
                "总分": round(r.total_score, 1),
                "建议入场价": r.entry_price,
                "止损价": r.stop_loss,
                "建议仓位%": r.position_pct,
                "当前价": r.current_price,
                "潜在收益%": round((r.entry_price * 1.05 / r.current_price - 1) * 100, 1) if r.entry_price > 0 else "",
                "潜在风险%": round((1 - r.stop_loss / r.current_price) * 100, 1) if r.stop_loss > 0 and r.current_price > 0 else "",
                "操作备注": r.playbook_note,
                "出场条件": r.exit_condition,
            })
    df_plan = pd.DataFrame(plan_rows)

    # Sheet 3: 行业分布
    industry_counts = {}
    for r in factor_results:
        ind = r.industry_name or "其他"
        industry_counts[ind] = industry_counts.get(ind, 0) + 1
    industry_rows = [{"行业": k, "命中数量": v} for k, v in sorted(industry_counts.items(), key=lambda x: x[1], reverse=True)]
    df_industry = pd.DataFrame(industry_rows)

    # Sheet 4: 市场环境
    env_data = {}
    if market_env:
        env_data = {
            "指标": ["指数名称", "指数价格", "MA20", "MA60", "市场状态", "熔断状态"],
            "数值": [
                market_env.index_name,
                market_env.index_price,
                market_env.index_ma20,
                market_env.index_ma60,
                str(market_env.regime),
                "是" if market_env.circuit_breaker else "否",
            ],
        }
    df_env = pd.DataFrame(env_data)

    # 写入 Excel
    filepath = os.path.join(out_path, f"stock_scores_{date_str}.xlsx")
    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df_detail.to_excel(writer, sheet_name="评分明细", index=False)
        df_plan.to_excel(writer, sheet_name="交易计划", index=False)
        df_industry.to_excel(writer, sheet_name="行业分布", index=False)
        if env_data:
            df_env.to_excel(writer, sheet_name="市场环境", index=False)

        # 调整列宽
        for sheet_name in ["评分明细", "交易计划", "行业分布", "市场环境"]:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 30)

    logger.info("Excel 报表已生成: {}", filepath)
    return filepath


# ============================================================
# Markdown 摘要 (微信/钉钉推送用)
# ============================================================

def generate_markdown_summary(
    factor_results: list,
    market_env=None,
    max_items: int = 15,
) -> str:
    """
    生成 Markdown 格式的选股摘要

    格式适合 PushPlus/钉钉/企业微信推送
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    s_count = sum(1 for r in factor_results if r.level == "S")
    a_count = sum(1 for r in factor_results if r.level == "A")
    b_count = sum(1 for r in factor_results if r.level == "B")

    lines = [
        f"# 📊 智能选股日报",
        f"",
        f"> 时间: {now}",
        f"> 命中: S级 **{s_count}** | A级 **{a_count}** | B级 **{b_count}**",
    ]

    if market_env:
        regime_map = {"BULLISH": "做多环境", "CAUTION": "谨慎环境", "BEARISH": "空仓熔断"}
        regime_text = regime_map.get(
            market_env.regime.value if hasattr(market_env.regime, 'value') else str(market_env.regime),
            "未知"
        )
        lines.append(f"> 大盘: {market_env.index_name} {regime_text} ({market_env.index_price} / MA20 {market_env.index_ma20})")
        if market_env.circuit_breaker:
            lines.append(f"> ⚠️ **空仓熔断生效，建议观望**")

    lines.append(f"")
    lines.append(f"## 🎯 综合排名 Top {min(max_items, len(factor_results))}")
    lines.append(f"")
    lines.append(
        f"| # | 代码 | 名称 | 等级 | 总分 | 现价 | 涨跌 | 行业适配 | 入场 | 止损 |"
    )
    lines.append(
        f"|---|------|------|------|------|------|------|---------|------|------|"
    )

    for i, r in enumerate(factor_results[:max_items], 1):
        level_icon = {"S": "🔥", "A": "🚀", "B": "📊", "C": "📉"}.get(r.level, "?")
        entry_str = f"{r.entry_price:.2f}" if r.entry_price > 0 else "-"
        stop_str = f"{r.stop_loss:.2f}" if r.stop_loss > 0 else "-"
        lines.append(
            f"| {i} | {r.code} | {r.name} | {level_icon}{r.level} | "
            f"**{r.total_score:.0f}** | {r.current_price:.2f} | "
            f"{r.change_pct:+.2f}% | {r.industry_score:.0f} | "
            f"{entry_str} | {stop_str} |"
        )

    # Top 3 交易计划详情
    top3 = [r for r in factor_results[:3] if r.level in ("S", "A")]
    if top3:
        lines.append(f"")
        lines.append(f"## 📋 Top 交易 Playbook")
        for i, r in enumerate(top3, 1):
            lines.append(f"")
            lines.append(f"### {i}. {r.code} {r.name} ({r.level}级 | {r.total_score:.0f}分)")
            lines.append(f"- 当前价格: **{r.current_price:.2f}** ({r.change_pct:+.2f}%)")
            lines.append(f"- 建议入场: **{r.entry_price:.2f}** → {r.playbook_note}")
            lines.append(f"- 硬性止损: **{r.stop_loss:.2f}** → {r.exit_condition}")
            lines.append(f"- 建议仓位: **{r.position_pct:.0f}%**")
            lines.append(f"- 行业分类: {r.industry_name} (适配 {r.industry_score:.0f}分)")

    lines.append(f"")
    lines.append(f"> ⚠️ 量化信号扫描结果，不构成投资建议。股市有风险，投资需谨慎。")

    return "\n".join(lines)


# ============================================================
# 控制台美化输出
# ============================================================

def print_to_console(factor_results: list, market_env=None, max_display: int = 20):
    """控制台美化打印选股结果"""
    sep = "=" * 80
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(f"\n{sep}")
    print(f"  📊 智能选股决策报告 | {now} | 共评估 {len(factor_results)} 只")
    print(sep)

    if market_env:
        regime_map = {"BULLISH": "做多环境", "CAUTION": "谨慎环境", "BEARISH": "空仓熔断"}
        regime_text = regime_map.get(
            market_env.regime.value if hasattr(market_env.regime, 'value') else str(market_env.regime),
            "未知"
        )
        print(f"  大盘: {market_env.index_name} {regime_text}")
        print(f"  指数={market_env.index_price} MA20={market_env.index_ma20} MA60={market_env.index_ma60}")
        if market_env.circuit_breaker:
            print(f"  ⚠️ 空仓熔断生效")

    s_count = sum(1 for r in factor_results if r.level == "S")
    a_count = sum(1 for r in factor_results if r.level == "A")
    b_count = sum(1 for r in factor_results if r.level == "B")
    print(f"  S级={s_count} | A级={a_count} | B级={b_count} | C级={len(factor_results) - s_count - a_count - b_count}")
    print(sep)

    for i, r in enumerate(factor_results[:max_display], 1):
        level_icon = {"S": "🔥", "A": "🚀", "B": "📊", "C": "📉"}.get(r.level, "?")
        print(f"\n  [{level_icon} {r.level}级] {r.code} {r.name} | 总分: {r.total_score:.0f}")
        print(f"  {'-' * 76}")
        print(f"  现价: {r.current_price:.2f} ({r.change_pct:+.2f}%) | "
              f"行业: {r.industry_name}({r.industry_score:.0f}) | "
              f"市值: {r.marketcap_score:.0f} | 趋势: {r.trend_score:.0f}")
        print(f"  动量: {r.momentum_score:.0f} | 质量: {r.quality_score:.0f} | 仓位: {r.position_score:.0f}")
        print(f"  3月涨幅: {r.surge_3m:+.1f}% | 1月涨幅: {r.surge_1m:+.1f}% | "
              f"均线多头: {'是' if r.ma_alignment_ok else '否'} | "
              f"缩量回调: {'是' if r.vol_contraction else '否'}")
        print(f"  📋 入场: {r.entry_price:.2f} | 止损: {r.stop_loss:.2f} | "
              f"仓位: {r.position_pct:.0f}% | {r.playbook_note}")

    print(f"\n{sep}")
    print(f"  ⚠️ 量化信号扫描结果，不构成投资建议。股市有风险，投资需谨慎。")
    print(f"{sep}\n")
