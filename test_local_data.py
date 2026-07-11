#!/usr/bin/env python3
"""本地环境数据获取能力测试脚本"""
import requests
import json
import random
import time
import pandas as pd

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

print("=" * 60)
print("🧪 本地环境数据获取能力测试")
print("=" * 60)

# ── 测试1: 东财股票列表 ──
print("\n【测试1】东财 HTTP 获取全市场股票列表")
url = "http://push2.eastmoney.com/api/qt/clist/get"
base_params = {
    "po": 1, "np": 1, "fltt": 2, "invt": 2, "fid": "f12",
    "fs": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",
    "fields": "f12,f14,f2,f3",
}
all_rows = []
page = 1
total = 0
errors = 0
start = time.time()
while page <= 60:  # 最多60页
    params = {**base_params, "pn": page, "pz": 100}
    try:
        resp = requests.get(url, params=params, timeout=10, headers={"User-Agent": UA})
        resp.raise_for_status()
        data = resp.json()
        if page == 1:
            total = data.get("data", {}).get("total", 0)
            print(f"  东财返回 total={total}")
        diff = data.get("data", {}).get("diff")
        if not diff:
            break
        items = diff if isinstance(diff, list) else diff.values()
        page_count = 0
        for item in items:
            code = str(item.get("f12", "")).strip()
            name = item.get("f14", "")
            if code and name:
                all_rows.append({"代码": code, "名称": name})
                page_count += 1
        if page_count == 0 or len(all_rows) >= total:
            break
        page += 1
        time.sleep(random.uniform(0.3, 0.8))
    except Exception as e:
        errors += 1
        print(f"  ⚠️ 第{page}页失败: {e}")
        if errors >= 3:
            break
        page += 1

elapsed = time.time() - start
print(f"  ✅ 共获取 {len(all_rows)} 只股票 (耗时 {elapsed:.1f}s, 错误 {errors} 次)")
unique = len(set(r["代码"] for r in all_rows))
print(f"  ✅ 去重后 {unique} 只")

if all_rows:
    # 过滤ST
    filtered = [r for r in all_rows if "ST" not in r["名称"] and "退" not in r["名称"] and "B股" not in r["名称"]]
    print(f"  ✅ 过滤ST后 {len(filtered)} 只")

# ── 测试2: 东财K线 ──
print("\n【测试2】东财 HTTP 获取个股K线 (平安银行 000001)")
url2 = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
params2 = {
    "secid": "0.000001",
    "fields1": "f1,f2,f3,f4,f5,f6",
    "fields2": "f51,f52,f53,f54,f55,f56,f57",
    "klt": 101, "fqt": 1,
    "beg": "20250101", "end": "20260711", "lmt": 500,
}
try:
    resp = requests.get(url2, params=params2, timeout=15, headers={"User-Agent": UA})
    resp.raise_for_status()
    data = resp.json()
    klines = data.get("data", {}).get("klines", [])
    print(f"  ✅ 获取 {len(klines)} 条K线数据")
    if klines:
        print(f"  📅 最早: {klines[0].split(',')[0]}  最新: {klines[-1].split(',')[0]}")
except Exception as e:
    print(f"  ❌ 失败: {e}")

# ── 测试3: 腾讯K线 ──
print("\n【测试3】腾讯 HTTP 获取个股K线 (平安银行 000001)")
url3 = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
params3 = {"param": "sz000001,day,,,240,qfq"}
try:
    resp = requests.get(url3, params=params3, timeout=15, headers={"User-Agent": UA})
    data = resp.json()
    stock_data = data.get("data", {}).get("sz000001", {})
    klines = stock_data.get("qfqday") or stock_data.get("day", [])
    print(f"  ✅ 获取 {len(klines)} 条K线数据")
    if klines:
        print(f"  📅 最早: {klines[0][0]}  最新: {klines[-1][0]}")
except Exception as e:
    print(f"  ❌ 失败: {e}")

# ── 测试4: 新浪K线 ──
print("\n【测试4】新浪 HTTP 获取个股K线 (平安银行 000001)")
url4 = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
params4 = {"symbol": "sz000001", "scale": "240", "datalen": "240", "ma": "no"}
try:
    resp = requests.get(url4, params=params4, timeout=15, headers={"User-Agent": UA})
    klines = json.loads(resp.text)
    print(f"  ✅ 获取 {len(klines)} 条K线数据")
    if klines:
        print(f"  📅 最早: {klines[0]['day']}  最新: {klines[-1]['day']}")
except Exception as e:
    print(f"  ❌ 失败: {e}")

print("\n" + "=" * 60)
print("📝 测试完成")
print("=" * 60)
