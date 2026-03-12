#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
运行今日预测
采集最新数据并预测明天的热点板块和股票
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime, timedelta
from loguru import logger
import json

# 导入模块
from agents.data_agent import DataAgent
from agents.predict_agent import PredictAgent
from analysis.hotspot_detector import HotspotDetector
from data.name_mapper import get_block_name


def run_prediction():
    """运行完整预测流程"""

    print("=" * 60)
    print("A 股热点预测系统 - 今日预测")
    print("=" * 60)
    print(f"运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 1. 采集最新数据
    print("[1/4] 采集最新板块数据...")
    print("-" * 50)
    data_agent = DataAgent()

    # 采集昨日数据（最新交易日）
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    data_result = data_agent.run(task="daily")
    print(f"数据采集完成：{data_result}")
    print()

    # 2. 识别热点
    print("[2/4] 识别热点板块...")
    print("-" * 50)
    detector = HotspotDetector()

    # 从数据库加载数据进行热点识别
    from data.database import get_database
    db = get_database()

    # 获取最新交易日所有板块数据
    latest_data = db.query("""
        SELECT ts_code, trade_date, close, pct_change, vol, amount, turnover_rate
        FROM concept_daily
        WHERE trade_date = (SELECT MAX(trade_date) FROM concept_daily)
        ORDER BY pct_change DESC
    """)

    if not latest_data:
        print("错误：未找到板块数据")
        return

    # 转换为 DataFrame 格式供热点检测
    import pandas as pd
    concept_data = pd.DataFrame(latest_data, columns=['ts_code', 'trade_date', 'close', 'pct_change', 'vol', 'amount', 'turnover_rate'])

    # 填充空值
    concept_data['amount'] = concept_data['amount'].fillna(0)
    concept_data['vol'] = concept_data['vol'].fillna(0)

    # 计算热点得分（使用 compute_hotspot_score）
    # 重命名列以匹配预期格式
    concept_data_for_score = concept_data.rename(columns={'ts_code': 'concept_code', 'pct_change': 'pct_chg'})
    concept_data_for_score['name'] = concept_data_for_score['concept_code']

    # 计算热点得分
    scores_df = detector.compute_hotspot_score(concept_data_for_score)

    # 按得分排序获取热点
    hotspots = scores_df.nlargest(15, 'hotspot_score')

    print(f"识别到 {len(hotspots)} 个热点板块:")
    for i, (_, hs) in enumerate(hotspots[:10].iterrows(), 1):
        block_name = get_block_name(hs['concept_code'])
        print(f"  {i}. {block_name} ({hs['concept_code']})")
        print(f"     涨幅：{hs.get('pct_chg', 0):.2f}% | 热度：{hs.get('hotspot_score', 0):.2f}")
    print()

    # 3. 运行模型预测
    print("[3/4] 运行模型预测...")
    print("-" * 50)
    predict_agent = PredictAgent()

    # 执行预测
    predict_result = predict_agent.run(task="predict")

    if predict_result.get('success'):
        predictions = predict_result.get('result', {}).get('predictions', [])
        top_10 = predict_result.get('result', {}).get('top_10', [])

        print(f"预测完成，共 {len(predictions)} 条预测")
        print()

        # 显示 Top 10 预测
        print("🔮 明日热点预测 Top 10:")
        print("-" * 50)
        for i, pred in enumerate(top_10[:10], 1):
            block_name = get_block_name(pred['concept_code'])
            pred_1d = pred.get('pred_1d', 0)
            combined = pred.get('combined_score', 0)

            # 涨跌幅符号
            arrow = "📈" if pred_1d > 0 else "📉"

            print(f"  {i}. {block_name} ({pred['concept_code']})")
            print(f"     预测涨幅：{arrow} {abs(pred_1d):.2f}% | 综合得分：{combined:.2f}")
    else:
        print(f"预测失败：{predict_result.get('error', '未知错误')}")
        predictions = []
        top_10 = []
    print()

    # 4. 推荐关注股票
    print("[4/4] 推荐关注股票...")
    print("-" * 50)

    # 获取热点板块的成分股
    if top_10:
        print("基于热点板块的成分股推荐:")

        # 获取前 3 个热点板块的成分股
        for pred in top_10[:3]:
            ts_code = pred['concept_code']
            block_name = get_block_name(ts_code)

            # 查询成分股
            constituents = db.query("""
                SELECT stock_code, stock_name
                FROM concept_constituent
                WHERE concept_code = ?
                LIMIT 5
            """, (ts_code,))

            if constituents:
                print(f"\n  {block_name} 成分股:")
                for c in constituents[:5]:
                    print(f"    - {c[1]} ({c[0]})")

    db.close()

    # 保存结果
    print()
    print("=" * 60)
    print("预测结果摘要")
    print("=" * 60)

    # 汇总结果
    result_summary = {
        "date": datetime.now().strftime('%Y-%m-%d'),
        "trade_date": yesterday,
        "hotspots_count": len(hotspots),
        "predictions_count": len(predictions) if predictions else 0,
        "top_10": [
            {
                "code": p['concept_code'],
                "name": get_block_name(p['concept_code']),
                "pred_1d": round(p.get('pred_1d', 0), 2),
                "combined_score": round(p.get('combined_score', 0), 2)
            }
            for p in (top_10[:10] if top_10 else [])
        ],
        "hotspots": [
            {
                "code": h['concept_code'],
                "name": get_block_name(h['concept_code']),
                "pct_chg": round(h.get('pct_chg', 0), 2),
                "hotspot_score": round(h.get('hotspot_score', 0), 2)
            }
            for _, h in hotspots[:10].iterrows()
        ]
    }

    # 打印摘要
    print(f"\n热点板块 (按今日涨幅):")
    for hs in result_summary['hotspots'][:5]:
        print(f"  • {hs['name']}: {hs['pct_chg']:+.2f}%")

    print(f"\n明日预测 (按综合得分):")
    for pred in result_summary['top_10'][:5]:
        arrow = "↑" if pred['pred_1d'] > 0 else "↓"
        print(f"  • {pred['name']}: {arrow} {abs(pred['pred_1d']):.2f}%")

    # 保存结果到文件
    output_path = 'data/results/daily_prediction.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result_summary, f, indent=2, ensure_ascii=False)

    print(f"\n详细结果已保存至：{output_path}")
    print("=" * 60)

    return result_summary


if __name__ == "__main__":
    result = run_prediction()
