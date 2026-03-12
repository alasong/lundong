#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
成分股数据采集脚本
采集已导入成分股的历史数据
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data.database import SQLiteDatabase
from data.stock_collector import StockCollector
from datetime import datetime, timedelta


def main():
    """主函数"""
    print("=" * 70)
    print("成分股数据采集脚本")
    print("=" * 70)

    db = SQLiteDatabase(db_path='data/stock.db')

    # 获取所有成分股
    print("\n获取成分股列表...")

    # 获取所有板块代码
    concepts = ['885311.TI', '885394.TI', '885368.TI']
    stock_codes = []

    for concept in concepts:
        constituents = db.get_concept_constituents(concept)
        for const in constituents:
            code = const.get('stock_code')
            if code and code not in stock_codes:
                stock_codes.append(code)

    print(f"发现 {len(stock_codes)} 只成分股")
    print(f"示例：{stock_codes[:10]}")

    # 采集历史数据（过去 3 年）
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y%m%d')

    print(f"\n采集日期范围：{start_date} - {end_date}")

    collector = StockCollector(db=db)

    print(f"\n开始批量采集 {len(stock_codes)} 只股票...")
    stats = collector.collect_stocks_batch(
        stock_codes=stock_codes,
        start_date=start_date,
        end_date=end_date,
        n_jobs=8  # 使用 8 个并发
    )

    total_records = sum(stats.values())
    print(f"\n采集完成：")
    print(f"  成功采集：{sum(1 for v in stats.values() if v > 0)} 只")
    print(f"  总记录数：{total_records} 条")

    # 验证
    print("\n=== 验证 ===")
    cursor = db.get_connection()
    cursor.execute('SELECT COUNT(*) FROM stock_daily')
    count = cursor.fetchone()[0]
    print(f"stock_daily 表：{count} 条记录")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
