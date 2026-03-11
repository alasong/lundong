#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SQLite 数据库功能测试
验证高并发、实时去重、增量更新功能
"""
import os
import sys
import pandas as pd
import tempfile
import time
from datetime import datetime

# 添加 src 目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))
from data.database import SQLiteDatabase
from data.storage_manager import StorageManager


def test_database_basic():
    """测试数据库基本功能"""
    print("\n" + "=" * 60)
    print("测试 1: 数据库基本功能")
    print("=" * 60)

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        db = SQLiteDatabase(db_path, pool_size=2)
        print("✓ 数据库创建成功")

        # 测试写入数据
        test_data = [
            {'ts_code': '885001', 'trade_date': '20240101', 'open': 1000.0, 'high': 1050.0,
             'low': 990.0, 'close': 1030.0, 'vol': 10000, 'amount': 100000, 'change_pct': 3.0},
            {'ts_code': '885001', 'trade_date': '20240102', 'open': 1030.0, 'high': 1080.0,
             'low': 1020.0, 'close': 1060.0, 'vol': 12000, 'amount': 120000, 'change_pct': 2.9},
            {'ts_code': '885002', 'trade_date': '20240101', 'open': 2000.0, 'high': 2100.0,
             'low': 1980.0, 'close': 2050.0, 'vol': 8000, 'amount': 160000, 'change_pct': 2.5},
        ]

        count = db.batch_insert('concept_daily', test_data, replace=False)
        print(f"✓ 插入 {count} 条记录")

        # 测试查询
        df = db.get_all_concept_data()
        print(f"✓ 查询数据：{len(df)} 条记录")

        # 测试获取最新日期
        latest = db.get_latest_date()
        print(f"✓ 最新日期：{latest}")

        # 测试获取板块最新日期
        latest_885001 = db.get_latest_date('885001')
        print(f"✓ 885001 最新日期：{latest_885001}")

        db.close()
        print("\n测试 1 通过 ✓")
        return True

    except Exception as e:
        print(f"\n测试 1 失败 ✗: {e}")
        return False
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_deduplication():
    """测试实时去重功能"""
    print("\n" + "=" * 60)
    print("测试 2: 实时去重功能")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        db = SQLiteDatabase(db_path, pool_size=2)

        # 首次插入
        data1 = [
            {'ts_code': '885001', 'trade_date': '20240101', 'open': 1000.0, 'high': 1050.0,
             'low': 990.0, 'close': 1030.0, 'vol': 10000, 'amount': 100000, 'change_pct': 3.0},
        ]
        db.batch_insert('concept_daily', data1, replace=False)
        print("✓ 首次插入 1 条记录")

        # 重复插入（相同数据）
        data2 = [
            {'ts_code': '885001', 'trade_date': '20240101', 'open': 1100.0, 'high': 1150.0,
             'low': 1090.0, 'close': 1130.0, 'vol': 20000, 'amount': 200000, 'change_pct': 5.0},
        ]
        db.batch_insert('concept_daily', data2, replace=True)  # 覆盖模式
        print("✓ 重复插入（覆盖模式）")

        # 验证数据被覆盖
        df = db.get_data_range('885001', '20240101', '20240101')
        if len(df) == 1 and df.iloc[0]['close'] == 1130.0:
            print("✓ 数据覆盖成功")
        else:
            print("✗ 数据覆盖失败")
            return False

        # 测试跳过模式
        data3 = [
            {'ts_code': '885001', 'trade_date': '20240101', 'open': 1200.0, 'high': 1250.0,
             'low': 1190.0, 'close': 1230.0, 'vol': 30000, 'amount': 300000, 'change_pct': 8.0},
        ]
        db.batch_insert('concept_daily', data3, replace=False)  # 跳过模式

        df = db.get_data_range('885001', '20240101', '20240101')
        if len(df) == 1 and df.iloc[0]['close'] == 1130.0:  # 保留原数据
            print("✓ 跳过重复数据成功")
        else:
            print("✗ 跳过重复数据失败")
            return False

        db.close()
        print("\n测试 2 通过 ✓")
        return True

    except Exception as e:
        print(f"\n测试 2 失败 ✗: {e}")
        return False
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_storage_manager():
    """测试 StorageManager 功能"""
    print("\n" + "=" * 60)
    print("测试 3: StorageManager 功能")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        # 设置临时数据库路径
        os.environ['DATABASE_URL'] = f'sqlite:///{db_path}'

        # 重新导入以使用新的数据库路径
        from importlib import reload
        import src.config as config
        reload(config)

        db = SQLiteDatabase(db_path, pool_size=2)
        manager = StorageManager(db=db)

        # 测试增量更新
        new_data = pd.DataFrame([
            {'ts_code': '885001', 'trade_date': '20240101', 'open': 1000.0, 'high': 1050.0,
             'low': 990.0, 'close': 1030.0, 'vol': 10000, 'amount': 100000, 'change_pct': 3.0},
            {'ts_code': '885001', 'trade_date': '20240102', 'open': 1030.0, 'high': 1080.0,
             'low': 1020.0, 'close': 1060.0, 'vol': 12000, 'amount': 120000, 'change_pct': 2.9},
        ])

        new_count, total_count = manager.incremental_update(new_data)
        print(f"✓ 增量更新：新增 {new_count} 条，总计 {total_count} 条")

        # 重复更新（测试去重）
        new_count, total_count = manager.incremental_update(new_data)
        print(f"✓ 重复更新：新增 {new_count} 条，总计 {total_count} 条")

        # 验证数据
        df = manager.load_merged_data()
        print(f"✓ 加载数据：{len(df)} 条记录")

        # 验证完整性
        stats = manager.verify_data_integrity()
        print(f"✓ 数据验证：{stats['status']}")

        db.close()
        print("\n测试 3 通过 ✓")
        return True

    except Exception as e:
        print(f"\n测试 3 失败 ✗: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_concurrent_writes():
    """测试并发写入（模拟）"""
    print("\n" + "=" * 60)
    print("测试 4: 并发写入测试")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        db = SQLiteDatabase(db_path, pool_size=5)

        # 模拟多次写入
        start_time = time.time()

        for i in range(10):
            ts_code = f'88500{i}'
            data = [
                {'ts_code': ts_code, 'trade_date': '20240101', 'open': 1000.0 + i, 'high': 1050.0 + i,
                 'low': 990.0 + i, 'close': 1030.0 + i, 'vol': 10000, 'amount': 100000, 'change_pct': 3.0},
            ]
            db.batch_insert('concept_daily', data, replace=True)

        elapsed = time.time() - start_time
        print(f"✓ 写入 10 个板块，耗时 {elapsed:.2f}秒")

        # 验证数据
        stats = db.get_statistics()
        print(f"✓ 数据库统计：{stats['total_records']} 条记录，{stats['concept_count']} 个板块")

        db.close()
        print("\n测试 4 通过 ✓")
        return True

    except Exception as e:
        print(f"\n测试 4 失败 ✗: {e}")
        return False
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def main():
    """运行所有测试"""
    print("\n" + "=" * 60)
    print("SQLite 数据库功能测试")
    print("=" * 60)

    results = []

    results.append(("数据库基本功能", test_database_basic()))
    results.append(("实时去重功能", test_deduplication()))
    results.append(("StorageManager 功能", test_storage_manager()))
    results.append(("并发写入测试", test_concurrent_writes()))

    # 汇总结果
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"  {name}: {status}")

    print(f"\n总计：{passed}/{total} 个测试通过")
    print("=" * 60)

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
