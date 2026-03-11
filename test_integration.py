#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
集成测试 - 验证完整的数据库功能
"""
import sys
import os
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from data.database import SQLiteDatabase, get_database, init_database
from data.storage_manager import StorageManager
from data.fast_collector import HighSpeedDataCollector
from data.data_organizer import DataOrganizer
from data.csv_migrator import CSVMigrator
from agents.data_agent import DataAgent

print('=== 模块导入测试 ===')
print('✓ 所有模块导入成功')

print()
print('=== 数据库表结构验证 ===')

with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    db_path = f.name

try:
    db = SQLiteDatabase(db_path)

    # 验证表是否存在
    tables = db.query("SELECT name FROM sqlite_master WHERE type='table'")
    table_names = [t[0] for t in tables]
    print(f'数据表：{table_names}')

    # 验证概念日线表结构
    schema = db.query('PRAGMA table_info(concept_daily)')
    print()
    print('concept_daily 表结构:')
    for col in schema:
        print(f'  {col[1]} ({col[2]})')

    # 验证主键
    print()
    print('验证主键约束...')
    import pandas as pd
    data1 = [{'ts_code': '885001', 'trade_date': '20240101', 'open': 1000, 'high': 1050,
              'low': 990, 'close': 1030, 'vol': 10000, 'amount': 100000, 'change_pct': 3.0}]
    data2 = [{'ts_code': '885001', 'trade_date': '20240101', 'open': 2000, 'high': 2050,
              'low': 1990, 'close': 2030, 'vol': 20000, 'amount': 200000, 'change_pct': 5.0}]

    db.batch_insert('concept_daily', data1, replace=False)
    db.batch_insert('concept_daily', data2, replace=True)  # 覆盖

    result = db.query('SELECT * FROM concept_daily')
    assert len(result) == 1, '主键去重失败'
    assert result[0][2] == 2000, '覆盖失败'  # open 价格应该是 2000 (索引 2)

    print('✓ 主键约束验证通过')

    db.close()
    os.remove(db_path)

    print()
    print('=== 所有验证通过 ===')

except Exception as e:
    print(f'✗ 验证失败：{e}')
    import traceback
    traceback.print_exc()
