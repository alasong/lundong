#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
成分股数据导入工具
用于手动导入或从其他数据源获取成分股列表
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data.database import SQLiteDatabase
import pandas as pd


def load_constituents_from_csv(csv_path: str, concept_code: str) -> int:
    """
    从 CSV 文件导入成分股

    CSV 格式:
    stock_code,stock_name
    000001.SZ，平安银行
    000002.SZ，万科 A
    """
    db = SQLiteDatabase(db_path='data/stock.db')

    df = pd.read_csv(csv_path)
    constituents = []

    for _, row in df.iterrows():
        constituents.append({
            'stock_code': row['stock_code'],
            'stock_name': row.get('stock_name', '')
        })

    db.save_concept_constituents(concept_code, constituents)
    print(f"已导入 {len(constituents)} 只成分股到 {concept_code}")
    return len(constituents)


def create_sample_constituents():
    """
    创建示例成分股数据（用于测试）
    实际使用时应该替换为真实的成分股列表
    """
    db = SQLiteDatabase(db_path='data/stock.db')

    # 示例：半导体板块成分股（部分）
    sample_constituents = {
        '885311.TI': [  # 半导体
            {'stock_code': '002049.SZ', 'stock_name': '紫光国微'},
            {'stock_code': '600584.SH', 'stock_name': '长电科技'},
            {'stock_code': '603019.SH', 'stock_name': '中科曙光'},
            {'stock_code': '002156.SZ', 'stock_name': '通富微电'},
            {'stock_code': '603986.SH', 'stock_name': '兆易创新'},
            {'stock_code': '300782.SZ', 'stock_name': '卓胜微'},
            {'stock_code': '600703.SH', 'stock_name': '三安光电'},
            {'stock_code': '002371.SZ', 'stock_name': '北方华创'},
            {'stock_code': '300661.SZ', 'stock_name': '圣邦股份'},
            {'stock_code': '688981.SH', 'stock_name': '中芯国际'},
        ],
        '885394.TI': [  # 人工智能
            {'stock_code': '002230.SZ', 'stock_name': '科大讯飞'},
            {'stock_code': '002415.SZ', 'stock_name': '海康威视'},
            {'stock_code': '300059.SZ', 'stock_name': '东方财富'},
            {'stock_code': '300760.SZ', 'stock_name': '迈瑞医疗'},
            {'stock_code': '000661.SZ', 'stock_name': '长春高新'},
            {'stock_code': '300142.SZ', 'stock_name': '沃森生物'},
            {'stock_code': '300601.SZ', 'stock_name': '康泰生物'},
            {'stock_code': '002007.SZ', 'stock_name': '华兰生物'},
            {'stock_code': '300122.SZ', 'stock_name': '智飞生物'},
            {'stock_code': '002001.SZ', 'stock_name': '新和成'},
        ],
        '885368.TI': [  # 汽车芯片
            {'stock_code': '002049.SZ', 'stock_name': '紫光国微'},
            {'stock_code': '002156.SZ', 'stock_name': '通富微电'},
            {'stock_code': '600703.SH', 'stock_name': '三安光电'},
            {'stock_code': '002371.SZ', 'stock_name': '北方华创'},
            {'stock_code': '603986.SH', 'stock_name': '兆易创新'},
            {'stock_code': '002036.SZ', 'stock_name': '联创电子'},
            {'stock_code': '002405.SZ', 'stock_name': '四维图新'},
            {'stock_code': '002920.SZ', 'stock_name': '德赛西威'},
            {'stock_code': '002126.SZ', 'stock_name': '银轮股份'},
            {'stock_code': '600741.SH', 'stock_name': '华域汽车'},
        ],
    }

    total = 0
    for concept, constituents in sample_constituents.items():
        db.save_concept_constituents(concept, constituents)
        print(f"已导入 {concept}: {len(constituents)} 只成分股")
        total += len(constituents)

    print(f"\n总计导入 {total} 只成分股")
    return total


def main():
    """主函数"""
    print("=" * 70)
    print("成分股数据导入工具")
    print("=" * 70)

    # 创建示例数据
    print("\n创建示例成分股数据...")
    create_sample_constituents()

    # 验证
    print("\n=== 验证导入结果 ===")
    db = SQLiteDatabase(db_path='data/stock.db')

    test_concepts = ['885311.TI', '885394.TI', '885368.TI']
    for concept in test_concepts:
        result = db.get_concept_constituents(concept)
        print(f"{concept}: {len(result)} 只")

    print("\n使用说明:")
    print("1. 如果有真实成分股数据，使用 CSV 格式导入:")
    print("   python src/import_constituents.py --csv your_file.csv --concept 885311.TI")
    print("\n2. 或者修改 create_sample_constituents() 函数添加真实数据")


if __name__ == "__main__":
    main()
