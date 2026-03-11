#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建板块名称映射文件
从同花顺数据文件中提取行业、概念、地区等名称映射
"""
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


def create_name_mapping():
    """创建板块名称映射"""
    raw_dir = settings.raw_data_dir
    mapping = {}

    # 1. 加载行业名称 (ths_industries_l1.csv)
    industry_file = os.path.join(raw_dir, 'ths_industries_l1.csv')
    if os.path.exists(industry_file):
        df = pd.read_csv(industry_file)
        for _, row in df.iterrows():
            ts_code = str(row['ts_code'])
            name = str(row['name'])
            mapping[ts_code] = name
        print(f"加载了 {len(df)} 个行业名称")

    # 2. 加载地区指数名称 (ths_indices.csv 中的地区部分)
    indices_file = os.path.join(raw_dir, 'ths_indices.csv')
    if os.path.exists(indices_file):
        df = pd.read_csv(indices_file)
        for _, row in df.iterrows():
            ts_code = str(row['ts_code'])
            name = str(row['name'])
            if ts_code not in mapping:
                mapping[ts_code] = name
        print(f"加载了地区指数名称")

    # 3. 从概念数据文件名提取概念名称 (如果有概念列表文件)
    concept_list_file = os.path.join(raw_dir, 'ths_concepts.csv')
    if os.path.exists(concept_list_file):
        df = pd.read_csv(concept_list_file)
        for _, row in df.iterrows():
            ts_code = str(row.get('ts_code', row.get('concept_code', '')))
            name = str(row.get('name', ''))
            if ts_code and ts_code not in mapping and name:
                mapping[ts_code] = name
        print(f"加载了概念名称")

    # 4. 保存映射
    mapping_df = pd.DataFrame([
        {'ts_code': code, 'name': name}
        for code, name in mapping.items()
    ])

    output_file = os.path.join(raw_dir, 'ths_name_mapping.csv')
    mapping_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"名称映射已保存：{output_file}")
    print(f"总计 {len(mapping_df)} 个板块名称")

    return mapping


def load_name_mapping():
    """加载名称映射"""
    raw_dir = settings.raw_data_dir
    mapping_file = os.path.join(raw_dir, 'ths_name_mapping.csv')

    if os.path.exists(mapping_file):
        df = pd.read_csv(mapping_file)
        return dict(zip(df['ts_code'].astype(str), df['name']))
    else:
        # 如果映射文件不存在，创建它
        return create_name_mapping()


def get_block_name(ts_code: str, mapping: dict = None) -> str:
    """
    获取板块名称

    Args:
        ts_code: 板块代码 (如 881101.TI)
        mapping: 名称映射字典

    Returns:
        板块名称
    """
    if mapping is None:
        mapping = load_name_mapping()

    # 去除 .TI 后缀进行匹配
    code_no_suffix = ts_code.replace('.TI', '')

    # 尝试完整匹配
    if ts_code in mapping:
        return mapping[ts_code]

    # 尝试不带后缀的匹配
    if code_no_suffix in mapping:
        return mapping[code_no_suffix]

    # 返回带代码的默认名称
    return f"板块_{code_no_suffix}"


if __name__ == "__main__":
    create_name_mapping()

    # 测试
    mapping = load_name_mapping()
    print("\n名称映射测试:")
    for code in ['881101.TI', '885311.TI', '882001.TI']:
        name = get_block_name(code, mapping)
        print(f"  {code} -> {name}")
