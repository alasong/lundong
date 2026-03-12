#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
扩展成分股数据
从同花顺板块数据中获取成分股列表并导入数据库
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from loguru import logger
from data.database import get_database
from data.tushare_ths_client import TushareTHSClient
from config import settings


def fetch_ths_constituents(client: TushareTHSClient, concept_codes: list) -> list:
    """
    获取同花顺板块成分股

    Args:
        client: Tushare 客户端
        concept_codes: 板块代码列表

    Returns:
        成分股列表
    """
    all_constituents = []

    for concept_code in concept_codes:
        try:
            logger.info(f"获取 {concept_code} 成分股...")

            # 尝试使用东财成分股接口
            # 同花顺代码格式 885xxx.TI，东财代码格式 BKxxxx
            if concept_code.startswith('885') or concept_code.startswith('881'):
                # 转换为东财代码
                if concept_code.startswith('885'):
                    bk_code = 'BK' + concept_code[2:8]  # 885311.TI -> BK0811
                else:
                    bk_code = 'BK' + concept_code[2:8]  # 881101.TI -> BK1101

                logger.info(f"尝试使用东财代码 {bk_code} 获取成分股...")

                df = client.pro.concept_member(concept_code=bk_code)
                if df is not None and len(df) > 0:
                    for _, row in df.iterrows():
                        all_constituents.append({
                            'stock_code': row.get('ts_code', ''),
                            'stock_name': row.get('name', ''),
                            'concept_code': concept_code
                        })
                    logger.info(f"获取成功：{len(df)} 只成分股")
                else:
                    logger.warning(f"获取失败：{concept_code}")

        except Exception as e:
            logger.error(f"获取 {concept_code} 成分股失败：{e}")
            continue

    return all_constituents


def import_major_concepts(db, client: TushareTHSClient):
    """
    导入主要板块成分股

    优先级：
    1. 半导体产业链相关板块
    2. 人工智能相关板块
    3. 医药医疗板块
    4. 消费板块
    5. 新能源板块
    """
    # 定义要导入的板块
    major_concepts = [
        # 半导体产业链
        '885311.TI',  # 半导体
        '885368.TI',  # 汽车芯片
        '885522.TI',  # 芯片
        '885320.TI',  # 集成电路

        # 人工智能
        '885394.TI',  # 人工智能
        '885500.TI',  # 大数据
        '885517.TI',  # 云计算
        '885398.TI',  # 5G
        '885357.TI',  # 物联网

        # 医药医疗
        '885388.TI',  # 创新药
        '885356.TI',  # 医疗器械
        '885329.TI',  # 生物医药

        # 大消费
        '885366.TI',  # 消费电子
        '885369.TI',  # 白酒
        '885373.TI',  # 食品饮料

        # 新能源
        '885355.TI',  # 光伏
        '885363.TI',  # 锂电池
        '885358.TI',  # 新能源汽车
        '885361.TI',  # 风电

        # 高端制造
        '885319.TI',  # 工业母机
        '885520.TI',  # 机器人
        '885354.TI',  # 航天航空

        # 周期
        '885317.TI',  # 有色金属
        '885326.TI',  # 化工
        '885348.TI',  # 钢铁
        '885343.TI',  # 煤炭

        # 金融
        '885312.TI',  # 银行
        '885314.TI',  # 证券
        '885315.TI',  # 保险
    ]

    logger.info(f"计划导入 {len(major_concepts)} 个板块成分股")

    # 获取成分股
    constituents = fetch_ths_constituents(client, major_concepts)

    if not constituents:
        logger.warning("未能获取任何成分股数据")
        return 0

    # 按板块分组保存
    from collections import defaultdict
    by_concept = defaultdict(list)

    for c in constituents:
        by_concept[c['concept_code']].append({
            'stock_code': c['stock_code'],
            'stock_name': c['stock_name']
        })

    # 保存到数据库
    total_count = 0
    for concept_code, stocks in by_concept.items():
        if stocks:
            db.save_concept_constituents(concept_code, stocks)
            logger.info(f"保存 {concept_code}: {len(stocks)} 只成分股")
            total_count += len(stocks)

    return total_count


def import_from_local_file(db, csv_dir: str = "data/constituents"):
    """
    从本地 CSV 文件导入成分股

    文件格式: {concept_code}_constituents.csv
    列：stock_code, stock_name
    """
    if not os.path.exists(csv_dir):
        logger.warning(f"目录不存在：{csv_dir}")
        return 0

    total_count = 0

    for filename in os.listdir(csv_dir):
        if not filename.endswith('.csv'):
            continue

        # 从文件名提取板块代码
        concept_code = filename.replace('_constituents.csv', '')

        csv_path = os.path.join(csv_dir, filename)
        df = pd.read_csv(csv_path)

        constituents = []
        for _, row in df.iterrows():
            constituents.append({
                'stock_code': row['stock_code'],
                'stock_name': row.get('stock_name', '')
            })

        if constituents:
            db.save_concept_constituents(concept_code, constituents)
            logger.info(f"导入 {concept_code}: {len(constituents)} 只成分股")
            total_count += len(constituents)

    return total_count


def fetch_all_ths_concepts(client: TushareTHSClient) -> list:
    """
    获取所有同花顺概念板块
    """
    try:
        logger.info("获取同花顺概念板块列表...")
        # 获取概念板块列表
        df = client.pro.concept()
        if df is not None:
            concepts = []
            for _, row in df.iterrows():
                concepts.append({
                    'concept_code': row.get('ts_code', ''),
                    'concept_name': row.get('name', ''),
                })
            logger.info(f"获取到 {len(concepts)} 个概念板块")
            return concepts
        return []
    except Exception as e:
        logger.error(f"获取概念板块失败：{e}")
        return []


def auto_expand_concepts(db, client: TushareTHSClient, max_concepts: int = 100):
    """
    自动扩展概念板块成分股
    """
    # 获取所有概念
    concepts = fetch_all_ths_concepts(client)

    if not concepts:
        logger.warning("未能获取概念板块列表")
        return 0

    # 跳过已存在的板块
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT concept_code FROM concept_constituent")
    existing = set(row[0] for row in cursor.fetchall())

    logger.info(f"已存在 {len(existing)} 个板块")

    # 筛选需要获取的板块
    to_fetch = [c for c in concepts if c['concept_code'] not in existing][:max_concepts]
    logger.info(f"计划获取 {len(to_fetch)} 个新板块")

    total_count = 0
    success_count = 0

    for i, concept in enumerate(to_fetch):
        concept_code = concept['concept_code']
        concept_name = concept['concept_name']

        try:
            logger.info(f"[{i+1}/{len(to_fetch)}] 获取 {concept_code} ({concept_name}) 成分股...")

            # 转换为东财代码格式
            if concept_code.startswith('885'):
                bk_code = 'BK' + concept_code[2:8]
            elif concept_code.startswith('881'):
                bk_code = 'BK' + concept_code[2:6].zfill(4)
            else:
                continue

            df = client.pro.concept_member(concept_code=bk_code)
            if df is not None and len(df) > 0:
                constituents = []
                for _, row in df.iterrows():
                    constituents.append({
                        'stock_code': row.get('ts_code', ''),
                        'stock_name': row.get('name', '')
                    })

                db.save_concept_constituents(concept_code, constituents)
                logger.info(f"  保存成功：{len(constituents)} 只成分股")
                total_count += len(constituents)
                success_count += 1
            else:
                logger.warning(f"  获取失败：成分股为空")

        except Exception as e:
            logger.error(f"  获取失败：{e}")
            continue

    conn.close()
    logger.info(f"完成：成功 {success_count}/{len(to_fetch)} 个板块，共 {total_count} 只成分股")
    return total_count


def main():
    """主函数"""
    print("=" * 70)
    print("成分股数据扩展工具")
    print("=" * 70)

    # 初始化
    db = get_database()
    client = TushareTHSClient(token=settings.tushare_token)

    # 查看当前成分股覆盖
    print("\n【当前成分股覆盖】")
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT concept_code, COUNT(*) as stock_count
        FROM concept_constituent
        GROUP BY concept_code
        ORDER BY stock_count DESC
    """)
    current = cursor.fetchall()
    for row in current:
        print(f"  {row[0]}: {row[1]} 只")

    print(f"总计：{len(current)} 个板块，{sum(r[1] for r in current)} 只成分股")

    # 询问导入方式
    print("\n请选择导入方式:")
    print("  1. 从同花顺接口获取（需要 Tushare 权限）")
    print("  2. 从本地 CSV 文件导入")
    print("  3. 两者都执行")
    print("  4. 自动扩展所有概念板块（推荐）")

    choice = input("\n请输入选择 (1/2/3/4): ").strip()

    new_count = 0

    if choice in ['1', '3']:
        print("\n从同花顺接口获取...")
        new_count += import_major_concepts(db, client)

    if choice in ['2', '3']:
        print("\n从本地 CSV 文件导入...")
        new_count += import_from_local_file(db)

    if choice == '4':
        print("\n自动扩展概念板块...")
        new_count += auto_expand_concepts(db, client, max_concepts=100)

    # 显示结果
    print("\n" + "=" * 70)
    print("导入完成")
    print(f"新增成分股记录：{new_count} 条")

    cursor.execute("""
        SELECT concept_code, COUNT(*) as stock_count
        FROM concept_constituent
        GROUP BY concept_code
        ORDER BY stock_count DESC
    """)
    updated = cursor.fetchall()
    print(f"\n更新后总计：{len(updated)} 个板块，{sum(r[1] for r in updated)} 只成分股")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
