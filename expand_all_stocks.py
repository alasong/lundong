#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
扩展成分股数据 - 自动获取所有 A 股上市公司
并批量导入主要板块成分股
"""
import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import sqlite3
import pandas as pd
from loguru import logger
from data.database import get_database
from data.tushare_ths_client import TushareTHSClient
from data.stock_collector import StockCollector
from config import settings


def get_all_a_stocks(client: TushareTHSClient) -> list:
    """
    获取所有 A 股上市公司列表

    Returns:
        股票列表 [{'ts_code': '000001.SZ', 'name': '平安银行'}, ...]
    """
    logger.info("获取 A 股上市公司列表...")

    try:
        # 获取 Stock basic 数据
        df = client.get_stock_list()

        if df is not None and len(df) > 0:
            stocks = []
            for _, row in df.iterrows():
                stocks.append({
                    'ts_code': row['ts_code'],
                    'name': row.get('name', ''),
                    'industry': row.get('industry', ''),
                    'area': row.get('area', ''),
                    'market': row.get('market', '')
                })
            logger.info(f"获取到 {len(stocks)} 只 A 股上市公司")
            return stocks
        else:
            logger.warning("返回空数据")
            return []

    except Exception as e:
        logger.error(f"获取失败：{e}")
        return []


def get_concept_list(client: TushareTHSClient) -> list:
    """
    获取所有概念板块列表

    Returns:
        概念板块列表 [{'ts_code': '885311.TI', 'name': '半导体'}, ...]
    """
    logger.info("获取概念板块列表...")

    try:
        df = client.pro.concept()

        if df is not None and len(df) > 0:
            concepts = []
            for _, row in df.iterrows():
                concepts.append({
                    'ts_code': row['ts_code'],
                    'name': row.get('name', ''),
                    'src': row.get('src', ''),
                })
            logger.info(f"获取到 {len(concepts)} 个概念板块")
            return concepts
        else:
            logger.warning("返回空数据")
            return []

    except Exception as e:
        logger.error(f"获取失败：{e}")
        return []


def get_ths_industry_constituents(client: TushareTHSClient, index_code: str) -> list:
    """
    获取同花顺行业成分股

    Args:
        client: Tushare 客户端
        index_code: 行业代码 (e.g., '881101.TI')

    Returns:
        成分股列表 [{'stock_code': '000001.SZ', 'stock_name': '平安银行'}, ...]
    """
    try:
        # 使用 ths_member 接口 - 返回全市场成分股总表
        df = client.pro.ths_member(index_code=index_code)

        if df is not None and len(df) > 0:
            # 过滤出当前行业的成分股
            # ts_code 是行业指数代码，con_code 是个股代码
            industry_stocks = df[df['ts_code'] == index_code]

            if len(industry_stocks) == 0:
                logger.debug(f"  {index_code} 无成分股")
                return []

            constituents = []
            for _, row in industry_stocks.iterrows():
                constituents.append({
                    'stock_code': row.get('con_code', ''),
                    'stock_name': row.get('con_name', ''),
                    'concept_code': index_code
                })
            return constituents
        else:
            return []

    except Exception as e:
        logger.error(f"获取 {index_code} 成分股失败：{e}")
        return []


def get_major_ths_industries() -> list:
    """
    获取主要同花顺行业代码列表（90 个一级行业）
    """
    return [
        # 农林牧渔
        '881101.TI', '881102.TI', '881103.TI',
        # 周期资源
        '881105.TI', '881107.TI', '881108.TI', '881109.TI', '881112.TI',
        '881114.TI', '881115.TI', '881116.TI', '881117.TI', '881118.TI',
        # 制造
        '881119.TI', '881120.TI', '881121.TI', '881122.TI', '881123.TI',
        '881124.TI', '881125.TI', '881126.TI', '881127.TI', '881128.TI',
        # TMT
        '881129.TI', '881130.TI', '881131.TI', '881132.TI', '881133.TI',
        '881134.TI', '881135.TI', '881136.TI', '881137.TI', '881138.TI',
        # 医药消费
        '881139.TI', '881140.TI', '881141.TI', '881142.TI', '881143.TI',
        '881144.TI', '881145.TI', '881146.TI', '881147.TI', '881148.TI',
        # 金融地产
        '881149.TI', '881150.TI', '881151.TI', '881152.TI', '881153.TI',
        '881154.TI', '881155.TI', '881156.TI', '881157.TI', '881158.TI',
        # 其他
        '881159.TI', '881160.TI', '881161.TI', '881162.TI', '881163.TI',
        '881164.TI', '881165.TI', '881166.TI', '881167.TI', '881168.TI',
        '881169.TI', '881170.TI', '881171.TI', '881172.TI', '881173.TI',
        '881174.TI', '881175.TI', '881176.TI', '881177.TI', '881178.TI',
        '881179.TI', '881180.TI', '881181.TI', '881182.TI', '881183.TI',
        '881184.TI', '881185.TI', '881186.TI', '881187.TI', '881188.TI',
        '881189.TI', '881190.TI', '881191.TI', '881192.TI', '881193.TI',
        '881194.TI', '881195.TI', '881196.TI', '881197.TI', '881198.TI',
    ]


def get_major_concepts() -> list:
    """
    获取主要板块代码列表（50+ 板块）
    """
    return [
        # ==================== 科技/半导体 ====================
        '885311.TI', '885368.TI', '885522.TI', '885320.TI', '885526.TI', '885321.TI',
        # 人工智能/科技
        '885394.TI', '885500.TI', '885517.TI', '885398.TI', '885357.TI', '885396.TI',
        '885553.TI', '885386.TI', '885509.TI',
        # 电子
        '885350.TI', '885346.TI', '885518.TI', '885324.TI',
        # ==================== 医药医疗 ====================
        '885388.TI', '885356.TI', '885329.TI', '885330.TI', '885539.TI',
        '885367.TI', '885333.TI',
        # ==================== 大消费 ====================
        '885369.TI', '885373.TI', '885375.TI', '885372.TI', '885377.TI',
        '885374.TI', '885364.TI', '885365.TI', '885338.TI', '885380.TI', '885378.TI',
        # ==================== 新能源 ====================
        '885355.TI', '885363.TI', '885358.TI', '885361.TI', '885528.TI',
        '885323.TI', '885536.TI', '885532.TI',
        # ==================== 高端制造 ====================
        '885319.TI', '885520.TI', '885354.TI', '885351.TI', '885352.TI',
        '885353.TI', '885322.TI',
        # ==================== 周期资源 ====================
        '885317.TI', '885326.TI', '885348.TI', '885343.TI', '885339.TI',
        '885340.TI', '885341.TI', '885342.TI', '885344.TI', '885345.TI',
        '885318.TI', '885316.TI', '885325.TI',
        # ==================== 金融 ====================
        '885312.TI', '885314.TI', '885315.TI', '885313.TI', '885392.TI',
        # ==================== 其他 ====================
        '885334.TI', '885335.TI', '885336.TI', '885337.TI', '885347.TI',
        '885349.TI', '885360.TI', '885362.TI', '885381.TI', '885382.TI',
        '885383.TI', '885384.TI', '885385.TI', '885387.TI', '885389.TI',
        '885390.TI', '885391.TI', '885393.TI', '885395.TI', '885397.TI',
        '885399.TI', '885400.TI', '885401.TI', '885402.TI', '885403.TI',
        '885404.TI', '885405.TI', '885406.TI', '885407.TI', '885408.TI',
        '885409.TI', '885410.TI', '885411.TI', '885412.TI', '885413.TI',
    ]


def expand_all():
    """
    扩展成分股数据
    """
    # 初始化
    db = get_database()
    collector = StockCollector()

    # 1. 采集股票列表
    logger.info("步骤 1: 采集 A 股上市公司列表...")
    stock_count = collector.collect_stock_list()
    logger.info(f"股票列表采集完成：{stock_count} 只")

    # 2. 获取同花顺行业成分股
    ths_industries = get_major_ths_industries()
    logger.info(f"步骤 2: 计划获取 {len(ths_industries)} 个同花顺一级行业成分股")

    total_constituents = 0
    success_count = 0
    retry_count = 0

    for i, index_code in enumerate(ths_industries):
        logger.info(f"[{i+1}/{len(ths_industries)}] 获取 {index_code} 成分股...")

        # 重试机制（API 限流时）
        constituents = []
        for attempt in range(3):
            try:
                constituents = get_ths_industry_constituents(collector.ths_client, index_code)
                if constituents:
                    break
                retry_count += 1
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"  重试 ({attempt+1}/3): {e}")
                    import time
                    time.sleep(2)
                else:
                    logger.error(f"  最终失败：{e}")

        if constituents:
            # 提取股票列表
            stock_list = [{'stock_code': c['stock_code'], 'stock_name': c['stock_name']}
                         for c in constituents]

            db.save_concept_constituents(index_code, stock_list)
            logger.info(f"  保存 {len(constituents)} 只成分股")
            total_constituents += len(constituents)
            success_count += 1
        else:
            logger.warning(f"  获取失败或无成分股")

    logger.info(f"\n完成：成功 {success_count}/{len(ths_industries)} 个板块")
    logger.info(f"共导入 {total_constituents} 条成分股记录")

    # 显示统计
    conn2 = sqlite3.connect('data/stock.db')
    cursor2 = conn2.cursor()
    cursor2.execute("""
        SELECT concept_code, COUNT(*) as stock_count
        FROM concept_constituent
        GROUP BY concept_code
        ORDER BY stock_count DESC
    """)

    print("\n" + "=" * 70)
    print("成分股统计（TOP 20）")
    print("=" * 70)
    for row in cursor2.fetchmany(20):
        print(f"  {row[0]}: {row[1]} 只")

    cursor2.execute("SELECT COUNT(DISTINCT concept_code) FROM concept_constituent")
    concept_count = cursor2.fetchone()[0]

    cursor2.execute("SELECT COUNT(DISTINCT stock_code) FROM concept_constituent")
    unique_stocks = cursor2.fetchone()[0]

    print("=" * 70)
    print(f"总计：{concept_count} 个板块，{unique_stocks} 只不同股票")
    print("=" * 70)

    conn2.close()

    return total_constituents


def main():
    """主函数"""
    print("=" * 70)
    print("A 股成分股数据扩展工具")
    print("=" * 70)
    print()
    print("此工具将:")
    print("  1. 获取所有 A 股上市公司列表")
    print("  2. 获取 50+ 主要板块成分股数据")
    print("  3. 保存到数据库")
    print()

    confirm = input("是否继续？(y/n): ").strip().lower()

    if confirm != 'y':
        print("已取消")
        return

    try:
        expand_all()
        print("\n完成!")
    except Exception as e:
        logger.error(f"执行失败：{e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
