#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量导入成分股
基于已有股票数据，自动创建主要板块的成分股列表
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import sqlite3
from loguru import logger
from data.database import get_database


# 板块定义（板块代码，板块名称，关键词）
CONCEPT_DEFINITIONS = [
    # 科技/半导体
    ('885311.TI', '半导体', ['半导体', '芯片', '集成电路', '存储']),
    ('885368.TI', '汽车芯片', ['汽车', '芯片', '半导体']),
    ('885522.TI', '芯片', ['芯片', '半导体', '集成电路']),
    ('885320.TI', '集成电路', ['集成电路', '芯片', '半导体']),

    # 人工智能/科技
    ('885394.TI', '人工智能', ['人工', '智能', 'AI', '算法', '语音', '图像']),
    ('885500.TI', '大数据', ['大数据', '数据', '分析']),
    ('885517.TI', '云计算', ['云', '计算', 'SaaS', 'PaaS']),
    ('885398.TI', '5G', ['5G', '通信', '网络']),
    ('885357.TI', '物联网', ['物联', '传感', 'RFID']),

    # 医药医疗
    ('885388.TI', '创新药', ['创新药', '生物药', '制药']),
    ('885356.TI', '医疗器械', ['医疗', '器械', '设备']),
    ('885329.TI', '生物医药', ['生物', '医药', '疫苗', '血液']),

    # 大消费
    ('885366.TI', '消费电子', ['消费电子', '手机', '终端']),
    ('885369.TI', '白酒', ['白酒', '酒']),
    ('885373.TI', '食品饮料', ['食品', '饮料', '乳制品']),

    # 新能源
    ('885355.TI', '光伏', ['光伏', '太阳能', '硅片']),
    ('885363.TI', '锂电池', ['锂电', '电池', '储能']),
    ('885358.TI', '新能源汽车', ['新能源', '汽车', '电动']),
    ('885361.TI', '风电', ['风电', '风力']),

    # 高端制造
    ('885319.TI', '工业母机', ['机床', '数控', '制造']),
    ('885520.TI', '机器人', ['机器人', '自动化', '智能']),
    ('885354.TI', '航天航空', ['航天', '航空', '飞机', '卫星']),

    # 周期
    ('885317.TI', '有色金属', ['有色', '金属', '铜', '铝', '黄金', '稀土']),
    ('885326.TI', '化工', ['化工', '化学', '材料']),
    ('885348.TI', '钢铁', ['钢铁', '钢材']),
    ('885343.TI', '煤炭', ['煤炭', '煤']),

    # 金融
    ('885312.TI', '银行', ['银行']),
    ('885314.TI', '证券', ['证券', '券商', '投资']),
    ('885315.TI', '保险', ['保险']),
]


def get_all_stocks_with_names() -> list:
    """
    从成分股表中获取所有股票及其名称
    """
    conn = sqlite3.connect('data/stock.db')
    cursor = conn.cursor()

    # 从 concept_constituent 获取
    cursor.execute("""
        SELECT DISTINCT stock_code, stock_name
        FROM concept_constituent
    """)
    stocks = cursor.fetchall()

    # 从 stock_daily_basic 获取
    cursor.execute("""
        SELECT DISTINCT b.ts_code,
               (SELECT stock_name FROM concept_constituent
                WHERE stock_code = b.ts_code LIMIT 1) as name
        FROM stock_daily_basic b
    """)
    more_stocks = cursor.fetchall()

    # 合并去重
    stock_dict = {s[0]: s[1] for s in stocks if s[1]}
    for s in more_stocks:
        if s[0] not in stock_dict and s[1]:
            stock_dict[s[0]] = s[1]

    conn.close()

    result = [{'stock_code': k, 'stock_name': v} for k, v in stock_dict.items() if v]
    logger.info(f"获取到 {len(result)} 只股票")
    return result


def match_stocks_to_concept(stocks: list, keywords: list) -> list:
    """
    根据关键词匹配股票到板块

    Args:
        stocks: 股票列表
        keywords: 关键词列表

    Returns:
        匹配的股票列表
    """
    matched = []
    for stock in stocks:
        name = stock.get('stock_name', '')
        # 检查股票名称是否包含任何关键词
        for keyword in keywords:
            if keyword in name:
                matched.append(stock)
                break
    return matched


def bulk_import_constituents():
    """
    批量导入成分股
    """
    db = get_database()

    # 获取所有股票
    logger.info("获取所有股票...")
    all_stocks = get_all_stocks_with_names()

    if not all_stocks:
        logger.error("未获取到任何股票数据")
        return

    total_imported = 0

    # 逐个板块导入
    for concept_code, concept_name, keywords in CONCEPT_DEFINITIONS:
        logger.info(f"处理板块：{concept_code} ({concept_name})")

        # 匹配股票
        matched = match_stocks_to_concept(all_stocks, keywords)

        if not matched:
            logger.warning(f"  未匹配到股票")
            continue

        logger.info(f"  匹配到 {len(matched)} 只股票")

        # 保存到数据库
        constituents = [{'stock_code': s['stock_code'], 'stock_name': s['stock_name']} for s in matched]
        db.save_concept_constituents(concept_code, constituents)

        total_imported += len(matched)
        logger.info(f"  已保存 {len(matched)} 只成分股")

    logger.info(f"\n导入完成，总计导入 {total_imported} 条成分股记录")

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
    print("成分股统计")
    print("=" * 70)
    for row in cursor2.fetchall():
        print(f"  {row[0]}: {row[1]} 只")
    print("=" * 70)

    conn2.close()


def main():
    """主函数"""
    print("=" * 70)
    print("批量导入成分股工具")
    print("=" * 70)

    print("\n此工具将根据股票名称关键词，自动匹配股票到各个板块")
    print("板块定义在 CONCEPT_DEFINITIONS 列表中\n")

    confirm = input("是否继续？(y/n): ").strip().lower()

    if confirm != 'y':
        print("已取消")
        return

    bulk_import_constituents()

    print("\n完成!")


if __name__ == "__main__":
    main()
