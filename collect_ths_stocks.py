#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
同花顺成分股数据采集脚本
从 Tushare 获取同花顺板块成分股和基本面数据
"""
import sys
import os
import sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import time
from datetime import datetime
from loguru import logger
from data.database import get_database
from data.tushare_ths_client import TushareTHSClient
from config import settings


def collect_ths_constituents(client: TushareTHSClient, db, concept_codes: list = None):
    """
    获取同花顺板块成分股

    由于同花顺成分股接口不可用，使用替代方案：
    1. 从已导入的成分股文件中读取
    2. 或从东财成分股接口获取
    """
    logger.info("获取同花顺板块成分股...")

    # 方案 1：从 dc_member (东财成分股) 接口获取
    # 同花顺板块代码需要转换为东财代码
    all_constituents = []

    for concept_code in (concept_codes or []):
        try:
            # 尝试使用东财成分股接口
            # 注意：同花顺代码格式 885xxx.TI，东财代码格式 BKxxxx
            if concept_code.startswith('885'):
                # 同花顺概念板块，转换为东财代码
                bk_code = 'BK' + concept_code[2:8]  # 885311.TI -> BK0811
                logger.info(f"尝试获取 {concept_code} ({bk_code}) 成分股...")

                df = client.pro.concept_member(concept_code=bk_code)
                if df is not None and len(df) > 0:
                    df['concept_code'] = concept_code
                    all_constituents.append(df)
                    logger.info(f"获取成功：{len(df)} 只成分股")
                else:
                    logger.warning(f"获取失败：{concept_code}")

        except Exception as e:
            logger.error(f"获取 {concept_code} 成分股失败：{e}")
            continue

    if all_constituents:
        result = pd.concat(all_constituents, ignore_index=True)
        return result

    return pd.DataFrame()


def collect_stock_basic_info(client: TushareTHSClient, db, stock_codes: list):
    """
    获取个股基本信息（从 stock_basic 接口）
    """
    logger.info(f"获取 {len(stock_codes)} 只股票的基本信息...")

    all_info = []
    for code in stock_codes:
        try:
            # 从 stock_basic 获取
            df = client.pro.stock_basic(ts_code=code)
            if df is not None and len(df) > 0:
                all_info.append(df.iloc[0])
        except Exception as e:
            logger.error(f"获取 {code} 基本信息失败：{e}")
            continue

    if all_info:
        return pd.DataFrame(all_info)
    return pd.DataFrame()


def collect_daily_basic(client: TushareTHSClient, db, stock_codes: list,
                        start_date: str, end_date: str):
    """
    获取个股每日基本面数据（PE、PB、市值等）
    """
    logger.info(f"获取 {len(stock_codes)} 只股票的基本面数据 ({start_date} - {end_date})...")

    all_data = []
    success_count = 0

    for i, code in enumerate(stock_codes):
        try:
            df = client.pro.daily_basic(ts_code=code,
                                       start_date=start_date,
                                       end_date=end_date)
            if df is not None and len(df) > 0:
                all_data.append(df)
                success_count += 1

                if success_count % 10 == 0:
                    logger.info(f"已获取 {success_count}/{len(stock_codes)} 只股票")
                    time.sleep(0.5)  # 限流

        except Exception as e:
            logger.error(f"获取 {code} 基本面数据失败：{e}")
            continue

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"获取成功：{len(result)} 条记录")
        return result

    return pd.DataFrame()


def main():
    """主函数"""
    print("=" * 70)
    print("同花顺成分股及基本面数据采集脚本")
    print("=" * 70)

    # 初始化
    db = get_database()
    client = TushareTHSClient(token=settings.tushare_token)

    # 从成分股表中获取股票代码
    print("\n[Step 1] 获取成分股列表...")
    conn = sqlite3.connect('data/stock.db')
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT stock_code FROM concept_constituent')
    stock_codes = [row[0] for row in cursor.fetchall()]
    print(f"成分股数量：{len(stock_codes)}")
    print(f"示例：{stock_codes[:10]}")

    # 确定日期范围
    print("\n[Step 2] 确定日期范围...")
    latest_date = db.get_latest_date()
    if latest_date:
        end_date = latest_date
        # 从 2023 年开始采集
        start_date = '20230101'
    else:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = '20230101'

    print(f"日期范围：{start_date} - {end_date}")

    # 采集每日基本面数据
    print("\n[Step 3] 采集每日基本面数据...")
    daily_basic = collect_daily_basic(client, db, stock_codes, start_date, end_date)

    if not daily_basic.empty:
        # 保存到数据库
        print("\n[Step 4] 保存到数据库...")

        # 确保表存在
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_daily_basic (
                ts_code VARCHAR(20),
                trade_date VARCHAR(10),
                close DECIMAL(20,4),
                turnover_rate DECIMAL(20,4),
                pe DECIMAL(20,4),
                pe_ttm DECIMAL(20,4),
                pb DECIMAL(20,4),
                ps DECIMAL(20,4),
                total_mv DECIMAL(20,4),
                circ_mv DECIMAL(20,4),
                total_share DECIMAL(20,4),
                float_share DECIMAL(20,4),
                PRIMARY KEY (ts_code, trade_date)
            )
        ''')

        # 插入数据
        for _, row in daily_basic.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_daily_basic
                    (ts_code, trade_date, close, turnover_rate, pe, pe_ttm, pb, ps,
                     total_mv, circ_mv, total_share, float_share)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('ts_code'),
                    row.get('trade_date'),
                    row.get('close'),
                    row.get('turnover_rate'),
                    row.get('pe'),
                    row.get('pe_ttm'),
                    row.get('pb'),
                    row.get('ps'),
                    row.get('total_mv'),
                    row.get('circ_mv'),
                    row.get('total_share'),
                    row.get('float_share')
                ))
            except Exception as e:
                logger.error(f"保存 {row.get('ts_code')} 失败：{e}")
                continue

        conn.commit()

        # 验证
        cursor.execute('SELECT COUNT(*) FROM stock_daily_basic')
        count = cursor.fetchone()[0]
        print(f"\n保存完成：{count:,} 条记录")

    # 显示统计
    print("\n" + "=" * 70)
    print("数据采集完成")
    print("=" * 70)
    cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM stock_daily_basic')
    stock_count = cursor.fetchone()[0]
    cursor.execute('SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily_basic')
    date_range = cursor.fetchone()
    print(f"股票数量：{stock_count}")
    print(f"日期范围：{date_range[0]} - {date_range[1]}")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
