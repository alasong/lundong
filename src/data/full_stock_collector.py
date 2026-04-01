#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
完整A股股票列表采集器
获取全部A股股票并加入数据库
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
from loguru import logger
import time

from config import settings
from data.database import get_database, SQLiteDatabase
import tushare as ts


class FullStockCollector:
    """完整A股股票列表采集器"""

    def __init__(self, db: SQLiteDatabase = None):
        """初始化"""
        self.db = db or get_database()
        ts.set_token(settings.tushare_token)
        self.pro = ts.pro_api()
        logger.info("完整A股股票采集器初始化完成")

    def get_all_a_stocks(self) -> pd.DataFrame:
        """获取全部A股股票列表"""
        logger.info("获取全部A股股票列表...")

        # 获取全部A股基本信息
        df = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,name,area,industry,list_date,delist_date",
        )

        if df is None or df.empty:
            logger.error("获取A股股票列表失败")
            return pd.DataFrame()

        logger.info(f"获取到 {len(df)} 只A股股票")
        return df

    def save_stock_list_to_db(self, df: pd.DataFrame):
        """将股票列表保存到数据库"""
        if df.empty:
            logger.warning("股票列表为空，跳过保存")
            return

        # 准备数据
        stock_list_data = []
        for _, row in df.iterrows():
            stock_list_data.append(
                {
                    "ts_code": row["ts_code"],
                    "name": row["name"],
                    "area": row["area"],
                    "industry": row["industry"],
                    "list_date": row["list_date"],
                    "delist_date": row["delist_date"]
                    if pd.notna(row["delist_date"])
                    else None,
                }
            )

        # 创建股票列表表（如果不存在）
        with self.db.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_list (
                    ts_code TEXT PRIMARY KEY,
                    name TEXT,
                    area TEXT,
                    industry TEXT,
                    list_date TEXT,
                    delist_date TEXT
                )
            """)

            # 批量插入或更新
            conn.executemany(
                """
                INSERT OR REPLACE INTO stock_list 
                (ts_code, name, area, industry, list_date, delist_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    (
                        item["ts_code"],
                        item["name"],
                        item["area"],
                        item["industry"],
                        item["list_date"],
                        item["delist_date"],
                    )
                    for item in stock_list_data
                ],
            )

            conn.commit()
            logger.info(f"成功保存 {len(stock_list_data)} 只股票到 stock_list 表")

    def collect_full_stock_list(self):
        """采集完整A股股票列表"""
        logger.info("=" * 60)
        logger.info("开始采集完整A股股票列表")
        logger.info("=" * 60)

        # 获取股票列表
        df = self.get_all_a_stocks()
        if df.empty:
            logger.error("未能获取股票列表，退出")
            return False

        # 保存到数据库
        self.save_stock_list_to_db(df)

        logger.info("=" * 60)
        logger.info("完整A股股票列表采集完成")
        logger.info("=" * 60)
        return True


def main():
    """主函数"""
    collector = FullStockCollector()
    collector.collect_full_stock_list()


if __name__ == "__main__":
    main()
