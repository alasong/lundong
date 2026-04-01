#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单股票列表采集器
获取全部A股股票列表并保存到现有stock_daily表
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from loguru import logger
import tushare as ts
from config import settings
from data.database import get_database


def main():
    """主函数"""
    logger.info("开始采集完整A股股票列表")

    # 初始化Tushare
    ts.set_token(settings.tushare_token)
    pro = ts.pro_api()

    # 获取全部A股列表
    try:
        df = pro.stock_basic(
            exchange="", list_status="L", fields="ts_code,name,area,industry,list_date"
        )
        logger.info(f"成功获取 {len(df)} 只A股股票")
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        return

    # 准备最小数据集（只包含stock_daily表中存在的列）
    stock_data = []
    today = datetime.now().strftime("%Y%m%d")

    for _, row in df.iterrows():
        stock_data.append(
            {
                "ts_code": row["ts_code"],
                "trade_date": today,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "pre_close": None,
                "change": None,
                "pct_chg": None,
                "vol": None,
                "amount": None,
                "turnover_rate": None,
                "pe": None,
                "pb": None,
                "ps": None,
                "total_mv": None,
                "circ_mv": None,
            }
        )

    # 保存到数据库
    db = get_database()
    try:
        db.batch_insert("stock_daily", stock_data, batch_size=5000)
        logger.info(f"成功保存 {len(stock_data)} 条股票记录到 stock_daily 表")
    except Exception as e:
        logger.error(f"保存到数据库失败: {str(e)}")
        return

    logger.info("完整A股股票列表采集完成")


if __name__ == "__main__":
    main()
