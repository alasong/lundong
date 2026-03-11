#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据整理工具
从数据库读取数据，导出 CSV 供下游分析使用
"""
import os
import sys
import pandas as pd
import numpy as np
from typing import List, Dict
from loguru import logger
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from data.database import get_database


class DataOrganizer:
    """数据整理器（基于 SQLite 数据库）"""

    def __init__(self, db=None):
        """
        初始化整理器

        Args:
            db: 数据库实例，如果为 None 则使用全局单例
        """
        self.db = db or get_database()
        self.processed_dir = os.path.join(settings.data_dir, "processed")
        self.raw_dir = settings.raw_data_dir
        os.makedirs(self.processed_dir, exist_ok=True)

    def merge_all_data(self, output_file: str = "merged_concept_data.csv") -> pd.DataFrame:
        """
        从数据库导出合并数据

        Args:
            output_file: 输出文件名

        Returns:
            合并后的 DataFrame
        """
        logger.info("从数据库导出合并数据...")

        # 从数据库查询所有数据
        df = self.db.get_all_concept_data()

        if df.empty:
            logger.warning("数据库中没有数据")
            return pd.DataFrame()

        # 去重（数据库已有唯一约束，这里作为额外保证）
        df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')

        # 按板块和日期排序
        df = df.sort_values(['ts_code', 'trade_date'])

        # 保存 CSV
        output_path = os.path.join(self.processed_dir, output_file)
        df.to_csv(output_path, index=False)

        logger.info("=" * 60)
        logger.info("数据导出完成")
        logger.info(f"总记录数：{len(df):,}")
        logger.info(f"唯一下板块：{df['ts_code'].nunique()}")
        logger.info(f"日期范围：{df['trade_date'].min()} - {df['trade_date'].max()}")
        logger.info(f"输出文件：{output_path}")
        logger.info("=" * 60)

        return df

    def split_by_concept(self, data: pd.DataFrame = None):
        """
        按板块拆分数据

        Args:
            data: 输入数据，如果为 None 则从数据库加载
        """
        if data is None:
            data = self.db.get_all_concept_data()
            if data.empty:
                logger.error("数据库中没有数据")
                return

        logger.info("按板块拆分数据...")

        # 按板块分组
        grouped = data.groupby('ts_code')
        logger.info(f"共 {len(grouped)} 个板块")

        output_dir = os.path.join(self.processed_dir, "by_concept")
        os.makedirs(output_dir, exist_ok=True)

        for code, group in grouped:
            filename = f"ths_{code}.csv"
            filepath = os.path.join(output_dir, filename)
            group.to_csv(filepath, index=False)

        logger.info(f"拆分完成：{len(grouped)} 个文件输出到 {output_dir}")

    def remove_duplicates(self, check_column: str = 'trade_date'):
        """
        数据库已有唯一约束，此方法不再需要
        去重操作由数据库在写入时自动完成
        """
        logger.info("数据库已启用实时去重（唯一约束），无需手动去重")
        logger.info("如需检查重复数据，可调用 verify_data_integrity()")

    def update_from_history(self, history_file: str = None):
        """
        从 CSV 合集文件导入数据到数据库（迁移工具）

        Args:
            history_file: 合集文件名，如果为 None 则自动查找最新的
        """
        if history_file is None:
            # 查找最新的合集文件
            history_files = [f for f in os.listdir(self.raw_dir)
                           if 'all_history' in f and f.endswith('.csv')]
            if history_files:
                history_files.sort(reverse=True)
                history_file = history_files[0]
                logger.info(f"使用合集文件：{history_file}")
            else:
                logger.error("未找到合集文件")
                return

        history_path = os.path.join(self.raw_dir, history_file)
        logger.info(f"加载合集文件：{history_path}")

        history_data = pd.read_csv(history_path)
        logger.info(f"合集记录数：{len(history_data):,}")

        # 批量导入到数据库
        self.db.save_concept_daily_batch(history_data, replace=True)

        logger.info(f"导入完成：{len(history_data):,} 条记录已写入数据库")

    def organize_directory(self):
        """
        完整整理流程（从数据库导出 CSV）
        """
        logger.info("=" * 60)
        logger.info("开始整理数据目录")
        logger.info("=" * 60)

        # 从数据库导出合并数据
        self.merge_all_data()

        # 按板块拆分
        self.split_by_concept()

        logger.info("数据整理完成")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="数据整理工具")
    parser.add_argument("--action", choices=["merge", "split", "dedup", "update", "organize"],
                       default="organize", help="操作类型")
    parser.add_argument("--output", type=str, help="输出文件名")
    parser.add_argument("--history", type=str, help="合集文件名")

    args = parser.parse_args()

    organizer = DataOrganizer()

    if args.action == "merge":
        organizer.merge_all_data(args.output or "merged_concept_data.csv")
    elif args.action == "split":
        organizer.split_by_concept()
    elif args.action == "dedup":
        organizer.remove_duplicates()
    elif args.action == "update":
        organizer.update_from_history(args.history)
    elif args.action == "organize":
        organizer.organize_directory()


if __name__ == "__main__":
    main()
