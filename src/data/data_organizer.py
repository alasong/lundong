#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据整理工具
合并、去重、优化数据存储
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


class DataOrganizer:
    """数据整理器"""

    def __init__(self, raw_dir: str = None):
        self.raw_dir = raw_dir or settings.raw_data_dir
        self.processed_dir = os.path.join(settings.data_dir, "processed")
        os.makedirs(self.processed_dir, exist_ok=True)

    def merge_all_data(self, output_file: str = "merged_concept_data.csv") -> pd.DataFrame:
        """
        合并所有概念板块数据

        Args:
            output_file: 输出文件名

        Returns:
            合并后的 DataFrame
        """
        logger.info("开始合并数据...")

        # 加载所有 TI.csv 文件
        ti_files = [f for f in os.listdir(self.raw_dir) if f.endswith('_TI.csv')]
        history_files = [f for f in os.listdir(self.raw_dir) if 'all_history' in f and f.endswith('.csv')]

        all_data = []

        # 处理单板块文件
        logger.info(f"发现 {len(ti_files)} 个单板块文件")
        for f in ti_files:
            try:
                df = pd.read_csv(os.path.join(self.raw_dir, f))
                if len(df) > 0:
                    all_data.append(df)
            except Exception as e:
                logger.warning(f"加载失败：{f} - {e}")

        # 处理合集文件
        logger.info(f"发现 {len(history_files)} 个合集文件")
        for f in history_files:
            try:
                df = pd.read_csv(os.path.join(self.raw_dir, f))
                if len(df) > 0:
                    all_data.append(df)
            except Exception as e:
                logger.warning(f"加载失败：{f} - {e}")

        if not all_data:
            logger.error("未找到任何数据")
            return pd.DataFrame()

        # 合并
        merged = pd.concat(all_data, ignore_index=True)

        # 去重
        logger.info(f"合并后记录数：{len(merged):,}")
        logger.info("执行去重...")
        merged = merged.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')

        # 按板块和日期排序
        merged = merged.sort_values(['ts_code', 'trade_date'])

        # 保存
        output_path = os.path.join(self.processed_dir, output_file)
        merged.to_csv(output_path, index=False)

        logger.info("=" * 60)
        logger.info("数据合并完成")
        logger.info(f"总记录数：{len(merged):,}")
        logger.info(f"唯一下板块：{merged['ts_code'].nunique()}")
        logger.info(f"日期范围：{merged['trade_date'].min()} - {merged['trade_date'].max()}")
        logger.info(f"输出文件：{output_path}")
        logger.info("=" * 60)

        return merged

    def split_by_concept(self, data: pd.DataFrame = None):
        """
        按板块拆分数据

        Args:
            data: 输入数据，如果为 None 则加载 merged 文件
        """
        if data is None:
            merged_file = os.path.join(self.processed_dir, "merged_concept_data.csv")
            if os.path.exists(merged_file):
                data = pd.read_csv(merged_file)
            else:
                logger.error("未找到合并文件，请先执行 merge_all_data")
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
        对所有单文件去重

        Args:
            check_column: 用于检查重复的列
        """
        logger.info("执行批量去重...")

        ti_files = [f for f in os.listdir(self.raw_dir) if f.endswith('_TI.csv')]

        total_removed = 0
        files_processed = 0

        for f in ti_files:
            filepath = os.path.join(self.raw_dir, f)
            try:
                df = pd.read_csv(filepath)
                original_count = len(df)

                if check_column in df.columns:
                    df_dedup = df.drop_duplicates(subset=[check_column], keep='last')
                else:
                    df_dedup = df

                dedup_count = len(df_dedup)
                removed = original_count - dedup_count

                if removed > 0:
                    df_dedup.to_csv(filepath, index=False)
                    total_removed += removed
                    files_processed += 1
                    logger.debug(f"{f}: 移除 {removed} 条重复")

            except Exception as e:
                logger.warning(f"处理失败：{f} - {e}")

        logger.info(f"去重完成：处理 {files_processed} 个文件，移除 {total_removed} 条重复记录")

    def update_from_history(self, history_file: str = None):
        """
        从合集文件更新单板块文件

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

        # 按板块分组保存
        grouped = history_data.groupby('ts_code')
        logger.info(f"共 {len(grouped)} 个板块")

        updated_count = 0

        for code, group in grouped:
            filename = f"ths_{code}.csv"
            filepath = os.path.join(self.raw_dir, filename)

            # 如果单文件不存在或记录数少于合集，则更新
            if not os.path.exists(filepath):
                group.to_csv(filepath, index=False)
                updated_count += 1
            else:
                existing = pd.read_csv(filepath)
                if len(existing) < len(group) * 0.9:  # 单文件记录明显少于合集
                    group.to_csv(filepath, index=False)
                    updated_count += 1

        logger.info(f"更新完成：更新/创建 {updated_count} 个文件")

    def organize_directory(self):
        """
        完整整理流程
        """
        logger.info("=" * 60)
        logger.info("开始整理数据目录")
        logger.info("=" * 60)

        # 1. 去重
        self.remove_duplicates()

        # 2. 从合集更新单文件
        self.update_from_history()

        # 3. 合并所有数据
        self.merge_all_data()

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
