#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据存储管理器
基于 SQLite 数据库的统一数据存取、实时去重、增量更新
"""
import os
import sys
import pandas as pd
from typing import List, Optional, Tuple
from loguru import logger
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from data.database import SQLiteDatabase, get_database


class StorageManager:
    """统一存储管理器（基于 SQLite）"""

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化存储管理器

        Args:
            db: 数据库实例，如果为 None 则使用全局单例
        """
        self.db = db or get_database()
        self.processed_dir = os.path.join(settings.data_dir, "processed")
        self.merged_file = os.path.join(self.processed_dir, "merged_concept_data.csv")
        os.makedirs(self.processed_dir, exist_ok=True)

    def load_merged_data(self, trade_date: str = None) -> pd.DataFrame:
        """
        加载合并数据（从数据库读取）

        Args:
            trade_date: 交易日期，如果为 None 则读取所有数据

        Returns:
            pandas DataFrame
        """
        if trade_date:
            df = self.db.get_all_concept_data(trade_date)
        else:
            df = self.db.get_all_concept_data()

        if df.empty:
            logger.warning("数据库中没有数据，返回空 DataFrame")
            return pd.DataFrame()

        logger.info(f"从数据库加载数据：{len(df):,} 条记录")
        return df

    def save_merged_data(self, df: pd.DataFrame):
        """
        保存合并数据到数据库

        Args:
            df: pandas DataFrame (必须包含 ts_code, trade_date 等列)
        """
        if df.empty:
            logger.warning("数据为空，跳过保存")
            return

        # 批量插入数据库（自动去重）
        self.db.save_concept_daily_batch(df, replace=True)
        logger.info(f"保存数据到数据库：{len(df):,} 条记录")

        # 同时导出 CSV 文件以保持向后兼容
        self.db.export_to_csv(
            "SELECT * FROM concept_daily ORDER BY ts_code, trade_date",
            (),
            self.merged_file
        )

    def incremental_update(
        self,
        new_data: pd.DataFrame,
        key_columns: List[str] = None
    ) -> Tuple[int, int]:
        """
        增量更新合并数据

        Args:
            new_data: 新数据
            key_columns: 去重键（已废弃，数据库自动通过主键去重）

        Returns:
            (新增记录数，总记录数)
        """
        if new_data.empty:
            logger.warning("新数据为空，跳过更新")
            return 0, len(self.load_merged_data())

        # 获取更新前的统计
        stats_before = self.db.get_statistics()

        # 批量插入数据库（自动去重）
        self.db.save_concept_daily_batch(new_data, replace=True)

        # 获取更新后的统计
        stats_after = self.db.get_statistics()

        # 计算新增记录数
        new_count = stats_after['total_records'] - stats_before['total_records']

        logger.info(f"新增 {new_count:,} 条记录，总计 {stats_after['total_records']:,} 条")

        # 更新 CSV 导出
        self.db.export_to_csv(
            "SELECT * FROM concept_daily ORDER BY ts_code, trade_date",
            (),
            self.merged_file
        )

        return new_count, stats_after['total_records']

    def get_latest_date(self, code: str = None) -> Optional[str]:
        """
        获取最新交易日期

        Args:
            code: 板块代码，如果为 None 则返回所有板块的最新日期

        Returns:
            最新日期字符串 (YYYYMMDD)
        """
        return self.db.get_latest_date(code)

    def export_to_csv(self, output_path: str = None, query: str = None):
        """
        导出数据库数据到 CSV

        Args:
            output_path: 输出文件路径
            query: SQL 查询语句，默认导出所有数据
        """
        if output_path is None:
            output_path = self.merged_file

        if query is None:
            query = "SELECT * FROM concept_daily ORDER BY ts_code, trade_date"

        self.db.export_to_csv(query, (), output_path)
        logger.info(f"导出数据到：{output_path}")

    def get_missing_dates(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        获取缺失的交易日期

        Args:
            ts_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            缺失的日期列表
        """
        return self.db.get_missing_dates(ts_code, start_date, end_date)

    def get_data_range(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定范围的数据

        Args:
            code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            过滤后的 DataFrame
        """
        return self.db.get_data_range(code, start_date, end_date)

    def cleanup_raw_files(self, keep_history: bool = False) -> int:
        """
        清理 raw 目录的单板块 CSV 文件（迁移到数据库后）

        Args:
            keep_history: 是否保留历史合集文件

        Returns:
            删除的文件数
        """
        if not os.path.exists(settings.raw_data_dir):
            return 0

        deleted = 0
        kept = 0

        # 需要保留的元数据文件
        metadata_files = [
            'ths_indices.csv', 'ths_industries_l1.csv', 'ths_industries_l2.csv',
            'ths_name_mapping.csv', 'stock_basic.csv'
        ]

        for f in os.listdir(settings.raw_data_dir):
            filepath = os.path.join(settings.raw_data_dir, f)

            # 跳过目录
            if not os.path.isfile(filepath):
                continue

            # 保留元数据文件
            if f in metadata_files:
                kept += 1
                continue

            # 跳过数据库文件
            if f.endswith('.db') or f.endswith('.db-wal') or f.endswith('.db-shm'):
                kept += 1
                continue

            # 删除单板块文件（ths_{code}.TI.csv 或 ths_{code}_TI.csv 或 ths_{code}.csv）
            if f.startswith('ths_') and ('.TI.csv' in f or f.endswith('_TI.csv') or f.endswith('.csv')):
                # 跳过合集文件
                if 'all_history' in f:
                    if keep_history:
                        kept += 1
                    else:
                        os.remove(filepath)
                        deleted += 1
                        logger.debug(f"删除合集文件：{f}")
                    continue

                os.remove(filepath)
                deleted += 1
                logger.debug(f"删除单板块文件：{f}")
                continue

        logger.info(f"清理完成：删除 {deleted} 个文件，保留 {kept} 个文件")
        return deleted

    def sync_from_raw(self) -> Tuple[int, int]:
        """
        将 raw 目录的零散 CSV 数据导入到数据库

        Returns:
            (导入记录数，总记录数)
        """
        from data.data_organizer import DataOrganizer

        logger.info("开始同步 raw 目录数据到数据库...")

        # 使用 DataOrganizer 合并所有 CSV 数据
        organizer = DataOrganizer()
        merged = organizer.merge_all_data()

        if len(merged) == 0:
            logger.warning("未找到任何 CSV 数据")
            return 0, 0

        # 导入到数据库
        self.db.save_concept_daily_batch(merged, replace=True)

        # 导出合并文件
        self.db.export_to_csv(
            "SELECT * FROM concept_daily ORDER BY ts_code, trade_date",
            (),
            self.merged_file
        )

        # 清理 raw 目录的单板块文件
        deleted = self.cleanup_raw_files(keep_history=False)
        logger.info(f"清理 {deleted} 个冗余文件")

        stats = self.db.get_statistics()
        return stats['total_records'], stats['total_records']

    def verify_data_integrity(self) -> dict:
        """
        验证数据完整性

        Returns:
            验证结果
        """
        stats = self.db.get_statistics()

        # 检查空值
        df = self.load_merged_data()
        null_counts = df.isnull().sum() if not df.empty else {}

        stats["null_fields"] = {k: int(v) for k, v in null_counts.items() if v > 0}

        status = "ok" if stats.get('duplicates', 0) == 0 else "warning"
        stats["status"] = status

        logger.info(f"数据验证：{status}")
        logger.info(f"  总记录：{stats['total_records']:,}")
        logger.info(f"  板块数：{stats['concept_count']}")
        logger.info(f"  日期范围：{stats['date_range']}")

        if stats.get('duplicates', 0) > 0:
            logger.warning(f"  发现 {stats['duplicates']:,} 条重复记录")

        return stats


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="存储管理器")
    parser.add_argument("--action", choices=["verify", "cleanup", "stats"],
                       default="stats", help="操作类型")

    args = parser.parse_args()

    manager = StorageManager()

    if args.action == "verify":
        result = manager.verify_data_integrity()
        print(f"\n验证状态：{result['status']}")
    elif args.action == "cleanup":
        deleted = manager.cleanup_raw_files()
        print(f"\n清理完成：删除 {deleted} 个文件")
    elif args.action == "stats":
        result = manager.verify_data_integrity()
        print(f"\n=== 数据统计 ===")
        print(f"总记录数：{result['total_records']:,}")
        print(f"板块数量：{result['concept_count']}")
        print(f"日期范围：{result['date_range'][0]} - {result['date_range'][1]}")
        if result.get('duplicates', 0) > 0:
            print(f"重复记录：{result['duplicates']:,}")


if __name__ == "__main__":
    main()
