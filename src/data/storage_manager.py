#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据存储管理器
统一数据存取、去重、增量更新
"""
import os
import sys
import pandas as pd
from typing import List, Optional, Tuple
from loguru import logger
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class StorageManager:
    """统一存储管理器"""

    def __init__(self):
        self.raw_dir = settings.raw_data_dir
        self.processed_dir = os.path.join(settings.data_dir, "processed")
        self.merged_file = os.path.join(self.processed_dir, "merged_concept_data.csv")
        os.makedirs(self.processed_dir, exist_ok=True)

    def load_merged_data(self) -> pd.DataFrame:
        """加载合并数据"""
        if not os.path.exists(self.merged_file):
            logger.warning("合并文件不存在，返回空 DataFrame")
            return pd.DataFrame()

        df = pd.read_csv(self.merged_file)
        logger.info(f"加载合并数据：{len(df):,} 条记录")
        return df

    def save_merged_data(self, df: pd.DataFrame):
        """保存合并数据"""
        df.to_csv(self.merged_file, index=False)
        logger.info(f"保存合并数据：{len(df):,} 条记录")

    def incremental_update(
        self,
        new_data: pd.DataFrame,
        key_columns: List[str] = None
    ) -> Tuple[int, int]:
        """
        增量更新合并数据

        Args:
            new_data: 新数据
            key_columns: 去重键，默认 ['ts_code', 'trade_date']

        Returns:
            (新增记录数，总记录数)
        """
        if key_columns is None:
            key_columns = ['ts_code', 'trade_date']

        # 加载现有数据
        existing = self.load_merged_data()

        if existing.empty:
            # 首次保存
            self.save_merged_data(new_data)
            return len(new_data), len(new_data)

        # 去重新数据
        new_data = new_data.drop_duplicates(subset=key_columns, keep='last')

        # 找出真正的新增记录（排除已存在的）
        existing_keys = set(zip(existing[key_columns[0]], existing[key_columns[1]]))

        new_records = []
        for _, row in new_data.iterrows():
            key = (row[key_columns[0]], row[key_columns[1]])
            if key not in existing_keys:
                new_records.append(row)

        if not new_records:
            logger.info("没有新增数据")
            return 0, len(existing)

        # 合并并保存
        new_df = pd.concat([existing, pd.DataFrame(new_records)], ignore_index=True)

        # 按板块和日期排序
        new_df = new_df.sort_values(key_columns)

        self.save_merged_data(new_df)

        logger.info(f"新增 {len(new_records):,} 条记录，总计 {len(new_df):,} 条")
        return len(new_records), len(new_df)

    def get_latest_date(self, code: str = None) -> Optional[str]:
        """
        获取最新交易日期

        Args:
            code: 板块代码，如果为 None 则返回所有板块的最新日期

        Returns:
            最新日期字符串 (YYYYMMDD)
        """
        df = self.load_merged_data()

        if df.empty:
            return None

        if code:
            df = df[df['ts_code'] == code]

        if df.empty:
            return None

        return str(df['trade_date'].max())

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
        df = self.load_merged_data()

        if df.empty:
            return pd.DataFrame()

        df = df[
            (df['ts_code'] == code) &
            (df['trade_date'] >= int(start_date)) &
            (df['trade_date'] <= int(end_date))
        ]

        return df

    def cleanup_raw_files(self, keep_history: bool = True) -> int:
        """
        清理单板块文件，只保留合集文件

        Args:
            keep_history: 是否保留历史合集文件

        Returns:
            删除的文件数
        """
        deleted = 0

        for f in os.listdir(self.raw_dir):
            filepath = os.path.join(self.raw_dir, f)

            # 跳过合集文件
            if keep_history and 'all_history' in f:
                continue

            # 跳过其他非数据文件（名称映射、列表文件等）
            if f.endswith('.csv') and not f.startswith('ths_'):
                continue

            # 删除单板块文件（ths_{code}.TI.csv 或 ths_{code}_TI.csv）
            if f.startswith('ths_') and ('.TI.csv' in f or f.endswith('_TI.csv')):
                # 跳过特殊文件
                if f in ['ths_indices.csv', 'ths_industries_l1.csv', 'ths_name_mapping.csv',
                         'tst_indices.csv', 'ths_industries.csv']:
                    continue
                os.remove(filepath)
                deleted += 1
                logger.debug(f"删除：{f}")

        logger.info(f"清理完成：删除 {deleted} 个单板块文件")
        return deleted

    def verify_data_integrity(self) -> dict:
        """
        验证数据完整性

        Returns:
            验证结果
        """
        df = self.load_merged_data()

        if df.empty:
            return {"status": "error", "message": "数据为空"}

        # 检查重复
        dup_count = df.duplicated(subset=['ts_code', 'trade_date']).sum()

        # 检查空值
        null_counts = df.isnull().sum()

        # 统计信息
        stats = {
            "total_records": len(df),
            "unique_codes": df['ts_code'].nunique(),
            "date_range": (str(df['trade_date'].min()), str(df['trade_date'].max())),
            "duplicates": int(dup_count),
            "null_fields": {k: int(v) for k, v in null_counts.items() if v > 0}
        }

        status = "ok" if dup_count == 0 else "warning"
        stats["status"] = status

        logger.info(f"数据验证：{status}")
        logger.info(f"  总记录：{stats['total_records']:,}")
        logger.info(f"  板块数：{stats['unique_codes']}")
        logger.info(f"  日期范围：{stats['date_range']}")

        if dup_count > 0:
            logger.warning(f"  发现 {dup_count:,} 条重复记录")

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
        print(f"板块数量：{result['unique_codes']}")
        print(f"日期范围：{result['date_range'][0]} - {result['date_range'][1]}")
        if result.get('duplicates', 0) > 0:
            print(f"重复记录：{result['duplicates']:,}")


if __name__ == "__main__":
    main()
