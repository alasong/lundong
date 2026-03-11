#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CSV 数据迁移工具
将现有 CSV 文件导入 SQLite 数据库
"""
import os
import sys
import pandas as pd
from typing import List, Tuple
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from data.database import SQLiteDatabase, init_database
from data.data_organizer import DataOrganizer


class CSVMigrator:
    """CSV 到 SQLite 迁移工具"""

    def __init__(self, db_path: str = None):
        """
        初始化迁移工具

        Args:
            db_path: 数据库文件路径
        """
        # 初始化数据库
        self.db = init_database(db_path)
        self.raw_dir = settings.raw_data_dir
        self.processed_dir = os.path.join(settings.data_dir, "processed")

    def migrate_csv_files(self) -> Tuple[int, int]:
        """
        迁移所有 CSV 文件到数据库

        Returns:
            (成功记录数，失败记录数)
        """
        logger.info("=" * 60)
        logger.info("开始迁移 CSV 数据到 SQLite")
        logger.info("=" * 60)

        success_count = 0
        error_count = 0
        total_records = 0

        # 查找所有 CSV 文件
        csv_files = self._find_csv_files()
        logger.info(f"发现 {len(csv_files)} 个 CSV 文件")

        for filepath in csv_files:
            filename = os.path.basename(filepath)
            logger.info(f"处理文件：{filename}")

            try:
                df = pd.read_csv(filepath)

                if df.empty:
                    logger.warning(f"文件为空：{filename}")
                    continue

                # 检查必要的列
                if 'ts_code' not in df.columns or 'trade_date' not in df.columns:
                    logger.warning(f"缺少必要列，跳过：{filename}")
                    error_count += 1
                    continue

                # 批量插入数据库
                self.db.save_concept_daily_batch(df, replace=True)

                records = len(df)
                total_records += records
                success_count += 1
                logger.info(f"  导入成功：{records:,} 条记录")

            except Exception as e:
                logger.error(f"导入失败：{filename} - {e}")
                error_count += 1

        logger.info("=" * 60)
        logger.info("迁移完成")
        logger.info(f"成功文件数：{success_count}")
        logger.info(f"失败文件数：{error_count}")
        logger.info(f"总记录数：{total_records:,}")

        # 显示数据库统计
        stats = self.db.get_statistics()
        logger.info(f"数据库总记录：{stats['total_records']:,}")
        logger.info(f"数据库板块数：{stats['concept_count']}")
        logger.info(f"日期范围：{stats['date_range'][0]} - {stats['date_range'][1]}")
        logger.info("=" * 60)

        return success_count, error_count

    def _find_csv_files(self) -> List[str]:
        """查找所有需要迁移的 CSV 文件"""
        csv_files = []

        if not os.path.exists(self.raw_dir):
            logger.warning(f"raw 目录不存在：{self.raw_dir}")
            return csv_files

        for f in os.listdir(self.raw_dir):
            if not f.endswith('.csv'):
                continue

            # 跳过元数据文件
            if f in ['ths_indices.csv', 'ths_industries_l1.csv', 'ths_industries_l2.csv',
                     'ths_name_mapping.csv', 'stock_basic.csv']:
                continue

            # 跳过合集文件（会被单独处理）
            if 'all_history' in f:
                continue

            filepath = os.path.join(self.raw_dir, f)
            csv_files.append(filepath)

        # 查找合集文件
        history_files = [f for f in os.listdir(self.raw_dir)
                        if 'all_history' in f and f.endswith('.csv')]

        # 优先使用最新的合集文件
        if history_files:
            history_files.sort(reverse=True)
            latest_history = history_files[0]
            logger.info(f"使用合集文件：{latest_history}")
            csv_files.append(os.path.join(self.raw_dir, latest_history))

        # 查找 processed 目录的合并文件
        merged_file = os.path.join(self.processed_dir, "merged_concept_data.csv")
        if os.path.exists(merged_file):
            logger.info(f"使用合并文件：merged_concept_data.csv")
            csv_files.append(merged_file)

        return csv_files

    def migrate_from_organizer(self) -> bool:
        """
        使用 DataOrganizer 迁移数据（直接从 CSV 文件读取）

        Returns:
            是否成功
        """
        logger.info("从 CSV 文件迁移数据...")

        try:
            # 直接读取 CSV 文件
            csv_files = self._find_csv_files()

            if not csv_files:
                logger.warning("没有找到任何 CSV 文件")
                return False

            total_records = 0

            for filepath in csv_files:
                filename = os.path.basename(filepath)
                logger.info(f"处理文件：{filename}")

                try:
                    df = pd.read_csv(filepath)

                    if df.empty:
                        logger.warning(f"文件为空：{filename}")
                        continue

                    # 检查必要的列
                    if 'ts_code' not in df.columns or 'trade_date' not in df.columns:
                        logger.warning(f"缺少必要列，跳过：{filename}")
                        continue

                    # 批量插入数据库
                    self.db.save_concept_daily_batch(df, replace=True)

                    records = len(df)
                    total_records += records
                    logger.info(f"  导入成功：{records:,} 条记录")

                except Exception as e:
                    logger.error(f"导入失败：{filename} - {e}")

            logger.info(f"迁移完成：{total_records:,} 条记录")
            return True

        except Exception as e:
            logger.error(f"迁移失败：{e}")
            return False

    def cleanup_csv_files(self, keep_metadata: bool = True) -> int:
        """
        清理已迁移的 CSV 文件

        Args:
            keep_metadata: 是否保留元数据文件

        Returns:
            删除的文件数
        """
        logger.info("清理已迁移的 CSV 文件...")

        deleted = 0

        # 需要保留的元数据文件
        metadata_files = [
            'ths_indices.csv', 'ths_industries_l1.csv', 'ths_industries_l2.csv',
            'ths_name_mapping.csv', 'stock_basic.csv'
        ]

        # 清理 raw 目录
        if os.path.exists(self.raw_dir):
            for f in os.listdir(self.raw_dir):
                if not f.endswith('.csv'):
                    continue

                # 保留元数据文件
                if keep_metadata and f in metadata_files:
                    continue

                # 跳过合集文件（用户可能想保留）
                if 'all_history' in f:
                    continue

                filepath = os.path.join(self.raw_dir, f)
                os.remove(filepath)
                deleted += 1
                logger.debug(f"删除：{f}")

        # 清理 processed 目录的合并文件
        merged_file = os.path.join(self.processed_dir, "merged_concept_data.csv")
        if os.path.exists(merged_file):
            os.remove(merged_file)
            deleted += 1
            logger.debug("删除：merged_concept_data.csv")

        logger.info(f"清理完成：删除 {deleted} 个文件")
        return deleted

    def verify_migration(self) -> dict:
        """
        验证迁移结果

        Returns:
            验证结果
        """
        logger.info("验证迁移结果...")

        stats = self.db.get_statistics()

        # 检查数据质量
        issues = []

        if stats['total_records'] == 0:
            issues.append("数据库为空")

        if stats.get('concept_count', 0) == 0:
            issues.append("没有板块数据")

        if stats.get('duplicates', 0) > 0:
            issues.append(f"发现 {stats['duplicates']:,} 条重复记录")

        if stats['date_range'][0] is None:
            issues.append("日期范围为空")

        # 输出验证结果
        print("\n" + "=" * 60)
        print("迁移验证结果")
        print("=" * 60)
        print(f"总记录数：{stats['total_records']:,}")
        print(f"板块数量：{stats.get('concept_count', 'N/A')}")
        print(f"日期范围：{stats['date_range'][0]} - {stats['date_range'][1]}")
        print(f"重复记录：{stats.get('duplicates', 0):,}")

        if issues:
            print("\n发现问题:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("\n验证通过：数据完整")

        print("=" * 60)

        return {
            'stats': stats,
            'issues': issues,
            'success': len(issues) == 0
        }


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="CSV 数据迁移工具")
    parser.add_argument("--action", choices=["migrate", "verify", "cleanup", "all"],
                       default="all", help="操作类型")
    parser.add_argument("--db-path", type=str, help="数据库文件路径")
    parser.add_argument("--keep-csv", action="store_true",
                       help="保留已迁移的 CSV 文件")

    args = parser.parse_args()

    migrator = CSVMigrator(args.db_path)

    if args.action == "migrate":
        migrator.migrate_from_organizer()
    elif args.action == "verify":
        migrator.verify_migration()
    elif args.action == "cleanup":
        if not args.keep_csv:
            migrator.cleanup_csv_files()
        else:
            logger.info("--keep-csv 已设置，跳过清理")
    elif args.action == "all":
        # 完整迁移流程
        migrator.migrate_from_organizer()
        migrator.verify_migration()
        if not args.keep_csv:
            migrator.cleanup_csv_files()
        else:
            logger.info("--keep-csv 已设置，跳过清理")


if __name__ == "__main__":
    main()
