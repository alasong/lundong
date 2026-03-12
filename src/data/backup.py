#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据备份系统
提供数据库备份、恢复、归档功能

功能：
1. 自动备份 - 定时备份数据库
2. 增量备份 - 只备份变更数据
3. 备份恢复 - 从备份文件恢复数据
4. 备份清理 - 自动清理过期备份
5. 远程备份 - 支持备份到远程存储
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
import shutil
import gzip
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from loguru import logger
import pandas as pd


class DatabaseBackup:
    """
    数据库备份系统

    功能：
    1. 完整备份 - 备份整个数据库
    2. 增量备份 - 只备份变更的表
    3. 备份恢复 - 从备份文件恢复
    4. 备份清理 - 清理过期备份
    """

    def __init__(
        self,
        db_path: str = "data/stock.db",
        backup_dir: str = "data/backups",
        compression: bool = True,
        retention_days: int = 30,
        max_backups: int = 20
    ):
        """
        初始化备份系统

        Args:
            db_path: 数据库路径
            backup_dir: 备份目录
            compression: 是否压缩
            retention_days: 备份保留天数
            max_backups: 最大备份数量
        """
        self.db_path = Path(db_path)
        self.backup_dir = Path(backup_dir)
        self.compression = compression
        self.retention_days = retention_days
        self.max_backups = max_backups

        # 确保备份目录存在
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"数据库备份系统初始化完成：{backup_dir}")

    def calculate_checksum(self, file_path: Path) -> str:
        """计算文件 MD5 校验和"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def backup_full(self, description: str = "") -> Dict:
        """
        完整备份数据库

        Args:
            description: 备份描述

        Returns:
            备份结果
        """
        logger.info("开始完整备份数据库...")

        if not self.db_path.exists():
            logger.error(f"数据库文件不存在：{self.db_path}")
            return {"success": False, "error": "数据库文件不存在"}

        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_full_{timestamp}"

        if self.compression:
            backup_file = self.backup_dir / f"{backup_name}.db.gz"
        else:
            backup_file = self.backup_dir / f"{backup_name}.db"

        # 备份元数据
        metadata = {
            "backup_type": "full",
            "backup_time": datetime.now().isoformat(),
            "source_file": str(self.db_path),
            "backup_file": str(backup_file),
            "description": description,
            "original_size": self.db_path.stat().st_size if self.db_path.exists() else 0
        }

        try:
            # 复制数据库文件
            if self.compression:
                # 压缩备份
                with open(self.db_path, 'rb') as f_in:
                    with gzip.open(backup_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                # 普通备份
                shutil.copy2(self.db_path, backup_file)

            # 计算校验和
            metadata["checksum"] = self.calculate_checksum(backup_file)
            metadata["backup_size"] = backup_file.stat().st_size

            # 保存元数据
            metadata_file = self.backup_dir / f"{backup_name}.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            # 压缩率
            if self.compression:
                compression_ratio = metadata["backup_size"] / metadata["original_size"] if metadata["original_size"] > 0 else 0
                logger.info(f"备份完成：{backup_file}，压缩率：{compression_ratio:.2%}")
            else:
                logger.info(f"备份完成：{backup_file}")

            return {
                "success": True,
                "backup_file": str(backup_file),
                "metadata_file": str(metadata_file),
                "metadata": metadata
            }

        except Exception as e:
            logger.error(f"备份失败：{e}")
            return {"success": False, "error": str(e)}

    def backup_incremental(
        self,
        tables: List[str] = None,
        since: datetime = None
    ) -> Dict:
        """
        增量备份

        Args:
            tables: 要备份的表列表（None 表示所有表）
            since: 备份此时间之后的数据

        Returns:
            备份结果
        """
        logger.info("开始增量备份...")

        if not self.db_path.exists():
            return {"success": False, "error": "数据库文件不存在"}

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_incremental_{timestamp}"
        backup_dir = self.backup_dir / backup_name
        backup_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "backup_type": "incremental",
            "backup_time": datetime.now().isoformat(),
            "tables": [],
            "total_records": 0
        }

        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()

            # 获取所有表
            if tables is None:
                cursor.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """)
                tables = [row[0] for row in cursor.fetchall()]

            # 备份每个表的数据
            for table in tables:
                try:
                    # 构建查询
                    query = f"SELECT * FROM {table}"
                    if since:
                        # 假设有 update_time 或 create_time 字段
                        query += f" WHERE update_time >= '{since.isoformat()}' OR create_time >= '{since.isoformat()}'"

                    df = pd.read_sql_query(query, conn)

                    if len(df) > 0:
                        # 保存为 CSV
                        csv_file = backup_dir / f"{table}.csv"
                        df.to_csv(csv_file, index=False, encoding='utf-8-sig')

                        metadata["tables"].append({
                            "table_name": table,
                            "records": len(df),
                            "file": str(csv_file)
                        })
                        metadata["total_records"] += len(df)

                        logger.info(f"备份表 {table}: {len(df)} 条记录")

                except Exception as e:
                    logger.warning(f"备份表 {table} 失败：{e}")

            conn.close()

            # 保存元数据
            metadata_file = backup_dir / "metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            # 压缩目录
            if self.compression:
                archive_path = shutil.make_archive(
                    str(self.backup_dir / backup_name),
                    'gztar',
                    root_dir=str(backup_dir)
                )
                # 删除原始目录
                shutil.rmtree(backup_dir)
                logger.info(f"增量备份完成：{archive_path}")
            else:
                logger.info(f"增量备份完成：{backup_dir}")

            return {
                "success": True,
                "backup_path": str(backup_dir),
                "metadata": metadata
            }

        except Exception as e:
            logger.error(f"增量备份失败：{e}")
            return {"success": False, "error": str(e)}

    def restore(self, backup_file: str, target_path: str = None) -> Dict:
        """
        恢复数据库

        Args:
            backup_file: 备份文件路径
            target_path: 恢复目标路径（None 表示原路径）

        Returns:
            恢复结果
        """
        logger.info(f"开始恢复数据库：{backup_file}")

        backup_path = Path(backup_file)
        if not backup_path.exists():
            return {"success": False, "error": f"备份文件不存在：{backup_file}"}

        target = Path(target_path) if target_path else self.db_path

        try:
            # 确保目标目录存在
            target.parent.mkdir(parents=True, exist_ok=True)

            # 备份当前数据库（以防万一）
            if target.exists():
                backup_current = target.parent / f"stock.db.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                shutil.copy2(target, backup_current)
                logger.info(f"已备份当前数据库：{backup_current}")

            # 恢复
            if str(backup_path).endswith('.gz'):
                # 压缩文件
                with gzip.open(backup_path, 'rb') as f_in:
                    with open(target, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                # 普通文件
                shutil.copy2(backup_path, target)

            logger.info(f"数据库恢复完成：{target}")

            return {
                "success": True,
                "target": str(target),
                "message": "数据库恢复成功"
            }

        except Exception as e:
            logger.error(f"恢复失败：{e}")
            return {"success": False, "error": str(e)}

    def list_backups(self) -> List[Dict]:
        """列出所有备份"""
        backups = []

        for metadata_file in self.backup_dir.glob("*.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    backups.append(metadata)
            except Exception as e:
                logger.warning(f"读取元数据失败 {metadata_file}: {e}")

        # 按备份时间排序
        backups.sort(key=lambda x: x.get('backup_time', ''), reverse=True)

        return backups

    def cleanup_old_backups(self) -> Dict:
        """清理过期备份"""
        logger.info("清理过期备份...")

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        removed = []
        kept = []

        backups = self.list_backups()

        for backup in backups:
            try:
                backup_time = datetime.fromisoformat(backup['backup_time'])

                if backup_time < cutoff_date:
                    # 删除备份文件
                    backup_file = Path(backup.get('backup_file', ''))
                    metadata_file = backup_file.with_suffix('.json')

                    if backup_file.exists():
                        backup_file.unlink()
                        removed.append(str(backup_file))
                        logger.info(f"删除过期备份：{backup_file}")

                    if metadata_file.exists():
                        metadata_file.unlink()

                else:
                    kept.append(backup.get('backup_file', ''))

            except Exception as e:
                logger.warning(f"清理备份失败：{e}")

        # 检查备份数量限制
        if len(kept) > self.max_backups:
            # 删除最旧的备份
            backups_to_remove = kept[self.max_backups:]
            for backup_file in backups_to_remove:
                backup_path = Path(backup_file)
                if backup_path.exists():
                    backup_path.unlink()
                    metadata_file = backup_path.with_suffix('.json')
                    if metadata_file.exists():
                        metadata_file.unlink()
                    removed.append(backup_file)
                    logger.info(f"删除超出数量限制的备份：{backup_file}")

        logger.info(f"清理完成：删除 {len(removed)} 个，保留 {len(kept)} 个")

        return {
            "removed": removed,
            "kept": kept,
            "removed_count": len(removed),
            "kept_count": len(kept)
        }

    def verify_backup(self, backup_file: str) -> Dict:
        """验证备份文件完整性"""
        logger.info(f"验证备份文件：{backup_file}")

        backup_path = Path(backup_file)
        if not backup_path.exists():
            return {"valid": False, "error": "备份文件不存在"}

        # 查找元数据文件
        metadata_file = backup_path.with_suffix('.json')
        if not metadata_file.exists():
            # 尝试在备份目录中查找
            metadata_file = self.backup_dir / f"{backup_path.stem}.json"

        if metadata_file.exists():
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                # 验证校验和
                if 'checksum' in metadata:
                    current_checksum = self.calculate_checksum(backup_path)
                    if current_checksum != metadata['checksum']:
                        return {
                            "valid": False,
                            "error": "校验和不匹配",
                            "expected": metadata['checksum'],
                            "actual": current_checksum
                        }

                return {
                    "valid": True,
                    "checksum": metadata.get('checksum', ''),
                    "backup_size": metadata.get('backup_size', 0),
                    "backup_time": metadata.get('backup_time', '')
                }

            except Exception as e:
                return {"valid": False, "error": f"读取元数据失败：{e}"}
        else:
            # 没有元数据，只能检查文件是否存在和可读
            try:
                if str(backup_path).endswith('.gz'):
                    with gzip.open(backup_path, 'rb') as f:
                        f.read(1024)
                else:
                    with open(backup_path, 'rb') as f:
                        f.read(1024)

                return {"valid": True, "message": "文件可读（无元数据验证）"}

            except Exception as e:
                return {"valid": False, "error": str(e)}

    def get_backup_stats(self) -> Dict:
        """获取备份统计信息"""
        backups = self.list_backups()

        if not backups:
            return {"total_backups": 0}

        total_size = sum(b.get('backup_size', 0) for b in backups)
        full_backups = len([b for b in backups if b.get('backup_type') == 'full'])
        incremental_backups = len([b for b in backups if b.get('backup_type') == 'incremental'])

        # 最早和最晚备份
        backup_times = [b.get('backup_time', '') for b in backups if b.get('backup_time')]
        backup_times.sort()

        stats = {
            "total_backups": len(backups),
            "full_backups": full_backups,
            "incremental_backups": incremental_backups,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "first_backup": backup_times[0] if backup_times else None,
            "last_backup": backup_times[-1] if backup_times else None,
            "avg_backup_size_mb": round(total_size / (1024 * 1024) / len(backups), 2) if backups else 0
        }

        return stats


def create_backup_schedule(
    backup_time: str = "02:00",
    db_path: str = "data/stock.db",
    backup_dir: str = "data/backups"
):
    """
    创建定时备份任务

    Args:
        backup_time: 备份时间（HH:MM 格式）
        db_path: 数据库路径
        backup_dir: 备份目录
    """
    from utils.audit_logger import get_audit_logger

    audit = get_audit_logger()
    backup_system = DatabaseBackup(db_path=db_path, backup_dir=backup_dir)

    # 执行备份
    result = backup_system.backup_full(description="定时备份")

    if result.get('success'):
        audit.log_system_event(
            event_type="backup_success",
            message="数据库备份成功",
            details=result.get('metadata', {})
        )
    else:
        audit.log_system_event(
            event_type="backup_failure",
            message="数据库备份失败",
            details={"error": result.get('error', '')}
        )

    return result


def main():
    """测试函数"""
    print("=" * 90)
    print("数据库备份系统测试")
    print("=" * 90)

    # 初始化备份系统
    backup = DatabaseBackup(
        db_path="data/stock.db",
        backup_dir="data/test_backups",
        compression=True
    )

    # 1. 完整备份
    print("\n[1] 完整备份数据库...")
    result = backup.backup_full(description="测试备份")
    if result.get('success'):
        print(f"备份成功：{result['backup_file']}")
        print(f"原始大小：{result['metadata']['original_size'] / (1024*1024):.2f} MB")
        print(f"备份大小：{result['metadata']['backup_size'] / (1024*1024):.2f} MB")
        if result['metadata']['original_size'] > 0:
            ratio = result['metadata']['backup_size'] / result['metadata']['original_size']
            print(f"压缩率：{ratio:.2%}")
    else:
        print(f"备份失败：{result.get('error')}")

    # 2. 列出备份
    print("\n[2] 列出所有备份...")
    backups = backup.list_backups()
    print(f"共 {len(backups)} 个备份:")
    for b in backups[:5]:
        print(f"  - {b.get('backup_time')}: {b.get('backup_type')} ({b.get('backup_size', 0) / (1024*1024):.2f} MB)")

    # 3. 验证备份
    print("\n[3] 验证备份文件...")
    if backups:
        latest_backup = backups[0].get('backup_file')
        if latest_backup:
            verify_result = backup.verify_backup(latest_backup)
            print(f"验证结果：{'有效' if verify_result.get('valid') else '无效'}")
            if verify_result.get('valid'):
                print(f"校验和：{verify_result.get('checksum', 'N/A')}")

    # 4. 备份统计
    print("\n[4] 备份统计...")
    stats = backup.get_backup_stats()
    print(f"总备份数：{stats.get('total_backups', 0)}")
    print(f"完整备份：{stats.get('full_backups', 0)}")
    print(f"增量备份：{stats.get('incremental_backups', 0)}")
    print(f"总大小：{stats.get('total_size_mb', 0):.2f} MB")

    # 5. 清理过期备份
    print("\n[5] 清理过期备份...")
    cleanup_result = backup.cleanup_old_backups()
    print(f"删除 {cleanup_result['removed_count']} 个，保留 {cleanup_result['kept_count']} 个")

    print("\n" + "=" * 90)
    print("数据库备份系统测试完成!")
    print("=" * 90)


if __name__ == "__main__":
    main()
