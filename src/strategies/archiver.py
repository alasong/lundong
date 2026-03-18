"""
策略归档管理器
- 识别低收益策略
- 自动归档到 archived/ 目录
- 支持恢复归档策略
"""

import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger

from .performance_tracker import StrategyPerformanceTracker
from .plugin_loader import PluginLoader


class StrategyArchiver:
    """策略归档管理器"""

    ARCHIVE_THRESHOLD_DAYS = 30  # 统计最近30天表现
    MIN_RUNS_FOR_ARCHIVE = 10  # 最少运行次数
    LOW_RETURN_THRESHOLD = 0.0  # 收益率阈值（0%）

    def __init__(self, db=None):
        self.db = db
        self.performance_tracker = StrategyPerformanceTracker(db)
        self.plugin_loader = PluginLoader(db)

        self.plugins_dir = Path(__file__).parent / "plugins"
        self.archived_dir = Path(__file__).parent / "archived"

    def identify_low_performers(self, days: int = None) -> List[Dict]:
        """
        识别低收益策略

        Returns:
            低收益策略列表，包含：
            - strategy_name
            - avg_return
            - total_return
            - sharpe
            - max_drawdown
            - run_count
            - recommendation: "archive" / "monitor"
        """
        days = days or self.ARCHIVE_THRESHOLD_DAYS

        low_performers = self.performance_tracker.get_low_performers(
            threshold_return=self.LOW_RETURN_THRESHOLD,
            min_runs=self.MIN_RUNS_FOR_ARCHIVE,
        )

        recommendations = []
        for strategy in low_performers:
            # 获取详细绩效
            perf = self.performance_tracker.get_performance(
                strategy["strategy_name"], days=days
            )

            # 计算推荐
            avg_return = strategy.get("avg_return", 0)
            sharpe = perf.get("avg_sharpe", 0)
            max_drawdown = perf.get("avg_max_drawdown", 0)

            if avg_return < -0.05 or (sharpe < 0 and max_drawdown > 0.2):
                recommendation = "archive"
            else:
                recommendation = "monitor"

            recommendations.append(
                {
                    "strategy_name": strategy["strategy_name"],
                    "avg_return": avg_return,
                    "total_return": strategy.get("total_return", 0),
                    "sharpe": sharpe,
                    "max_drawdown": max_drawdown,
                    "run_count": strategy.get("run_count", 0),
                    "recommendation": recommendation,
                    "performance": perf,
                }
            )

        return recommendations

    def archive_strategy(
        self, strategy_name: str, reason: str = "low_performance"
    ) -> bool:
        """
        归档策略

        Args:
            strategy_name: 策略名称
            reason: 归档原因

        Returns:
            是否成功
        """
        try:
            # 检查插件是否存在
            plugin_dir = self.plugins_dir / strategy_name
            if not plugin_dir.exists():
                logger.warning(f"Plugin directory not found: {plugin_dir}")
                return False

            # 创建归档目录（带时间戳）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{strategy_name}_{timestamp}"
            archive_target = self.archived_dir / archive_name

            # 移动目录
            shutil.move(str(plugin_dir), str(archive_target))

            # 更新归档元数据
            metadata_file = archive_target / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

                metadata["archived_at"] = datetime.now().isoformat()
                metadata["archive_reason"] = reason
                metadata["original_name"] = strategy_name
                metadata["enabled"] = False

                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)

            # 从 StrategyFactory 注销
            try:
                from .strategy_factory import StrategyFactory

                StrategyFactory.unregister_strategy(strategy_name)
            except Exception as e:
                logger.warning(f"Failed to unregister strategy: {e}")

            # 更新数据库状态
            self._update_archive_status(strategy_name, reason)

            logger.success(f"Strategy archived: {strategy_name} -> {archive_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to archive strategy {strategy_name}: {e}")
            return False

    def restore_strategy(self, archived_name: str) -> bool:
        """
        恢复归档策略

        Args:
            archived_name: 归档策略名称（如 "daban_v1_20260318_143000"）

        Returns:
            是否成功
        """
        try:
            archived_path = self.archived_dir / archived_name
            if not archived_path.exists():
                logger.warning(f"Archived strategy not found: {archived_path}")
                return False

            # 读取原始策略名
            metadata_file = archived_path / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                original_name = metadata.get(
                    "original_name", archived_name.split("_")[0]
                )
            else:
                original_name = archived_name.split("_")[0]

            # 目标目录
            target_path = self.plugins_dir / original_name

            # 检查是否已存在同名策略
            if target_path.exists():
                logger.warning(
                    f"Strategy {original_name} already exists. "
                    f"Please remove it first or use a different name."
                )
                return False

            # 移动目录
            shutil.move(str(archived_path), str(target_path))

            # 更新元数据
            if metadata_file.exists():
                metadata_file = target_path / "metadata.json"
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

                metadata["enabled"] = True
                metadata["restored_at"] = datetime.now().isoformat()
                del metadata["archived_at"]
                del metadata["archive_reason"]

                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)

            # 重新加载插件
            self.plugin_loader.load_plugin(original_name)

            logger.success(f"Strategy restored: {archived_name} -> {original_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to restore strategy {archived_name}: {e}")
            return False

    def list_archived(self) -> List[Dict]:
        """列出所有归档策略"""
        archived = []

        for item in self.archived_dir.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                metadata_file = item / "metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, "r", encoding="utf-8") as f:
                            metadata = json.load(f)
                        archived.append(
                            {
                                "name": item.name,
                                "original_name": metadata.get(
                                    "original_name", item.name
                                ),
                                "display_name": metadata.get("display_name", item.name),
                                "archived_at": metadata.get("archived_at", "unknown"),
                                "archive_reason": metadata.get(
                                    "archive_reason", "unknown"
                                ),
                                "version": metadata.get("version", "unknown"),
                            }
                        )
                    except Exception as e:
                        logger.warning(f"Failed to read metadata for {item.name}: {e}")

        return archived

    def auto_archive(self, dry_run: bool = True) -> List[Dict]:
        """
        自动归档低收益策略

        Args:
            dry_run: 仅模拟，不实际执行

        Returns:
            归档结果列表
        """
        low_performers = self.identify_low_performers()
        results = []

        for strategy in low_performers:
            if strategy["recommendation"] == "archive":
                if dry_run:
                    results.append(
                        {
                            "strategy": strategy["strategy_name"],
                            "action": "would_archive",
                            "reason": f"avg_return={strategy['avg_return']:.2%}, "
                            f"sharpe={strategy['sharpe']:.2f}",
                        }
                    )
                else:
                    success = self.archive_strategy(
                        strategy["strategy_name"],
                        reason=f"auto_archive: return={strategy['avg_return']:.2%}",
                    )
                    results.append(
                        {
                            "strategy": strategy["strategy_name"],
                            "action": "archived" if success else "failed",
                            "reason": f"avg_return={strategy['avg_return']:.2%}",
                        }
                    )

        return results

    def _update_archive_status(self, strategy_name: str, reason: str):
        """更新数据库中的归档状态"""
        try:
            from src.data.database import get_database

            db = self.db or get_database()

            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE strategy_performance 
                    SET status = 'archived', archived_at = CURRENT_TIMESTAMP
                    WHERE strategy_name = ?
                    """,
                    (strategy_name,),
                )
                conn.commit()

        except Exception as e:
            logger.warning(f"Failed to update archive status in database: {e}")
