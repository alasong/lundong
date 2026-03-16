#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
在线学习模块
支持增量模型更新，无需全量重训练
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List, Tuple
from loguru import logger
import sys
import os
import pickle
import json
from datetime import datetime
from pathlib import Path
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DataBuffer:
    """数据缓冲区，管理新数据窗口"""

    def __init__(
        self,
        max_size: int = 1000,
        min_update_size: int = 50,
        staleness_threshold: int = 7
    ):
        """
        初始化数据缓冲区

        Args:
            max_size: 缓冲区最大容量
            min_update_size: 触发增量更新的最小数据量
            staleness_threshold: 数据过期阈值（天）
        """
        self.max_size = max_size
        self.min_update_size = min_update_size
        self.staleness_threshold = staleness_threshold
        self.buffer: List[pd.DataFrame] = []
        self.last_update_time: Optional[datetime] = None
        self.total_samples = 0

    def add(self, data: pd.DataFrame) -> int:
        """
        添加数据到缓冲区

        Args:
            data: 新数据 DataFrame

        Returns:
            缓冲区当前样本数
        """
        if data.empty:
            return self.total_samples

        self.buffer.append(data)
        self.total_samples += len(data)

        # 超过最大容量时，移除最旧的数据
        while self.total_samples > self.max_size and self.buffer:
            removed = self.buffer.pop(0)
            self.total_samples -= len(removed)

        logger.debug(f"缓冲区添加 {len(data)} 条数据，当前共 {self.total_samples} 条")
        return self.total_samples

    def get_data(self) -> Optional[pd.DataFrame]:
        """
        获取缓冲区所有数据

        Returns:
            合并后的 DataFrame 或 None
        """
        if not self.buffer:
            return None

        return pd.concat(self.buffer, ignore_index=True)

    def should_update(self) -> Tuple[bool, str]:
        """
        检查是否应该触发增量更新

        Returns:
            (是否更新, 原因)
        """
        if self.total_samples < self.min_update_size:
            return False, f"数据量不足: {self.total_samples} < {self.min_update_size}"

        if self.last_update_time is None:
            return True, "首次更新"

        days_since_update = (datetime.now() - self.last_update_time).days
        if days_since_update >= self.staleness_threshold:
            return True, f"数据过期: {days_since_update} 天"

        return True, f"数据量充足: {self.total_samples}"

    def clear(self):
        """清空缓冲区"""
        self.buffer = []
        self.total_samples = 0
        logger.info("缓冲区已清空")

    def mark_updated(self):
        """标记已更新"""
        self.last_update_time = datetime.now()
        self.clear()

    def get_state(self) -> Dict[str, Any]:
        """获取缓冲区状态"""
        return {
            "total_samples": self.total_samples,
            "num_batches": len(self.buffer),
            "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
            "max_size": self.max_size,
            "min_update_size": self.min_update_size,
        }


class ModelVersionManager:
    """模型版本管理器"""

    def __init__(self, model_dir: str = "data/models"):
        """
        初始化版本管理器

        Args:
            model_dir: 模型存储目录
        """
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.versions_file = self.model_dir / "versions.json"
        self.versions = self._load_versions()

    def _load_versions(self) -> Dict[str, List[Dict]]:
        """加载版本信息"""
        if self.versions_file.exists():
            with open(self.versions_file, "r") as f:
                return json.load(f)
        return {}

    def _save_versions(self):
        """保存版本信息"""
        with open(self.versions_file, "w") as f:
            json.dump(self.versions, f, indent=2, default=str)

    def save_version(
        self,
        model_name: str,
        model_data: Dict[str, Any],
        metrics: Optional[Dict[str, float]] = None,
        description: str = ""
    ) -> str:
        """
        保存模型版本

        Args:
            model_name: 模型名称
            model_data: 模型数据字典
            metrics: 性能指标
            description: 版本描述

        Returns:
            版本 ID
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_id = f"{model_name}_v{timestamp}"

        # 保存模型文件
        model_path = self.model_dir / f"{version_id}.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model_data, f)

        # 记录版本信息
        version_info = {
            "version_id": version_id,
            "timestamp": timestamp,
            "model_path": str(model_path),
            "metrics": metrics or {},
            "description": description,
        }

        if model_name not in self.versions:
            self.versions[model_name] = []
        self.versions[model_name].append(version_info)
        self._save_versions()

        logger.info(f"保存模型版本: {version_id}")
        return version_id

    def load_version(self, version_id: str) -> Optional[Dict[str, Any]]:
        """
        加载指定版本模型

        Args:
            version_id: 版本 ID

        Returns:
            模型数据或 None
        """
        # 查找模型路径
        model_path = self.model_dir / f"{version_id}.pkl"
        if not model_path.exists():
            logger.error(f"模型版本不存在: {version_id}")
            return None

        with open(model_path, "rb") as f:
            return pickle.load(f)

    def get_latest_version(self, model_name: str) -> Optional[str]:
        """获取最新版本 ID"""
        if model_name not in self.versions or not self.versions[model_name]:
            return None
        return self.versions[model_name][-1]["version_id"]

    def list_versions(self, model_name: str = None) -> List[Dict]:
        """
        列出版本历史

        Args:
            model_name: 模型名称，None 则列出所有

        Returns:
            版本信息列表
        """
        if model_name:
            return self.versions.get(model_name, [])

        all_versions = []
        for name, versions in self.versions.items():
            for v in versions:
                v["model_name"] = name
                all_versions.append(v)
        return sorted(all_versions, key=lambda x: x["timestamp"], reverse=True)

    def rollback(self, model_name: str, version_id: str) -> bool:
        """
        回滚到指定版本

        Args:
            model_name: 模型名称
            version_id: 目标版本 ID

        Returns:
            是否成功
        """
        model_data = self.load_version(version_id)
        if model_data is None:
            return False

        # 更新当前模型
        current_path = self.model_dir / f"{model_name}.pkl"
        with open(current_path, "wb") as f:
            pickle.dump(model_data, f)

        logger.info(f"回滚模型 {model_name} 到版本 {version_id}")
        return True

    def cleanup_old_versions(self, model_name: str = None, keep_n: int = 5):
        """
        清理旧版本，只保留最近 N 个

        Args:
            model_name: 模型名称，None 则清理所有
            keep_n: 保留版本数
        """
        models = [model_name] if model_name else list(self.versions.keys())

        for name in models:
            if name not in self.versions:
                continue

            versions = self.versions[name]
            if len(versions) <= keep_n:
                continue

            # 删除旧版本文件
            for old_version in versions[:-keep_n]:
                old_path = Path(old_version["model_path"])
                if old_path.exists():
                    old_path.unlink()
                    logger.debug(f"删除旧版本: {old_version['version_id']}")

            # 更新版本列表
            self.versions[name] = versions[-keep_n:]

        self._save_versions()
        logger.info(f"清理完成，每个模型保留最近 {keep_n} 个版本")


class IncrementalLearner:
    """增量学习器"""

    def __init__(
        self,
        model_type: str = "xgboost",
        model_dir: str = "data/models",
        buffer_size: int = 1000,
        min_update_size: int = 50,
        learning_rate_decay: float = 0.95
    ):
        """
        初始化增量学习器

        Args:
            model_type: 模型类型 (xgboost/lightgbm)
            model_dir: 模型目录
            buffer_size: 缓冲区大小
            min_update_size: 最小更新数据量
            learning_rate_decay: 学习率衰减系数
        """
        self.model_type = model_type
        self.model_dir = model_dir
        self.learning_rate_decay = learning_rate_decay

        # 初始化组件
        self.buffer = DataBuffer(max_size=buffer_size, min_update_size=min_update_size)
        self.version_manager = ModelVersionManager(model_dir)

        # 当前模型
        self.models: Dict[str, Any] = {}  # horizon -> model
        self.feature_names: List[str] = []
        self.update_count = 0

    def load_base_model(self, model_path: str) -> bool:
        """
        加载基础模型

        Args:
            model_path: 模型文件路径

        Returns:
            是否成功
        """
        with open(model_path, "rb") as f:
            data = pickle.load(f)

        self.models = {
            "1d": data.get("model_1d"),
            "5d": data.get("model_5d"),
            "20d": data.get("model_20d"),
        }
        self.feature_names = data.get("feature_names", [])

        if not any(self.models.values()):
            logger.error("未找到有效模型")
            return False

        logger.info(f"加载基础模型成功，特征数: {len(self.feature_names)}")
        return True

    def add_new_data(self, data: pd.DataFrame):
        """
        添加新数据到缓冲区

        Args:
            data: 新数据
        """
        count = self.buffer.add(data)
        logger.info(f"添加 {len(data)} 条数据，缓冲区共 {count} 条")

    def should_update(self) -> Tuple[bool, str]:
        """检查是否应该更新"""
        return self.buffer.should_update()

    def incremental_update(
        self,
        features: pd.DataFrame,
        targets: Dict[str, np.ndarray],
        epochs: int = 1,
        validation_split: float = 0.2
    ) -> Dict[str, Any]:
        """
        执行增量更新

        Args:
            features: 特征 DataFrame
            targets: 目标变量字典 {"1d": array, "5d": array, "20d": array}
            epochs: 增量训练轮数
            validation_split: 验证集比例

        Returns:
            更新结果
        """
        if not self.models:
            logger.error("未加载模型，请先调用 load_base_model()")
            return {"success": False, "error": "未加载模型"}

        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

        X = features[self.feature_names].values
        results = {"success": True, "horizons": {}}

        # 衰减学习率
        decayed_lr = None

        for horizon, model in self.models.items():
            if model is None:
                continue

            y = targets.get(horizon)
            if y is None or len(y) != len(X):
                logger.warning(f"[{horizon}] 目标变量长度不匹配，跳过")
                continue

            # 划分数据
            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=validation_split, shuffle=False
            )

            # 增量训练
            for epoch in range(epochs):
                if self.model_type == "xgboost":
                    # XGBoost 增量训练
                    if decayed_lr is None:
                        original_lr = model.get_params().get("learning_rate", 0.05)
                        decayed_lr = original_lr * (self.learning_rate_decay ** self.update_count)

                    # 使用 warm start
                    model.set_params(
                        learning_rate=decayed_lr,
                        n_estimators=model.n_estimators + 10,
                    )
                    model.fit(
                        X_train, y_train,
                        eval_set=[(X_val, y_val)],
                        verbose=False,
                        xgb_model=model if epoch > 0 else None
                    )

                elif self.model_type == "lightgbm":
                    # LightGBM 增量训练
                    if decayed_lr is None:
                        original_lr = model.get_params().get("learning_rate", 0.05)
                        decayed_lr = original_lr * (self.learning_rate_decay ** self.update_count)

                    model.set_params(
                        learning_rate=decayed_lr,
                        n_estimators=model.n_estimators + 10,
                    )
                    model.fit(
                        X_train, y_train,
                        eval_set=[(X_val, y_val)],
                        init_model=model if epoch > 0 else None
                    )

            # 评估
            y_pred = model.predict(X_val)
            metrics = {
                "mse": mean_squared_error(y_val, y_pred),
                "mae": mean_absolute_error(y_val, y_pred),
                "r2": r2_score(y_val, y_pred),
            }

            results["horizons"][horizon] = {
                "metrics": metrics,
                "samples_used": len(X_train),
            }

            logger.info(
                f"[{horizon}] 增量更新完成 - MSE: {metrics['mse']:.4f}, "
                f"MAE: {metrics['mae']:.4f}, R2: {metrics['r2']:.4f}"
            )

        self.update_count += 1
        self.buffer.mark_updated()

        return results

    def save_incremental_model(
        self,
        model_name: str = "unified_model",
        metrics: Dict[str, float] = None,
        description: str = ""
    ) -> str:
        """
        保存增量更新后的模型

        Args:
            model_name: 模型名称
            metrics: 性能指标
            description: 版本描述

        Returns:
            版本 ID
        """
        model_data = {
            "model_1d": self.models.get("1d"),
            "model_5d": self.models.get("5d"),
            "model_20d": self.models.get("20d"),
            "feature_names": self.feature_names,
            "update_count": self.update_count,
            "train_date": datetime.now().strftime("%Y%m%d"),
        }

        version_id = self.version_manager.save_version(
            model_name, model_data, metrics, description
        )

        # 同时更新主模型文件
        main_path = Path(self.model_dir) / f"{model_name}.pkl"
        with open(main_path, "wb") as f:
            pickle.dump(model_data, f)

        return version_id

    def get_model_info(self) -> Dict[str, Any]:
        """获取模型信息"""
        return {
            "model_type": self.model_type,
            "update_count": self.update_count,
            "feature_count": len(self.feature_names),
            "horizons": list(self.models.keys()),
            "buffer_state": self.buffer.get_state(),
        }


class OnlineLearner:
    """在线学习管理器"""

    def __init__(
        self,
        model_dir: str = "data/models",
        auto_update: bool = True,
        update_interval: int = 7,
        performance_threshold: float = 0.1
    ):
        """
        初始化在线学习管理器

        Args:
            model_dir: 模型目录
            auto_update: 是否自动更新
            update_interval: 自动更新间隔（天）
            performance_threshold: 性能退化阈值
        """
        self.model_dir = model_dir
        self.auto_update = auto_update
        self.update_interval = update_interval
        self.performance_threshold = performance_threshold

        self.learner = IncrementalLearner(model_dir=model_dir)
        self.last_metrics: Dict[str, float] = {}
        self.update_history: List[Dict] = []

    def check_and_update(
        self,
        new_data: pd.DataFrame,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        检查并执行增量更新

        Args:
            new_data: 新数据
            force: 是否强制更新

        Returns:
            更新结果
        """
        # 添加新数据到缓冲区
        self.learner.add_new_data(new_data)

        # 检查是否需要更新
        if not force:
            should_update, reason = self.learner.should_update()
            if not should_update:
                return {"updated": False, "reason": reason}

        logger.info(f"触发增量更新: {reason if not force else '强制更新'}")

        # 准备特征和目标
        # 注意：这里需要调用 UnifiedPredictor 的特征准备方法
        # 简化处理，假设 new_data 已经是特征数据

        # 执行增量更新
        result = self._perform_update(new_data)

        return result

    def _perform_update(self, data: pd.DataFrame) -> Dict[str, Any]:
        """执行增量更新"""
        # 提取目标和特征
        targets = {}
        if "target_1d" in data.columns:
            targets["1d"] = data["target_1d"].values
        if "target_5d" in data.columns:
            targets["5d"] = data["target_5d"].values
        if "target_20d" in data.columns:
            targets["20d"] = data["target_20d"].values

        if not targets:
            return {"updated": False, "reason": "缺少目标变量"}

        # 执行增量训练
        result = self.learner.incremental_update(data, targets)

        if result.get("success"):
            # 检查性能退化
            if self._check_degradation(result):
                logger.warning("检测到性能退化，回滚模型")
                # 回滚到上一版本
                latest = self.learner.version_manager.get_latest_version("unified_model")
                if latest:
                    self.learner.version_manager.rollback("unified_model", latest)

            # 保存新版本
            metrics = {
                h: info["metrics"]
                for h, info in result.get("horizons", {}).items()
            }
            version_id = self.learner.save_incremental_model(
                metrics=metrics,
                description=f"增量更新 #{self.learner.update_count}"
            )

            result["version_id"] = version_id
            result["updated"] = True

            # 记录历史
            self.update_history.append({
                "timestamp": datetime.now().isoformat(),
                "version_id": version_id,
                "metrics": metrics,
            })

        return result

    def _check_degradation(self, result: Dict) -> bool:
        """检查性能退化"""
        if not self.last_metrics:
            self.last_metrics = result.get("horizons", {})
            return False

        for horizon, info in result.get("horizons", {}).items():
            if horizon not in self.last_metrics:
                continue

            old_mse = self.last_metrics[horizon].get("metrics", {}).get("mse", 0)
            new_mse = info.get("metrics", {}).get("mse", 0)

            if old_mse > 0 and new_mse > old_mse * (1 + self.performance_threshold):
                return True

        # 更新最新指标
        self.last_metrics = result.get("horizons", {})
        return False

    def get_status(self) -> Dict[str, Any]:
        """获取在线学习状态"""
        return {
            "learner_info": self.learner.get_model_info(),
            "auto_update": self.auto_update,
            "update_interval": self.update_interval,
            "total_updates": len(self.update_history),
            "last_update": self.update_history[-1] if self.update_history else None,
        }


if __name__ == "__main__":
    # 测试
    print("在线学习模块测试")
    print("=" * 50)

    # 测试数据缓冲区
    print("\n测试 DataBuffer...")
    buffer = DataBuffer(max_size=100, min_update_size=10)
    for i in range(5):
        df = pd.DataFrame({"value": range(i * 10, (i + 1) * 10)})
        buffer.add(df)
    print(f"缓冲区状态: {buffer.get_state()}")
    should, reason = buffer.should_update()
    print(f"是否更新: {should}, 原因: {reason}")

    # 测试版本管理
    print("\n测试 ModelVersionManager...")
    version_mgr = ModelVersionManager("data/models/test")
    version_id = version_mgr.save_version(
        "test_model",
        {"param": 1},
        {"accuracy": 0.9},
        "测试版本"
    )
    print(f"保存版本: {version_id}")
    print(f"版本列表: {len(version_mgr.list_versions())} 个")

    print("\n测试成功!")