#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
模型优化模块
包含超参数调优（Optuna）和集成学习（Stacking）
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List, Tuple
from loguru import logger
import sys
import os
import time
import pickle

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class HyperparameterTuner:
    """超参数调优器（使用 Optuna）"""

    def __init__(self, n_trials: int = 50, timeout: int = 600):
        """
        初始化

        Args:
            n_trials: 最大试验次数
            timeout: 超时时间（秒）
        """
        self.n_trials = n_trials
        self.timeout = timeout
        self.best_params = None
        self.study = None

    def tune_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        n_trials: int = None
    ) -> Dict[str, Any]:
        """
        调优 XGBoost 超参数

        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征
            y_val: 验证目标
            n_trials: 试验次数

        Returns:
            最佳参数
        """
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            logger.warning("Optuna 未安装，使用默认参数")
            return self._get_default_xgboost_params()

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 500),
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                "gamma": trial.suggest_float("gamma", 0, 5),
                "reg_alpha": trial.suggest_float("reg_alpha", 0, 5),
                "reg_lambda": trial.suggest_float("reg_lambda", 0, 5),
            }

            try:
                from xgboost import XGBRegressor
                model = XGBRegressor(**params, random_state=42, n_jobs=-1)
                model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
                y_pred = model.predict(X_val)
                from sklearn.metrics import mean_squared_error
                return mean_squared_error(y_val, y_pred)
            except Exception as e:
                logger.debug(f"Trial failed: {e}")
                return float("inf")

        trials = n_trials or self.n_trials
        logger.info(f"开始 XGBoost 超参数调优，最大试验次数: {trials}")

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=trials, timeout=self.timeout, show_progress_bar=False)

        self.study = study
        self.best_params = study.best_params

        logger.info(f"最佳 MSE: {study.best_value:.4f}")
        logger.info(f"最佳参数: {study.best_params}")

        return study.best_params

    def tune_lightgbm(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        n_trials: int = None
    ) -> Dict[str, Any]:
        """
        调优 LightGBM 超参数

        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征
            y_val: 验证目标
            n_trials: 试验次数

        Returns:
            最佳参数
        """
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            logger.warning("Optuna 未安装，使用默认参数")
            return self._get_default_lightgbm_params()

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 500),
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                "num_leaves": trial.suggest_int("num_leaves", 20, 100),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
                "reg_alpha": trial.suggest_float("reg_alpha", 0, 5),
                "reg_lambda": trial.suggest_float("reg_lambda", 0, 5),
            }

            try:
                from lightgbm import LGBMRegressor
                model = LGBMRegressor(**params, random_state=42, n_jobs=-1, verbose=-1)
                model.fit(X_train, y_train, eval_set=[(X_val, y_val)])
                y_pred = model.predict(X_val)
                from sklearn.metrics import mean_squared_error
                return mean_squared_error(y_val, y_pred)
            except Exception as e:
                logger.debug(f"Trial failed: {e}")
                return float("inf")

        trials = n_trials or self.n_trials
        logger.info(f"开始 LightGBM 超参数调优，最大试验次数: {trials}")

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=trials, timeout=self.timeout, show_progress_bar=False)

        self.study = study
        self.best_params = study.best_params

        logger.info(f"最佳 MSE: {study.best_value:.4f}")
        logger.info(f"最佳参数: {study.best_params}")

        return study.best_params

    def _get_default_xgboost_params(self) -> Dict[str, Any]:
        """获取默认 XGBoost 参数"""
        return {
            "n_estimators": 200,
            "max_depth": 5,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
        }

    def _get_default_lightgbm_params(self) -> Dict[str, Any]:
        """获取默认 LightGBM 参数"""
        return {
            "n_estimators": 200,
            "max_depth": 5,
            "learning_rate": 0.05,
            "num_leaves": 31,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
        }


class StackingEnsemble:
    """Stacking 集成学习"""

    def __init__(self, n_folds: int = 5):
        """
        初始化

        Args:
            n_folds: 交叉验证折数
        """
        self.n_folds = n_folds
        self.base_models = {}
        self.meta_model = None
        self.meta_features = None

    def create_base_models(self) -> Dict[str, Any]:
        """创建基础模型"""
        models = {}

        # XGBoost
        try:
            from xgboost import XGBRegressor
            models["xgboost"] = XGBRegressor(
                n_estimators=200,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1
            )
        except ImportError:
            pass

        # LightGBM
        try:
            from lightgbm import LGBMRegressor
            models["lightgbm"] = LGBMRegressor(
                n_estimators=200,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1,
                verbose=-1
            )
        except ImportError:
            pass

        # Random Forest
        from sklearn.ensemble import RandomForestRegressor
        models["random_forest"] = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )

        # Gradient Boosting
        from sklearn.ensemble import GradientBoostingRegressor
        models["gradient_boosting"] = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.05,
            random_state=42
        )

        # Extra Trees
        from sklearn.ensemble import ExtraTreesRegressor
        models["extra_trees"] = ExtraTreesRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )

        logger.info(f"创建了 {len(models)} 个基础模型: {list(models.keys())}")
        return models

    def create_meta_model(self) -> Any:
        """创建元模型"""
        from sklearn.linear_model import Ridge
        return Ridge(alpha=1.0, random_state=42)

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        base_models: Dict[str, Any] = None
    ) -> "StackingEnsemble":
        """
        训练 Stacking 模型

        Args:
            X: 特征
            y: 目标
            base_models: 基础模型字典

        Returns:
            self
        """
        from sklearn.model_selection import KFold

        if base_models is None:
            base_models = self.create_base_models()

        self.base_models = base_models
        n_samples = X.shape[0]
        n_models = len(base_models)

        # 创建元特征矩阵
        meta_features = np.zeros((n_samples, n_models))
        kf = KFold(n_splits=self.n_folds, shuffle=True, random_state=42)

        logger.info(f"开始 Stacking 训练，{n_models} 个基础模型，{self.n_folds} 折交叉验证")

        for model_idx, (name, model) in enumerate(base_models.items()):
            logger.info(f"训练基础模型: {name}")

            for fold_idx, (train_idx, val_idx) in enumerate(kf.split(X)):
                X_train_fold, X_val_fold = X[train_idx], X[val_idx]
                y_train_fold = y[train_idx]

                # 克隆模型
                from sklearn.base import clone
                model_clone = clone(model)
                model_clone.fit(X_train_fold, y_train_fold)

                # 预测验证集
                meta_features[val_idx, model_idx] = model_clone.predict(X_val_fold)

        self.meta_features = meta_features

        # 训练元模型
        self.meta_model = self.create_meta_model()
        self.meta_model.fit(meta_features, y)

        # 重新在整个数据集上训练基础模型
        for name, model in self.base_models.items():
            model.fit(X, y)

        logger.info("Stacking 训练完成")
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        预测

        Args:
            X: 特征

        Returns:
            预测值
        """
        # 基础模型预测
        meta_features = np.column_stack([
            model.predict(X) for model in self.base_models.values()
        ])

        # 元模型预测
        return self.meta_model.predict(meta_features)

    def save(self, path: str):
        """保存模型"""
        with open(path, "wb") as f:
            pickle.dump({
                "base_models": self.base_models,
                "meta_model": self.meta_model,
                "n_folds": self.n_folds,
            }, f)
        logger.info(f"Stacking 模型已保存: {path}")

    def load(self, path: str) -> "StackingEnsemble":
        """加载模型"""
        with open(path, "rb") as f:
            data = pickle.load(f)
            self.base_models = data["base_models"]
            self.meta_model = data["meta_model"]
            self.n_folds = data["n_folds"]
        logger.info(f"Stacking 模型已加载: {path}")
        return self


class ModelOptimizer:
    """模型优化器"""

    def __init__(
        self,
        use_tuning: bool = True,
        use_stacking: bool = True,
        n_trials: int = 30
    ):
        """
        初始化

        Args:
            use_tuning: 是否使用超参数调优
            use_stacking: 是否使用 Stacking 集成
            n_trials: 调优试验次数
        """
        self.use_tuning = use_tuning
        self.use_stacking = use_stacking
        self.n_trials = n_trials
        self.tuner = HyperparameterTuner(n_trials=n_trials)
        self.stacking_models = {}

    def optimize(
        self,
        X: np.ndarray,
        y: np.ndarray,
        horizon: str = "1d"
    ) -> Dict[str, Any]:
        """
        优化模型

        Args:
            X: 特征
            y: 目标
            horizon: 预测周期

        Returns:
            优化后的模型和参数
        """
        from sklearn.model_selection import train_test_split

        # 划分数据
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, shuffle=False
        )

        result = {"horizon": horizon}

        # 1. 超参数调优
        if self.use_tuning:
            logger.info(f"[{horizon}] 开始超参数调优...")

            # XGBoost 调优
            xgb_params = self.tuner.tune_xgboost(
                X_train, y_train, X_val, y_val, n_trials=self.n_trials
            )
            result["xgb_params"] = xgb_params

            # LightGBM 调优
            lgb_params = self.tuner.tune_lightgbm(
                X_train, y_train, X_val, y_val, n_trials=self.n_trials
            )
            result["lgb_params"] = lgb_params

        # 2. Stacking 集成
        if self.use_stacking:
            logger.info(f"[{horizon}] 开始 Stacking 集成训练...")

            stacking = StackingEnsemble(n_folds=5)
            stacking.fit(X, y)
            self.stacking_models[horizon] = stacking
            result["stacking_model"] = stacking

        # 3. 评估
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

        # 使用 Stacking 预测
        if self.use_stacking and horizon in self.stacking_models:
            y_pred = self.stacking_models[horizon].predict(X_val)
            result["stacking_metrics"] = {
                "mse": mean_squared_error(y_val, y_pred),
                "mae": mean_absolute_error(y_val, y_pred),
                "r2": r2_score(y_val, y_pred)
            }
            logger.info(f"[{horizon}] Stacking - MSE: {result['stacking_metrics']['mse']:.4f}, "
                       f"R2: {result['stacking_metrics']['r2']:.4f}")

        return result

    def save(self, path: str):
        """保存所有模型"""
        data = {
            "stacking_models": self.stacking_models,
            "use_tuning": self.use_tuning,
            "use_stacking": self.use_stacking,
        }
        with open(path, "wb") as f:
            pickle.dump(data, f)
        logger.info(f"模型优化器已保存: {path}")

    def load(self, path: str) -> "ModelOptimizer":
        """加载模型"""
        with open(path, "rb") as f:
            data = pickle.load(f)
            self.stacking_models = data.get("stacking_models", {})
        logger.info(f"模型优化器已加载: {path}")
        return self


if __name__ == "__main__":
    # 测试
    from sklearn.datasets import make_regression

    print("模型优化模块测试")
    print("=" * 50)

    # 生成测试数据
    X, y = make_regression(n_samples=1000, n_features=50, noise=0.1, random_state=42)

    # 测试 Stacking
    print("\n测试 Stacking 集成...")
    stacking = StackingEnsemble(n_folds=3)
    stacking.fit(X[:500], y[:500])

    y_pred = stacking.predict(X[500:600])
    from sklearn.metrics import mean_squared_error
    mse = mean_squared_error(y[500:600], y_pred)
    print(f"MSE: {mse:.4f}")

    print("\n测试成功!")