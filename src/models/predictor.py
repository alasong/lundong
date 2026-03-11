"""
高性能预测模型
使用 XGBoost/LightGBM 进行预测，支持高并发批处理
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List
from loguru import logger
import os
import sys
import pickle
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories


class UnifiedPredictor:
    """高性能统一预测器"""

    def __init__(self):
        ensure_directories()
        self.models_dir = os.path.join(settings.data_dir, "models")
        os.makedirs(self.models_dir, exist_ok=True)
        self.models = {}

    def prepare_features(
        self,
        concept_data: pd.DataFrame,
        lookback: int = 10,
        use_parallel: bool = True,
        n_jobs: int = 32
    ) -> pd.DataFrame:
        """
        准备预测特征（高性能向量化版本）

        Args:
            concept_data: 概念板块行情
            lookback: 回溯天数
            use_parallel: 是否使用并行处理
            n_jobs: 并行任务数
        """
        start_time = time.time()

        # 数据预处理
        df = concept_data.copy()
        if "pct_chg" in df.columns:
            df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0)
        if "vol" in df.columns:
            df["vol"] = pd.to_numeric(df["vol"], errors="coerce").fillna(0)

        # 按 concept_code 分组
        grouped = df.groupby("concept_code")
        concept_codes = list(grouped.groups.keys())

        logger.debug(f"准备处理 {len(concept_codes)} 个板块的特征")

        if use_parallel:
            from joblib import Parallel, delayed
            # 动态调整并发数，避免过度并发
            actual_jobs = min(n_jobs, len(concept_codes))
            if actual_jobs <= 0:
                actual_jobs = 1

            logger.debug(f"使用 {actual_jobs} 个并行任务")
            results = Parallel(
                n_jobs=actual_jobs,
                backend="threading",
                verbose=0
            )(
                delayed(self._process_single_concept_vectorized)(name, grouped.get_group(name), lookback)
                for name in concept_codes
            )
            all_features = [r for r in results if r is not None]
        else:
            all_features = []
            for concept_code, concept_df in grouped:
                result = self._process_single_concept_vectorized(concept_code, concept_df, lookback)
                if result is not None:
                    all_features.append(result)

        if not all_features:
            logger.warning("未能生成任何特征数据")
            return pd.DataFrame()

        result_df = pd.concat(all_features, ignore_index=True)
        elapsed = time.time() - start_time
        logger.info(f"特征准备完成：{len(result_df)} 条样本，耗时 {elapsed:.2f}s")

        return result_df

    def _process_single_concept_vectorized(
        self,
        concept_code: str,
        concept_df: pd.DataFrame,
        lookback: int = 10
    ) -> Optional[pd.DataFrame]:
        """处理单个 concept 的特征（向量化优化版）"""
        concept_df = concept_df.sort_values("trade_date").reset_index(drop=True)
        min_required = lookback + 20

        if len(concept_df) < min_required:
            return None

        n = len(concept_df)
        name = concept_df["name"].iloc[0] if "name" in concept_df.columns else ""

        pct_chg = concept_df["pct_chg"].values
        vol = concept_df["vol"].values if "vol" in concept_df.columns else None
        trade_dates = concept_df["trade_date"].values

        # 计算有效样本数
        valid_samples = n - min_required

        if valid_samples <= 0:
            return None

        # 预分配数组
        features = {
            "concept_code": [concept_code] * valid_samples,
            "trade_date": trade_dates[lookback:lookback + valid_samples],
            "name": [name] * valid_samples,
        }

        # 使用滚动窗口向量化计算
        for j in range(lookback):
            features[f"pct_chg_{j}"] = pct_chg[j:j + valid_samples]

        # 滚动统计特征
        for period in [3, 5, 10]:
            # 使用 stride_tricks 创建滚动窗口
            window_data = np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=period
            )[:valid_samples]
            features[f"pct_mean_{period}"] = np.mean(window_data, axis=1)
            features[f"pct_std_{period}"] = np.std(window_data, axis=1)
            features[f"pct_max_{period}"] = np.max(window_data, axis=1)
            features[f"pct_min_{period}"] = np.min(window_data, axis=1)

        # 动量特征（向量化）
        features["momentum_3"] = pct_chg[lookback - 1:lookback - 1 + valid_samples] - \
                                 pct_chg[lookback - 3:lookback - 3 + valid_samples]
        features["momentum_5"] = pct_chg[lookback - 1:lookback - 1 + valid_samples] - \
                                 pct_chg[lookback - 5:lookback - 5 + valid_samples]
        features["momentum_10"] = pct_chg[lookback - 1:lookback - 1 + valid_samples] - \
                                  pct_chg[:valid_samples]

        # 趋势特征
        window_matrix = np.lib.stride_tricks.sliding_window_view(
            pct_chg, window_shape=lookback
        )[:valid_samples]
        features["trend"] = np.sum(window_matrix > 0, axis=1) / lookback

        # 连续上涨天数（需要循环，但只处理最后几天）
        features["连续上涨天数"] = np.array([
            self._count_continuous_up_vectorized(window_matrix[i])
            for i in range(valid_samples)
        ])

        # 成交量特征
        if vol is not None:
            vol_window = np.lib.stride_tricks.sliding_window_view(
                vol, window_shape=lookback
            )[:valid_samples]
            vol_tail = np.lib.stride_tricks.sliding_window_view(
                vol, window_shape=5
            )[:valid_samples]
            features["vol_mean_5"] = np.mean(vol_tail, axis=1)
            vol_mean_5 = features["vol_mean_5"]
            features["vol_ratio"] = np.where(
                vol_mean_5 > 0,
                vol[lookback - 1:lookback - 1 + valid_samples] / vol_mean_5,
                1.0
            )

        # 目标值（向量化）
        features["target_1d"] = pct_chg[lookback + 1:lookback + 1 + valid_samples]
        features["target_5d"] = np.sum(
            np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=5
            )[lookback:lookback + valid_samples],
            axis=1
        )
        features["target_20d"] = np.sum(
            np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=20
            )[lookback:lookback + valid_samples],
            axis=1
        )

        return pd.DataFrame(features)

    def _count_continuous_up_vectorized(self, pct_array: np.ndarray) -> int:
        """向量化计算连续上涨天数"""
        # 从后往前找第一个非正数的位置
        for i in range(len(pct_array) - 1, -1, -1):
            if pct_array[i] <= 0:
                return len(pct_array) - 1 - i
        return len(pct_array)

    def train(
        self,
        features: pd.DataFrame,
        model_type: str = "xgboost",
        n_jobs: int = -1
    ) -> Dict:
        """
        训练模型（高并发版本）

        Args:
            features: 特征数据
            model_type: 模型类型
            n_jobs: 训练并发数
        """
        start_time = time.time()
        logger.info(f"开始训练模型 (n_jobs={n_jobs})...")

        # 准备训练数据
        train_data = features.dropna(subset=["target_1d", "target_5d", "target_20d"])
        feature_cols = [c for c in train_data.columns
                       if c not in ["concept_code", "trade_date", "target_1d",
                                    "target_5d", "target_20d", "name"]]

        X = train_data[feature_cols].values
        y_1d = train_data["target_1d"].values
        y_5d = train_data["target_5d"].values
        y_20d = train_data["target_20d"].values

        # 划分训练集和测试集
        from sklearn.model_selection import train_test_split
        X_train, X_test, y1_train, y1_test = train_test_split(
            X, y_1d, test_size=0.2, shuffle=False
        )
        _, _, y5_train, y5_test = train_test_split(
            X, y_5d, test_size=0.2, shuffle=False
        )
        _, _, y20_train, y20_test = train_test_split(
            X, y_20d, test_size=0.2, shuffle=False
        )

        # 训练三个模型
        models = {}
        metrics = {}

        for horizon_name, y_train, y_test in [
            ("1d", y1_train, y1_test),
            ("5d", y5_train, y5_test),
            ("20d", y20_train, y20_test)
        ]:
            model = self._create_model(model_type, n_jobs=n_jobs)
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
            model_metrics = {
                "mse": mean_squared_error(y_test, y_pred),
                "mae": mean_absolute_error(y_test, y_pred),
                "r2": r2_score(y_test, y_pred)
            }

            models[horizon_name] = model
            metrics[f"horizon_{horizon_name}"] = model_metrics
            logger.info(f"{horizon_name}日模型：MSE={model_metrics['mse']:.4f}, R2={model_metrics['r2']:.4f}")

        # 保存模型
        model_path = os.path.join(self.models_dir, "unified_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump({"models": models, "feature_cols": feature_cols}, f)

        elapsed = time.time() - start_time
        logger.info(f"模型训练完成，耗时 {elapsed:.2f}s，已保存：{model_path}")

        return {
            "models": models,
            "metrics": metrics,
            "feature_cols": feature_cols
        }

    def _create_model(self, model_type: str, n_jobs: int = -1):
        """创建模型实例"""
        if model_type == "xgboost":
            try:
                from xgboost import XGBRegressor
                return XGBRegressor(
                    n_estimators=200,
                    max_depth=5,
                    learning_rate=0.05,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    random_state=42,
                    n_jobs=n_jobs
                )
            except ImportError:
                logger.warning("XGBoost 未安装，使用 LightGBM")
                model_type = "lightgbm"

        if model_type == "lightgbm":
            try:
                from lightgbm import LGBMRegressor
                return LGBMRegressor(
                    n_estimators=200,
                    max_depth=5,
                    learning_rate=0.05,
                    random_state=42,
                    n_jobs=n_jobs
                )
            except ImportError:
                logger.warning("LightGBM 未安装，使用 RandomForest")
                model_type = "randomforest"

        from sklearn.ensemble import RandomForestRegressor
        return RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=n_jobs
        )

    def load_model(self) -> Optional[Dict]:
        """加载已训练的模型"""
        model_path = os.path.join(self.models_dir, "unified_model.pkl")
        if os.path.exists(model_path):
            with open(model_path, "rb") as f:
                return pickle.load(f)
        return None

    def predict(
        self,
        model_result: Optional[Dict],
        features: pd.DataFrame,
        batch_size: int = 10000
    ) -> pd.DataFrame:
        """
        批量预测（高性能版本）

        Args:
            model_result: 训练结果（包含 models 和 feature_cols）
            features: 特征数据
            batch_size: 批处理大小
        """
        start_time = time.time()

        if model_result is None:
            model_result = self.load_model()
            if model_result is None:
                logger.warning("未找到已训练的模型")
                return pd.DataFrame()

        models = model_result["models"]
        feature_cols = model_result["feature_cols"]

        # 特征对齐
        missing_cols = set(feature_cols) - set(features.columns)
        if missing_cols:
            logger.error(f"缺少特征列：{missing_cols}")
            return pd.DataFrame()

        X = features[feature_cols].values

        # 批量预测
        n_samples = len(X)
        num_batches = (n_samples + batch_size - 1) // batch_size

        logger.debug(f"开始批量预测：{n_samples} 样本，{num_batches} 批次")

        # 存储预测结果
        pred_1d_list = []
        pred_5d_list = []
        pred_20d_list = []

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, n_samples)
            X_batch = X[start_idx:end_idx]

            pred_1d_list.append(models["1d"].predict(X_batch))
            pred_5d_list.append(models["5d"].predict(X_batch))
            pred_20d_list.append(models["20d"].predict(X_batch))

        # 合并结果
        pred_1d = np.concatenate(pred_1d_list)
        pred_5d = np.concatenate(pred_5d_list)
        pred_20d = np.concatenate(pred_20d_list)

        # 构建结果 DataFrame
        pred_cols = ["concept_code", "trade_date"]
        if "name" in features.columns:
            pred_cols.append("name")

        predictions = features[pred_cols].copy()
        predictions["pred_1d"] = pred_1d
        predictions["pred_5d"] = pred_5d
        predictions["pred_20d"] = pred_20d

        # 综合评分（加权）
        predictions["combined_score"] = (
            predictions["pred_1d"] * 0.3 +
            predictions["pred_5d"] * 0.5 +
            predictions["pred_20d"] * 0.2
        )

        elapsed = time.time() - start_time
        logger.info(f"预测完成：{n_samples} 样本，耗时 {elapsed:.2f}s")

        return predictions

    def predict_latest(
        self,
        concept_data: pd.DataFrame,
        n_jobs: int = 32
    ) -> pd.DataFrame:
        """
        端到端预测（特征准备 + 预测）

        Args:
            concept_data: 概念板块行情数据
            n_jobs: 特征准备并发数
        """
        start_time = time.time()
        logger.info("开始端到端预测...")

        # 准备特征
        features = self.prepare_features(concept_data, n_jobs=n_jobs)
        if features.empty:
            logger.warning("特征为空，返回空结果")
            return pd.DataFrame()

        # 加载模型并预测
        model_result = self.load_model()
        if model_result is None:
            logger.warning("未找到模型，返回空结果")
            return pd.DataFrame()

        predictions = self.predict(model_result, features)

        elapsed = time.time() - start_time
        logger.info(f"端到端预测完成，总耗时 {elapsed:.2f}s")

        return predictions


def main():
    """主函数"""
    predictor = UnifiedPredictor()
    logger.info("高性能预测模型已就绪")


if __name__ == "__main__":
    main()
