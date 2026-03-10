"""
简化预测模型
使用 XGBoost 进行统一预测，支持 1 日/5 日/20 日多个周期
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List
from loguru import logger
import os
import sys
import pickle

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories


class UnifiedPredictor:
    """统一预测器 - 简化版"""

    def __init__(self):
        ensure_directories()
        self.models_dir = os.path.join(settings.data_dir, "models")
        os.makedirs(self.models_dir, exist_ok=True)
        self.models = {}

    def prepare_features(
        self,
        concept_data: pd.DataFrame,
        lookback: int = 10,
        use_parallel: bool = True
    ) -> pd.DataFrame:
        """
        准备预测特征（优化版，使用向量化操作）

        Args:
            concept_data: 概念板块行情
            lookback: 回溯天数
            use_parallel: 是否使用并行处理
        """
        # 数据预处理：重命名字段，确保数据类型正确
        df = concept_data.copy()
        if "pct_chg" in df.columns:
            df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0)
        if "vol" in df.columns:
            df["vol"] = pd.to_numeric(df["vol"], errors="coerce").fillna(0)

        # 按 concept_code 分组处理
        all_features = []

        # 按 concept_code 分组
        grouped = df.groupby("concept_code")

        if use_parallel:
            from joblib import Parallel, delayed
            results = Parallel(n_jobs=-1, backend="loky")(
                delayed(self._process_single_concept)(name, group, lookback)
                for name, group in grouped
            )
            all_features = [r for r in results if r is not None]
        else:
            for concept_code, concept_df in grouped:
                result = self._process_single_concept(concept_code, concept_df, lookback)
                if result is not None:
                    all_features.append(result)

        if not all_features:
            return pd.DataFrame()

        return pd.concat(all_features, ignore_index=True)

    def _process_single_concept(
        self,
        concept_code: str,
        concept_df: pd.DataFrame,
        lookback: int = 10
    ) -> Optional[pd.DataFrame]:
        """处理单个 concept 的特征"""
        concept_df = concept_df.sort_values("trade_date").reset_index(drop=True)

        if len(concept_df) < lookback + 20:
            return None

        # 使用向量化操作计算特征
        n = len(concept_df)
        features_list = []

        pct_chg = concept_df["pct_chg"].values
        vol = concept_df["vol"].values if "vol" in concept_df.columns else None

        for i in range(lookback, n - 20):
            window = pct_chg[i-lookback:i+1]

            feature_row = {
                "concept_code": concept_code,
                "trade_date": concept_df.iloc[i]["trade_date"],
            }

            # 涨跌幅序列特征（向量化）
            for j in range(lookback):
                feature_row[f"pct_chg_{j}"] = window[j]

            # 滚动统计特征（向量化）
            for period in [3, 5, 10]:
                tail = window[-period:]
                feature_row[f"pct_mean_{period}"] = np.mean(tail)
                feature_row[f"pct_std_{period}"] = np.std(tail) if len(tail) > 1 else 0
                feature_row[f"pct_max_{period}"] = np.max(tail)
                feature_row[f"pct_min_{period}"] = np.min(tail)

            # 动量特征
            feature_row["momentum_3"] = window[-1] - window[-3]
            feature_row["momentum_5"] = window[-1] - window[-5]
            feature_row["momentum_10"] = window[-1] - window[0]

            # 趋势特征
            feature_row["trend"] = np.sum(window > 0) / lookback
            feature_row["连续上涨天数"] = self._count_continuous_up_fast(window)

            # 成交量特征
            if vol is not None:
                vol_window = vol[i-lookback:i+1]
                feature_row["vol_mean_5"] = np.mean(vol_window[-5:])
                vol_mean_5 = np.mean(vol_window[-5:])
                feature_row["vol_ratio"] = vol_window[-1] / vol_mean_5 if vol_mean_5 > 0 else 1.0

            # 目标值
            feature_row["target_1d"] = pct_chg[i + 1]
            feature_row["target_5d"] = np.sum(pct_chg[i + 1:i + 6])
            feature_row["target_20d"] = np.sum(pct_chg[i + 1:i + 21])

            features_list.append(feature_row)

        if not features_list:
            return None

        return pd.DataFrame(features_list)

    def _count_continuous_up_fast(self, pct_array: np.ndarray) -> int:
        """快速计算连续上涨天数"""
        count = 0
        for val in reversed(pct_array):
            if val > 0:
                count += 1
            else:
                break
        return count

    def train(
        self,
        features: pd.DataFrame,
        model_type: str = "xgboost"
    ) -> Dict:
        """
        训练模型

        Args:
            features: 特征数据
            model_type: 模型类型
        """
        logger.info("开始训练模型...")

        # 准备训练数据
        train_data = features.dropna(subset=["target_1d", "target_5d", "target_20d"])
        feature_cols = [c for c in train_data.columns if c not in ["concept_code", "trade_date", "target_1d", "target_5d", "target_20d"]]

        X = train_data[feature_cols].values
        y_1d = train_data["target_1d"].values
        y_5d = train_data["target_5d"].values
        y_20d = train_data["target_20d"].values

        # 划分训练集和测试集
        from sklearn.model_selection import train_test_split
        X_train, X_test, y1_train, y1_test = train_test_split(X, y_1d, test_size=0.2, shuffle=False)
        _, _, y5_train, y5_test = train_test_split(X, y_5d, test_size=0.2, shuffle=False)
        _, _, y20_train, y20_test = train_test_split(X, y_20d, test_size=0.2, shuffle=False)

        # 训练三个模型
        models = {}
        metrics = {}

        for name, y_train, y_test in [
            ("1d", y1_train, y1_test),
            ("5d", y5_train, y5_test),
            ("20d", y20_train, y20_test)
        ]:
            if model_type == "xgboost":
                try:
                    from xgboost import XGBRegressor
                    model = XGBRegressor(
                        n_estimators=200,
                        max_depth=5,
                        learning_rate=0.05,
                        subsample=0.8,
                        colsample_bytree=0.8,
                        random_state=42,
                        n_jobs=-1
                    )
                except ImportError:
                    logger.warning("XGBoost 未安装，使用 LightGBM")
                    from lightgbm import LGBMRegressor
                    model = LGBMRegressor(
                        n_estimators=200,
                        max_depth=5,
                        learning_rate=0.05,
                        random_state=42,
                        n_jobs=-1
                    )
            else:
                from sklearn.ensemble import RandomForestRegressor
                model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42,
                    n_jobs=-1
                )

            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            # 评估
            from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
            model_metrics = {
                "mse": mean_squared_error(y_test, y_pred),
                "mae": mean_absolute_error(y_test, y_pred),
                "r2": r2_score(y_test, y_pred)
            }

            models[name] = model
            metrics[f"horizon_{name}"] = model_metrics
            logger.info(f"{name}日模型评估：MSE={model_metrics['mse']:.4f}, R2={model_metrics['r2']:.4f}")

        # 保存模型
        model_path = os.path.join(self.models_dir, "unified_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump({"models": models, "feature_cols": feature_cols}, f)
        logger.info(f"模型已保存：{model_path}")

        return {
            "models": models,
            "metrics": metrics,
            "feature_cols": feature_cols
        }

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
        features: pd.DataFrame
    ) -> pd.DataFrame:
        """
        进行预测

        Args:
            model_result: 训练结果（包含 models 和 feature_cols）
            features: 特征数据
        """
        if model_result is None:
            # 尝试加载已保存的模型
            model_result = self.load_model()
            if model_result is None:
                logger.warning("未找到已训练的模型")
                return pd.DataFrame()

        models = model_result["models"]
        feature_cols = model_result["feature_cols"]

        # 确保特征列对齐
        X = features[feature_cols].values

        # 预测
        predictions = features[["concept_code", "trade_date"]].copy()
        predictions["pred_1d"] = models["1d"].predict(X)
        predictions["pred_5d"] = models["5d"].predict(X)
        predictions["pred_20d"] = models["20d"].predict(X)

        # 综合评分（加权）
        predictions["combined_score"] = (
            predictions["pred_1d"] * 0.3 +
            predictions["pred_5d"] * 0.5 +
            predictions["pred_20d"] * 0.2
        )

        return predictions


def main():
    """主函数"""
    predictor = UnifiedPredictor()
    logger.info("简化预测模型已就绪")


if __name__ == "__main__":
    main()
