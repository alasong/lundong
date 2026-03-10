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
        lookback: int = 10
    ) -> pd.DataFrame:
        """
        准备预测特征

        Args:
            concept_data: 概念板块行情
            lookback: 回溯天数
        """
        features = []

        for concept_code in concept_data["concept_code"].unique():
            concept_df = concept_data[concept_data["concept_code"] == concept_code].sort_values("trade_date")

            if len(concept_df) < lookback + 20:
                continue

            for i in range(lookback, len(concept_df) - 20):
                window = concept_df.iloc[i - lookback:i + 1]

                feature_row = {
                    "concept_code": concept_code,
                    "trade_date": concept_df.iloc[i]["trade_date"],
                }

                # 涨跌幅序列特征
                for j in range(lookback):
                    feature_row[f"pct_chg_{j}"] = window.iloc[j]["pct_chg"]

                # 滚动统计特征
                for period in [3, 5, 10]:
                    tail = window["pct_chg"].tail(period)
                    feature_row[f"pct_mean_{period}"] = tail.mean()
                    feature_row[f"pct_std_{period}"] = tail.std() if len(tail) > 1 else 0
                    feature_row[f"pct_max_{period}"] = tail.max()
                    feature_row[f"pct_min_{period}"] = tail.min()

                # 动量特征
                feature_row["momentum_3"] = window.iloc[-1]["pct_chg"] - window.iloc[-3]["pct_chg"]
                feature_row["momentum_5"] = window.iloc[-1]["pct_chg"] - window.iloc[-5]["pct_chg"]
                feature_row["momentum_10"] = window.iloc[-1]["pct_chg"] - window.iloc[0]["pct_chg"]

                # 趋势特征
                feature_row["trend"] = (window["pct_chg"] > 0).sum() / lookback
                feature_row["连续上涨天数"] = self._count_continuous_up(window["pct_chg"])

                # 成交量特征（如果有）
                if "vol" in concept_df.columns:
                    feature_row["vol_mean_5"] = window["vol"].tail(5).mean()
                    feature_row["vol_ratio"] = window["vol"].iloc[-1] / window["vol"].tail(5).mean()

                # 目标：1 日、5 日、20 日涨幅
                if i + 20 < len(concept_df):
                    feature_row["target_1d"] = concept_df.iloc[i + 1]["pct_chg"]
                    feature_row["target_5d"] = concept_df.iloc[i + 1:i + 6]["pct_chg"].sum()
                    feature_row["target_20d"] = concept_df.iloc[i + 1:i + 21]["pct_chg"].sum()
                else:
                    feature_row["target_1d"] = None
                    feature_row["target_5d"] = None
                    feature_row["target_20d"] = None

                features.append(feature_row)

        return pd.DataFrame(features)

    def _count_continuous_up(self, pct_series: pd.Series) -> int:
        """计算连续上涨天数"""
        count = 0
        for val in reversed(pct_series.values):
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
