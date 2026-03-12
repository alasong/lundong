"""
个股预测模型
独立于板块预测的参数和模型
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


class StockPredictor:
    """个股预测器 - 独立模型参数"""

    # 综合评分权重配置 (与板块预测不同)
    HORIZON_WEIGHTS = {"1d": 0.4, "5d": 0.4, "20d": 0.2}

    # 模型文件名
    MODEL_FILE = "stock_model.pkl"

    def __init__(self):
        ensure_directories()
        self.models_dir = os.path.join(settings.data_dir, "models")
        os.makedirs(self.models_dir, exist_ok=True)
        self.models = {}
        logger.info("个股预测器初始化完成")

    def prepare_features(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 10,
        use_parallel: bool = True,
        n_jobs: int = 32
    ) -> pd.DataFrame:
        """
        准备个股预测特征

        Args:
            stock_data: 个股行情数据
            lookback: 回溯天数
            use_parallel: 是否使用并行处理
            n_jobs: 并行任务数

        Returns:
            特征 DataFrame
        """
        start_time = time.time()

        # 数据预处理
        df = stock_data.copy()
        if "pct_chg" in df.columns:
            df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0)
        if "vol" in df.columns:
            df["vol"] = pd.to_numeric(df["vol"], errors="coerce").fillna(0)
        if "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

        # 按 ts_code 分组
        grouped = df.groupby("ts_code")
        stock_codes = list(grouped.groups.keys())

        logger.debug(f"准备处理 {len(stock_codes)} 只股票的特征")

        if use_parallel:
            from joblib import Parallel, delayed
            # CPU 密集型任务使用 multiprocessing backend
            actual_jobs = min(n_jobs, len(stock_codes))
            if actual_jobs <= 0:
                actual_jobs = 1

            logger.debug(f"使用 {actual_jobs} 个并行任务 (multiprocessing)")
            results = Parallel(
                n_jobs=actual_jobs,
                backend="multiprocessing",
                verbose=0
            )(
                delayed(self._process_single_stock_vectorized)(
                    code, grouped.get_group(code), lookback
                )
                for code in stock_codes
            )
            all_features = [r for r in results if r is not None]
        else:
            all_features = []
            for stock_code, stock_df in grouped:
                result = self._process_single_stock_vectorized(stock_code, stock_df, lookback)
                if result is not None:
                    all_features.append(result)

        if not all_features:
            logger.warning("未能生成任何特征数据")
            return pd.DataFrame()

        result_df = pd.concat(all_features, ignore_index=True)
        elapsed = time.time() - start_time
        logger.info(f"个股特征准备完成：{len(result_df)} 条样本，耗时 {elapsed:.2f}s")

        return result_df

    def _process_single_stock_vectorized(
        self,
        stock_code: str,
        stock_df: pd.DataFrame,
        lookback: int = 10
    ) -> Optional[pd.DataFrame]:
        """
        处理单只股票的特征（向量化优化版）

        类似板块预测的特征结构，但针对个股优化
        """
        stock_df = stock_df.sort_values("trade_date").reset_index(drop=True)
        min_required = lookback + 20

        if len(stock_df) < min_required:
            logger.debug(f"{stock_code} 数据不足 ({len(stock_df)} < {min_required})")
            return None

        n = len(stock_df)
        stock_name = stock_df["name"].iloc[0] if "name" in stock_df.columns else ""

        pct_chg = stock_df["pct_chg"].values
        vol = stock_df["vol"].values if "vol" in stock_df.columns else None
        amount = stock_df["amount"].values if "amount" in stock_df.columns else None
        trade_dates = stock_df["trade_date"].values

        # 计算有效样本数
        valid_samples = n - min_required

        if valid_samples <= 0:
            return None

        # 预分配数组
        features = {
            "ts_code": [stock_code] * valid_samples,
            "trade_date": trade_dates[lookback:lookback + valid_samples],
            "stock_name": [stock_name] * valid_samples,
        }

        # 历史收益率特征
        for j in range(lookback):
            features[f"pct_chg_{j}"] = pct_chg[j:j + valid_samples]

        # 滚动统计特征
        for period in [3, 5, 10, 20]:
            window_data = np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=period
            )[:valid_samples]
            features[f"pct_mean_{period}"] = np.mean(window_data, axis=1)
            features[f"pct_std_{period}"] = np.std(window_data, axis=1)
            features[f"pct_max_{period}"] = np.max(window_data, axis=1)
            features[f"pct_min_{period}"] = np.min(window_data, axis=1)

        # 动量特征
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

        # 成交量特征 (个股特有)
        if vol is not None:
            vol_window = np.lib.stride_tricks.sliding_window_view(vol, window_shape=5)[:valid_samples]
            features["vol_mean_5"] = np.mean(vol_window, axis=1)
            features["vol_ratio"] = vol[lookback - 1:lookback - 1 + valid_samples] / (np.mean(vol_window, axis=1) + 1e-8)

        # 成交额特征 (个股特有)
        if amount is not None:
            amount_window = np.lib.stride_tricks.sliding_window_view(amount, window_shape=5)[:valid_samples]
            features["amount_mean_5"] = np.mean(amount_window, axis=1)
            features["amount_ratio"] = amount[lookback - 1:lookback - 1 + valid_samples] / (np.mean(amount_window, axis=1) + 1e-8)

        # 目标变量 (未来收益)
        # 确保不越界
        target_1d_end = lookback + valid_samples + 1
        target_5d_end = lookback + valid_samples + 5
        target_20d_end = lookback + valid_samples + 20

        if target_1d_end <= n:
            features["target_1d"] = pct_chg[lookback:lookback + valid_samples]
        else:
            # 数据不足时使用可用部分
            available = n - lookback
            if available > 0:
                features["target_1d"] = pct_chg[lookback:n]
                # 填充 NaN 使长度一致
                features["target_1d"] = np.pad(features["target_1d"], (0, valid_samples - available), constant_values=np.nan)
            else:
                return None

        if target_5d_end <= n:
            target_5d = np.array([pct_chg[i:i+5].sum() for i in range(lookback, n - 4)])
            features["target_5d"] = target_5d[:valid_samples] if len(target_5d) >= valid_samples else \
                                    np.pad(target_5d, (0, valid_samples - len(target_5d)), constant_values=np.nan)
        else:
            features["target_5d"] = np.nan

        if target_20d_end <= n:
            target_20d = np.array([pct_chg[i:i+20].sum() for i in range(lookback, n - 19)])
            features["target_20d"] = target_20d[:valid_samples] if len(target_20d) >= valid_samples else \
                                     np.pad(target_20d, (0, valid_samples - len(target_20d)), constant_values=np.nan)
        else:
            features["target_20d"] = np.nan

        result_df = pd.DataFrame(features)

        # 移除 NaN 过多的行
        result_df = result_df.dropna(subset=["target_1d", "target_5d", "target_20d"], how="all")

        return result_df

    def train(
        self,
        features: pd.DataFrame,
        model_type: str = "xgboost",
        n_jobs: int = -1
    ) -> Dict[str, Any]:
        """
        训练个股预测模型

        Args:
            features: 特征 DataFrame
            model_type: 模型类型 (xgboost/lightgbm/random_forest)
            n_jobs: 并行任务数

        Returns:
            模型结果字典
        """
        logger.info(f"开始训练个股预测模型 ({model_type})...")
        start_time = time.time()

        # 准备训练数据
        feature_cols = [c for c in features.columns if c not in
                       ["ts_code", "trade_date", "stock_name", "target_1d", "target_5d", "target_20d"]]

        X = features[feature_cols].fillna(0)
        y_1d = features["target_1d"].fillna(0)
        y_5d = features["target_5d"].fillna(0)
        y_20d = features["target_20d"].fillna(0)

        logger.info(f"训练样本：{len(X)} 条，特征数：{len(feature_cols)}")

        # 训练三个模型
        models = {}

        try:
            if model_type == "xgboost":
                import xgboost as xgb
                model_class = xgb.XGBRegressor
                model_params = {
                    'n_estimators': 200,
                    'max_depth': 6,
                    'learning_rate': 0.05,
                    'subsample': 0.8,
                    'colsample_bytree': 0.8,
                    'n_jobs': n_jobs,
                    'random_state': 42
                }
            elif model_type == "lightgbm":
                import lightgbm as lgb
                model_class = lgb.LGBMRegressor
                model_params = {
                    'n_estimators': 200,
                    'max_depth': 6,
                    'learning_rate': 0.05,
                    'subsample': 0.8,
                    'colsample_bytree': 0.8,
                    'n_jobs': n_jobs,
                    'random_state': 42,
                    'verbose': -1
                }
            else:  # random_forest
                from sklearn.ensemble import RandomForestRegressor
                model_class = RandomForestRegressor
                model_params = {
                    'n_estimators': 200,
                    'max_depth': 10,
                    'n_jobs': n_jobs,
                    'random_state': 42
                }

            # 训练三个目标
            logger.info("训练 1 日预测模型...")
            models["1d"] = model_class(**model_params).fit(X, y_1d)

            logger.info("训练 5 日预测模型...")
            models["5d"] = model_class(**model_params).fit(X, y_5d)

            logger.info("训练 20 日预测模型...")
            models["20d"] = model_class(**model_params).fit(X, y_20d)

        except ImportError as e:
            logger.error(f"模型库导入失败：{e}，回退到随机森林")
            from sklearn.ensemble import RandomForestRegressor
            model_params = {'n_estimators': 200, 'max_depth': 10, 'n_jobs': n_jobs, 'random_state': 42}

            models["1d"] = RandomForestRegressor(**model_params).fit(X, y_1d)
            models["5d"] = RandomForestRegressor(**model_params).fit(X, y_5d)
            models["20d"] = RandomForestRegressor(**model_params).fit(X, y_20d)

        # 计算训练集指标
        from sklearn.metrics import r2_score, mean_absolute_error

        metrics = {}
        for horizon, model in models.items():
            y_pred = model.predict(X)
            y_true = features[f"target_{horizon}"].fillna(0)

            metrics[horizon] = {
                "r2": r2_score(y_true, y_pred),
                "mae": mean_absolute_error(y_true, y_pred)
            }

        elapsed = time.time() - start_time
        logger.info(f"模型训练完成，耗时 {elapsed:.2f}s")
        logger.info(f"1 日模型 R²={metrics['1d']['r2']:.4f}, 5 日模型 R²={metrics['5d']['r2']:.4f}, "
                   f"20 日模型 R²={metrics['20d']['r2']:.4f}")

        # 保存模型
        model_path = os.path.join(self.models_dir, self.MODEL_FILE)
        model_data = {
            "models": models,
            "feature_cols": feature_cols,
            "model_type": model_type,
            "metrics": metrics
        }

        with open(model_path, "wb") as f:
            pickle.dump(model_data, f)
        logger.info(f"模型已保存：{model_path}")

        return model_data

    def predict(
        self,
        model_result: Optional[Dict],
        features: pd.DataFrame,
        with_confidence: bool = True,
        n_jobs: int = 32
    ) -> pd.DataFrame:
        """
        执行个股预测

        Args:
            model_result: 模型结果（包含 models, feature_cols）
            features: 特征 DataFrame
            with_confidence: 是否计算置信度
            n_jobs: 并行任务数

        Returns:
            预测结果 DataFrame
        """
        logger.info("开始个股预测...")
        start_time = time.time()

        if model_result is None:
            logger.warning("无可用模型，返回空结果")
            return pd.DataFrame()

        models = model_result.get("models", {})
        feature_cols = model_result.get("feature_cols", [])

        if not models or not feature_cols:
            logger.warning("模型数据不完整")
            return pd.DataFrame()

        # 准备特征矩阵
        X = features[feature_cols].fillna(0)

        # 并行预测每个样本
        from joblib import Parallel, delayed

        def predict_single(idx, row):
            """单样本预测"""
            X_row = X.iloc[[idx]]

            pred_1d = models["1d"].predict(X_row)[0]
            pred_5d = models["5d"].predict(X_row)[0]
            pred_20d = models["20d"].predict(X_row)[0]

            result = {
                "ts_code": row.get("ts_code", ""),
                "trade_date": row.get("trade_date", ""),
                "stock_name": row.get("stock_name", ""),
                "pred_1d": pred_1d,
                "pred_5d": pred_5d,
                "pred_20d": pred_20d,
            }

            # 综合评分
            weights = self.HORIZON_WEIGHTS
            result["combined_score"] = (
                pred_1d * weights["1d"] +
                pred_5d * weights["5d"] +
                pred_20d * weights["20d"]
            )

            return result

        # 批量预测
        results = Parallel(n_jobs=n_jobs, backend="threading", verbose=0)(
            delayed(predict_single)(idx, row)
            for idx, row in features.iterrows()
        )

        predictions = pd.DataFrame(results)

        # 计算置信度
        if with_confidence and len(predictions) > 0:
            predictions = self._calculate_confidence(predictions, model_result)

        elapsed = time.time() - start_time
        logger.info(f"预测完成：{len(predictions)} 样本，耗时 {elapsed:.2f}s")

        return predictions

    def _calculate_confidence(
        self,
        predictions: pd.DataFrame,
        model_result: Dict
    ) -> pd.DataFrame:
        """
        计算预测置信度

        基于:
        1. 模型 R² 分数
        2. 综合得分排名
        """
        # 模型 R² (作为基础置信度)
        metrics = model_result.get("metrics", {})
        avg_r2 = np.mean([m.get("r2", 0) for m in metrics.values()])

        # 基于综合得分排名的置信度
        score_rank = predictions["combined_score"].rank(pct=True)

        def get_conf_level(score_pct: float) -> str:
            if score_pct >= 0.70:
                return "高"
            elif score_pct >= 0.40:
                return "中"
            else:
                return "低"

        predictions["confidence_level"] = score_rank.apply(get_conf_level)
        predictions["confidence"] = avg_r2 * score_rank  # 综合置信度分数

        logger.info(f"预测置信度范围：[{predictions['confidence'].min():.3f}, {predictions['confidence'].max():.3f}]")

        return predictions

    def load_model(self) -> Optional[Dict]:
        """加载预训练模型"""
        model_path = os.path.join(self.models_dir, self.MODEL_FILE)

        if not os.path.exists(model_path):
            logger.warning(f"模型文件不存在：{model_path}")
            return None

        try:
            with open(model_path, "rb") as f:
                model_data = pickle.load(f)
            logger.info(f"加载模型成功：{model_path}")
            return model_data
        except Exception as e:
            logger.error(f"加载模型失败：{e}")
            return None


def main():
    """测试函数"""
    from data.database import get_database

    predictor = StockPredictor()

    # 加载测试数据
    db = get_database()
    stock_data = db.get_all_stock_data()

    if stock_data.empty:
        print("无个股数据")
        return

    print(f"\n加载数据：{len(stock_data)} 条记录")

    # 准备特征
    features = predictor.prepare_features(stock_data, lookback=10, n_jobs=8)
    print(f"特征数据：{len(features)} 条样本")

    if features.empty:
        print("特征为空")
        return

    # 训练模型
    model_result = predictor.train(features, model_type="xgboost")
    print(f"训练结果：{model_result['metrics']}")

    # 预测
    model_data = predictor.load_model()
    predictions = predictor.predict(model_data, features)
    print(f"\n预测结果 TOP10:")
    print(predictions.nlargest(10, "combined_score")[["ts_code", "stock_name", "pred_1d", "pred_5d", "combined_score"]])


if __name__ == "__main__":
    main()
