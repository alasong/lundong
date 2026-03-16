"""
高性能预测模型
使用 XGBoost/LightGBM 进行预测，支持高并发批处理
支持增强特征（情绪因子、资金流向因子）
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

    # 综合评分权重配置
    HORIZON_WEIGHTS = {"1d": 0.3, "5d": 0.5, "20d": 0.2}

    # 是否使用增强特征
    USE_ENHANCED_FEATURES = True

    def __init__(self, use_enhanced_features: bool = True):
        ensure_directories()
        self.models_dir = os.path.join(settings.data_dir, "models")
        os.makedirs(self.models_dir, exist_ok=True)
        self.models = {}
        self.use_enhanced_features = use_enhanced_features

    def prepare_features(
        self,
        concept_data: pd.DataFrame,
        lookback: int = 10,
        use_parallel: bool = True,
        n_jobs: int = 32,
        use_enhanced: bool = None
    ) -> pd.DataFrame:
        """
        准备预测特征（高性能向量化版本）

        Args:
            concept_data: 概念板块行情
            lookback: 回溯天数
            use_parallel: 是否使用并行处理
            n_jobs: 并行任务数
            use_enhanced: 是否使用增强特征（None 则使用类默认值）
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
            # CPU 密集型任务使用 multiprocessing backend
            actual_jobs = min(n_jobs, len(concept_codes))
            if actual_jobs <= 0:
                actual_jobs = 1

            logger.debug(f"使用 {actual_jobs} 个并行任务 (multiprocessing)")
            results = Parallel(
                n_jobs=actual_jobs,
                backend="multiprocessing",
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

        # 添加增强特征
        if use_enhanced is None:
            use_enhanced = self.use_enhanced_features
        if use_enhanced:
            result_df = self._add_enhanced_features(result_df, concept_data, lookback)

        elapsed = time.time() - start_time
        logger.info(f"特征准备完成：{len(result_df)} 条样本，{len(result_df.columns)} 个特征，耗时 {elapsed:.2f}s")

        return result_df

    def _add_enhanced_features(
        self,
        base_features: pd.DataFrame,
        concept_data: pd.DataFrame,
        lookback: int = 10
    ) -> pd.DataFrame:
        """
        添加增强特征（情绪因子、资金流向因子）

        Args:
            base_features: 基础特征
            concept_data: 原始数据
            lookback: 回溯天数

        Returns:
            包含增强特征的特征矩阵
        """
        try:
            from models.enhanced_features import EnhancedFeatureEngineer
            engineer = EnhancedFeatureEngineer()

            # 获取需要的列
            enhanced_cols = []
            for col in ["pct_chg", "pct_change", "vol", "amount", "turnover_rate"]:
                if col in concept_data.columns:
                    enhanced_cols.append(col)

            if len(enhanced_cols) < 2:
                logger.warning("数据不足，跳过增强特征")
                return base_features

            # 合并增强特征
            all_enhanced = []
            code_col = "concept_code" if "concept_code" in concept_data.columns else "ts_code"

            for code, group in concept_data.groupby(code_col):
                group = group.sort_values("trade_date").reset_index(drop=True)
                if len(group) < lookback + 5:
                    continue

                # 计算情绪因子
                sentiment = engineer.compute_sentiment_factors(group, lookback)

                # 计算资金流向因子
                capital = engineer.compute_capital_flow_factors(group, lookback)

                # 计算市场宽度因子
                breadth = engineer.compute_market_breadth_factors(group, lookback)

                # 合并
                enhanced = pd.concat([sentiment, capital, breadth], axis=1)
                enhanced[code_col] = code
                enhanced["trade_date"] = group["trade_date"].values[-len(enhanced):]

                all_enhanced.append(enhanced)

            if all_enhanced:
                enhanced_df = pd.concat(all_enhanced, ignore_index=True)

                # 去除重复列
                enhanced_df = enhanced_df.loc[:, ~enhanced_df.columns.duplicated()]

                # 合并到基础特征
                merge_cols = [code_col, "trade_date"]
                result = base_features.merge(
                    enhanced_df,
                    on=merge_cols,
                    how="left"
                )

                # 填充缺失值
                for col in result.columns:
                    if col not in merge_cols and col not in ["name"]:
                        result[col] = result[col].fillna(0)

                logger.info(f"增强特征已添加：{len(base_features.columns)} -> {len(result.columns)} 个特征")
                return result

        except Exception as e:
            logger.warning(f"增强特征计算失败: {e}，使用基础特征")

        return base_features

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

        # ===== 扩展特征工程 =====

        # 1. 波动率特征
        for period in [3, 5, 10, 20]:
            window_data = np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=period
            )[:valid_samples]
            # 波动率（标准差/均值）
            features[f"volatility_{period}"] = np.std(window_data, axis=1) / (np.mean(np.abs(window_data), axis=1) + 1e-8)
            # 偏度（衡量分布不对称性）
            features[f"skewness_{period}"] = np.mean(((window_data - np.mean(window_data, axis=1, keepdims=True)) /
                                                       (np.std(window_data, axis=1, keepdims=True) + 1e-8)) ** 3, axis=1)
            # 峰度（衡量分布尾部厚度）
            features[f"kurtosis_{period}"] = np.mean(((window_data - np.mean(window_data, axis=1, keepdims=True)) /
                                                       (np.std(window_data, axis=1, keepdims=True) + 1e-8)) ** 4, axis=1) - 3

        # 2. 价格位置特征
        for period in [5, 10, 20]:
            window_data = np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=period
            )[:valid_samples]
            # 当前值在窗口中的百分位
            current_vals = pct_chg[lookback - 1:lookback - 1 + valid_samples]
            features[f"pct_rank_{period}"] = np.array([
                np.sum(window_data[i] <= current_vals[i]) / period
                for i in range(valid_samples)
            ])
            # 突破近期高点
            features[f"breakout_{period}"] = (current_vals >= np.max(window_data, axis=1)).astype(float)

        # 3. MACD 类特征
        ema_12 = pd.Series(pct_chg).ewm(span=12, adjust=False).mean().values
        ema_26 = pd.Series(pct_chg).ewm(span=26, adjust=False).mean().values
        macd_line = ema_12 - ema_26
        signal_line = pd.Series(macd_line).ewm(span=9, adjust=False).mean().values
        macd_hist = macd_line - signal_line

        # 确保数组长度匹配
        features["macd"] = macd_line[lookback - 1:lookback - 1 + valid_samples]
        features["macd_signal"] = signal_line[lookback - 1:lookback - 1 + valid_samples]
        features["macd_hist"] = macd_hist[lookback - 1:lookback - 1 + valid_samples]

        # 4. RSI 类特征
        for period in [6, 12]:
            gains = np.maximum(pct_chg, 0)
            losses = np.maximum(-pct_chg, 0)
            avg_gain = pd.Series(gains).ewm(span=period, adjust=False).mean().values
            avg_loss = pd.Series(losses).ewm(span=period, adjust=False).mean().values
            rs = avg_gain / (avg_loss + 1e-8)
            rsi = 100 - (100 / (1 + rs))
            features[f"rsi_{period}"] = rsi[lookback - 1:lookback - 1 + valid_samples]

        # 5. 成交量特征扩展
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
            # 成交量趋势
            features["vol_trend"] = np.sum(vol_tail > np.mean(vol_tail, axis=1, keepdims=True), axis=1) / 5
            # 成交量波动率
            features["vol_volatility"] = np.std(vol_tail, axis=1) / (vol_mean_5 + 1e-8)

        # 6. 量价关系特征
        if vol is not None:
            # 量价相关性
            for period in [5, 10]:
                vol_window_local = np.lib.stride_tricks.sliding_window_view(
                    vol, window_shape=period
                )[:valid_samples]
                pct_window_local = np.lib.stride_tricks.sliding_window_view(
                    pct_chg, window_shape=period
                )[:valid_samples]

                # 计算相关系数
                def corr_coeff(v, p):
                    if np.std(v) < 1e-8 or np.std(p) < 1e-8:
                        return 0
                    return np.corrcoef(v, p)[0, 1]

                features[f"vol_price_corr_{period}"] = np.array([
                    corr_coeff(vol_window_local[i], pct_window_local[i])
                    for i in range(valid_samples)
                ])

        # 7. 缺口特征（大幅跳空）
        pct_diff = np.diff(pct_chg, prepend=pct_chg[0])
        features["gap_up"] = (pct_diff[lookback - 1:lookback - 1 + valid_samples] > 2).astype(float)
        features["gap_down"] = (pct_diff[lookback - 1:lookback - 1 + valid_samples] < -2).astype(float)

        # 8. 极端涨跌幅特征
        features["extreme_up"] = (pct_chg[lookback - 1:lookback - 1 + valid_samples] > 5).astype(float)
        features["extreme_down"] = (pct_chg[lookback - 1:lookback - 1 + valid_samples] < -5).astype(float)

        # 9. 动量加速/减速
        features["momentum_accel"] = pct_chg[lookback - 1:lookback - 1 + valid_samples] - 2 * pct_chg[lookback - 2:lookback - 2 + valid_samples] + pct_chg[lookback - 3:lookback - 3 + valid_samples]

        # 10. 均值回归信号
        for period in [5, 10]:
            window_data = np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=period
            )[:valid_samples]
            mean_vals = np.mean(window_data, axis=1)
            std_vals = np.std(window_data, axis=1)
            current_vals = pct_chg[lookback - 1:lookback - 1 + valid_samples]
            # Z-Score
            features[f"zscore_{period}"] = (current_vals - mean_vals) / (std_vals + 1e-8)
            # 均值回归信号（Z-Score < -1 表示超卖）
            features[f"mean_revert_{period}"] = (features[f"zscore_{period}"] < -1).astype(float)

        # 确保所有特征数组长度一致
        min_length = min(len(v) for v in features.values() if isinstance(v, np.ndarray))
        for key in features:
            if isinstance(features[key], np.ndarray) and len(features[key]) > min_length:
                features[key] = features[key][:min_length]

        # 目标值（向量化）- 确保长度匹配
        features["target_1d"] = pct_chg[lookback + 1:lookback + 1 + valid_samples][:min_length]
        features["target_5d"] = np.sum(
            np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=5
            )[lookback:lookback + valid_samples][:min_length],
            axis=1
        )
        features["target_20d"] = np.sum(
            np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=20
            )[lookback:lookback + valid_samples][:min_length],
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

        # 划分训练集和测试集（一次性划分，复用索引）
        from sklearn.model_selection import train_test_split
        train_idx, test_idx = train_test_split(
            range(len(X)), test_size=0.2, shuffle=False
        )
        X_train, X_test = X[train_idx], X[test_idx]
        y1_train, y1_test = y_1d[train_idx], y_1d[test_idx]
        y5_train, y5_test = y_5d[train_idx], y_5d[test_idx]
        y20_train, y20_test = y_20d[train_idx], y_20d[test_idx]

        # 训练三个模型
        models = {}
        metrics = {}
        feature_importances = {}

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

            # 提取特征重要性
            if hasattr(model, 'feature_importances_'):
                importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
                # 按重要性排序
                sorted_importance = sorted(importance.items(), key=lambda x: x[1], reverse=True)
                feature_importances[f"horizon_{horizon_name}"] = sorted_importance

                # 输出 TOP10 重要特征
                logger.info(f"{horizon_name}日模型 TOP10 特征:")
                for feat, imp in sorted_importance[:10]:
                    logger.info(f"  {feat}: {imp:.4f}")

            logger.info(f"{horizon_name}日模型：MSE={model_metrics['mse']:.4f}, R2={model_metrics['r2']:.4f}")

        # 保存模型（包含特征重要性和指标）
        model_path = os.path.join(self.models_dir, "unified_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump({
                "models": models,
                "feature_cols": feature_cols,
                "feature_importances": feature_importances,
                "metrics": metrics,
                "train_date": pd.Timestamp.now().strftime("%Y%m%d")
            }, f)

        elapsed = time.time() - start_time
        logger.info(f"模型训练完成，耗时 {elapsed:.2f}s，已保存：{model_path}")

        # 保存特征重要性到 JSON 文件
        self._save_feature_importance(feature_importances, feature_cols)

        return {
            "models": models,
            "metrics": metrics,
            "feature_cols": feature_cols,
            "feature_importances": feature_importances
        }

    def _save_feature_importance(self, feature_importances: dict, feature_cols: list):
        """保存特征重要性到 JSON 文件"""
        import json

        # 转换为可序列化格式
        serializable = {}
        for key, importance_list in feature_importances.items():
            serializable[key] = [{"feature": feat, "importance": float(imp)}
                                 for feat, imp in importance_list]

        # 保存
        output_path = os.path.join(self.models_dir, "feature_importance.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
        logger.info(f"特征重要性已保存：{output_path}")

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

    def get_feature_importance(self) -> Optional[Dict]:
        """获取特征重要性（从已保存的文件）"""
        import json
        importance_path = os.path.join(self.models_dir, "feature_importance.json")
        if os.path.exists(importance_path):
            with open(importance_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def print_feature_importance(self, top_n: int = 20):
        """打印特征重要性 TOP N"""
        importance = self.get_feature_importance()
        if importance is None:
            logger.warning("未找到特征重要性数据")
            return

        print("\n" + "=" * 70)
        print("特征重要性分析")
        print("=" * 70)

        for horizon_key in ["horizon_1d", "horizon_5d", "horizon_20d"]:
            if horizon_key in importance:
                print(f"\n【{horizon_key.replace('horizon_', '').upper()} 预测】")
                print("-" * 70)
                print(f"{'排名':<6}{'特征名':<35}{'重要性':<15}")
                print("-" * 70)
                for i, item in enumerate(importance[horizon_key][:top_n], 1):
                    feat = item["feature"]
                    imp = item["importance"]
                    print(f"{i:<6}{feat:<35}{imp:<15.4f}")
                print()

        print("=" * 70)

    def predict(
        self,
        model_result: Optional[Dict],
        features: pd.DataFrame,
        batch_size: int = 10000,
        with_confidence: bool = True,
        n_jobs: int = 32
    ) -> pd.DataFrame:
        """
        批量预测（高性能并行版本，支持置信度评估）

        Args:
            model_result: 训练结果（包含 models 和 feature_cols）
            features: 特征数据
            batch_size: 批处理大小
            with_confidence: 是否计算预测置信度
            n_jobs: 并行任务数（默认 32）

        Returns:
            预测结果 DataFrame
        """
        start_time = time.time()

        if model_result is None:
            model_result = self.load_model()
            if model_result is None:
                logger.warning("未找到已训练的模型")
                return pd.DataFrame()

        models = model_result["models"]
        feature_cols = model_result["feature_cols"]
        model_metrics = model_result.get("metrics", {})

        # 获取权重配置
        weights = self.HORIZON_WEIGHTS

        # 特征对齐
        missing_cols = set(feature_cols) - set(features.columns)
        if missing_cols:
            logger.error(f"缺少特征列：{missing_cols}")
            return pd.DataFrame()

        X = features[feature_cols].values

        # 批量预测（32 并发优化）
        n_samples = len(X)
        num_batches = (n_samples + batch_size - 1) // batch_size

        logger.debug(f"开始批量预测：{n_samples} 样本，{num_batches} 批次，{n_jobs} 并发")

        # 创建批次列表
        batches = []
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, n_samples)
            batches.append(X[start_idx:end_idx])

        # 32 并发预测（使用 threading backend，适合轻量级预测）
        from joblib import Parallel, delayed

        def predict_batch(X_batch):
            return (
                models["1d"].predict(X_batch),
                models["5d"].predict(X_batch),
                models["20d"].predict(X_batch)
            )

        results = Parallel(
            n_jobs=n_jobs,
            backend="threading",
            verbose=0
        )(
            delayed(predict_batch)(batch) for batch in batches
        )

        # 合并结果
        pred_1d_list, pred_5d_list, pred_20d_list = zip(*results)
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

        # 先计算基础综合评分（用于置信度等级划分）
        base_score = (
            predictions["pred_1d"] * weights["1d"] +
            predictions["pred_5d"] * weights["5d"] +
            predictions["pred_20d"] * weights["20d"]
        )

        # 计算预测置信度
        if with_confidence:
            logger.debug("计算预测置信度...")

            # 获取模型 R² 分数（带默认值保护）
            r2_1d = model_metrics.get("horizon_1d", {}).get("r2", 0.0)
            r2_5d = model_metrics.get("horizon_5d", {}).get("r2", 0.0)
            r2_20d = model_metrics.get("horizon_20d", {}).get("r2", 0.0)

            # 如果 R² 为负或未保存，使用默认值
            if r2_1d <= 0:
                r2_1d = 0.1
            if r2_5d <= 0:
                r2_5d = 0.1
            if r2_20d <= 0:
                r2_20d = 0.1

            # 基础置信度（模型整体性能）
            base_confidence = (
                r2_1d * weights["1d"] +
                r2_5d * weights["5d"] +
                r2_20d * weights["20d"]
            )

            # 方法 2：预测一致性（各周期预测方向是否一致）
            direction_consistency = (
                (np.sign(pred_1d) == np.sign(pred_5d)).astype(float) * 0.5 +
                (np.sign(pred_5d) == np.sign(pred_20d)).astype(float) * 0.5
            )

            # 方法 3：预测幅度合理性（过大的预测值置信度降低）
            magnitude_penalty = np.exp(-np.abs(base_score) / 10.0)  # 预测值越大，惩罚越大

            # 方法 4：特征空间密度（简化的欧氏距离）
            X_mean = np.mean(X, axis=0)
            X_std = np.std(X, axis=0) + 1e-8
            X_normalized = (X - X_mean) / X_std
            distances = np.sqrt(np.sum(X_normalized ** 2, axis=1))
            distance_confidence = np.exp(-distances / (2 * np.sqrt(X.shape[1])))

            # 综合置信度计算
            # 1. 模型性能权重 (30%)
            model_confidence = base_confidence

            # 2. 预测一致性权重 (30%)
            consistency_confidence = direction_consistency * 0.8 + 0.2

            # 3. 幅度合理性权重 (20%)
            magnitude_confidence = magnitude_penalty

            # 4. 特征空间距离权重 (20%)
            spatial_confidence = distance_confidence

            # 最终置信度
            predictions["confidence"] = (
                model_confidence * 0.30 +
                consistency_confidence * 0.30 +
                magnitude_confidence * 0.20 +
                spatial_confidence * 0.20
            )

            # 限制置信度范围在 [0.1, 0.95]
            predictions["confidence"] = np.clip(predictions["confidence"], 0.1, 0.95)

            # 各周期置信度（基于方向一致性和预测幅度）
            predictions["confidence_1d"] = np.clip(r2_1d * consistency_confidence * magnitude_confidence, 0.1, 0.95)
            predictions["confidence_5d"] = np.clip(r2_5d * consistency_confidence * magnitude_confidence, 0.1, 0.95)
            predictions["confidence_20d"] = np.clip(r2_20d * consistency_confidence * magnitude_confidence, 0.1, 0.95)

            # 置信度等级（基于置信度绝对值）
            def get_conf_level(conf: float) -> str:
                if conf >= 0.6:
                    return "高"
                elif conf >= 0.4:
                    return "中"
                else:
                    return "低"

            predictions["confidence_level"] = predictions["confidence"].apply(get_conf_level)

            logger.info(f"预测置信度范围：[{predictions['confidence'].min():.3f}, {predictions['confidence'].max():.3f}]")
            logger.info(f"高置信度样本数：{(predictions['confidence_level'] == '高').sum()}")
            logger.info(f"中置信度样本数：{(predictions['confidence_level'] == '中').sum()}")
            logger.info(f"低置信度样本数：{(predictions['confidence_level'] == '低').sum()}")

            # 综合评分 = 基础评分 * 置信度因子（让高置信度的预测排前面）
            # 归一化置信度到 0.8-1.2 范围，适度影响排序
            conf_min = predictions["confidence"].min()
            conf_max = predictions["confidence"].max()
            conf_normalized = 0.8 + 0.4 * (predictions["confidence"] - conf_min) / (conf_max - conf_min + 1e-8)
            predictions["combined_score"] = base_score * conf_normalized
        else:
            # 无置信度时使用原始预测值
            predictions["combined_score"] = base_score

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
