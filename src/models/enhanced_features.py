#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增强特征工程模块
添加情绪因子、资金流向因子等高级特征
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class EnhancedFeatureEngineer:
    """增强特征工程"""

    # 情绪因子权重
    SENTIMENT_WEIGHTS = {
        "turnover_sentiment": 0.3,    # 换手率情绪
        "momentum_sentiment": 0.3,    # 动量情绪
        "volatility_sentiment": 0.2,  # 波动率情绪
        "volume_sentiment": 0.2,      # 成交量情绪
    }

    def __init__(self):
        """初始化"""
        logger.info("增强特征工程器初始化完成")

    def compute_sentiment_factors(
        self,
        df: pd.DataFrame,
        lookback: int = 10
    ) -> pd.DataFrame:
        """
        计算情绪因子

        基于市场行为推断的情绪指标：
        1. 换手率情绪 - 高换手率表示市场关注度高
        2. 动量情绪 - 连续上涨产生乐观情绪
        3. 波动率情绪 - 低波动+上涨表示稳定乐观
        4. 成交量情绪 - 放量上涨表示情绪积极

        Args:
            df: 包含 pct_chg, vol, turnover_rate 等字段的 DataFrame
            lookback: 回溯天数

        Returns:
            包含情绪因子的 DataFrame
        """
        pct_chg = df["pct_chg"].values if "pct_chg" in df.columns else df["pct_change"].values
        vol = df["vol"].values if "vol" in df.columns else None
        turnover = df["turnover_rate"].values if "turnover_rate" in df.columns else None

        n = len(df)
        valid_samples = max(0, n - lookback)

        if valid_samples <= 0:
            return pd.DataFrame()

        # 创建结果 DataFrame，长度为 valid_samples
        result = pd.DataFrame(index=range(valid_samples))

        # 1. 换手率情绪（换手率相对于历史的百分位）
        if turnover is not None:
            turnover_windows = np.lib.stride_tricks.sliding_window_view(
                turnover, window_shape=lookback
            )[:valid_samples]
            current_turnover = turnover[lookback - 1:lookback - 1 + valid_samples]

            # 换手率百分位排名
            result["turnover_rank"] = np.array([
                np.sum(turnover_windows[i] <= current_turnover[i]) / lookback
                for i in range(valid_samples)
            ])

            # 换手率变化率
            result["turnover_change"] = current_turnover / (np.mean(turnover_windows, axis=1) + 1e-8)

            # 换手率情绪得分（高换手率 = 高关注度 = 高情绪）
            result["turnover_sentiment"] = result["turnover_rank"]

        # 2. 动量情绪（连续上涨天数和涨幅）
        momentum_windows = np.lib.stride_tricks.sliding_window_view(
            pct_chg, window_shape=lookback
        )[:valid_samples]

        # 上涨天数占比
        result["up_ratio"] = np.sum(momentum_windows > 0, axis=1) / lookback

        # 累计动量
        result["cumulative_momentum"] = np.sum(momentum_windows, axis=1)

        # 动量情绪得分
        result["momentum_sentiment"] = (
            result["up_ratio"] * 0.5 +
            np.tanh(result["cumulative_momentum"] / 10) * 0.5
        )

        # 3. 波动率情绪（低波动+上涨 = 稳定乐观）
        volatility = np.std(momentum_windows, axis=1)
        result["volatility_level"] = volatility

        # 波动率情绪：低波动且上涨时情绪积极
        result["volatility_sentiment"] = np.where(
            volatility < np.median(volatility),
            1 - volatility / (np.max(volatility) + 1e-8),  # 低波动 = 高情绪
            0.5
        ) * result["up_ratio"]

        # 4. 成交量情绪（放量上涨 = 情绪积极）
        if vol is not None:
            vol_windows = np.lib.stride_tricks.sliding_window_view(
                vol, window_shape=lookback
            )[:valid_samples]
            current_vol = vol[lookback - 1:lookback - 1 + valid_samples]

            # 量比
            result["volume_ratio"] = current_vol / (np.mean(vol_windows, axis=1) + 1e-8)

            # 成交量情绪：放量上涨时积极
            result["volume_sentiment"] = np.where(
                result["volume_ratio"] > 1.0,
                result["volume_ratio"] * result["up_ratio"],
                result["up_ratio"] * 0.5
            )

        # 5. 综合情绪得分
        sentiment_cols = ["turnover_sentiment", "momentum_sentiment",
                         "volatility_sentiment", "volume_sentiment"]
        available_cols = [c for c in sentiment_cols if c in result.columns]

        if available_cols:
            weights = [self.SENTIMENT_WEIGHTS.get(c, 0.25) for c in available_cols]
            total_weight = sum(weights)
            weights = [w / total_weight for w in weights]

            result["sentiment_score"] = sum(
                result[c] * w for c, w in zip(available_cols, weights)
            )

        # 6. 情绪极端信号
        if "sentiment_score" in result.columns:
            sentiment = result["sentiment_score"].values
            result["sentiment_extreme_high"] = (sentiment > np.percentile(sentiment, 90)).astype(float)
            result["sentiment_extreme_low"] = (sentiment < np.percentile(sentiment, 10)).astype(float)

        return result

    def compute_capital_flow_factors(
        self,
        df: pd.DataFrame,
        lookback: int = 10
    ) -> pd.DataFrame:
        """
        计算资金流向因子

        基于成交量和价格行为推断的资金流向：
        1. 资金净流入 - 价涨量增
        2. 资金净流出 - 价跌量增
        3. 资金流向强度
        4. 资金流向趋势

        Args:
            df: 包含 pct_chg, vol, amount 等字段的 DataFrame
            lookback: 回溯天数

        Returns:
            包含资金流向因子的 DataFrame
        """
        pct_chg = df["pct_chg"].values if "pct_chg" in df.columns else df["pct_change"].values
        vol = df["vol"].values if "vol" in df.columns else None
        amount = df["amount"].values if "amount" in df.columns else None

        n = len(df)
        valid_samples = max(0, n - lookback)

        if valid_samples <= 0:
            return pd.DataFrame()

        # 创建结果 DataFrame，长度为 valid_samples
        result = pd.DataFrame(index=range(valid_samples))

        # 1. 资金流向强度（价量配合度）
        if vol is not None:
            vol_windows = np.lib.stride_tricks.sliding_window_view(
                vol, window_shape=lookback
            )[:valid_samples]
            pct_windows = np.lib.stride_tricks.sliding_window_view(
                pct_chg, window_shape=lookback
            )[:valid_samples]

            current_vol = vol[lookback - 1:lookback - 1 + valid_samples]
            vol_mean = np.mean(vol_windows, axis=1)

            # 资金流向：价涨量增 = 流入，价跌量增 = 流出
            vol_change = (current_vol - vol_mean) / (vol_mean + 1e-8)
            current_pct = pct_chg[lookback - 1:lookback - 1 + valid_samples]

            result["capital_flow"] = np.where(
                current_pct > 0,
                vol_change,  # 上涨时放量 = 流入
                -vol_change  # 下跌时放量 = 流出
            )

            # 资金流向强度（绝对值）
            result["capital_flow_strength"] = np.abs(result["capital_flow"])

            # 2. 资金流向趋势（连续流入/流出天数）
            flow_sign = np.sign(result["capital_flow"].values)
            result["capital_flow_trend"] = np.array([
                self._count_continuous_flow(flow_sign[i:])
                for i in range(len(flow_sign))
            ])

        # 3. 金额流向因子
        if amount is not None:
            # 检查是否有有效数据
            amount_arr = np.array(amount, dtype=float)
            if not np.all(np.isnan(amount_arr)):
                amount_arr = np.nan_to_num(amount_arr, nan=0.0)
                amount_windows = np.lib.stride_tricks.sliding_window_view(
                    amount_arr, window_shape=lookback
                )[:valid_samples]
                current_amount = amount_arr[lookback - 1:lookback - 1 + valid_samples]
                amount_mean = np.mean(amount_windows, axis=1)

                result["amount_ratio"] = current_amount / (amount_mean + 1e-8)

                # 大额交易信号
                result["large_trade_signal"] = (
                    result["amount_ratio"] > np.percentile(result["amount_ratio"], 80)
                ).astype(float)

        # 4. 资金流向动量（使用滚动求和，保持长度一致）
        if "capital_flow" in result.columns and len(result) >= 5:
            flow_values = result["capital_flow"].values
            # 使用 pandas 滚动窗口保持长度
            flow_series = pd.Series(flow_values)
            result["capital_flow_momentum"] = flow_series.rolling(5).sum().fillna(0).values

        # 5. 资金流向综合得分
        if "capital_flow" in result.columns and "capital_flow_strength" in result.columns:
            result["capital_score"] = (
                np.tanh(result["capital_flow"]) * 0.5 +
                result["capital_flow_strength"] * 0.5
            )

        return result

    def _count_continuous_flow(self, flow_sign: np.ndarray) -> int:
        """计算连续流入/流出天数"""
        if len(flow_sign) == 0:
            return 0
        sign = flow_sign[0]
        count = 0
        for s in flow_sign:
            if s == sign:
                count += 1
            else:
                break
        return count * sign

    def compute_market_breadth_factors(
        self,
        df: pd.DataFrame,
        lookback: int = 10
    ) -> pd.DataFrame:
        """
        计算市场宽度因子

        基于涨跌分布的市场强度指标：
        1. 涨跌比
        2. 涨跌强度
        3. 市场强度指标

        Args:
            df: DataFrame
            lookback: 回溯天数

        Returns:
            包含市场宽度因子的 DataFrame
        """
        pct_chg = df["pct_chg"].values if "pct_chg" in df.columns else df["pct_change"].values
        n = len(df)
        valid_samples = max(0, n - lookback)

        if valid_samples <= 0:
            return pd.DataFrame()

        # 创建结果 DataFrame，长度为 valid_samples
        result = pd.DataFrame(index=range(valid_samples))

        pct_windows = np.lib.stride_tricks.sliding_window_view(
            pct_chg, window_shape=lookback
        )[:valid_samples]

        # 1. 涨跌比（上涨天数/总天数）
        up_days = np.sum(pct_windows > 0, axis=1)
        down_days = np.sum(pct_windows < 0, axis=1)
        result["advance_decline_ratio"] = up_days / (down_days + 1e-8)

        # 2. 涨跌强度（上涨幅度和 vs 下跌幅度和）
        up_sum = np.sum(np.maximum(pct_windows, 0), axis=1)
        down_sum = np.sum(np.maximum(-pct_windows, 0), axis=1)
        result["advance_decline_strength"] = up_sum / (down_sum + 1e-8)

        # 3. 市场强度指标
        result["market_strength"] = (
            result["advance_decline_ratio"] * 0.5 +
            result["advance_decline_strength"] * 0.5
        )

        # 4. 极端涨跌天数
        result["extreme_up_days"] = np.sum(pct_windows > 5, axis=1)
        result["extreme_down_days"] = np.sum(pct_windows < -5, axis=1)

        return result

    def compute_all_enhanced_features(
        self,
        df: pd.DataFrame,
        lookback: int = 10
    ) -> pd.DataFrame:
        """
        计算所有增强特征

        Args:
            df: 原始数据
            lookback: 回溯天数

        Returns:
            包含所有增强特征的 DataFrame
        """
        logger.info("计算增强特征...")

        sentiment = self.compute_sentiment_factors(df, lookback)
        capital = self.compute_capital_flow_factors(df, lookback)
        breadth = self.compute_market_breadth_factors(df, lookback)

        # 合并所有特征
        result = pd.concat([sentiment, capital, breadth], axis=1)

        logger.info(f"增强特征计算完成：{len(result.columns)} 个特征")

        return result


def integrate_enhanced_features(
    base_features: pd.DataFrame,
    concept_data: pd.DataFrame,
    lookback: int = 10
) -> pd.DataFrame:
    """
    将增强特征集成到基础特征中

    Args:
        base_features: 基础特征 DataFrame
        concept_data: 原始板块数据
        lookback: 回溯天数

    Returns:
        合并后的特征 DataFrame
    """
    engineer = EnhancedFeatureEngineer()

    # 按 concept_code 分组计算增强特征
    all_enhanced = []

    for concept_code, group in concept_data.groupby("concept_code" if "concept_code" in concept_data.columns else "ts_code"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        enhanced = engineer.compute_all_enhanced_features(group, lookback)

        # 添加标识列
        enhanced["concept_code"] = concept_code
        if "trade_date" in group.columns:
            enhanced["trade_date"] = group["trade_date"].values[-len(enhanced):]
        if "name" in group.columns:
            enhanced["name"] = group["name"].iloc[0]

        all_enhanced.append(enhanced)

    if not all_enhanced:
        return base_features

    enhanced_df = pd.concat(all_enhanced, ignore_index=True)

    # 合并到基础特征
    merge_keys = ["concept_code", "trade_date"]
    result = base_features.merge(
        enhanced_df,
        on=merge_keys,
        how="left"
    )

    logger.info(f"特征合并完成：{len(base_features.columns)} -> {len(result.columns)} 个特征")

    return result


# 新增特征列表（用于文档和验证）
ENHANCED_FEATURES = {
    "sentiment": [
        "turnover_rank", "turnover_change", "turnover_sentiment",
        "up_ratio", "cumulative_momentum", "momentum_sentiment",
        "volatility_level", "volatility_sentiment",
        "volume_ratio", "volume_sentiment",
        "sentiment_score", "sentiment_extreme_high", "sentiment_extreme_low"
    ],
    "capital_flow": [
        "capital_flow", "capital_flow_strength", "capital_flow_trend",
        "amount_ratio", "large_trade_signal", "capital_flow_momentum",
        "capital_score"
    ],
    "market_breadth": [
        "advance_decline_ratio", "advance_decline_strength",
        "market_strength", "extreme_up_days", "extreme_down_days"
    ]
}

TOTAL_ENHANCED_FEATURES = sum(len(v) for v in ENHANCED_FEATURES.values())


if __name__ == "__main__":
    # 测试
    from data.database import get_database

    db = get_database()
    data = db.get_concept_data(codes=["885001.TI"])

    if data is not None and not data.empty:
        engineer = EnhancedFeatureEngineer()
        features = engineer.compute_all_enhanced_features(data)
        print(f"\n增强特征数量: {len(features.columns)}")
        print(f"特征列表:\n{features.columns.tolist()}")
    else:
        print("无测试数据")