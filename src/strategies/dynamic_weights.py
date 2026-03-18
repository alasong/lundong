"""
动态权重调整模块
根据策略表现动态调整多策略组合中的权重
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from loguru import logger
from .evaluator import StrategyEvaluator


class DynamicWeightAllocator:
    """动态权重分配器"""

    def __init__(
        self,
        lookback_days: int = 60,
        rebalance_freq: int = 5,
        min_weight: float = 0.05,
        max_weight: float = 0.40,
    ):
        """
        初始化权重分配器

        Args:
            lookback_days: 回看天数
            rebalance_freq: 再平衡频率 (交易日)
            min_weight: 最小权重
            max_weight: 最大权重
        """
        self.lookback_days = lookback_days
        self.rebalance_freq = rebalance_freq
        self.min_weight = min_weight
        self.max_weight = max_weight
        self.evaluator = StrategyEvaluator()

        logger.info(
            f"动态权重分配器初始化完成："
            f"回看={lookback_days}天，再平衡={rebalance_freq}天，"
            f"权重范围=[{min_weight:.0%}, {max_weight:.0%}]"
        )

    def allocate_weights(
        self, strategy_returns: Dict[str, pd.Series], method: str = "sharpe"
    ) -> Dict[str, float]:
        """
        分配策略权重

        Args:
            strategy_returns: 策略收益率序列
                {"strategy1": Series1, "strategy2": Series2, ...}
            method: 分配方法
                - sharpe: 按夏普比率分配
                - return: 按收益率分配
                - volatility: 按波动率倒数分配
                - equal: 等权

        Returns:
            权重字典
        """
        if not strategy_returns:
            return {}

        if method == "equal":
            return self._equal_weights(strategy_returns)
        elif method == "sharpe":
            return self._sharpe_weights(strategy_returns)
        elif method == "return":
            return self._return_weights(strategy_returns)
        elif method == "volatility":
            return self._volatility_weights(strategy_returns)
        else:
            logger.warning(f"未知方法 {method}，使用等权")
            return self._equal_weights(strategy_returns)

    def _equal_weights(
        self, strategy_returns: Dict[str, pd.Series]
    ) -> Dict[str, float]:
        """等权分配"""
        n = len(strategy_returns)
        if n == 0:
            return {}

        weight = 1.0 / n
        return {name: weight for name in strategy_returns.keys()}

    def _sharpe_weights(
        self, strategy_returns: Dict[str, pd.Series]
    ) -> Dict[str, float]:
        """按夏普比率分配"""
        sharpe_ratios = {}

        for name, returns in strategy_returns.items():
            if len(returns) < 10:
                sharpe_ratios[name] = 0
                continue

            # 计算夏普比率
            ann_return = returns.mean() * 252
            ann_vol = returns.std() * np.sqrt(252)

            if ann_vol > 0:
                sharpe = (ann_return - 0.03) / ann_vol
            else:
                sharpe = 0

            # 只取正夏普比率
            sharpe_ratios[name] = max(0, sharpe)

        # 如果所有夏普比率都是 0，返回等权
        total_sharpe = sum(sharpe_ratios.values())
        if total_sharpe == 0:
            return self._equal_weights(strategy_returns)

        # 归一化并应用约束
        weights = {}
        for name, sharpe in sharpe_ratios.items():
            w = sharpe / total_sharpe
            w = max(self.min_weight, min(self.max_weight, w))
            weights[name] = w

        # 归一化使总和为 1
        total = sum(weights.values())
        weights = {k: v / total for k, v in weights.items()}

        return weights

    def _return_weights(
        self, strategy_returns: Dict[str, pd.Series]
    ) -> Dict[str, float]:
        """按收益率分配"""
        returns = {}

        for name, ret in strategy_returns.items():
            if len(ret) < 10:
                returns[name] = 0
            else:
                # 计算累计收益
                total_return = (1 + ret).prod() - 1
                returns[name] = max(0, total_return)

        total_return = sum(returns.values())
        if total_return == 0:
            return self._equal_weights(strategy_returns)

        weights = {}
        for name, ret in returns.items():
            w = ret / total_return
            w = max(self.min_weight, min(self.max_weight, w))
            weights[name] = w

        total = sum(weights.values())
        weights = {k: v / total for k, v in weights.items()}

        return weights

    def _volatility_weights(
        self, strategy_returns: Dict[str, pd.Series]
    ) -> Dict[str, float]:
        """按波动率倒数分配 (低波策略)"""
        volatilities = {}

        for name, returns in strategy_returns.items():
            if len(returns) < 10:
                volatilities[name] = 999
            else:
                vol = returns.std() * np.sqrt(252)
                volatilities[name] = max(0.01, vol)

        # 波动率倒数
        inv_vol = {name: 1.0 / vol for name, vol in volatilities.items()}
        total_inv_vol = sum(inv_vol.values())

        if total_inv_vol == 0:
            return self._equal_weights(strategy_returns)

        weights = {}
        for name, iv in inv_vol.items():
            w = iv / total_inv_vol
            w = max(self.min_weight, min(self.max_weight, w))
            weights[name] = w

        total = sum(weights.values())
        weights = {k: v / total for k, v in weights.items()}

        return weights

    def should_rebalance(
        self, current_weights: Dict[str, float], target_weights: Dict[str, float]
    ) -> bool:
        """
        判断是否需要再平衡

        Args:
            current_weights: 当前权重
            target_weights: 目标权重

        Returns:
            是否需要再平衡
        """
        if not current_weights or not target_weights:
            return False

        # 检查权重偏离
        for name in target_weights:
            current = current_weights.get(name, 0)
            target = target_weights[name]

            if abs(current - target) > 0.10:  # 偏离超过 10%
                return True

        return False

    def get_allocation_report(
        self, strategy_returns: Dict[str, pd.Series], method: str = "sharpe"
    ) -> Dict[str, Any]:
        """
        获取权重分配报告

        Returns:
            包含权重和评估指标的报告
        """
        weights = self.allocate_weights(strategy_returns, method)

        # 计算各策略指标
        metrics = {}
        for name, returns in strategy_returns.items():
            if len(returns) > 10:
                evaluator = StrategyEvaluator()
                # 模拟净值
                nav = (1 + returns).cumprod()
                result = evaluator.evaluate(nav, strategy_name=name)
                metrics[name] = {
                    "sharpe": result.get("sharpe", 0),
                    "annual_return": result.get("annual_return", 0),
                    "max_drawdown": result.get("max_drawdown", 0),
                    "weight": weights.get(name, 0),
                }

        return {
            "method": method,
            "weights": weights,
            "metrics": metrics,
            "num_strategies": len(weights),
        }
