"""
多策略组合器
合并多个策略的信号，生成最终投资组合
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from strategies.base_strategy import BaseStrategy, StrategySignal


class MultiStrategyPortfolio:
    """多策略组合器 - 合并多个策略的信号"""

    def __init__(
        self,
        strategies: List[BaseStrategy],
        strategy_weights: Optional[Dict[str, float]] = None,
        combination_method: str = "weighted_score",
    ):
        """
        初始化多策略组合器

        Args:
            strategies: 策略列表
            strategy_weights: 策略权重
                {"hot_rotation": 0.6, "momentum": 0.4}
            combination_method: 信号合并方法
                - weighted_score: 加权评分
                - voting: 投票
                - stacking: 堆叠（需要学习器）
        """
        self.strategies = strategies
        self.strategy_weights = strategy_weights or {}
        self.combination_method = combination_method

        # 如果未指定权重，默认等权
        if not self.strategy_weights:
            weight = 1.0 / len(strategies) if strategies else 0
            self.strategy_weights = {s.name: weight for s in strategies}

        logger.info(
            f"多策略组合器初始化完成："
            f"{len(strategies)} 个策略，合并方法：{combination_method}"
        )

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """
        所有策略生成信号

        Returns:
            合并后的信号列表
        """
        all_signals = {}  # ts_code -> List[StrategySignal]

        # 1. 每个策略独立生成信号
        for strategy in self.strategies:
            logger.info(f"策略 {strategy.name} 生成信号...")
            try:
                signals = strategy.generate_signals(**kwargs)
                signals = strategy.validate_signals(signals)

                for sig in signals:
                    if sig.ts_code not in all_signals:
                        all_signals[sig.ts_code] = []
                    all_signals[sig.ts_code].append(sig)

                logger.info(f"策略 {strategy.name} 生成 {len(signals)} 个有效信号")
            except Exception as e:
                logger.error(f"策略 {strategy.name} 生成信号失败：{e}")

        # 2. 合并信号
        if self.combination_method == "weighted_score":
            merged = self._merge_by_weighted_score(all_signals)
        elif self.combination_method == "voting":
            merged = self._merge_by_voting(all_signals)
        else:
            merged = self._merge_by_weighted_score(all_signals)

        logger.info(f"合并后信号：{len(merged)} 个")
        return merged

    def _merge_by_weighted_score(
        self, all_signals: Dict[str, List[StrategySignal]]
    ) -> List[StrategySignal]:
        """
        按加权评分合并

        对同一只股票，多个策略的信号加权平均
        """
        merged = []

        for ts_code, signals in all_signals.items():
            if not signals:
                continue

            # 计算加权评分
            total_weight = 0
            weighted_score = 0
            avg_weight = 0

            for sig in signals:
                w = self.strategy_weights.get(sig.strategy_type, 1.0)
                weighted_score += sig.score * w
                total_weight += w
                avg_weight += sig.weight * w

            if total_weight > 0:
                final_score = weighted_score / total_weight
                final_weight = avg_weight / total_weight
            else:
                final_score = sum(s.score for s in signals) / len(signals)
                final_weight = sum(s.weight for s in signals) / len(signals)

            # 使用评分最高的策略的信息
            best_sig = max(signals, key=lambda s: s.score)

            merged_signal = StrategySignal(
                ts_code=ts_code,
                stock_name=best_sig.stock_name,
                strategy_type="multi_strategy",
                signal_type=best_sig.signal_type,
                weight=final_weight,
                score=final_score,
                reason=f"多策略合并：{', '.join(s.strategy_type for s in signals)}",
                metadata={
                    "component_signals": [s.to_dict() for s in signals],
                    "strategy_count": len(signals),
                },
            )
            merged.append(merged_signal)

        # 按评分排序
        merged.sort(key=lambda s: s.score, reverse=True)
        return merged

    def _merge_by_voting(
        self, all_signals: Dict[str, List[StrategySignal]]
    ) -> List[StrategySignal]:
        """
        按投票合并

        多数策略看好才买入
        """
        merged = []

        for ts_code, signals in all_signals.items():
            if not signals:
                continue

            # 统计买入信号数量
            buy_count = sum(1 for s in signals if s.signal_type == "buy")
            total_count = len(signals)

            # 超过半数看好才买入
            if buy_count > total_count / 2:
                best_sig = max(signals, key=lambda s: s.score)
                merged_signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=best_sig.stock_name,
                    strategy_type="multi_strategy",
                    signal_type="buy",
                    weight=best_sig.weight,
                    score=best_sig.score,
                    reason=f"投票通过：{buy_count}/{total_count} 策略看好",
                    metadata={
                        "buy_votes": buy_count,
                        "total_votes": total_count,
                        "component_signals": [s.to_dict() for s in signals],
                    },
                )
                merged.append(merged_signal)

        merged.sort(key=lambda s: s.score, reverse=True)
        return merged

    def optimize_portfolio(
        self, signals: List[StrategySignal], **kwargs
    ) -> Dict[str, Any]:
        """
        组合优化

        使用主策略的优化器，或等权组合
        """
        if not signals:
            return {"portfolio": [], "metrics": {}}

        # 使用第一个策略的优化器（如果有）
        for strategy in self.strategies:
            if hasattr(strategy, "optimizer"):
                logger.info(f"使用策略 {strategy.name} 的优化器")
                return strategy.optimize_portfolio(signals, **kwargs)

        # 默认等权组合
        logger.info("使用等权组合")
        return self._equal_weight_portfolio(signals)

    def _equal_weight_portfolio(self, signals: List[StrategySignal]) -> Dict[str, Any]:
        """等权组合"""
        n = len(signals)
        if n == 0:
            return {"portfolio": [], "metrics": {}}

        weight = 1.0 / n

        portfolio = []
        for sig in signals:
            portfolio.append(
                {
                    "ts_code": sig.ts_code,
                    "stock_name": sig.stock_name,
                    "weight": weight,
                    "score": sig.score,
                    "strategy": sig.strategy_type,
                }
            )

        return {
            "portfolio": portfolio,
            "metrics": {
                "num_stocks": n,
                "avg_score": sum(s.score for s in signals) / n,
                "strategy_distribution": self._get_strategy_distribution(signals),
            },
        }

    def _get_strategy_distribution(
        self, signals: List[StrategySignal]
    ) -> Dict[str, int]:
        """统计各策略的信号数量"""
        dist = {}
        for sig in signals:
            # 从 metadata 中提取原始策略
            if "component_signals" in sig.metadata:
                for comp in sig.metadata["component_signals"]:
                    stype = comp.get("strategy_type", "unknown")
                    dist[stype] = dist.get(stype, 0) + 1
            else:
                dist[sig.strategy_type] = dist.get(sig.strategy_type, 0) + 1
        return dist

    def get_strategy_info(self) -> List[Dict]:
        """获取所有策略信息"""
        return [s.get_info() for s in self.strategies]
