"""
策略运行器
支持多策略回测和实盘运行
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from loguru import logger
from strategies.register import *
from strategies.strategy_factory import StrategyFactory
from strategies.multi_strategy import MultiStrategyPortfolio
from strategies.evaluator import StrategyEvaluator
from strategies.dynamic_weights import DynamicWeightAllocator


class StrategyRunner:
    """策略运行器 - 支持回测和实盘"""

    def __init__(self, db=None):
        """初始化"""
        self.db = db
        if db is None:
            from data.database import get_database

            self.db = get_database()

        self.evaluator = StrategyEvaluator()
        self.weight_allocator = DynamicWeightAllocator()
        logger.info("策略运行器初始化完成")

    def run_single_strategy(
        self,
        strategy_type: str,
        start_date: str,
        end_date: str,
        params: Optional[Dict] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        运行单个策略

        Args:
            strategy_type: 策略类型
            start_date: 开始日期
            end_date: 结束日期
            params: 策略参数

        Returns:
            运行结果
        """
        logger.info(f"运行策略：{strategy_type} ({start_date} - {end_date})")

        # 创建策略
        strategy = StrategyFactory.create_strategy(strategy_type, params)

        # 生成信号
        signals = strategy.generate_signals()

        if not signals:
            return {"success": False, "error": "无信号"}

        # 评估信号
        signal_df = pd.DataFrame([s.to_dict() for s in signals])

        return {
            "success": True,
            "strategy": strategy_type,
            "num_signals": len(signals),
            "signals": signal_df.to_dict("records"),
            "avg_score": signal_df["score"].mean(),
        }

    def run_multi_strategy(
        self,
        strategy_config: Dict[str, Dict],
        start_date: str,
        end_date: str,
        combination_method: str = "weighted_score",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        运行多策略组合

        Args:
            strategy_config: 策略配置
                {
                    "hot_rotation": {"enabled": True, "weight": 0.4},
                    "momentum": {"enabled": True, "weight": 0.3},
                    ...
                }
            start_date: 开始日期
            end_date: 结束日期
            combination_method: 信号合并方法

        Returns:
            运行结果
        """
        logger.info(f"运行多策略组合：{list(strategy_config.keys())}")

        # 创建策略
        strategies = StrategyFactory.create_multiple_strategies(strategy_config)

        if not strategies:
            return {"success": False, "error": "无可用策略"}

        # 创建多策略组合器
        strategy_weights = {
            name: cfg.get("weight", 1.0 / len(strategies))
            for name, cfg in strategy_config.items()
            if cfg.get("enabled", True)
        }

        multi = MultiStrategyPortfolio(
            strategies=strategies,
            strategy_weights=strategy_weights,
            combination_method=combination_method,
        )

        # 生成合并信号
        signals = multi.generate_signals()

        if not signals:
            return {"success": False, "error": "无信号"}

        # 组合优化
        portfolio = multi.optimize_portfolio(signals, **kwargs)

        return {
            "success": True,
            "num_strategies": len(strategies),
            "num_signals": len(signals),
            "portfolio": portfolio,
            "strategy_weights": strategy_weights,
        }

    def backtest(
        self,
        strategy_type: str,
        start_date: str,
        end_date: str,
        initial_capital: float = 1000000,
        params: Optional[Dict] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        策略回测

        Args:
            strategy_type: 策略类型
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            params: 策略参数

        Returns:
            回测结果
        """
        logger.info(f"回测策略：{strategy_type} ({start_date} - {end_date})")

        # 简化回测：获取策略信号，模拟持仓收益
        # 实际回测需要更复杂的逻辑

        strategy = StrategyFactory.create_strategy(strategy_type, params)
        signals = strategy.generate_signals()

        if not signals:
            return {"success": False, "error": "无信号"}

        # 模拟：假设持有信号股票等权组合
        # 实际应从数据库获取历史收益

        # 生成模拟净值曲线
        dates = pd.date_range(start=start_date, end=end_date, freq="B")
        np.random.seed(42)  # 可重现

        # 模拟日收益率 (均值 + 波动)
        avg_score = np.mean([s.score for s in signals])
        daily_mean = avg_score / 100 / 252  # 年化转日化
        daily_std = 0.02  # 2% 日波动

        returns = np.random.normal(daily_mean, daily_std, len(dates))
        nav = initial_capital * (1 + returns).cumprod()

        nav_series = pd.Series(nav, index=dates)

        # 评估绩效
        metrics = self.evaluator.evaluate(nav_series, strategy_name=strategy_type)

        return {
            "success": True,
            "strategy": strategy_type,
            "metrics": metrics,
            "nav": nav_series.to_dict(),
            "num_signals": len(signals),
        }

    def get_strategy_list(self) -> List[str]:
        """获取可用策略列表"""
        return StrategyFactory.get_available_strategies()

    def dynamic_rebalance(
        self,
        strategy_config: Dict[str, Dict],
        lookback_days: int = 60,
        method: str = "sharpe",
    ) -> Dict[str, Any]:
        """
        动态权重再平衡

        Args:
            strategy_config: 策略配置
            lookback_days: 回看天数
            method: 权重分配方法

        Returns:
            再平衡结果
        """
        logger.info(f"动态再平衡：{method} 方法，回看{lookback_days}天")

        # 模拟各策略历史收益 (实际应从回测获取)
        strategy_returns = {}
        for name in strategy_config.keys():
            # 模拟 60 天收益
            np.random.seed(hash(name) % 1000)
            returns = pd.Series(np.random.normal(0.0005, 0.02, lookback_days))
            strategy_returns[name] = returns

        # 计算权重
        self.weight_allocator.lookback_days = lookback_days
        weights = self.weight_allocator.allocate_weights(strategy_returns, method)

        # 获取分配报告
        report = self.weight_allocator.get_allocation_report(strategy_returns, method)

        return {
            "success": True,
            "weights": weights,
            "report": report,
        }
