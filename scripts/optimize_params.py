#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略参数优化脚本
使用网格搜索优化策略参数
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import numpy as np
from itertools import product
from strategy_runner import StrategyRunner
from strategies.evaluator import StrategyEvaluator


class GridSearchOptimizer:
    """网格搜索优化器"""

    def __init__(self, strategy_type: str):
        self.strategy_type = strategy_type
        self.runner = StrategyRunner()
        self.evaluator = StrategyEvaluator()

    def optimize(
        self,
        param_grid: dict,
        start_date: str,
        end_date: str,
        metric: str = "composite_score",
    ) -> dict:
        """
        网格搜索优化

        Args:
            param_grid: 参数网格
                {"param1": [1, 2, 3], "param2": [0.1, 0.2]}
            start_date: 开始日期
            end_date: 结束日期
            metric: 优化指标

        Returns:
            最佳参数和结果
        """
        logger.info(f"开始网格搜索：{self.strategy_type}")
        logger.info(f"参数网格：{param_grid}")

        # 生成所有参数组合
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        combinations = list(product(*param_values))

        logger.info(f"共 {len(combinations)} 种参数组合")

        results = []

        for i, values in enumerate(combinations, 1):
            params = dict(zip(param_names, values))

            try:
                # 运行策略
                result = self.runner.backtest(
                    strategy_type=self.strategy_type,
                    start_date=start_date,
                    end_date=end_date,
                    params=params,
                )

                if result.get("success"):
                    metrics = result.get("metrics", {})
                    score = metrics.get(metric, 0)

                    results.append(
                        {"params": params, "score": score, "metrics": metrics}
                    )

                    if i % 10 == 0:
                        logger.info(f"已完成 {i}/{len(combinations)}")

            except Exception as e:
                logger.warning(f"参数 {params} 失败：{e}")

        if not results:
            return {"error": "无有效结果"}

        # 找到最佳参数
        best = max(results, key=lambda x: x["score"])

        logger.info(f"最佳参数：{best['params']}")
        logger.info(f"最佳评分：{best['score']:.1f}")

        return {
            "best_params": best["params"],
            "best_score": best["score"],
            "best_metrics": best["metrics"],
            "all_results": results,
        }


def optimize_value_strategy():
    """优化价值策略参数"""
    optimizer = GridSearchOptimizer("value")

    param_grid = {
        "max_pe": [15, 20, 25, 30],
        "max_pb": [1.5, 2.0, 2.5],
        "min_roe": [0.08, 0.10, 0.12, 0.15],
    }

    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    result = optimizer.optimize(param_grid, start_date, end_date)

    print("\n" + "=" * 80)
    print("价值策略参数优化结果")
    print("=" * 80)
    print(f"最佳参数：{result.get('best_params', {})}")
    print(f"最佳评分：{result.get('best_score', 0):.1f}")
    print(f"最佳指标:")
    metrics = result.get("best_metrics", {})
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    print("=" * 80)


def optimize_momentum_strategy():
    """优化动量策略参数"""
    optimizer = GridSearchOptimizer("momentum")

    param_grid = {
        "momentum_window": [10, 20, 30],
        "min_momentum": [0.03, 0.05, 0.08],
        "min_volume_ratio": [1.2, 1.5, 2.0],
    }

    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    result = optimizer.optimize(param_grid, start_date, end_date)

    print("\n" + "=" * 80)
    print("动量策略参数优化结果")
    print("=" * 80)
    print(f"最佳参数：{result.get('best_params', {})}")
    print(f"最佳评分：{result.get('best_score', 0):.1f}")
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="策略参数优化")
    parser.add_argument(
        "--strategy",
        type=str,
        default="value",
        help="策略类型：value/momentum/quality/...",
    )
    parser.add_argument("--start-date", type=str, help="开始日期")
    parser.add_argument("--end-date", type=str, help="结束日期")

    args = parser.parse_args()

    # 配置日志
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    if args.strategy == "value":
        optimize_value_strategy()
    elif args.strategy == "momentum":
        optimize_momentum_strategy()
    else:
        print(f"暂不支持策略：{args.strategy}")
