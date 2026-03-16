#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略回测脚本
对 9 个策略进行历史回测
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
from strategy_runner import StrategyRunner
from strategies.evaluator import StrategyEvaluator


def backtest_all_strategies(
    start_date: str = None, end_date: str = None, initial_capital: float = 1000000
):
    """
    回测所有策略

    Args:
        start_date: 开始日期
        end_date: 结束日期
        initial_capital: 初始资金
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    runner = StrategyRunner()
    evaluator = StrategyEvaluator()

    strategies = runner.get_strategy_list()

    print("\n" + "=" * 80)
    print(f"策略回测：{start_date} - {end_date}")
    print(f"初始资金：{initial_capital:,.0f}")
    print("=" * 80)

    all_metrics = []

    for strategy_name in strategies:
        print(f"\n回测策略：{strategy_name}...")

        try:
            result = runner.backtest(
                strategy_type=strategy_name,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
            )

            if result.get("success"):
                metrics = result.get("metrics", {})
                all_metrics.append(metrics)

                print(f"  总收益：{metrics.get('total_return', 0):.1%}")
                print(f"  年化收益：{metrics.get('annual_return', 0):.1%}")
                print(f"  夏普比率：{metrics.get('sharpe', 0):.2f}")
                print(f"  最大回撤：{metrics.get('max_drawdown', 0):.1%}")
                print(f"  综合评分：{metrics.get('composite_score', 0):.1f}")
            else:
                print(f"  失败：{result.get('error', '未知错误')}")

        except Exception as e:
            print(f"  异常：{e}")

    # 对比所有策略
    if all_metrics:
        print("\n" + "=" * 80)
        print("策略对比")
        print("=" * 80)

        comparison = evaluator.compare_strategies(all_metrics)
        print(comparison.to_string(index=False))

        # 保存结果
        output_file = f"data/backtest_result_{start_date}_{end_date}.csv"
        comparison.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存到：{output_file}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="策略回测")
    parser.add_argument("--start-date", type=str, help="开始日期 YYYYMMDD")
    parser.add_argument("--end-date", type=str, help="结束日期 YYYYMMDD")
    parser.add_argument("--capital", type=float, default=1000000, help="初始资金")

    args = parser.parse_args()

    # 配置日志
    logger.remove()
    logger.add(sys.stderr, level="WARNING")

    backtest_all_strategies(
        start_date=args.start_date, end_date=args.end_date, initial_capital=args.capital
    )
