#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
打板策略回测脚本
完整回测首板、一进二和增强龙头股策略
"""

import sys
import os

# Add src to path
src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, src_path)

from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import numpy as np
from src.strategy_runner import StrategyRunner
from src.strategies.evaluator import StrategyEvaluator


class DabanStrategyBacktester:
    """打板策略回测器"""

    def __init__(self):
        self.runner = StrategyRunner()
        self.evaluator = StrategyEvaluator()
        logger.info("打板策略回测器初始化完成")

    def backtest_single_strategy(
        self,
        strategy_name: str,
        start_date: str,
        end_date: str,
        initial_capital: float = 1000000.0,
        params: dict = None,
    ) -> dict:
        """
        回测单个打板策略

        Args:
            strategy_name: 策略名称 (first_limit/one_to_two/enhanced_dragon_head)
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            params: 策略参数

        Returns:
            回测结果
        """
        logger.info(f"开始回测策略: {strategy_name}")

        try:
            # 运行回测
            result = self.runner.backtest(
                strategy_type=strategy_name,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                params=params,
            )

            if result.get("success"):
                metrics = result.get("metrics", {})
                logger.info(f"策略 {strategy_name} 回测完成")
                logger.info(f"总收益: {metrics.get('total_return', 0):.2%}")
                logger.info(f"夏普比率: {metrics.get('sharpe', 0):.2f}")
                logger.info(f"最大回撤: {metrics.get('max_drawdown', 0):.2%}")
                logger.info(f"胜率: {metrics.get('win_rate', 0):.2%}")

                return {
                    "strategy": strategy_name,
                    "success": True,
                    "metrics": metrics,
                    "portfolio": result.get("portfolio", []),
                    "trades": result.get("trades", []),
                }
            else:
                logger.error(f"策略 {strategy_name} 回测失败")
                return {
                    "strategy": strategy_name,
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                }

        except Exception as e:
            logger.error(f"策略 {strategy_name} 回测异常: {e}")
            return {
                "strategy": strategy_name,
                "success": False,
                "error": str(e),
            }

    def backtest_all_daban_strategies(
        self, start_date: str, end_date: str, initial_capital: float = 1000000.0
    ) -> dict:
        """
        回测所有打板策略

        Args:
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金

        Returns:
            所有策略的回测结果
        """
        strategies = ["first_limit", "one_to_two", "enhanced_dragon_head"]
        results = {}

        logger.info(f"开始回测所有打板策略 ({start_date} - {end_date})")

        for strategy in strategies:
            result = self.backtest_single_strategy(
                strategy_name=strategy,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
            )
            results[strategy] = result

        # 创建对比报告
        comparison_df = self._create_comparison_dataframe(results)

        logger.info("所有打板策略回测完成")
        return {
            "results": results,
            "comparison": comparison_df,
            "summary": self._generate_summary(results),
        }

    def _create_comparison_dataframe(self, results: dict) -> pd.DataFrame:
        """创建策略对比DataFrame"""
        data = []

        for strategy, result in results.items():
            if result.get("success"):
                metrics = result.get("metrics", {})
                row = {
                    "Strategy": strategy,
                    "Total Return": f"{metrics.get('total_return', 0):.2%}",
                    "Annual Return": f"{metrics.get('annual_return', 0):.2%}",
                    "Sharpe Ratio": f"{metrics.get('sharpe', 0):.2f}",
                    "Max Drawdown": f"{metrics.get('max_drawdown', 0):.2%}",
                    "Win Rate": f"{metrics.get('win_rate', 0):.2%}",
                    "Profit/Loss Ratio": f"{metrics.get('profit_loss_ratio', 0):.2f}",
                    "Composite Score": f"{metrics.get('composite_score', 0):.1f}",
                }
                data.append(row)
            else:
                row = {
                    "Strategy": strategy,
                    "Total Return": "Failed",
                    "Annual Return": "Failed",
                    "Sharpe Ratio": "Failed",
                    "Max Drawdown": "Failed",
                    "Win Rate": "Failed",
                    "Profit/Loss Ratio": "Failed",
                    "Composite Score": "Failed",
                }
                data.append(row)

        return pd.DataFrame(data)

    def _generate_summary(self, results: dict) -> dict:
        """生成回测摘要"""
        successful_strategies = [
            name for name, result in results.items() if result.get("success")
        ]

        if not successful_strategies:
            return {"best_strategy": None, "recommendation": "所有策略回测失败"}

        # 找到最佳策略（基于综合评分）
        best_strategy = None
        best_score = -1

        for strategy in successful_strategies:
            score = results[strategy].get("metrics", {}).get("composite_score", 0)
            if score > best_score:
                best_score = score
                best_strategy = strategy

        recommendations = {
            "first_limit": "适合震荡市和结构性行情，风险适中",
            "one_to_two": "适合趋势市，需要较强的市场情绪配合",
            "enhanced_dragon_head": "适合牛市，高风险高收益",
        }

        return {
            "best_strategy": best_strategy,
            "best_score": best_score,
            "recommendation": recommendations.get(
                best_strategy, "建议根据市场环境选择策略"
            ),
            "successful_count": len(successful_strategies),
            "total_count": len(results),
        }

    def print_report(self, backtest_results: dict) -> None:
        """打印回测报告"""
        print("\n" + "=" * 80)
        print("打板策略回测报告")
        print("=" * 80)

        # 摘要
        summary = backtest_results.get("summary", {})
        print(f"\n📊 摘要:")
        print(
            f"   成功策略数: {summary.get('successful_count', 0)}/{summary.get('total_count', 0)}"
        )
        print(f"   最佳策略: {summary.get('best_strategy', 'N/A')}")
        print(f"   推荐: {summary.get('recommendation', 'N/A')}")

        # 对比表格
        comparison = backtest_results.get("comparison", pd.DataFrame())
        if not comparison.empty:
            print(f"\n📈 策略对比:")
            print(comparison.to_string(index=False))

        print("=" * 80)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="打板策略回测")
    parser.add_argument(
        "--start-date", type=str, default=None, help="开始日期 (YYYYMMDD)"
    )
    parser.add_argument(
        "--end-date", type=str, default=None, help="结束日期 (YYYYMMDD)"
    )
    parser.add_argument("--capital", type=float, default=1000000.0, help="初始资金")
    parser.add_argument(
        "--strategy",
        type=str,
        default="all",
        help="策略类型: first_limit/one_to_two/enhanced_dragon_head/all",
    )

    args = parser.parse_args()

    # 配置日志
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    # 设置默认日期范围
    if args.start_date is None:
        start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
    else:
        start_date = args.start_date

    if args.end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    else:
        end_date = args.end_date

    # 创建回测器
    backtester = DabanStrategyBacktester()

    if args.strategy == "all":
        # 回测所有策略
        results = backtester.backtest_all_daban_strategies(
            start_date=start_date, end_date=end_date, initial_capital=args.capital
        )
        backtester.print_report(results)
    else:
        # 回测单个策略
        result = backtester.backtest_single_strategy(
            strategy_name=args.strategy,
            start_date=start_date,
            end_date=end_date,
            initial_capital=args.capital,
        )

        if result.get("success"):
            metrics = result.get("metrics", {})
            print("\n" + "=" * 80)
            print(f"{args.strategy} 策略回测结果")
            print("=" * 80)
            print(f"总收益: {metrics.get('total_return', 0):.2%}")
            print(f"年化收益: {metrics.get('annual_return', 0):.2%}")
            print(f"夏普比率: {metrics.get('sharpe', 0):.2f}")
            print(f"最大回撤: {metrics.get('max_drawdown', 0):.2%}")
            print(f"胜率: {metrics.get('win_rate', 0):.2%}")
            print(f"盈亏比: {metrics.get('profit_loss_ratio', 0):.2f}")
            print(f"综合评分: {metrics.get('composite_score', 0):.1f}")
            print("=" * 80)
        else:
            print(f"回测失败: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
