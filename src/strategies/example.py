"""
多策略使用示例
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .register import *  # 注册所有策略
from .strategy_factory import StrategyFactory
from .multi_strategy import MultiStrategyPortfolio


def example_single_strategy():
    """示例 1: 使用单个策略"""
    print("=== 示例 1: 单个策略 ===")

    # 创建热点轮动策略
    strategy = StrategyFactory.create_strategy("hot_rotation")
    print(f"策略：{strategy.name}")
    print(f"参数：{strategy.params}")

    # 生成信号
    signals = strategy.generate_signals()
    print(f"生成信号数：{len(signals)}")

    if signals:
        print(f"前 3 个信号:")
        for sig in signals[:3]:
            print(f"  - {sig.ts_code}: {sig.stock_name}, 评分={sig.score:.1f}")


def example_multi_strategy():
    """示例 2: 使用多策略组合"""
    print("\n=== 示例 2: 多策略组合 ===")

    # 批量创建策略
    strategies = StrategyFactory.create_multiple_strategies(
        {
            "hot_rotation": {"enabled": True, "params": {"top_n_concepts": 5}},
            "momentum": {"enabled": True, "params": {"top_n_stocks": 10}},
        }
    )

    print(f"创建策略数：{len(strategies)}")
    for s in strategies:
        print(f"  - {s.name}")

    # 创建多策略组合器
    multi_portfolio = MultiStrategyPortfolio(
        strategies=strategies,
        strategy_weights={"hot_rotation": 0.6, "momentum": 0.4},
        combination_method="weighted_score",
    )

    # 生成合并信号
    merged_signals = multi_portfolio.generate_signals()
    print(f"\n合并后信号数：{len(merged_signals)}")

    if merged_signals:
        print(f"前 5 个信号:")
        for sig in merged_signals[:5]:
            print(
                f"  - {sig.ts_code}: {sig.stock_name}, 评分={sig.score:.1f}, 策略={sig.strategy_type}"
            )

    # 组合优化
    portfolio = multi_portfolio.optimize_portfolio(merged_signals, top_n_stocks=10)
    print(f"\n最终组合：{len(portfolio.get('portfolio', []))} 只股票")

    if portfolio.get("portfolio"):
        print("持仓:")
        for pos in portfolio["portfolio"][:5]:
            print(
                f"  - {pos['ts_code']}: {pos['stock_name']}, 权重={pos['weight']:.1%}"
            )


def example_list_strategies():
    """示例 3: 查看可用策略"""
    print("\n=== 示例 3: 可用策略 ===")

    available = StrategyFactory.get_available_strategies()
    print(f"可用策略：{available}")


if __name__ == "__main__":
    # 查看可用策略
    example_list_strategies()

    # 注意：以下示例需要数据库中有数据
    # example_single_strategy()
    # example_multi_strategy()
