"""
打板策略回测示例
Example usage of the Daban backtesting framework
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.register import *  # Register all strategies
from strategies.daban_backtester import DabanBacktester
from strategies.strategy_factory import StrategyFactory


def example_daban_backtest():
    """Example: Backtesting 打板 strategies"""
    print("=== 打板策略回测示例 ===")
    
    # Create backtester
    backtester = DabanBacktester(initial_capital=1000000.0)
    
    # Create First Limit strategy
    first_limit_strategy = StrategyFactory.create_strategy(
        "first_limit",
        params={
            "min_turnover_amount": 1e4,
            "min_market_cap": 1e5,
            "max_market_cap": 1e8,
            "top_n_stocks": 5
        }
    )
    
    # Get sample stock codes (in real usage, you'd get from database)
    sample_stocks = ["000001.SZ", "600000.SH", "300750.SZ"]
    
    try:
        # Run backtest
        results = backtester.backtest_strategy(
            strategy=first_limit_strategy,
            stock_codes=sample_stocks,
            start_date="20260301",
            end_date="20260316",
            position_size=0.10,
            max_positions=3
        )
        
        print(f"回测完成!")
        print(f"策略: {results['strategy_name']}")
        print(f"总收益率: {results['total_return']:.2%}")
        print(f"交易次数: {len([t for t in results['trades'] if t.get('action') == 'sell'])}")
        
        # Generate report
        report = backtester.generate_report(results)
        print(report)
        
    except Exception as e:
        print(f"回测失败: {e}")
        print("注意: 此示例需要实际市场数据才能产生有意义的结果")


if __name__ == "__main__":
    example_daban_backtest()