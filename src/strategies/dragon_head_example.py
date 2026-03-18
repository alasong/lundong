"""
Enhanced Dragon Head (打板) Strategy Usage Example
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.register import *  # Register all strategies
from strategies.strategy_factory import StrategyFactory
from strategies.backtester import StrategyBacktester


def example_enhanced_dragon_head():
    """Example: Using Enhanced Dragon Head Strategy"""
    print("=== Enhanced Dragon Head Strategy Example ===")
    
    # Create enhanced dragon head strategy
    strategy = StrategyFactory.create_strategy(
        "enhanced_dragon_head", 
        params={
            "strategy_type": "first_limit",
            "top_n_stocks": 5,
            "min_volume_ratio": 3.0,
            "min_market_cap": 7e9,  # 7B RMB
            "max_market_cap": 5.2e10,  # 52B RMB
        }
    )
    print(f"Strategy: {strategy.name}")
    print(f"Parameters: {strategy.params}")
    
    # Generate signals
    signals = strategy.generate_signals()
    print(f"Generated {len(signals)} signals")
    
    if signals:
        print("Top 5 signals:")
        for sig in signals[:5]:
            print(f"  - {sig.ts_code}: {sig.stock_name}, Score={sig.score:.1f}, Type={sig.signal_type}")
            print(f"    Reason: {sig.reason}")
    
    # Portfolio optimization
    portfolio = strategy.optimize_portfolio(signals)
    print(f"\nOptimized portfolio: {len(portfolio.get('portfolio', []))} stocks")
    
    if portfolio.get("portfolio"):
        print("Portfolio holdings:")
        for pos in portfolio["portfolio"]:
            print(f"  - {pos['ts_code']}: {pos['stock_name']}, Weight={pos['weight']:.1%}, Score={pos['score']:.1f}")


def example_backtest_dragon_head():
    """Example: Backtesting Enhanced Dragon Head Strategy"""
    print("\n=== Backtesting Enhanced Dragon Head Strategy ===")
    
    # Initialize backtester
    backtester = StrategyBacktester()
    
    # Create strategy instance
    strategy = StrategyFactory.create_strategy(
        "enhanced_dragon_head",
        params={
            "strategy_type": "first_limit",
            "top_n_stocks": 3,
            "min_volume_ratio": 3.0,
            "stop_loss_pct": -0.03,
            "take_profit_pct": 0.01,
        }
    )
    
    # Register strategy
    backtester.register_strategy("enhanced_dragon_head", strategy)
    
    # Get a sample stock code for backtesting
    # In real usage, you'd want to test multiple stocks
    sample_stock = "000001.SZ"  # Ping An Bank
    
    try:
        # Run backtest (note: this requires the strategy to have a backtest method)
        # For now, we'll just generate signals and simulate
        signals = strategy.generate_signals()
        if signals:
            print(f"Strategy generated {len(signals)} signals for backtesting")
            print("Sample signal details:")
            for sig in signals[:2]:
                print(f"  - {sig.ts_code}: {sig.stock_name}")
                print(f"    Score: {sig.score:.1f}")
                print(f"    Metadata: {sig.metadata}")
        else:
            print("No signals generated - check data availability")
            
    except Exception as e:
        print(f"Backtest error: {e}")
        print("Note: Full backtesting requires implementing backtest() method in strategy")


def example_multi_strategy_with_dragon_head():
    """Example: Multi-strategy with Dragon Head"""
    print("\n=== Multi-Strategy with Dragon Head ===")
    
    from strategies.multi_strategy import MultiStrategyPortfolio
    
    # Create multiple strategies including dragon head
    strategies = StrategyFactory.create_multiple_strategies({
        "hot_rotation": {"enabled": True, "params": {"top_n_concepts": 5}},
        "momentum": {"enabled": True, "params": {"top_n_stocks": 8}},
        "enhanced_dragon_head": {"enabled": True, "params": {"strategy_type": "first_limit", "top_n_stocks": 5}}
    })
    
    print(f"Created {len(strategies)} strategies")
    for s in strategies:
        print(f"  - {s.name}")
    
    # Create multi-strategy portfolio
    multi_portfolio = MultiStrategyPortfolio(
        strategies=strategies,
        strategy_weights={"hot_rotation": 0.4, "momentum": 0.3, "enhanced_dragon_head": 0.3},
        combination_method="weighted_score"
    )
    
    # Generate merged signals
    merged_signals = multi_portfolio.generate_signals()
    print(f"\nMerged signals: {len(merged_signals)}")
    
    if merged_signals:
        print("Top 5 merged signals:")
        for sig in merged_signals[:5]:
            print(f"  - {sig.ts_code}: {sig.stock_name}, Score={sig.score:.1f}, Strategy={sig.strategy_type}")


if __name__ == "__main__":
    # List available strategies
    print("Available strategies:")
    available = StrategyFactory.get_available_strategies()
    print(f"  {available}")
    
    # Run examples
    print("\n" + "="*60)
    example_enhanced_dragon_head()
    example_backtest_dragon_head()
    example_multi_strategy_with_dragon_head()
    print("="*60)