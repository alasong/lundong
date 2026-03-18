#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
打板策略回测执行脚本
Run comprehensive backtests for 打板 strategies with version control
"""

import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.register import *  # Register all strategies
from strategies.daban_backtester import DabanBacktester
from strategies.strategy_factory import StrategyFactory
from strategies.daban_version import DabanStrategyVersion


def run_comprehensive_backtest():
    """Run comprehensive backtest for all 打板 strategies"""
    print("=" * 60)
    print("🚀 打板策略回测执行")
    print(f"版本: {DabanStrategyVersion.get_current_version()}")
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Log version usage
    DabanStrategyVersion.log_version_usage("comprehensive_backtest")

    # Get current parameters
    params = DabanStrategyVersion.get_current_parameters()
    performance_expectations = DabanStrategyVersion.get_expected_performance()

    print("\n📊 预期性能指标:")
    for key, value in performance_expectations.items():
        print(f"  {key}: {value}")

    # Initialize backtester
    backtester = DabanBacktester(initial_capital=1000000.0)

    # Get sample stock codes from database (in real usage, get actual codes)
    try:
        from data.database import get_database

        db = get_database()
        latest_date = db.get_latest_date()
        if latest_date:
            all_stocks = db.get_all_stock_data(latest_date)
            stock_codes = all_stocks["ts_code"].head(20).tolist()  # Use top 20 stocks
        else:
            stock_codes = ["000001.SZ", "600000.SH", "300750.SZ", "688525.SH"]
    except Exception as e:
        print(f"⚠️  无法从数据库获取股票代码: {e}")
        stock_codes = ["000001.SZ", "600000.SH", "300750.SZ", "688525.SH"]

    print(f"\n📈 回测股票数量: {len(stock_codes)}")
    print(f"股票代码示例: {stock_codes[:5]}")

    # Run backtests for each strategy
    results = {}

    # First Limit Strategy
    print("\n🔍 回测首板策略...")
    try:
        first_limit_strategy = StrategyFactory.create_strategy(
            "first_limit", params=params["first_limit"]
        )

        first_limit_results = backtester.backtest_strategy(
            strategy=first_limit_strategy,
            stock_codes=stock_codes,
            start_date="20260301",
            end_date="20260316",
            position_size=0.10,
            max_positions=8,
        )
        results["first_limit"] = first_limit_results
        print(
            f"✅ 首板策略回测完成 - 总收益率: {first_limit_results['total_return']:.2%}"
        )

    except Exception as e:
        print(f"❌ 首板策略回测失败: {e}")
        results["first_limit"] = {"error": str(e)}

    # One-to-Two Strategy
    print("\n🔍 回测一进二策略...")
    try:
        one_to_two_strategy = StrategyFactory.create_strategy(
            "one_to_two", params=params["one_to_two"]
        )

        one_to_two_results = backtester.backtest_strategy(
            strategy=one_to_two_strategy,
            stock_codes=stock_codes,
            start_date="20260301",
            end_date="20260316",
            position_size=0.10,
            max_positions=6,
        )
        results["one_to_two"] = one_to_two_results
        print(
            f"✅ 一进二策略回测完成 - 总收益率: {one_to_two_results['total_return']:.2%}"
        )

    except Exception as e:
        print(f"❌ 一进二策略回测失败: {e}")
        results["one_to_two"] = {"error": str(e)}

    # Generate comparison report
    print("\n" + "=" * 60)
    print("📊 回测结果汇总")
    print("=" * 60)

    for strategy_name, result in results.items():
        if "error" not in result:
            print(f"\n{strategy_name.upper()} 策略:")
            print(f"  总收益率: {result['total_return']:.2%}")
            print(
                f"  交易次数: {len([t for t in result['trades'] if t.get('action') == 'sell'])}"
            )
            if "metrics" in result:
                metrics = result["metrics"]
                print(f"  夏普比率: {metrics.get('sharpe_ratio', 0):.2f}")
                print(f"  最大回撤: {metrics.get('max_drawdown', 0):.2%}")
                print(f"  胜率: {metrics.get('win_rate', 0):.2%}")
        else:
            print(f"\n{strategy_name.upper()} 策略: 回测失败 - {result['error']}")

    # Save results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"daban_backtest_results_v{DabanStrategyVersion.CURRENT_VERSION}_{timestamp}.json"

    # Convert results to JSON serializable format
    serializable_results = {}
    for key, value in results.items():
        if "error" not in value:
            serializable_results[key] = {
                "strategy_name": value.get("strategy_name"),
                "total_return": value.get("total_return"),
                "final_value": value.get("final_value"),
                "trade_count": len(
                    [t for t in value.get("trades", []) if t.get("action") == "sell"]
                ),
                "metrics": value.get("metrics", {}),
                "version": DabanStrategyVersion.CURRENT_VERSION,
            }
        else:
            serializable_results[key] = value

    try:
        with open(results_file, "w", encoding="utf-8") as f:
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        print(f"\n💾 回测结果已保存到: {results_file}")
    except Exception as e:
        print(f"⚠️  无法保存回测结果: {e}")

    print("\n" + "=" * 60)
    print("✅ 打板策略回测执行完成!")
    print("=" * 60)

    return results


if __name__ == "__main__":
    run_comprehensive_backtest()
