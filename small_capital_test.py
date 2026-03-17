#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
小资金实盘测试配置
Small capital real trading test for 打板 strategies
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from strategies.register import *
from strategies.strategy_factory import StrategyFactory
from strategies.daban_version import DabanStrategyVersion


def small_capital_test():
    """小资金实盘测试 (10,000 RMB)"""
    print("🎯 小资金实盘测试配置")
    print(f"策略版本: {DabanStrategyVersion.get_current_version()}")
    print("资金规模: 10,000 RMB")
    print("风险控制: 严格-3%止损，11:28/14:50时段退出")
    print("-" * 50)

    # 创建首板策略实例
    first_limit_strategy = StrategyFactory.create_strategy(
        "first_limit",
        params={
            "limit_up_threshold": 0.095,
            "limit_up_threshold_20": 0.195,
            "min_volume_ratio": 3.0,
            "min_turnover_amount": 1e4,
            "min_market_cap": 1e5,
            "max_market_cap": 1e8,
            "min_price": 2.0,
            "max_price": 50.0,
            "first_limit_days": 180,
            "top_n_stocks": 5,  # 小资金只选5只
            "stop_loss_pct": -0.03,
            "take_profit_pct": 0.015,
        },
    )

    # 生成信号
    signals = first_limit_strategy.generate_signals()
    valid_signals = first_limit_strategy.validate_signals(signals)

    print(f"生成信号数量: {len(signals)}")
    print(f"有效信号数量: {len(valid_signals)}")

    if valid_signals:
        print("\n📈 推荐交易:")
        for i, sig in enumerate(valid_signals[:3]):  # 只显示前3个
            print(f"  {i + 1}. {sig.ts_code} {sig.stock_name}")
            print(f"      评分: {sig.score:.1f}, 建议仓位: {sig.weight * 100:.1f}%")
            print(f"      止损: {sig.metadata.get('stop_loss_pct', -0.03):.1%}")
            print(f"      止盈: {sig.metadata.get('take_profit_pct', 0.015):.1%}")
            print(f"      原因: {sig.reason}")
    else:
        print("⚠️  今日无有效交易信号")

    # 风险控制检查
    print("\n🛡️  风险控制规则:")
    print("  • 单股仓位 ≤ 20% 总资金 (2,000 RMB)")
    print("  • 严格-3%止损，无例外")
    print("  • 11:28分对盈利持仓执行50%止盈")
    print("  • 14:50分无论盈亏全部平仓")
    print("  • 日最大亏损 ≤ 5% (500 RMB)")

    return valid_signals


if __name__ == "__main__":
    small_capital_test()
