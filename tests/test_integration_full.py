#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
量化系统集成测试
验证所有模块协同工作
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from loguru import logger
from datetime import datetime

# 导入所有模块
from data.database import get_database
from data.stock_screener import StockScreener
from risk.risk_manager import RiskManager
from risk.transaction_cost import TransactionCostModel, estimate_impact_on_returns
from risk.signal_generator import SignalGenerator, print_signals
from strategies.market_regime import MarketRegimeDetector, print_regime_report
from strategies.multi_factor import MultiFactorStrategy
from trading.order_manager import OrderManager, print_portfolio_summary
from trading.rebalance_scheduler import RebalanceScheduler
from evaluation.enhanced_backtester import EnhancedBacktester


def test_all_modules():
    """测试所有模块"""
    print("=" * 70)
    print("量化系统集成测试")
    print("=" * 70)
    print(f"测试时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results = {}

    # 1. 数据库测试
    print("\n【测试 1】数据库连接...")
    try:
        db = get_database()
        latest_date = db.get_latest_date()
        print(f"  ✓ 数据库连接成功，最新日期：{latest_date}")
        results['database'] = True
    except Exception as e:
        print(f"  ✗ 数据库测试失败：{e}")
        results['database'] = False

    # 2. 风险管理器测试
    print("\n【测试 2】风险管理器...")
    try:
        rm = RiskManager()
        blacklist = rm.get_blacklist()
        print(f"  ✓ 黑名单股票：{len(blacklist)} 只")

        # 测试止损
        test_positions = [
            {'ts_code': '000001.SZ', 'cost_price': 10.0, 'shares': 1000, 'highest_price': 11.0}
        ]
        test_prices = pd.DataFrame([{'ts_code': '000001.SZ', 'close': 9.5}])
        stop_loss = rm.check_stop_loss(test_positions, test_prices)
        print(f"  ✓ 止损检查：{len(stop_loss)} 只触发")
        results['risk_manager'] = True
    except Exception as e:
        print(f"  ✗ 风险管理器测试失败：{e}")
        results['risk_manager'] = False

    # 3. 交易成本测试
    print("\n【测试 3】交易成本模型...")
    try:
        tcm = TransactionCostModel()
        cost = tcm.calculate_cost('000001.SZ', 'buy', 10.0, 1000)
        print(f"  ✓ 买入成本：¥{cost['total_cost']:.2f} ({cost['cost_rate']:.2%})")

        cost_sell = tcm.calculate_cost('000001.SZ', 'sell', 10.0, 1000)
        print(f"  ✓ 卖出成本：¥{cost_sell['total_cost']:.2f} ({cost_sell['cost_rate']:.2%})")

        impact = estimate_impact_on_returns(10)
        print(f"  ✓ 年换手 10x 成本拖累：{impact:.2f}%")
        results['transaction_cost'] = True
    except Exception as e:
        print(f"  ✗ 交易成本测试失败：{e}")
        results['transaction_cost'] = False

    # 4. 市场状态识别测试
    print("\n【测试 4】市场状态识别...")
    try:
        mrd = MarketRegimeDetector()
        regime = mrd.identify_regime()
        print(f"  ✓ 当前状态：{regime.get('regime_name', '未知')} (置信度{regime.get('confidence', 0):.0%})")
        results['market_regime'] = True
    except Exception as e:
        print(f"  ✗ 市场状态测试失败：{e}")
        results['market_regime'] = False

    # 5. 多因子模型测试
    print("\n【测试 5】多因子模型...")
    try:
        mfm = MultiFactorStrategy()
        conn = sqlite3.connect('data/stock.db')
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT stock_code FROM concept_constituent LIMIT 10")
        stocks = [r[0] for r in cursor.fetchall()]
        conn.close()

        result = mfm.get_top_stocks_by_factors(stocks, top_n=5)
        if not result.empty:
            print(f"  ✓ 多因子选股：{len(result)} 只")
            print(f"    最高得分：{result['composite_score'].max():.1f}")
        else:
            print(f"  ⚠ 结果为空")
        results['multi_factor'] = True
    except Exception as e:
        print(f"  ✗ 多因子测试失败：{e}")
        results['multi_factor'] = False

    # 6. 个股筛选器测试
    print("\n【测试 6】个股筛选器...")
    try:
        screener = StockScreener()
        result = screener.get_top_stocks(['885311.TI', '885368.TI'], top_n=5)
        if not result.empty:
            print(f"  ✓ 筛选结果：{len(result)} 只股票")
            print(f"    平均得分：{result['stock_score'].mean():.1f}")
        else:
            print(f"  ⚠ 筛选结果为空")
        results['screener'] = True
    except Exception as e:
        print(f"  ✗ 筛选器测试失败：{e}")
        results['screener'] = False

    # 7. 订单管理测试
    print("\n【测试 7】订单管理...")
    try:
        om = OrderManager(initial_capital=100000)

        # 测试信号生成订单
        signals = pd.DataFrame([
            {'ts_code': '000001.SZ', 'signal': 5},
            {'ts_code': '000002.SZ', 'signal': 1}
        ])
        prices = pd.DataFrame([
            {'ts_code': '000001.SZ', 'close': 10.0},
            {'ts_code': '000002.SZ', 'close': 20.0}
        ])

        orders = om.generate_orders_from_signals(signals, prices)
        print(f"  ✓ 生成订单：{len(orders)} 个")

        summary = om.get_portfolio_summary()
        print(f"  ✓ 组合总资产：¥{summary['total_assets']:,.2f}")
        results['order_manager'] = True
    except Exception as e:
        print(f"  ✗ 订单管理测试失败：{e}")
        results['order_manager'] = False

    # 8. 调仓调度器测试
    print("\n【测试 8】调仓调度器...")
    try:
        scheduler = RebalanceScheduler(initial_capital=500000)
        status = scheduler.get_status()
        print(f"  ✓ 调度器初始化成功")
        print(f"    再平衡频率：{status['rebalance_frequency']}")
        results['scheduler'] = True
    except Exception as e:
        print(f"  ✗ 调度器测试失败：{e}")
        results['scheduler'] = False

    # 汇总结果
    print("\n" + "=" * 70)
    print("测试结果汇总")
    print("=" * 70)

    all_passed = all(results.values())

    for module, passed in results.items():
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"  {status} - {module}")

    print()
    if all_passed:
        print("🎉 所有测试通过！系统运行正常。")
    else:
        failed_count = sum(1 for v in results.values() if not v)
        print(f"⚠️  {failed_count} 个模块测试失败，请检查。")

    print("=" * 70)

    return all_passed


if __name__ == "__main__":
    import sqlite3
    success = test_all_modules()
    sys.exit(0 if success else 1)
