#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
打板策略完整使用指南
Comprehensive guide for using 打板 strategies
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from strategies.register import *
from strategies.strategy_factory import StrategyFactory
from strategies.daban_version import DabanStrategyVersion
from data.database import get_database


def show_strategy_overview():
    """展示策略概览"""
    print("=" * 70)
    print("🚀 打板策略完整使用指南 v1.0.0")
    print("=" * 70)

    print("\n📊 一、策略方法选择")
    print("-" * 70)

    print("\n1️⃣  首板策略 (First Limit)")
    print("   目标: 识别首次涨停股票")
    print("   适用场景: ")
    print("     • 市场情绪好转，出现板块热点")
    print("     • 个股首次涨停，具有题材溢价")
    print("     • 成交量放大3-15倍，资金介入明显")
    print("   关键参数:")
    print("     • 涨幅阈值: 主板9.5%, 创业板/科创板19.5%")
    print("     • 成交量比: 3-15倍 (相比5日均量)")
    print("     • 市值范围: 100K-100M (根据数据库调整)")
    print("     • 止盈: +1.5%, 止损: -3%")
    print("   预期收益: 日均2-5%, 胜率45-50%")

    print("\n2️⃣  一进二策略 (One-to-Two)")
    print("   目标: 捕捉连板延续机会")
    print("   适用场景:")
    print("     • 昨日首板涨停，今日跳空高开")
    print("     • 跳空幅度1-5%，符合黄金区间")
    print("     • 成交量继续放大，市场认可度高")
    print("   关键参数:")
    print("     • 跳空幅度: 1-5% (避免过高风险)")
    print("     • 成交量比: >2倍昨日成交量")
    print("     • 止盈: +2.5%, 止损: -3%")
    print("   预期收益: 日均3-8%, 胜率40-45%")

    print("\n3️⃣  龙头股策略 (Leader Stock)")
    print("   目标: 识别板块龙头股票")
    print("   适用场景:")
    print("     • 板块整体走强，出现领涨股")
    print("     • 龙头股具有持续性和号召力")
    print("     • 板块资金集中度高")
    print("   关键参数:")
    print("     • 板块强度: >0.6 (相对强度指标)")
    print("     • 动量指标: 10日动量>5%, 20日动量>10%")
    print("     • 止盈: +2%, 止损: -3%")
    print("   预期收益: 日均5-10%, 胜率35-40%")


def show_selection_criteria():
    """展示选择标准"""
    print("\n\n📋 二、策略选择标准")
    print("-" * 70)

    print("\n🎯 市场环境判断:")
    print("   1. 查看涨停数量:")
    print("      • >10只: 市场活跃，适合首板策略")
    print("      • 5-10只: 市场平稳，适合一进二策略")
    print("      • <5只: 市场低迷，暂停交易")

    print("\n   2. 查看市场波动率:")
    print("      • <3%: 波动率低，可正常交易")
    print("      • 3-8%: 波动率中等，降低仓位")
    print("      • >8%: 波动率高，暂停交易")

    print("\n   3. 查看板块热点:")
    print("      • 单板块>3只涨停: 板块热点形成，适合龙头策略")
    print("      • 多板块各有涨停: 市场分散，适合首板策略")
    print("      • 无明显板块热点: 市场无主线，谨慎交易")

    print("\n⚖️  风险承受能力匹配:")
    print("   • 保守型: 仅使用首板策略，仓位<50%")
    print("   • 稳健型: 首板+一进二组合，仓位50-70%")
    print("   • 进取型: 三策略组合，仓位70-90%")


def show_realtime_operation():
    """展示实时操作流程"""
    print("\n\n⚡ 三、实时操作流程")
    print("-" * 70)

    print("\n🌅 早盘准备 (9:15-9:25):")
    print("   1. 运行监控仪表盘")
    print("      python monitoring_dashboard.py")
    print("   2. 检查市场状态和风险限制")
    print("   3. 确认昨日涨停股票列表")

    print("\n📊 集合竞价 (9:25-9:30):")
    print("   1. 观察一进二候选股开盘价")
    print("      • 跳空1-5%: 符合条件，准备买入")
    print("      • 跳空>5%: 风险过高，放弃")
    print("      • 跳空<1%: 动力不足，放弃")
    print("   2. 计算集合竞价成交量比")

    print("\n🔥 开盘执行 (9:30-10:00):")
    print("   1. 首板策略执行:")
    print("      • 观察接近涨停股票 (涨幅>8%)")
    print("      • 确认成交量放大 (>3倍)")
    print("      • 检查封单情况")
    print("      • 打板买入 (涨停价挂单)")

    print("\n   2. 一进二策略执行:")
    print("      • 确认跳空幅度符合条件")
    print("      • 观察开盘后走势")
    print("      • 快速买入 (开盘5分钟内)")

    print("\n⏰ 时段退出执行:")
    print("   • 11:28: 对盈利持仓执行50%止盈")
    print("   • 14:50: 强制平仓所有持仓")
    print("   • 全程: 监控止损线 (-3%)")

    print("\n📝 盘后复盘:")
    print("   1. 记录交易明细和决策依据")
    print("   2. 分析成功/失败原因")
    print("   3. 更新策略参数 (如需要)")


def show_usage_examples():
    """展示使用示例"""
    print("\n\n💻 四、代码使用示例")
    print("-" * 70)

    print("\n📌 示例1: 生成首板信号")
    print("""
from strategies.strategy_factory import StrategyFactory

# 创建首板策略
strategy = StrategyFactory.create_strategy(
    "first_limit",
    params={
        "min_volume_ratio": 3.0,
        "top_n_stocks": 5,
        "stop_loss_pct": -0.03,
        "take_profit_pct": 0.015
    }
)

# 生成信号
signals = strategy.generate_signals()

# 查看信号
for sig in signals:
    print(f"股票: {sig.ts_code} {sig.stock_name}")
    print(f"评分: {sig.score:.1f}")
    print(f"止损: {sig.metadata['stop_loss_pct']:.1%}")
    print(f"止盈: {sig.metadata['take_profit_pct']:.1%}")
    print(f"原因: {sig.reason}")
""")

    print("\n📌 示例2: 一进二策略")
    print("""
# 创建一进二策略
strategy = StrategyFactory.create_strategy(
    "one_to_two",
    params={
        "gap_open_min": 0.01,
        "gap_open_max": 0.05,
        "top_n_stocks": 3
    }
)

signals = strategy.generate_signals()
""")

    print("\n📌 示例3: 组合策略")
    print("""
# 创建多策略组合
from strategies.multi_strategy import MultiStrategyPortfolio

strategies = [
    StrategyFactory.create_strategy("first_limit", {"weight": 0.5}),
    StrategyFactory.create_strategy("one_to_two", {"weight": 0.5})
]

portfolio = MultiStrategyPortfolio(
    strategies=strategies,
    strategy_weights={"first_limit": 0.5, "one_to_two": 0.5}
)

# 生成合并信号
merged_signals = portfolio.generate_signals()
""")


def show_risk_management():
    """展示风险管理"""
    print("\n\n🛡️  五、风险管理体系")
    print("-" * 70)

    print("\n⚠️  硬性规则 (不可违反):")
    print("   ✅ 止损: 任何持仓亏损达到-3%立即平仓")
    print("   ✅ 时段: 11:28止盈50%, 14:50强制平仓")
    print("   ✅ 仓位: 单股≤10%, 单日≤30%总资金")
    print("   ✅ 熔断: 大盘跌停或个股跌停立即停止")

    print("\n📊 动态风控:")
    print("   • 日亏损达5%: 暂停当日交易")
    print("   • 连续3次亏损: 暂停策略，分析原因")
    print("   • 市场波动>8%: 降低仓位至50%")
    print("   • 板块集中度>40%: 分散持仓")

    print("\n🚨 应急处理:")
    print("   • 技术故障: 立即切换到保守模式")
    print("   • 数据异常: 人工审核，暂停交易")
    print("   • 市场异常: 启动熔断保护")


def run_interactive_demo():
    """运行交互式演示"""
    print("\n\n🎮 六、实时演示")
    print("=" * 70)

    # 获取版本信息
    version = DabanStrategyVersion.get_current_version()
    params = DabanStrategyVersion.get_current_parameters()
    performance = DabanStrategyVersion.get_expected_performance()

    print(f"\n当前版本: {version}")
    print(f"\n预期性能:")
    for key, value in performance.items():
        print(f"  • {key}: {value}")

    # 运行首板策略
    print("\n生成首板信号...")
    try:
        first_limit = StrategyFactory.create_strategy(
            "first_limit", params=params["first_limit"]
        )
        signals = first_limit.generate_signals()

        if signals:
            print(f"\n✅ 发现 {len(signals)} 个交易信号:")
            for i, sig in enumerate(signals[:3]):
                print(f"\n  {i + 1}. {sig.ts_code}")
                print(f"     评分: {sig.score:.1f}/100")
                print(f"     信号类型: {sig.signal_type}")
                print(f"     原因: {sig.reason}")
        else:
            print("\n⚠️  当前无有效交易信号")
            print("   建议: 等待市场机会或调整参数")

    except Exception as e:
        print(f"\n❌ 错误: {e}")

    # 运行市场监控
    print("\n\n市场状态监控...")
    try:
        db = get_database()
        latest_date = db.get_latest_date()
        market_data = db.get_all_stock_data(latest_date)

        limit_ups = len(market_data[market_data["pct_chg"] > 9.0])
        volatility = float(market_data["pct_chg"].std() / 100.0)

        print(f"\n日期: {latest_date}")
        print(f"涨停股票: {limit_ups} 只")
        print(f"市场波动率: {volatility:.2%}")

        if limit_ups >= 3:
            print("✅ 交易机会: 高")
        elif limit_ups > 0:
            print("⚠️  交易机会: 中")
        else:
            print("❌ 交易机会: 低")

    except Exception as e:
        print(f"监控错误: {e}")


def main():
    """主函数"""
    show_strategy_overview()
    show_selection_criteria()
    show_realtime_operation()
    show_usage_examples()
    show_risk_management()
    run_interactive_demo()

    print("\n\n" + "=" * 70)
    print("✅ 打板策略指南完成")
    print("=" * 70)
    print("\n💡 提示:")
    print("   • 从小资金开始 (10,000 RMB)")
    print("   • 严格执行风险控制规则")
    print("   • 每日复盘优化参数")
    print("   • 切勿盲目追加仓位")
    print("\n📚 相关文档:")
    print("   • monitoring_dashboard.py - 实时监控")
    print("   • small_capital_test.py - 小资金测试")
    print("   • src/strategies/daban_version.py - 版本管理")


if __name__ == "__main__":
    main()
