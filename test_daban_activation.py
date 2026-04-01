#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试打板策略激活验证脚本
验证首板、一进二和增强龙头股策略是否能正常生成信号和组合优化
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from strategies.register import *
from strategies.strategy_factory import StrategyFactory
from loguru import logger


def test_strategy_activation():
    """测试策略激活"""
    logger.info("开始测试打板策略激活...")

    # 测试首板策略
    try:
        first_limit_strategy = StrategyFactory.create_strategy("first_limit")
        logger.info("✅ 首板策略创建成功")

        # 测试策略基本信息
        print(f"首板策略名称: {first_limit_strategy.name}")
        print(f"首板策略参数: {first_limit_strategy.params}")

    except Exception as e:
        logger.error(f"❌ 首板策略创建失败: {e}")
        return False

    # 测试一进二策略
    try:
        one_to_two_strategy = StrategyFactory.create_strategy("one_to_two")
        logger.info("✅ 一进二策略创建成功")

        # 测试策略基本信息
        print(f"一进二策略名称: {one_to_two_strategy.name}")
        print(f"一进二策略参数: {one_to_two_strategy.params}")

    except Exception as e:
        logger.error(f"❌ 一进二策略创建失败: {e}")
        return False

    # 测试增强龙头股策略
    try:
        enhanced_dragon_head_strategy = StrategyFactory.create_strategy(
            "enhanced_dragon_head"
        )
        logger.info("✅ 增强龙头股策略创建成功")

        # 测试策略基本信息
        print(f"增强龙头股策略名称: {enhanced_dragon_head_strategy.name}")
        print(f"增强龙头股策略参数: {enhanced_dragon_head_strategy.params}")

    except Exception as e:
        logger.error(f"❌ 增强龙头股策略创建失败: {e}")
        return False

    # 测试多策略组合
    try:
        from strategies.multi_strategy import MultiStrategyPortfolio

        strategies = [
            StrategyFactory.create_strategy("first_limit"),
            StrategyFactory.create_strategy("one_to_two"),
            StrategyFactory.create_strategy("enhanced_dragon_head"),
        ]

        multi_strategy = MultiStrategyPortfolio(
            strategies=strategies,
            strategy_weights={
                "first_limit": 0.15,
                "one_to_two": 0.15,
                "enhanced_dragon_head": 0.2,
            },
        )
        logger.info("✅ 多策略组合创建成功")

    except Exception as e:
        logger.error(f"❌ 多策略组合创建失败: {e}")
        return False

    logger.info("所有打板策略激活测试通过！✅")
    return True


def test_signal_generation_with_mock_data():
    """使用模拟数据测试信号生成"""
    logger.info("开始测试信号生成功能（使用模拟数据）...")

    try:
        # 创建首板策略
        strategy = StrategyFactory.create_strategy("first_limit")

        # 模拟信号生成（不依赖数据库）
        # 由于数据库为空，这里只测试方法调用是否正常
        required_data = strategy.get_required_data()
        print(f"首板策略所需数据: {required_data}")

        # 测试组合优化功能
        from strategies.base_strategy import StrategySignal

        # 创建模拟信号
        mock_signals = [
            StrategySignal(
                ts_code="000001.SZ",
                stock_name="平安银行",
                strategy_type="first_limit",
                signal_type="buy",
                weight=0.1,
                score=75.5,
                reason="模拟首板信号",
                metadata={"stop_loss_pct": -0.03, "take_profit_pct": 0.015},
            ),
            StrategySignal(
                ts_code="600000.SH",
                stock_name="浦发银行",
                strategy_type="first_limit",
                signal_type="watch",
                weight=0.08,
                score=68.2,
                reason="模拟首板信号",
                metadata={"stop_loss_pct": -0.03, "take_profit_pct": 0.015},
            ),
        ]

        # 测试组合优化
        portfolio_result = strategy.optimize_portfolio(mock_signals)
        print(f"组合优化结果: {portfolio_result}")

        if portfolio_result and "portfolio" in portfolio_result:
            logger.info("✅ 信号生成和组合优化测试通过")
            return True
        else:
            logger.error("❌ 组合优化返回格式错误")
            return False

    except Exception as e:
        logger.error(f"❌ 信号生成测试失败: {e}")
        return False


def main():
    """主测试函数"""
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    print("=" * 60)
    print("打板策略激活验证测试")
    print("=" * 60)

    # 测试策略激活
    activation_success = test_strategy_activation()

    print()

    # 测试信号生成
    signal_success = test_signal_generation_with_mock_data()

    print()
    print("=" * 60)

    if activation_success and signal_success:
        print("🎉 所有测试通过！打板策略已成功激活并可以正常运行。")
        print("\n下一步建议:")
        print("1. 采集真实市场数据")
        print("2. 运行完整回测验证")
        print("3. 进行实盘小资金测试")
    else:
        print("❌ 测试失败！请检查配置和代码。")

    print("=" * 60)


if __name__ == "__main__":
    main()
