#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试策略版本管理和性能追踪功能
"""

import os
import sys
import json
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 修复导入路径
from src.strategies.version_manager import StrategyVersionManager
from src.strategies.performance_tracker import StrategyPerformanceTracker
from src.data.database import get_database


def test_version_manager():
    """测试版本管理功能"""
    print("=== 测试策略版本管理 ===")

    # 初始化版本管理器
    vm = StrategyVersionManager()

    # 测试策略参数
    strategy_params = {
        "window_size": 20,
        "threshold": 0.8,
        "min_volume": 1000000,
        "filters": ["momentum", "volatility"],
    }

    # 创建第一个版本
    version1 = vm.create_version("test_strategy", strategy_params, "Initial version")
    print(f"创建版本: {version1}")

    # 创建第二个版本（修改参数）
    strategy_params_v2 = strategy_params.copy()
    strategy_params_v2["threshold"] = 0.75
    strategy_params_v2["window_size"] = 30

    version2 = vm.create_version(
        "test_strategy", strategy_params_v2, "Updated threshold and window"
    )
    print(f"创建版本: {version2}")

    # 获取当前激活版本（应该没有，因为我们还没激活任何版本）
    current = vm.get_version("test_strategy")
    print(f"当前激活版本: {current.version if current else 'None'}")

    # 激活第二个版本
    activated = vm.activate_version("test_strategy", version2)
    print(f"激活版本 {version2}: {activated}")

    # 获取当前激活版本
    current = vm.get_version("test_strategy")
    print(f"当前激活版本: {current.version if current else 'None'}")
    print(f"当前激活版本参数: {current.params if current else 'None'}")

    # 列出所有版本
    all_versions = vm.list_versions("test_strategy")
    print(f"所有版本: {len(all_versions)} 个")
    for v in all_versions:
        print(f"  - {v.version}: {v.description} (active: {v.is_active})")

    # 比较两个版本
    comparison = vm.compare_versions("test_strategy", version1, version2)
    print(f"版本比较: {comparison['diff']}")

    # 测试回滚
    rolled_back = vm.rollback("test_strategy", 1)
    print(f"回滚到上一版本: {rolled_back}")

    # 获取当前版本（应该回到版本1）
    current = vm.get_version("test_strategy")
    print(f"回滚后当前激活版本: {current.version if current else 'None'}")

    print()


def test_performance_tracker():
    """测试性能追踪功能"""
    print("=== 测试策略性能追踪 ===")

    # 初始化性能追踪器
    pt = StrategyPerformanceTracker()

    # 模拟策略运行信号
    signals = [
        {"stock": "000001.SZ", "action": "buy", "confidence": 0.85},
        {"stock": "000002.SZ", "action": "buy", "confidence": 0.75},
        {"stock": "600000.SH", "action": "sell", "confidence": 0.65},
    ]

    # 记录一次策略运行
    run_id = pt.log_run("test_strategy", "v1.0.0", signals, portfolio_return=0.025)
    print(f"记录运行: run_id={run_id}")

    # 更新性能指标
    metrics = {
        "sharpe": 1.25,
        "max_drawdown": -0.08,
        "win_rate": 0.65,
        "total_return": 0.15,
        "volatility": 0.12,
        "benchmark_return": 0.08,
    }

    pt.update_performance("test_strategy", run_id, metrics)
    print(f"更新性能指标: {metrics}")

    # 获取性能汇总
    perf_summary = pt.get_performance("test_strategy", days=30)
    print(f"性能汇总: {perf_summary}")

    # 获取低收益策略
    low_performers = pt.get_low_performers(threshold_return=0.1, min_runs=1)
    print(f"低收益策略: {low_performers}")

    print()


def test_integration():
    """测试版本管理和性能追踪集成"""
    print("=== 测试集成 ===")

    vm = StrategyVersionManager()
    pt = StrategyPerformanceTracker()

    # 创建不同版本的策略
    params_v1 = {"param_a": 1, "param_b": 2}
    params_v2 = {"param_a": 1.5, "param_b": 2.5}

    v1 = vm.create_version("integration_test", params_v1, "Version 1")
    v2 = vm.create_version("integration_test", params_v2, "Version 2")

    print(f"创建版本: {v1}, {v2}")

    # 激活版本2
    vm.activate_version("integration_test", v2)

    # 记录两个版本的运行
    signals = [{"stock": "000001.SZ", "action": "buy", "confidence": 0.8}]

    run1 = pt.log_run("integration_test", v1, signals, portfolio_return=0.01)
    run2 = pt.log_run("integration_test", v2, signals, portfolio_return=0.03)

    print(f"记录运行: v1={run1}, v2={run2}")

    # 获取性能
    perf = pt.get_performance("integration_test", days=30)
    print(
        f"集成测试性能: {perf['total_runs']} runs, avg_return={perf['avg_return']:.4f}"
    )

    print("集成测试完成！")


if __name__ == "__main__":
    print("开始测试策略版本管理和性能追踪功能...\n")

    try:
        test_version_manager()
        test_performance_tracker()
        test_integration()

        print("✅ 所有测试通过！")
        print("\n新功能说明：")
        print("- StrategyVersionManager: 策略版本创建、激活、回滚、比较等功能")
        print(
            "- StrategyPerformanceTracker: 策略运行记录、性能指标追踪、低收益策略识别"
        )
        print(
            "- 数据库已添加 strategy_versions, strategy_runs, strategy_performance 三张表"
        )

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
