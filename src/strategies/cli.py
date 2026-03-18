"""
策略管理命令行接口
"""

import argparse
import json
from datetime import datetime
from typing import Optional

from loguru import logger

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 首先导入所有策略注册
import strategies.register

from strategies.version_manager import StrategyVersionManager
from strategies.performance_tracker import StrategyPerformanceTracker
from strategies.archiver import StrategyArchiver
from strategies.plugin_loader import PluginLoader
from strategies.strategy_factory import StrategyFactory


def list_strategies(args):
    """列出所有策略"""
    print("\n" + "=" * 70)
    print("策略列表")
    print("=" * 70)

    # 获取内置策略
    available = StrategyFactory.get_available_strategies()

    if not available:
        print("暂无已注册策略")
        return

    print(f"\n已注册策略 ({len(available)} 个):")
    print("-" * 50)

    for name in sorted(available):
        print(f"  - {name}")

    # 获取插件信息
    loader = PluginLoader()
    plugins = loader.list_plugins(
        include_archived=getattr(args, "include_archived", False)
    )

    if plugins:
        print(f"\n插件策略 ({len(plugins)} 个):")
        print("-" * 50)
        for plugin in plugins:
            status = "✓" if plugin.get("enabled", True) else "✗"
            location = plugin.get("location", "active")
            print(
                f"  [{status}] {plugin['name']} v{plugin.get('version', 'unknown')} ({location})"
            )

    # 性能摘要
    if getattr(args, "show_performance", False):
        print("\n" + "=" * 70)
        print("策略绩效摘要")
        print("=" * 70)

        tracker = StrategyPerformanceTracker()
        for name in sorted(available):
            perf = tracker.get_performance(name, days=30)
            if perf and perf.get("total_runs", 0) > 0:
                print(f"\n{name}:")
                print(f"  运行次数: {perf['total_runs']}")
                print(f"  平均收益: {perf['avg_return']:.2%}")
                print(f"  夏普比率: {perf['avg_sharpe']:.2f}")
                print(f"  最大回撤: {perf['avg_max_drawdown']:.2%}")


def show_strategy(args):
    """显示策略详情"""
    name = args.strategy_name

    print("\n" + "=" * 70)
    print(f"策略详情: {name}")
    print("=" * 70)

    # 基本信息
    try:
        strategy = StrategyFactory.create_strategy(name)
        info = strategy.get_info()
        print(f"\n基本信息:")
        print(f"  名称: {info['name']}")
        print(f"  参数: {json.dumps(info['params'], indent=4, ensure_ascii=False)}")
        print(f"  信号数: {info['num_signals']}")
    except ValueError as e:
        print(f"错误: {e}")
        return

    # 版本历史
    version_manager = StrategyVersionManager()
    versions = version_manager.list_versions(name)

    if versions:
        print(f"\n版本历史 ({len(versions)} 个版本):")
        print("-" * 50)
        for v in versions[:10]:  # 显示最近10个版本
            active = "★" if v.is_active else " "
            print(
                f"  [{active}] {v.version} - {v.created_at.strftime('%Y-%m-%d %H:%M')}"
            )
            if v.description:
                print(f"      {v.description}")

    # 性能数据
    tracker = StrategyPerformanceTracker()
    perf = tracker.get_performance(name, days=30)

    if perf and perf.get("total_runs", 0) > 0:
        print(f"\n绩效统计 (最近30天):")
        print("-" * 50)
        print(f"  运行次数: {perf['total_runs']}")
        print(f"  平均收益: {perf['avg_return']:.2%}")
        print(f"  总收益: {perf['total_return']:.2%}")
        print(f"  年化收益: {perf['annualized_return']:.2%}")
        print(f"  夏普比率: {perf['avg_sharpe']:.2f}")
        print(f"  最大回撤: {perf['avg_max_drawdown']:.2%}")
        print(f"  胜率: {perf['avg_win_rate']:.1%}")
        print(f"  最佳日: {perf['best_day']:.2%}")
        print(f"  最差日: {perf['worst_day']:.2%}")


def archive_strategy(args):
    """归档策略"""
    name = args.strategy_name

    archiver = StrategyArchiver()

    if args.dry_run:
        print("\n模拟归档模式 (dry-run)")
        print("-" * 50)

        low_performers = archiver.identify_low_performers()

        if name == "all":
            for strategy in low_performers:
                if strategy["recommendation"] == "archive":
                    print(f"  将归档: {strategy['strategy_name']}")
                    print(f"    原因: avg_return={strategy['avg_return']:.2%}")
        else:
            found = any(s["strategy_name"] == name for s in low_performers)
            if found:
                print(f"  将归档: {name}")
            else:
                print(f"  策略 {name} 不满足归档条件")
    else:
        if args.confirm or input(f"确认归档策略 {name}? [y/N]: ").lower() == "y":
            success = archiver.archive_strategy(name)
            if success:
                print(f"✓ 策略 {name} 已归档")
            else:
                print(f"✗ 归档失败")


def restore_strategy(args):
    """恢复归档策略"""
    archiver = StrategyArchiver()

    if args.list:
        archived = archiver.list_archived()
        if archived:
            print("\n已归档策略:")
            print("-" * 50)
            for s in archived:
                print(f"  - {s['name']} (原名: {s['original_name']})")
                print(f"      归档时间: {s['archived_at']}")
                print(f"      归档原因: {s['archive_reason']}")
        else:
            print("暂无归档策略")
        return

    name = args.strategy_name
    if args.confirm or input(f"确认恢复策略 {name}? [y/N]: ").lower() == "y":
        success = archiver.restore_strategy(name)
        if success:
            print(f"✓ 策略 {name} 已恢复")
        else:
            print(f"✗ 恢复失败")


def version_command(args):
    """版本管理"""
    name = args.strategy_name
    version_manager = StrategyVersionManager()

    if args.action == "list":
        versions = version_manager.list_versions(name)
        print(f"\n策略 {name} 版本历史:")
        print("-" * 50)
        for v in versions:
            active = "★" if v.is_active else " "
            print(f"  [{active}] {v.version}")
            print(f"      参数: {json.dumps(v.params, ensure_ascii=False)[:80]}...")
            print(f"      创建时间: {v.created_at.strftime('%Y-%m-%d %H:%M')}")

    elif args.action == "activate":
        version = args.version
        if version_manager.activate_version(name, version):
            print(f"✓ 已激活 {name} 的版本 {version}")
        else:
            print(f"✗ 激活失败，版本 {version} 不存在")

    elif args.action == "rollback":
        steps = args.steps or 1
        target = version_manager.rollback(name, steps)
        if target:
            print(f"✓ 已回滚 {name} 到版本 {target}")
        else:
            print(f"✗ 回滚失败")

    elif args.action == "compare":
        v1 = args.version
        v2 = args.version2
        diff = version_manager.compare_versions(name, v1, v2)
        print(f"\n版本比较: {v1} vs {v2}")
        print("-" * 50)
        print(json.dumps(diff, indent=2, ensure_ascii=False))


def performance_command(args):
    """性能统计"""
    tracker = StrategyPerformanceTracker()

    if args.strategy_name:
        perf = tracker.get_performance(args.strategy_name, days=args.days)
        print(f"\n策略 {args.strategy_name} 绩效 (最近 {args.days} 天):")
        print("-" * 50)
        print(json.dumps(perf, indent=2, ensure_ascii=False))
    else:
        low_performers = tracker.get_low_performers(
            threshold_return=args.threshold, min_runs=args.min_runs
        )
        if low_performers:
            print(f"\n低收益策略 (收益 < {args.threshold:.0%}):")
            print("-" * 50)
            for s in low_performers:
                print(f"  - {s['strategy_name']}")
                print(f"      平均收益: {s['avg_return']:.2%}")
                print(f"      运行次数: {s['run_count']}")
        else:
            print(f"没有收益低于 {args.threshold:.0%} 的策略")


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(
        description="策略管理命令",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # list 命令
    list_parser = subparsers.add_parser("list", help="列出所有策略")
    list_parser.add_argument(
        "--show-performance", "-p", action="store_true", help="显示绩效摘要"
    )
    list_parser.add_argument(
        "--include-archived", "-a", action="store_true", help="包含已归档策略"
    )
    list_parser.set_defaults(func=list_strategies)

    # show 命令
    show_parser = subparsers.add_parser("show", help="显示策略详情")
    show_parser.add_argument("strategy_name", help="策略名称")
    show_parser.set_defaults(func=show_strategy)

    # archive 命令
    archive_parser = subparsers.add_parser("archive", help="归档策略")
    archive_parser.add_argument("strategy_name", help="策略名称 (或 'all')")
    archive_parser.add_argument("--dry-run", action="store_true", help="仅模拟")
    archive_parser.add_argument("--confirm", "-y", action="store_true", help="跳过确认")
    archive_parser.set_defaults(func=archive_strategy)

    # restore 命令
    restore_parser = subparsers.add_parser("restore", help="恢复归档策略")
    restore_parser.add_argument("strategy_name", nargs="?", help="归档策略名称")
    restore_parser.add_argument(
        "--list", "-l", action="store_true", help="列出归档策略"
    )
    restore_parser.add_argument("--confirm", "-y", action="store_true", help="跳过确认")
    restore_parser.set_defaults(func=restore_strategy)

    # version 命令
    version_parser = subparsers.add_parser("version", help="版本管理")
    version_parser.add_argument(
        "action", choices=["list", "activate", "rollback", "compare"]
    )
    version_parser.add_argument("strategy_name", help="策略名称")
    version_parser.add_argument("--version", "-v", help="版本号")
    version_parser.add_argument("--version2", "-v2", help="比较版本2")
    version_parser.add_argument("--steps", "-s", type=int, help="回滚步数")
    version_parser.set_defaults(func=version_command)

    # performance 命令
    perf_parser = subparsers.add_parser("performance", help="性能统计")
    perf_parser.add_argument("strategy_name", nargs="?", help="策略名称")
    perf_parser.add_argument("--days", "-d", type=int, default=30, help="统计天数")
    perf_parser.add_argument(
        "--threshold", "-t", type=float, default=0.0, help="收益阈值"
    )
    perf_parser.add_argument(
        "--min-runs", "-m", type=int, default=10, help="最小运行次数"
    )
    perf_parser.set_defaults(func=performance_command)

    args = parser.parse_args()

    if args.command:
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
