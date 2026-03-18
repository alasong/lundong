#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据更新和实时数据获取指南
Comprehensive guide for data update and real-time data fetching
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from datetime import datetime, timedelta
from data.database import get_database


def show_data_source_overview():
    """展示数据源概览"""
    print("=" * 70)
    print("📊 数据更新与实时数据指南")
    print("=" * 70)

    print("\n\n📌 一、数据源配置")
    print("-" * 70)

    print("\n🔍 当前数据源: Tushare API (同花顺)")
    print("   官方网站: https://tushare.pro")
    print("   数据类型: A股板块行情、个股行情、成分股数据")
    print("   数据范围: 2020-01-02 至今")

    print("\n📋 数据源详情:")
    print("   ├─ 板块日线行情 (concept_daily)")
    print("   │   • 板块代码、名称、涨跌幅、成交量、成交额")
    print("   │   • 覆盖: 一级行业、二级行业、概念板块")
    print("   │")
    print("   ├─ 个股日线行情 (stock_daily)")
    print("   │   • 股票代码、价格、涨跌幅、市值、PE、PB")
    print("   │   • 覆盖: 全A股市场")
    print("   │")
    print("   └─ 板块成分股 (concept_constituent)")
    print("       • 板块代码、股票代码、权重")
    print("       • 关联关系: 板块 ↔ 个股")

    print("\n🔑 API配置 (位于 .env 文件):")
    print("   TUSHARE_TOKEN=b7afb358d4189e7d62bb3af4231d330d9245e3863a22ca86d8ff7ed9")
    print("   ✅ Token已配置，可直接使用")


def show_realtime_capability():
    """展示实时数据能力"""
    print("\n\n⚡ 二、实时数据获取能力")
    print("-" * 70)

    print("\n📊 Tushare API 数据更新频率:")
    print("   ├─ 日线数据: 每日收盘后更新 (15:30 后)")
    print("   ├─ 实时行情: 需要 pro_bar 接口 (付费)")
    print("   ├─ 分时数据: 需要 minute 接口 (付费)")
    print("   └─ 资金流向: 需要 moneyflow 接口 (付费)")

    print("\n🎯 当前系统能力:")
    print("   ✅ 历史数据采集 (fast_collector.py)")
    print("   ✅ 批量并发下载 (20线程)")
    print("   ✅ 自动限流保护 (450次/分钟)")
    print("   ✅ 断点续传支持")
    print("   ❌ 实时数据 (需要升级Tushare会员)")
    print("   ❌ 分时数据 (需要升级Tushare会员)")

    print("\n💡 实时数据获取方案:")
    print("   方案1: Tushare Pro会员 (推荐)")
    print("      • 费用: 约500元/年")
    print("      • 接口: pro_bar, minute, moneyflow")
    print("      • 频率: 实时/分钟级")
    print("      • 优点: 官方接口,稳定可靠")

    print("\n   方案2: 东方财富/新浪财经接口")
    print("      • 费用: 免费")
    print("      • 接口: HTTP爬虫")
    print("      • 频率: 分钟级 (需自行开发)")
    print("      • 风险: 可能被封禁")


def show_data_update_commands():
    """展示数据更新命令"""
    print("\n\n🚀 三、数据更新操作")
    print("-" * 70)

    print("\n📋 方法1: 命令行更新 (推荐)")
    print("-" * 40)
    print("   # 更新最新数据")
    print("   python src/main.py --mode collect")
    print("")
    print("   # 更新指定日期范围")
    print("   python src/main.py --mode collect --start 20260301 --end 20260318")
    print("")
    print("   # 更新个股数据")
    print("   python src/main.py --mode collect --stocks")

    print("\n📋 方法2: Python脚本更新")
    print("-" * 40)
    print("""
   from data.fast_collector import HighSpeedDataCollector
   from data.database import get_database
   
   # 初始化采集器
   collector = HighSpeedDataCollector(
       token="your_tushare_token",
       db=get_database(),
       max_workers=20
   )
   
   # 获取板块列表
   indices = collector.client.get_ths_indices()
   
   # 下载最新数据
   codes = indices['ts_code'].tolist()[:100]  # 取前100个
   collector.download_batch(
       codes=codes,
       start_date="20260301",
       end_date="20260318"
   )
   """)

    print("\n📋 方法3: 直接调用采集Skill")
    print("-" * 40)
    print("   # 使用skill命令")
    print("   /collect-data")


def show_update_frequency():
    """展示更新频率建议"""
    print("\n\n⏰ 四、数据更新频率建议")
    print("-" * 70)

    print("\n🎯 打板策略需求:")
    print("   ├─ 涨停数据: 必须每日更新")
    print("   ├─ 板块数据: 每日更新")
    print("   ├─ 成分股数据: 每周更新")
    print("   └─ 基本面数据: 每月更新")

    print("\n📅 推荐更新计划:")
    print("   每日 15:30 - 17:00")
    print("   ├─ 15:30: 自动触发数据采集")
    print("   ├─ 15:45: 更新板块行情")
    print("   ├─ 16:00: 更新个股行情")
    print("   └─ 16:30: 数据验证和备份")

    print("\n🔧 自动化配置:")
    print("   # Linux Cron 配置")
    print("   # 每日15:30执行")
    print("   30 15 * * 1-5 cd /home/song/lundong && python src/main.py --mode collect")


def show_database_status():
    """展示数据库当前状态"""
    print("\n\n📊 五、当前数据库状态")
    print("-" * 70)

    try:
        db = get_database()
        stats = db.get_performance_stats()

        print(f"\n📈 数据库统计:")
        print(f"   数据库大小: {stats['db_size_mb']:.2f} MB")
        print(f"   板块记录数: {stats['concept_daily_count']:,}")
        print(f"   个股记录数: {stats['stock_daily_count']:,}")
        print(
            f"   板块日期范围: {stats['concept_date_range'][0]} ~ {stats['concept_date_range'][1]}"
        )
        print(
            f"   个股日期范围: {stats['stock_date_range'][0]} ~ {stats['stock_date_range'][1]}"
        )

        # 计算数据新鲜度
        latest_date = stats["concept_date_range"][1]
        latest = datetime.strptime(latest_date, "%Y%m%d")
        today = datetime.now()
        days_ago = (today - latest).days

        print(f"\n🕐 数据新鲜度:")
        print(f"   最新数据日期: {latest_date}")
        print(f"   距今天数: {days_ago} 天")

        if days_ago <= 1:
            print("   ✅ 数据最新")
        elif days_ago <= 3:
            print("   ⚠️  数据稍旧，建议更新")
        else:
            print("   ❌ 数据过旧，必须更新")

    except Exception as e:
        print(f"   ❌ 获取数据库状态失败: {e}")


def run_data_update():
    """执行数据更新"""
    print("\n\n🔄 六、立即更新数据")
    print("=" * 70)

    print("\n选择更新方式:")
    print("   1. 使用 /collect-data skill (推荐)")
    print("   2. 使用 python src/main.py --mode collect")
    print("   3. 使用 Python 脚本直接调用")

    print("\n正在执行快速数据检查...")

    try:
        db = get_database()
        latest_date = db.get_latest_date()

        if latest_date:
            latest = datetime.strptime(latest_date, "%Y%m%d")
            today = datetime.now()
            days_ago = (today - latest).days

            if days_ago <= 1:
                print(f"\n✅ 数据已是最新: {latest_date}")
                print("   无需更新")
            else:
                print(f"\n⚠️  数据需要更新: {latest_date} ({days_ago}天前)")
                print("   建议运行: python src/main.py --mode collect")

    except Exception as e:
        print(f"\n❌ 检查失败: {e}")


def main():
    """主函数"""
    show_data_source_overview()
    show_realtime_capability()
    show_data_update_commands()
    show_update_frequency()
    show_database_status()
    run_data_update()

    print("\n\n" + "=" * 70)
    print("✅ 数据更新指南完成")
    print("=" * 70)

    print("\n💡 快速命令:")
    print("   更新数据: python src/main.py --mode collect")
    print("   查看状态: python src/main.py --mode db-stats")
    print("   使用Skill: /collect-data")

    print("\n📚 相关文档:")
    print("   • src/data/fast_collector.py - 高速采集器")
    print("   • src/data/tushare_ths_client.py - Tushare客户端")
    print("   • src/config.py - 配置管理")


if __name__ == "__main__":
    main()
