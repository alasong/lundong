#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
长期历史数据采集脚本
用于采集 10 年 + 历史数据，支持断点续传和完整性检查
"""
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from loguru import logger

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from data.database import get_database
from data.tushare_ths_client import TushareTHSClient
from data.fast_collector import HighSpeedDataCollector


def get_trade_dates(start_date: str, end_date: str) -> List[str]:
    """生成交易日列表（简化版，跳过周末）"""
    dates = []
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    current = start
    while current <= end:
        if current.weekday() < 5:  # 跳过周末
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def get_existing_dates(db, ts_code: str, start_date: str, end_date: str) -> set:
    """获取数据库中已有的日期"""
    df = db.get_data_range(ts_code, start_date, end_date)
    if df.empty:
        return set()
    return set(df['trade_date'].astype(str).tolist())


def collect_long_history(
    start_date: str = "20160101",
    end_date: str = None,
    sector_type: str = "all",
    max_workers: int = 8
):
    """
    采集长期历史数据

    Args:
        start_date: 开始日期（默认 10 年前）
        end_date: 结束日期（默认昨天）
        sector_type: 板块类型 (all/concept/industry/region)
        max_workers: 最大并发数
    """
    logger.info("=" * 60)
    logger.info("长期历史数据采集")
    logger.info("=" * 60)

    # 设置结束日期为昨天
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    logger.info(f"采集日期范围：{start_date} - {end_date}")
    logger.info(f"板块类型：{sector_type}")
    logger.info(f"并发数：{max_workers}")

    # 初始化
    db = get_database()
    collector = HighSpeedDataCollector(
        token=settings.tushare_token,
        max_workers=max_workers
    )

    # 获取所有板块
    logger.info("\n正在获取板块列表...")
    indices = collector.client.get_ths_indices()

    if len(indices) == 0:
        logger.error("获取板块列表失败")
        return

    # 根据类型筛选
    if sector_type == "all":
        target_codes = indices[indices['ts_code'].str.startswith(('881', '882', '885'), na=False)]
    elif sector_type == "concept":
        target_codes = indices[indices['ts_code'].str.startswith('885', na=False)]
    elif sector_type == "industry":
        target_codes = indices[indices['ts_code'].str.startswith('881', na=False)]
    elif sector_type == "region":
        target_codes = indices[indices['ts_code'].str.startswith('882', na=False)]
    else:
        target_codes = indices

    logger.info(f"发现 {len(target_codes)} 个板块")

    # 检查每个板块的数据情况
    logger.info("\n正在检查数据完整性...")

    need_update = []
    complete = []
    empty = []

    all_dates = get_trade_dates(start_date, end_date)
    expected_days = len(all_dates)
    logger.info(f"预期交易日数：{expected_days}")

    for i, row in target_codes.iterrows():
        code = row['ts_code']
        name = row.get('name', '未知')

        # 检查已有数据
        existing_dates = get_existing_dates(db, code, start_date, end_date)
        coverage = len(existing_dates) / expected_days * 100

        if len(existing_dates) == 0:
            empty.append((code, name))
            need_update.append((code, name))
        elif coverage < 95:  # 覆盖率低于 95% 需要补全
            need_update.append((code, name, coverage))
        else:
            complete.append((code, name, coverage))

    logger.info(f"\n数据情况:")
    logger.info(f"  - 完整板块：{len(complete)} 个 (覆盖率 >= 95%)")
    logger.info(f"  - 需要更新：{len(need_update)} 个")
    logger.info(f"  - 完全缺失：{len(empty)} 个")

    if len(need_update) == 0:
        logger.info("\n所有板块数据已完整，无需采集")
        return

    # 开始采集
    logger.info(f"\n开始采集 {len(need_update)} 个板块的历史数据...")
    start_time = time.time()

    codes_to_update = [item[0] for item in need_update]
    name_mapping = {item[0]: item[1] for item in need_update}

    # 批量下载
    collector.download_batch_concurrent(
        codes=codes_to_update,
        start_date=start_date,
        end_date=end_date,
        name_mapping=name_mapping,
        max_workers=max_workers
    )

    elapsed = time.time() - start_time
    logger.info(f"\n总耗时：{elapsed/60:.1f} 分钟")

    # 最终统计
    stats = db.get_statistics()
    logger.info("\n" + "=" * 60)
    logger.info("数据库最终状态")
    logger.info("=" * 60)
    logger.info(f"总记录数：{stats['total_records']:,}")
    logger.info(f"板块数：{stats['concept_count']}")
    logger.info(f"平均每个板块：{stats['total_records'] / max(stats['concept_count'], 1):.0f} 条记录")
    logger.info("=" * 60)

    return stats


def collect_stock_history(
    start_date: str = "20160101",
    end_date: str = None,
    stock_codes: List[str] = None,
    n_jobs: int = 16
):
    """
    采集个股长期历史数据

    Args:
        start_date: 开始日期
        end_date: 结束日期
        stock_codes: 股票代码列表（可选，如果不传则采集所有成分股）
        n_jobs: 并行任务数
    """
    logger.info("=" * 60)
    logger.info("个股长期历史数据采集")
    logger.info("=" * 60)

    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    logger.info(f"采集日期范围：{start_date} - {end_date}")

    # 初始化
    from data.stock_collector import StockCollector
    from data.database import get_database

    db = get_database()
    collector = StockCollector(db=db)

    # 获取股票列表
    if stock_codes is None:
        logger.info("\n正在获取成分股列表...")
        constituents = db.get_all_constituents()
        stock_codes = list(set([c['stock_code'] for c in constituents]))
        logger.info(f"获取到 {len(stock_codes)} 只成分股")

    if len(stock_codes) == 0:
        logger.warning("没有需要采集的股票")
        return

    # 批量采集
    logger.info(f"\n开始批量采集 {len(stock_codes)} 只股票...")
    start_time = time.time()

    stats = collector.collect_stocks_batch(stock_codes, start_date, end_date, n_jobs=n_jobs)

    elapsed = time.time() - start_time
    total_records = sum(stats.values())

    logger.info(f"\n总耗时：{elapsed/60:.1f} 分钟")
    logger.info(f"总记录数：{total_records:,}")
    logger.info(f"平均速度：{total_records/elapsed:.1f} 条/秒")

    return stats


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="长期历史数据采集")
    parser.add_argument("--type", choices=["concept", "stock", "both"], default="concept",
                       help="采集类型：板块/个股/全部")
    parser.add_argument("--start-date", type=str, default="20160101", help="开始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    parser.add_argument("--sector-type", choices=["all", "concept", "industry", "region"],
                       default="all", help="板块类型")
    parser.add_argument("--workers", type=int, default=8, help="并发数（板块采集）")
    parser.add_argument("--jobs", type=int, default=16, help="并行任务数（个股采集）")

    args = parser.parse_args()

    if not settings.tushare_token:
        logger.error("请先设置 TUSHARE_TOKEN 环境变量")
        return

    # 配置日志
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO"
    )
    logger.add(
        "data/logs/collect_long_history_{time:YYYYMMDD}.log",
        level="DEBUG"
    )

    logger.info("=" * 60)
    logger.info("长期历史数据采集脚本")
    logger.info(f"开始时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 采集板块数据
    if args.type in ["concept", "both"]:
        collect_long_history(
            start_date=args.start_date,
            end_date=args.end_date,
            sector_type=args.sector_type,
            max_workers=args.workers
        )

    # 采集个股数据
    if args.type in ["stock", "both"]:
        collect_stock_history(
            start_date=args.start_date,
            end_date=args.end_date,
            n_jobs=args.jobs
        )

    logger.info("\n" + "=" * 60)
    logger.info("采集完成!")
    logger.info(f"结束时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
