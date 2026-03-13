#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
高速数据采集模块
支持高并发、断点续传、批量下载、实时写入数据库、自动补全缺失数据
"""
import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
from loguru import logger
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from data.tushare_ths_client import TushareTHSClient
from data.database import SQLiteDatabase, get_database


class HighSpeedDataCollector:
    """高速数据采集器（基于 SQLite 数据库）"""

    def __init__(self, token: str, db: SQLiteDatabase = None, max_workers: int = 20, api_limit: int = 450):
        """
        初始化采集器

        Args:
            token: Tushare token
            db: 数据库实例，如果为 None 则使用全局单例
            max_workers: 最大并发数（默认 20）
            api_limit: API 每分钟请求限制（默认 450，预留缓冲）
        """
        self.client = TushareTHSClient(token)
        self.db = db or get_database()
        self.max_workers = max_workers
        self.api_limit = api_limit
        self.downloaded_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.missing_filled = 0
        self._lock = Lock()
        self._request_times = []  # 记录请求时间用于限流
        self._invalid_codes: Set[str] = set()  # 记录返回空数据的无效板块

        # 创建输出目录（用于导出 CSV）
        os.makedirs(settings.raw_data_dir, exist_ok=True)

        # 进度追踪
        self.last_progress_time = time.time()

    def _check_api_limit(self):
        """检查 API 限流，如果接近限制则等待"""
        now = time.time()
        # 移除 60 秒前的请求记录
        with self._lock:
            self._request_times = [t for t in self._request_times if now - t < 60]

            # 如果接近限制（80%），开始减速等待
            if len(self._request_times) >= self.api_limit * 0.8:
                wait_time = 60 - (now - self._request_times[0])
                if wait_time > 0:
                    logger.warning(f"触发 API 限流，等待 {wait_time:.1f}秒...")
                    time.sleep(wait_time + 1)
                    self._request_times = []

            # 记录当前请求
            self._request_times.append(now)

    def _get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        生成交易日列表（简化版，跳过周末）

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表
        """
        dates = []
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        current = start
        while current <= end:
            # 跳过周末
            if current.weekday() < 5:
                dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return dates

    def _check_missing_dates(self, ts_code: str, start_date: str, end_date: str) -> List[str]:
        """
        检查缺失的交易日期

        Args:
            ts_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            缺失的日期列表
        """
        # 获取该板块已有的数据
        df = self.db.get_data_range(ts_code, start_date, end_date)
        existing_dates = set(df['trade_date'].tolist()) if not df.empty else set()

        # 生成所有交易日
        all_dates = self._get_trade_dates(start_date, end_date)
        all_dates_set = set(all_dates)

        # 找出缺失的日期
        missing = sorted(list(all_dates_set - existing_dates))
        return missing

    def _fill_missing_data(self, code: str, name: str, missing_dates: List[str]) -> int:
        """
        补全缺失日期的数据（带限流保护）

        Args:
            code: 板块代码
            name: 板块名称
            missing_dates: 缺失的日期列表

        Returns:
            成功补全的数量
        """
        filled = 0
        for date in missing_dates:
            try:
                # 检查 API 限流
                self._check_api_limit()

                df = self.client.pro.ths_daily(
                    ts_code=code,
                    start_date=date,
                    end_date=date
                )
                if df is not None and len(df) > 0:
                    self.db.save_concept_daily_batch(df, replace=True)
                    filled += 1
                    logger.debug(f"补全 {code} ({name}) {date} 的数据")
                # 降低请求频率（每 5 个请求停 1 秒）
                if (filled + 1) % 5 == 0:
                    time.sleep(1.0)
            except Exception as e:
                error_msg = str(e)
                if "500" in error_msg or "每分钟" in error_msg:
                    # 触发限流，等待更长时间
                    logger.warning(f"触发限流，等待 60 秒：{code} {date}")
                    time.sleep(60)
                    # 重试一次
                    try:
                        df = self.client.pro.ths_daily(
                            ts_code=code,
                            start_date=date,
                            end_date=date
                        )
                        if df is not None and len(df) > 0:
                            self.db.save_concept_daily_batch(df, replace=True)
                            filled += 1
                    except Exception as e2:
                        logger.warning(f"重试失败：{code} {date} - {str(e2)[:50]}")
                else:
                    logger.warning(f"补全失败：{code} {date} - {str(e)[:50]}")
        return filled

    def _should_redownload(self, ts_code: str, target_days: int = 250) -> bool:
        """
        判断是否需要重新下载（从数据库检查）

        Args:
            ts_code: 板块代码
            target_days: 目标天数

        Returns:
            是否需要重新下载
        """
        # 从数据库检查数据是否存在
        latest_date = self.db.get_latest_date(ts_code)

        if latest_date is None:
            return True  # 没有数据，需要下载

        # 检查数据量
        df = self.db.get_data_range(ts_code, '20200101', latest_date)
        if len(df) < target_days:
            return True

        # 检查最新日期是否是最近 3 天内的
        today = datetime.now()
        latest = datetime.strptime(str(latest_date), '%Y%m%d')
        days_diff = (today - latest).days

        return days_diff > 3

    def _check_code_valid(self, ts_code: str) -> bool:
        """
        检查板块代码是否有效（ths_daily 接口是否返回数据）

        Args:
            ts_code: 板块代码

        Returns:
            是否有效
        """
        try:
            # 已经记录为无效的板块直接返回 False
            if ts_code in self._invalid_codes:
                return False

            # 测试获取 1 天数据
            df = self.client.pro.ths_daily(
                ts_code=ts_code,
                start_date=(datetime.now() - timedelta(days=10)).strftime('%Y%m%d'),
                end_date=(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            )
            return df is not None and len(df) > 0
        except Exception:
            return False

    def filter_valid_codes(self, codes: List[str]) -> List[str]:
        """
        过滤掉无效的板块代码（ths_daily 接口不支持的）

        Args:
            codes: 板块代码列表

        Returns:
            有效的板块代码列表
        """
        logger.info(f"检查板块代码有效性（共 {len(codes)} 个）...")
        valid_codes = []
        invalid_count = 0

        for code in codes:
            if self._check_code_valid(code):
                valid_codes.append(code)
            else:
                invalid_count += 1

        logger.info(f"有效板块：{len(valid_codes)} 个，无效板块：{invalid_count} 个")
        return valid_codes

    def _download_single(self, code: str, name: str, start_date: str, end_date: str) -> bool:
        """
        下载单个板块数据并写入数据库

        Args:
            code: 板块代码
            name: 板块名称
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            是否成功
        """
        # 检查是否需要重新下载
        if not self._should_redownload(code):
            with self._lock:
                self.skipped_count += 1
            logger.debug(f"跳过 {code} ({name}): 数据已存在且最新")
            return True

        try:
            # 使用 ths_daily 接口下载
            for retry in range(3):
                try:
                    df = self.client.pro.ths_daily(
                        ts_code=code,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if df is not None and len(df) > 0:
                        # 直接写入数据库（实时去重）
                        self.db.save_concept_daily_batch(df, replace=True)
                        with self._lock:
                            self.downloaded_count += 1

                        # 进度追踪
                        self._log_progress(f"下载完成：{code} ({name}) - {len(df)} 条记录")
                        return True
                    else:
                        logger.warning(f"空数据：{code}")
                        return False

                except Exception as e:
                    if retry < 2:
                        wait_time = (retry + 1) * 0.5
                        logger.warning(f"下载失败，{wait_time}s 后重试：{code} - {str(e)[:50]}")
                        time.sleep(wait_time)
                    else:
                        raise

        except Exception as e:
            with self._lock:
                self.failed_count += 1
            logger.error(f"下载失败：{code} ({name}) - {str(e)[:100]}")
            return False

    def _download_single_no_check(self, code: str, name: str, start_date: str, end_date: str) -> bool:
        """
        下载单个板块数据并写入数据库（不检查是否已存在，强制下载，自动补全缺失）

        Args:
            code: 板块代码
            name: 板块名称
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            是否成功
        """
        try:
            # 使用 ths_daily 接口下载
            for retry in range(3):
                try:
                    # 检查 API 限流
                    self._check_api_limit()

                    df = self.client.pro.ths_daily(
                        ts_code=code,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if df is not None and len(df) > 0:
                        # 直接写入数据库（实时去重）
                        self.db.save_concept_daily_batch(df, replace=True)
                        with self._lock:
                            self.downloaded_count += 1

                        logger.debug(f"下载完成：{code} ({name}) - {len(df)} 条记录")
                        return True
                    else:
                        logger.debug(f"空数据（Tushare 接口不支持）：{code} - {name}")
                        # 记录无效板块到列表，后续可过滤
                        self._invalid_codes.add(code)
                        return False

                except Exception as e:
                    if retry < 2:
                        wait_time = (retry + 1) * 0.5
                        logger.warning(f"下载失败，{wait_time}s 后重试：{code} - {str(e)[:50]}")
                        time.sleep(wait_time)
                    else:
                        raise

        except Exception as e:
            with self._lock:
                self.failed_count += 1
            logger.error(f"下载失败：{code} ({name}) - {str(e)[:100]}")
            return False

    def _check_and_fill_missing(self, code: str, name: str, start_date: str, end_date: str):
        """
        检查并补全缺失的日期数据

        Args:
            code: 板块代码
            name: 板块名称
            start_date: 开始日期
            end_date: 结束日期
        """
        missing = self._check_missing_dates(code, start_date, end_date)
        if missing:
            filled = self._fill_missing_data(code, name, missing)
            with self._lock:
                self.missing_filled += filled
            if filled > 0:
                logger.info(f"检查完整性：{code} ({name}) 补全 {filled}/{len(missing)} 天缺失数据")

    def _log_progress(self, message: str):
        """记录进度"""
        now = time.time()
        if now - self.last_progress_time > 5:  # 每 5 秒输出一次
            with self._lock:
                logger.info(f"[进度] 已下载：{self.downloaded_count}, 跳过：{self.skipped_count}, 失败：{self.failed_count}")
            self.last_progress_time = now

    def download_batch_concurrent(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        name_mapping: Optional[Dict[str, str]] = None,
        max_workers: int = None
    ):
        """
        高并发批量下载板块数据（多线程）

        Args:
            codes: 板块代码列表
            start_date: 开始日期
            end_date: 结束日期
            name_mapping: 代码 - 名称映射
            max_workers: 最大并发数（默认 8，降低以避免限流）
        """
        if max_workers is None:
            max_workers = self.max_workers

        # 降低并发数以避免触发 API 限流
        # Tushare 限制 500 次/分钟，每个线程约 2 秒完成一个请求，所以 8 个线程比较安全
        actual_workers = min(max_workers, 8)

        total = len(codes)
        logger.info(f"开始高并发下载：{total} 个板块，{actual_workers} 个线程")
        logger.info(f"日期范围：{start_date} - {end_date}")

        start_time = time.time()

        # 使用 ThreadPoolExecutor 实现并发下载
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            futures = {}
            for i, code in enumerate(codes):
                name = name_mapping.get(code, "未知") if name_mapping else "未知"
                # 提交下载任务
                future = executor.submit(
                    self._download_single_no_check,
                    code, name, start_date, end_date
                )
                futures[future] = (i + 1, code, name)

            # 等待所有任务完成
            for future in as_completed(futures):
                idx, code, name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[{idx}/{total}] {code} 下载异常：{e}")

        elapsed = time.time() - start_time

        # 最终统计
        logger.info("=" * 60)
        logger.info("下载完成")
        logger.info(f"总板块数：{total}")
        logger.info(f"新下载：{self.downloaded_count}")
        logger.info(f"跳过：{self.skipped_count}")
        logger.info(f"失败：{self.failed_count}")
        logger.info(f"耗时：{elapsed:.1f}s")
        logger.info(f"平均速度：{total/elapsed:.1f} 板块/秒")

        # 数据库统计
        stats = self.db.get_statistics()
        logger.info(f"数据库总记录：{stats['total_records']:,}")
        logger.info(f"数据库板块数：{stats['concept_count']}")
        logger.info("=" * 60)

    def download_batch(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        name_mapping: Optional[Dict[str, str]] = None,
        concurrent: bool = True
    ):
        """
        批量下载板块数据并写入数据库（默认高并发）

        Args:
            codes: 板块代码列表
            start_date: 开始日期
            end_date: 结束日期
            name_mapping: 代码 - 名称映射
            concurrent: 是否使用并发下载（默认 True）
        """
        if concurrent:
            # 使用高并发下载
            self.download_batch_concurrent(codes, start_date, end_date, name_mapping)
        else:
            # 顺序下载（兼容旧接口）
            self._download_sequential(codes, start_date, end_date, name_mapping)

    def _download_sequential(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        name_mapping: Optional[Dict[str, str]] = None
    ):
        """顺序下载（用于兼容）"""
        total = len(codes)
        logger.info(f"开始顺序下载：{total} 个板块")
        logger.info(f"日期范围：{start_date} - {end_date}")

        start_time = time.time()

        for i, code in enumerate(codes, 1):
            name = name_mapping.get(code, "未知") if name_mapping else "未知"
            logger.debug(f"[{i}/{total}] 下载 {code} ({name})")

            self._download_single_no_check(code, name, start_date, end_date)

            # API 限流控制
            if i % 10 == 0:
                time.sleep(0.5)

        elapsed = time.time() - start_time

        # 最终统计
        logger.info("=" * 60)
        logger.info("下载完成")
        logger.info(f"总板块数：{total}")
        logger.info(f"新下载：{self.downloaded_count}")
        logger.info(f"跳过：{self.skipped_count}")
        logger.info(f"失败：{self.failed_count}")
        logger.info(f"补全缺失：{self.missing_filled} 天")
        logger.info(f"耗时：{elapsed:.1f}s")
        logger.info("=" * 60)

    def download_all_history(
        self,
        start_date: str,
        end_date: str,
        output_file: str = None,
        concurrent: bool = True,
        sector_type: str = "all"
    ):
        """
        下载所有板块历史数据到数据库

        Args:
            start_date: 开始日期
            end_date: 结束日期
            output_file: 输出文件名（用于导出 CSV）
            concurrent: 是否使用并发下载
            sector_type: 板块类型 (all/concept/industry/region)
                - all: 全部板块 (881/882/885)
                - concept: 概念板块 (885xxx)
                - industry: 行业板块 (881xxx)
                - region: 地区板块 (882xxx)
        """
        logger.info("获取板块列表...")
        indices = self.client.get_ths_indices()

        if len(indices) == 0:
            logger.error("无法获取板块列表")
            return

        # 根据类型筛选板块
        if sector_type == "all":
            # 下载所有主要板块类型（881 行业、882 地区、885 概念）
            # 排除北交所板块（87xxxx）
            target_codes = indices[
                indices['ts_code'].str.startswith(('881', '882', '885'), na=False) &
                ~indices['ts_code'].str.startswith('87', na=False)
            ]
            logger.info(f"发现 {len(target_codes)} 个板块（行业 + 地区 + 概念，不含北交所）")
            # 细分统计
            industry_count = len(indices[indices['ts_code'].str.startswith('881', na=False)])
            region_count = len(indices[indices['ts_code'].str.startswith('882', na=False)])
            concept_count = len(indices[indices['ts_code'].str.startswith('885', na=False)])
            logger.info(f"  - 行业板块 (881): {industry_count} 个")
            logger.info(f"  - 地区板块 (882): {region_count} 个")
            logger.info(f"  - 概念板块 (885): {concept_count} 个")
        elif sector_type == "concept":
            # 排除北交所板块（87xxxx）
            target_codes = indices[
                indices['ts_code'].str.startswith('885', na=False) &
                ~indices['ts_code'].str.startswith('87', na=False)
            ]
            logger.info(f"发现 {len(target_codes)} 个概念板块（不含北交所）")
        elif sector_type == "industry":
            target_codes = indices[indices['ts_code'].str.startswith('881', na=False)]
            logger.info(f"发现 {len(target_codes)} 个行业板块")
        elif sector_type == "region":
            target_codes = indices[indices['ts_code'].str.startswith('882', na=False)]
            logger.info(f"发现 {len(target_codes)} 个地区板块")
        else:
            # 默认下载所有主要板块（排除北交所 87xxxx）
            target_codes = indices[
                indices['ts_code'].str.startswith(('881', '882', '885'), na=False) &
                ~indices['ts_code'].str.startswith('87', na=False)
            ]
            logger.info(f"发现 {len(target_codes)} 个板块（行业 + 地区 + 概念，不含北交所）")

        if concurrent:
            # 使用高并发下载
            logger.info("使用高并发模式下载...")
            self.download_batch_concurrent(
                codes=target_codes['ts_code'].tolist(),
                start_date=start_date,
                end_date=end_date,
                name_mapping=target_codes.set_index('ts_code')['name'].to_dict(),
                max_workers=self.max_workers
            )
        else:
            # 顺序下载
            all_data = []

            for i, row in target_codes.iterrows():
                code = row['ts_code']
                name = row.get('name', '未知')
                logger.debug(f"[{i+1}/{len(target_codes)}] 下载 {code} ({name})")

                try:
                    df = self.client.pro.ths_daily(
                        ts_code=code,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if df is not None and len(df) > 0:
                        # 直接写入数据库
                        self.db.save_concept_daily_batch(df, replace=True)
                        all_data.append(df)

                except Exception as e:
                    logger.warning(f"下载失败：{code} - {str(e)[:50]}")

                # 限流
                if (i + 1) % 10 == 0:
                    time.sleep(0.5)

            # 导出合集 CSV（可选）
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_file or f"ths_all_history_{start_date}_{end_date}.csv"
                output_path = os.path.join(settings.raw_data_dir, output_file)
                combined.to_csv(output_path, index=False)

                logger.info("=" * 60)
                logger.info("合集下载完成")
                logger.info(f"总记录数：{len(combined):,}")
                logger.info(f"板块数：{combined['ts_code'].nunique()}")
                logger.info(f"输出文件：{output_path}")

        # 数据库统计
        stats = self.db.get_statistics()
        logger.info("=" * 60)
        logger.info("下载完成")
        logger.info(f"数据库总记录：{stats['total_records']:,}")
        logger.info(f"数据库板块数：{stats['concept_count']}")
        logger.info("=" * 60)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="高速数据采集")
    parser.add_argument("--start-date", type=str, default="20200101", help="开始日期")
    parser.add_argument("--end-date", type=str, default="20251231", help="结束日期")
    parser.add_argument("--mode", choices=["batch", "all"], default="batch", help="下载模式")
    parser.add_argument("--output", type=str, help="输出文件名（all 模式使用）")

    args = parser.parse_args()

    if not settings.tushare_token:
        logger.error("请设置 TUSHARE_TOKEN")
        return

    collector = HighSpeedDataCollector(
        token=settings.tushare_token,
        max_workers=10
    )

    if args.mode == "batch":
        # 获取板块列表
        indices = collector.client.get_ths_indices()
        codes = indices['ts_code'].tolist()

        collector.download_batch(codes, args.start_date, args.end_date)

    elif args.mode == "all":
        collector.download_all_history(args.start_date, args.end_date, args.output)


if __name__ == "__main__":
    main()
