#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
高速数据采集模块
支持高并发、断点续传、批量下载
"""
import os
import sys
import time
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict
from loguru import logger
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from data.tushare_ths_client import TushareTHSClient


class HighSpeedDataCollector:
    """高速数据采集器"""

    def __init__(self, token: str, output_dir: str = None, max_workers: int = 10):
        """
        初始化采集器

        Args:
            token: Tushare token
            output_dir: 输出目录
            max_workers: 最大并发数（受 API 限制，建议 5-10）
        """
        self.client = TushareTHSClient(token)
        self.output_dir = output_dir or settings.raw_data_dir
        self.max_workers = max_workers
        self.downloaded_count = 0
        self.skipped_count = 0
        self.failed_count = 0

        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)

        # 进度追踪
        self.last_progress_time = time.time()

    def _get_file_hash(self, filepath: str) -> str:
        """计算文件 MD5"""
        if not os.path.exists(filepath):
            return ""
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def _should_redownload(self, code: str, target_days: int = 250) -> bool:
        """
        判断是否需要重新下载

        Args:
            code: 板块代码
            target_days: 目标天数

        Returns:
            是否需要重新下载
        """
        filepath = os.path.join(self.output_dir, f"ths_{code}.csv")
        if not os.path.exists(filepath):
            return True

        try:
            df = pd.read_csv(filepath)
            if len(df) < target_days:
                return True

            # 检查最新日期
            latest_date = df['trade_date'].max()
            today = datetime.now()
            latest = datetime.strptime(str(latest_date), '%Y%m%d')

            # 如果数据是最近 3 天内的，不需要重新下载
            days_diff = (today - latest).days
            return days_diff > 3

        except Exception:
            return True

    def _download_single(self, code: str, name: str, start_date: str, end_date: str) -> bool:
        """
        下载单个板块数据

        Args:
            code: 板块代码
            name: 板块名称
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            是否成功
        """
        filepath = os.path.join(self.output_dir, f"ths_{code}.csv")

        # 检查是否需要重新下载
        if not self._should_redownload(code):
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
                        # 保存数据
                        df.to_csv(filepath, index=False)
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
            self.failed_count += 1
            logger.error(f"下载失败：{code} ({name}) - {str(e)[:100]}")
            return False

    def _log_progress(self, message: str):
        """记录进度"""
        now = time.time()
        if now - self.last_progress_time > 5:  # 每 5 秒输出一次
            logger.info(f"[进度] 已下载：{self.downloaded_count}, 跳过：{self.skipped_count}, 失败：{self.failed_count}")
            self.last_progress_time = now

    def download_batch(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        name_mapping: Optional[Dict[str, str]] = None
    ):
        """
        批量下载板块数据

        Args:
            codes: 板块代码列表
            start_date: 开始日期
            end_date: 结束日期
            name_mapping: 代码 - 名称映射
        """
        total = len(codes)
        logger.info(f"开始批量下载：{total} 个板块")
        logger.info(f"日期范围：{start_date} - {end_date}")
        logger.info(f"并发策略：顺序下载（API 限制），智能跳过已有数据")

        start_time = time.time()

        for i, code in enumerate(codes, 1):
            name = name_mapping.get(code, "未知") if name_mapping else "未知"
            logger.debug(f"[{i}/{total}] 下载 {code} ({name})")

            self._download_single(code, name, start_date, end_date)

            # API 限流控制（Tushare 有速率限制）
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
        logger.info(f"耗时：{elapsed:.1f}s")
        logger.info(f"平均速度：{total/elapsed:.1f} 板块/秒")
        logger.info("=" * 60)

    def download_all_history(
        self,
        start_date: str,
        end_date: str,
        output_file: str = None
    ):
        """
        下载所有板块历史数据到单个文件

        Args:
            start_date: 开始日期
            end_date: 结束日期
            output_file: 输出文件名
        """
        logger.info("获取板块列表...")
        indices = self.client.get_ths_indices()

        if len(indices) == 0:
            logger.error("无法获取板块列表")
            return

        # 筛选概念板块 (885xxx)
        concept_codes = indices[indices['ts_code'].str.startswith('885', na=False)]
        logger.info(f"发现 {len(concept_codes)} 个概念板块")

        all_data = []

        for i, row in concept_codes.iterrows():
            code = row['ts_code']
            name = row.get('name', '未知')
            logger.debug(f"[{i+1}/{len(concept_codes)}] 下载 {code} ({name})")

            try:
                df = self.client.pro.ths_daily(
                    ts_code=code,
                    start_date=start_date,
                    end_date=end_date
                )

                if df is not None and len(df) > 0:
                    all_data.append(df)

            except Exception as e:
                logger.warning(f"下载失败：{code} - {str(e)[:50]}")

            # 限流
            if (i + 1) % 10 == 0:
                time.sleep(0.5)

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)

            output_file = output_file or f"ths_all_history_{start_date}_{end_date}.csv"
            output_path = os.path.join(self.output_dir, output_file)

            combined.to_csv(output_path, index=False)

            logger.info("=" * 60)
            logger.info("合集下载完成")
            logger.info(f"总记录数：{len(combined):,}")
            logger.info(f"板块数：{combined['ts_code'].nunique()}")
            logger.info(f"输出文件：{output_path}")
            logger.info("=" * 60)
        else:
            logger.error("未下载任何数据")


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
