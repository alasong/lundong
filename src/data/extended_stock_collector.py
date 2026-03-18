#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
扩展股票数据采集器
采集全量A股、中证500成分股、创业板、科创板数据
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import tushare as ts

from config import settings
from data.database import get_database, SQLiteDatabase


class ExtendedStockCollector:
    """扩展股票数据采集器"""

    # API 限流配置
    API_LIMIT = 480  # 每分钟最多 480 次（预留缓冲）
    REQUEST_TIMES: List[float] = []

    def __init__(self, db: SQLiteDatabase = None):
        """初始化"""
        self.db = db or get_database()
        ts.set_token(settings.tushare_token)
        self.pro = ts.pro_api()
        logger.info("扩展股票采集器初始化完成")

    def _check_rate_limit(self):
        """检查 API 限流"""
        now = time.time()
        # 清理 60 秒前的请求记录
        self.REQUEST_TIMES = [t for t in self.REQUEST_TIMES if now - t < 60]

        if len(self.REQUEST_TIMES) >= self.API_LIMIT * 0.9:
            # 接近限制，等待
            wait_time = 60 - (now - self.REQUEST_TIMES[0]) + 1
            logger.warning(f"接近 API 限流，等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
            self.REQUEST_TIMES = []

        self.REQUEST_TIMES.append(time.time())

    def get_all_stocks(self) -> List[str]:
        """获取全部A股股票列表（沪深两市，排除北交所）"""
        logger.info("获取全部A股股票列表...")

        all_codes = []

        # 获取上交所股票
        self._check_rate_limit()
        df_sse = self.pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,name')
        if df_sse is not None and not df_sse.empty:
            # 排除科创板(688)和北交所
            sse_codes = df_sse[~df_sse['ts_code'].str.startswith('689')]['ts_code'].tolist()
            all_codes.extend(sse_codes)
            logger.info(f"上交所股票: {len(sse_codes)} 只")

        # 获取深交所股票
        self._check_rate_limit()
        df_szse = self.pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,name')
        if df_szse is not None and not df_szse.empty:
            # 包含主板、中小板、创业板
            szse_codes = df_szse['ts_code'].tolist()
            all_codes.extend(szse_codes)
            logger.info(f"深交所股票: {len(szse_codes)} 只")

        # 获取北交所股票（排除）
        # 北交所代码以 8 或 4 开头，如 8xxxxx.BJ

        # 去重
        all_codes = list(set(all_codes))
        logger.info(f"全部A股: {len(all_codes)} 只")
        return all_codes

    def get_csi500_constituents(self) -> List[str]:
        """获取中证500成分股"""
        logger.info("获取中证500成分股...")

        self._check_rate_limit()
        df = self.pro.index_weight(index_code='000905.SH', start_date='20260101')

        if df is None or df.empty:
            logger.warning("获取中证500成分股失败")
            return []

        codes = df['con_code'].unique().tolist()
        logger.info(f"中证500成分股: {len(codes)} 只")
        return codes

    def get_gem_stocks(self) -> List[str]:
        """获取创业板股票列表"""
        logger.info("获取创业板股票列表...")

        self._check_rate_limit()
        df = self.pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,name')

        if df is None or df.empty:
            logger.warning("获取创业板股票失败")
            return []

        gem = df[df['ts_code'].str.startswith('300')]
        codes = gem['ts_code'].tolist()
        logger.info(f"创业板股票: {len(codes)} 只")
        return codes

    def get_star_stocks(self) -> List[str]:
        """获取科创板股票列表"""
        logger.info("获取科创板股票列表...")

        self._check_rate_limit()
        df = self.pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,name')

        if df is None or df.empty:
            logger.warning("获取科创板股票失败")
            return []

        star = df[df['ts_code'].str.startswith('688')]
        codes = star['ts_code'].tolist()
        logger.info(f"科创板股票: {len(codes)} 只")
        return codes

    def get_all_target_stocks(self, include_all: bool = False) -> Dict[str, List[str]]:
        """获取所有目标股票"""
        if include_all:
            return {'all': self.get_all_stocks()}
        return {
            'csi500': self.get_csi500_constituents(),
            'gem': self.get_gem_stocks(),
            'star': self.get_star_stocks()
        }

    def collect_stock_daily(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """采集单只股票日线数据"""
        self._check_rate_limit()

        try:
            # 日线行情
            df_daily = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df_daily is None or df_daily.empty:
                return None

            # 基本面数据
            self._check_rate_limit()
            df_basic = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,pe,pb,ps,total_mv,circ_mv,turnover_rate'
            )

            # 合并数据
            if df_basic is not None and not df_basic.empty:
                df = df_daily.merge(df_basic, on=['ts_code', 'trade_date'], how='left')
            else:
                df = df_daily
                # 添加空的基本面字段
                for col in ['pe', 'pb', 'ps', 'total_mv', 'circ_mv', 'turnover_rate']:
                    df[col] = None

            return df

        except Exception as e:
            logger.warning(f"{ts_code} 采集失败: {str(e)[:50]}")
            return None

    def collect_batch(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        max_workers: int = 20,
        batch_size: int = 100
    ) -> Dict[str, int]:
        """
        批量采集股票数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            max_workers: 最大并发数
            batch_size: 每批次数量

        Returns:
            采集统计
        """
        total = len(stock_codes)
        logger.info(f"开始批量采集 {total} 只股票，日期范围: {start_date} ~ {end_date}")

        all_data = []
        success_count = 0
        fail_count = 0

        # 分批处理
        for i in range(0, total, batch_size):
            batch = stock_codes[i:i + batch_size]
            batch_start = time.time()

            # 并发采集
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self.collect_stock_daily, code, start_date, end_date): code
                    for code in batch
                }

                for future in as_completed(futures):
                    code = futures[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            all_data.append(df)
                            success_count += 1
                        else:
                            fail_count += 1
                    except Exception as e:
                        fail_count += 1
                        logger.debug(f"{code} 处理失败: {str(e)[:30]}")

            # 批次进度
            elapsed = time.time() - batch_start
            progress = min(i + batch_size, total)
            logger.info(f"进度: {progress}/{total} ({progress*100/total:.1f}%), "
                       f"成功 {success_count}, 失败 {fail_count}, 耗时 {elapsed:.1f}s")

        # 保存到数据库
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            self._save_to_database(combined)
            logger.info(f"保存 {len(combined)} 条记录到数据库")

        return {
            'total': total,
            'success': success_count,
            'fail': fail_count,
            'records': sum(len(df) for df in all_data)
        }

    def _save_to_database(self, df: pd.DataFrame):
        """保存数据到数据库"""
        # 标准化字段
        df = df.rename(columns={
            'trade_date': 'trade_date',
            'ts_code': 'ts_code',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'pre_close': 'pre_close',
            'change': 'change',
            'pct_chg': 'pct_chg',
            'vol': 'vol',
            'amount': 'amount',
            'pe': 'pe',
            'pb': 'pb',
            'ps': 'ps',
            'total_mv': 'total_mv',
            'circ_mv': 'circ_mv',
            'turnover_rate': 'turnover_rate'
        })

        # 确保日期格式
        df['trade_date'] = df['trade_date'].astype(str).str.replace('-', '')

        # 批量插入
        self.db.batch_insert('stock_daily', df.to_dict('records'), batch_size=5000)

    def collect_all(
        self,
        start_date: str = '20200101',
        end_date: str = None,
        include_all: bool = False,
        include_csi500: bool = False,
        include_gem: bool = False,
        include_star: bool = False
    ) -> Dict:
        """
        采集股票数据

        Args:
            start_date: 开始日期
            end_date: 结束日期（默认今天）
            include_all: 是否采集全部A股（优先级最高）
            include_csi500: 是否包含中证500
            include_gem: 是否包含创业板
            include_star: 是否包含科创板
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        logger.info("=" * 60)
        logger.info("开始采集股票数据")
        logger.info("=" * 60)

        # 获取股票列表
        all_codes = []
        sources = {}

        # 优先处理全量采集
        if include_all:
            codes = self.get_all_stocks()
            all_codes.extend(codes)
            sources['all'] = len(codes)
        else:
            # 按指定类型采集
            if include_csi500:
                codes = self.get_csi500_constituents()
                all_codes.extend(codes)
                sources['csi500'] = len(codes)

            if include_gem:
                codes = self.get_gem_stocks()
                all_codes.extend(codes)
                sources['gem'] = len(codes)

            if include_star:
                codes = self.get_star_stocks()
                all_codes.extend(codes)
                sources['star'] = len(codes)

            # 如果没有指定任何类型，默认采集全部
            if not sources:
                codes = self.get_all_stocks()
                all_codes.extend(codes)
                sources['all'] = len(codes)

        # 去重
        all_codes = list(set(all_codes))
        logger.info(f"总计 {len(all_codes)} 只股票（去重后）")
        logger.info(f"来源: {sources}")

        # 批量采集
        stats = self.collect_batch(all_codes, start_date, end_date)

        logger.info("=" * 60)
        logger.info("采集完成")
        logger.info(f"  总股票数: {stats['total']}")
        logger.info(f"  成功: {stats['success']}")
        logger.info(f"  失败: {stats['fail']}")
        logger.info(f"  总记录数: {stats['records']}")
        logger.info("=" * 60)

        return {
            'sources': sources,
            'stats': stats
        }


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='扩展股票数据采集')
    parser.add_argument('--start-date', default='20200101', help='开始日期')
    parser.add_argument('--end-date', default=None, help='结束日期')
    parser.add_argument('--csi500', action='store_true', help='仅采集中证500')
    parser.add_argument('--gem', action='store_true', help='仅采集创业板')
    parser.add_argument('--star', action='store_true', help='仅采集科创板')

    args = parser.parse_args()

    collector = ExtendedStockCollector()

    # 如果没有指定特定类型，则采集全部
    if not (args.csi500 or args.gem or args.star):
        # 采集全部
        result = collector.collect_all(
            start_date=args.start_date,
            end_date=args.end_date
        )
    else:
        # 采集指定类型
        result = collector.collect_all(
            start_date=args.start_date,
            end_date=args.end_date,
            include_csi500=args.csi500,
            include_gem=args.gem,
            include_star=args.star
        )

    print(f"\n采集结果: {result}")


if __name__ == "__main__":
    main()