"""
个股数据采集器
负责采集和管理个股数据
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from data.database import SQLiteDatabase, get_database
from data.tushare_ths_client import TushareTHSClient


class StockCollector:
    """个股数据采集器"""

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化采集器

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        self.ths_client = TushareTHSClient(token=settings.tushare_token)
        logger.info("个股采集器初始化完成")

    def collect_stock_list(self) -> int:
        """
        采集 A 股上市公司列表

        Returns:
            采集的股票数量
        """
        logger.info("开始采集股票列表...")

        # 获取所有 A 股列表
        stock_df = self.ths_client.get_stock_list()

        if stock_df.empty:
            logger.warning("获取股票列表失败")
            return 0

        # 保存到数据库（如果需要单独的股票信息表）
        # 当前主要使用 stock_daily 表

        logger.info(f"股票列表采集完成：{len(stock_df)} 只")
        return len(stock_df)

    def collect_single_stock(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        is_increment: bool = True
    ) -> int:
        """
        采集单只股票数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            is_increment: 是否增量采集

        Returns:
            采集的记录数
        """
        # 判断是否需要增量采集
        if is_increment:
            missing_dates = self.db.get_stock_missing_dates(ts_code, start_date, end_date)
            if not missing_dates:
                logger.debug(f"{ts_code} 数据已完整")
                return 0

            # 将缺失日期转换为采集区间
            if len(missing_dates) < 5:
                logger.debug(f"{ts_code} 仅缺失 {len(missing_dates)} 天，跳过")
                return 0

            # 重新计算起止日期
            start_date = missing_dates[0]
            end_date = missing_dates[-1]

        # 获取数据
        data = self.ths_client.get_stock_daily(ts_code, start_date, end_date)

        if data is None or data.empty:
            logger.warning(f"{ts_code} 获取数据失败")
            return 0

        # 标准化字段
        data = self._standardize_columns(data)

        # 保存到数据库
        self.db.save_stock_daily_batch(data, replace=True)

        logger.debug(f"{ts_code} 采集完成：{len(data)} 条")
        return len(data)

    def collect_stocks_batch(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        n_jobs: int = 16
    ) -> Dict[str, int]:
        """
        批量采集股票数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            n_jobs: 并行任务数

        Returns:
            采集统计 {ts_code: count}
        """
        logger.info(f"开始批量采集 {len(stock_codes)} 只股票...")

        from joblib import Parallel, delayed

        results = Parallel(n_jobs=n_jobs, backend="threading", verbose=0)(
            delayed(self.collect_single_stock)(code, start_date, end_date)
            for code in stock_codes
        )

        stats = {code: count for code, count in zip(stock_codes, results)}
        total = sum(stats.values())

        logger.info(f"批量采集完成：{total} 条记录")
        return stats

    def collect_constituent_stocks(
        self,
        concept_code: str,
        start_date: str,
        end_date: str,
        refresh: bool = False
    ) -> int:
        """
        采集板块成分股数据

        Args:
            concept_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期
            refresh: 是否刷新成分股

        Returns:
            采集的股票数量
        """
        logger.info(f"开始采集 {concept_code} 成分股...")

        # 1. 获取成分股列表
        if refresh:
            constituents_df = self.ths_client.get_ths_members(concept_code)
            if constituents_df is not None and not constituents_df.empty:
                # 保存到数据库
                constituents = []
                for _, row in constituents_df.iterrows():
                    constituents.append({
                        'stock_code': row.get('con_code', row.get('ts_code')),
                        'stock_name': row.get('con_name', ''),
                        'weight': None,
                        'is_core': 1
                    })
                self.db.save_concept_constituents(concept_code, constituents)
                logger.info(f"成分股列表已更新：{len(constituents)} 只")

        # 2. 从数据库读取成分股
        constituents = self.db.get_concept_constituents(concept_code)

        if not constituents:
            logger.warning(f"未找到 {concept_code} 的成分股")
            return 0

        stock_codes = [c['stock_code'] for c in constituents]

        # 3. 采集成分股数据
        stats = self.collect_stocks_batch(stock_codes, start_date, end_date)

        logger.info(f"{concept_code} 成分股采集完成：{len(stats)} 只股票")
        return len(stats)

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 DataFrame 列名"""
        # Tushare daily 接口返回的字段：
        # ts_code, trade_date, open, high, low, close, pre_close,
        # change, pct_chg, vol, amount

        column_mapping = {
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
        }

        # 选择需要的列
        available_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df[list(available_cols.keys())].rename(columns=available_cols)

        # 添加额外字段
        if 'turnover_rate' not in df.columns:
            df['turnover_rate'] = None
        if 'pe' not in df.columns:
            df['pe'] = None
        if 'pb' not in df.columns:
            df['pb'] = None
        if 'ps' not in df.columns:
            df['ps'] = None
        if 'total_mv' not in df.columns:
            df['total_mv'] = None
        if 'circ_mv' not in df.columns:
            df['circ_mv'] = None

        # 数据类型转换
        numeric_cols = ['open', 'high', 'low', 'close', 'pre_close', 'change',
                       'pct_chg', 'vol', 'amount', 'turnover_rate',
                       'pe', 'pb', 'ps', 'total_mv', 'circ_mv']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 日期格式转换（YYYYMMDD 字符串）
        if 'trade_date' in df.columns:
            df['trade_date'] = df['trade_date'].astype(str).str.replace('-', '')

        return df

    def calculate_stock_factors(
        self,
        ts_code: str,
        trade_date: str
    ) -> Optional[Dict]:
        """
        计算个股技术因子

        Args:
            ts_code: 股票代码
            trade_date: 交易日期

        Returns:
            因子数据字典
        """
        # 获取过去 60 天数据用于计算因子
        start_date = self._get_previous_trade_date(trade_date, 60)
        data = self.db.get_stock_data(ts_code, start_date, trade_date)

        if data.empty or len(data) < 20:
            logger.debug(f"{ts_code} 数据不足，无法计算因子")
            return None

        data = data.sort_values('trade_date').reset_index(drop=True)

        # 计算因子
        close = data['close'].values
        pct_chg = data['pct_chg'].values

        # 市值（使用最新值）
        market_cap = data.iloc[-1].get('total_mv')

        # 估值因子
        pe_ttm = data.iloc[-1].get('pe')
        pb_ttm = data.iloc[-1].get('pb')
        ps_ttm = data.iloc[-1].get('ps')

        # 动量因子
        if len(close) >= 20:
            momentum_20d = (close[-1] / close[-20] - 1) * 100
        else:
            momentum_20d = None

        if len(close) >= 60:
            momentum_60d = (close[-1] / close[-60] - 1) * 100
        else:
            momentum_60d = None

        # 波动率因子（20 日收益率标准差）
        if len(pct_chg) >= 20:
            volatility_20d = np.std(pct_chg[-20:]) * np.sqrt(252)
        else:
            volatility_20d = None

        # 流动性因子
        if len(data) >= 20:
            avg_turnover_20d = data['turnover_rate'].iloc[-20:].mean() if 'turnover_rate' in data.columns else None
            avg_amount_20d = data['amount'].iloc[-20:].mean() if 'amount' in data.columns else None
        else:
            avg_turnover_20d = None
            avg_amount_20d = None

        factors = {
            'trade_date': trade_date,
            'market_cap': market_cap,
            'pe_ttm': pe_ttm,
            'pb_ttm': pb_ttm,
            'ps_ttm': ps_ttm,
            'momentum_20d': momentum_20d,
            'momentum_60d': momentum_60d,
            'volatility_20d': volatility_20d,
            'avg_turnover_20d': avg_turnover_20d,
            'avg_amount_20d': avg_amount_20d
        }

        return factors

    def _get_previous_trade_date(self, date_str: str, days: int) -> str:
        """获取 N 个交易日前的日期（简单实现，跳过周末）"""
        date = datetime.strptime(date_str, "%Y%m%d")
        result = date - timedelta(days=days * 1.5)  # 粗略估计
        return result.strftime("%Y%m%d")


def main():
    """测试函数"""
    from config import settings

    collector = StockCollector()

    # 测试采集股票列表
    print("\n[测试 1] 采集股票列表...")
    count = collector.collect_stock_list()
    print(f"  结果：{count} 只股票")

    # 测试采集单只股票
    print("\n[测试 2] 采集单只股票 (000001.SZ)...")
    result = collector.collect_single_stock('000001.SZ', '20240101', '20241231')
    print(f"  结果：{result} 条记录")

    # 测试批量采集
    print("\n[测试 3] 批量采集...")
    test_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
    stats = collector.collect_stocks_batch(test_codes, '20240101', '20241231')
    print(f"  结果：{stats}")


if __name__ == "__main__":
    main()
