"""
多因子模型模块
在原有动量因子基础上，添加质量因子和成长因子
"""
import pandas as pd
import numpy as np
import sqlite3
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import SQLiteDatabase, get_database


class MultiFactorModel:
    """
    多因子选股模型

    因子类别：
    1. 动量因子（已有）
    2. 质量因子（ROE、毛利率、资产负债率）
    3. 成长因子（营收增速、利润增速）
    4. 估值因子（PE、PB）
    5. 流动性因子（换手率、成交额）
    """

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化多因子模型

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        logger.info("多因子模型初始化完成")

    def calculate_quality_factors(
        self,
        stock_codes: List[str],
        date: str = None
    ) -> pd.DataFrame:
        """
        计算质量因子

        Args:
            stock_codes: 股票代码列表
            date: 基准日期

        Returns:
            质量因子 DataFrame
        """
        if date is None:
            date = self.db.get_latest_date()
            if date is None:
                logger.warning("无法获取最新日期")
                return pd.DataFrame()

        factors = []

        for ts_code in stock_codes:
            # 从数据库获取基本面数据
            conn = sqlite3.connect('data/stock.db')
            cursor = conn.cursor()

            try:
                # 获取最新的基本面数据
                cursor.execute('''
                    SELECT pe_ttm, pb, total_mv, close
                    FROM stock_daily_basic
                    WHERE ts_code = ? AND trade_date = ?
                ''', (ts_code, date))

                row = cursor.fetchone()
                if not row:
                    continue

                pe_ttm, pb, total_mv, close = row

                # 计算质量因子（简化版，实际应该从财务报表获取）
                # 这里使用技术面数据近似

                # 获取历史数据计算稳定性指标
                cursor.execute('''
                    SELECT close, pct_chg
                    FROM stock_daily
                    WHERE ts_code = ? AND trade_date <= ?
                    ORDER BY trade_date DESC
                    LIMIT 250
                ''', (ts_code, date))

                history = cursor.fetchall()
                conn.close()

                if len(history) < 60:
                    continue

                closes = [r[0] for r in history]
                returns = [r[1] for r in history]

                # 质量因子代理指标：
                # 1. 价格稳定性（波动率越低，质量越高）
                volatility = np.std(returns[-60:]) * np.sqrt(252)

                # 2. 趋势稳定性（上涨天数占比）
                up_days = sum(1 for r in returns[-60:] if r > 0)
                up_ratio = up_days / 60

                # 3. 相对强度（相对于 60 日前涨幅）
                if closes[0] > 0:
                    return_60d = (closes[0] / closes[59] - 1) * 100
                else:
                    return_60d = 0

                factors.append({
                    'ts_code': ts_code,
                    'trade_date': date,
                    # 估值
                    'pe_ttm': pe_ttm,
                    'pb': pb,
                    'market_cap': total_mv / 10000 if total_mv else None,  # 万元转亿元
                    'close': close,
                    # 质量因子
                    'quality_volatility': volatility,
                    'quality_up_ratio': up_ratio,
                    'quality_return_60d': return_60d,
                })

            except Exception as e:
                logger.error(f"计算 {ts_code} 质量因子失败：{e}")
                conn.close()
                continue

        if factors:
            return pd.DataFrame(factors)
        return pd.DataFrame()

    def calculate_growth_factors(
        self,
        stock_codes: List[str],
        date: str = None,
        lookback_days: int = 250
    ) -> pd.DataFrame:
        """
        计算成长因子

        Args:
            stock_codes: 股票代码列表
            date: 基准日期
            lookback_days: 回看天数（用于计算增速）

        Returns:
            成长因子 DataFrame
        """
        if date is None:
            date = self.db.get_latest_date()
            if date is None:
                return pd.DataFrame()

        start_date = (datetime.strptime(date, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")

        factors = []

        for ts_code in stock_codes:
            df = self.db.get_stock_data(ts_code, start_date, date)

            if len(df) < 60:
                continue

            df = df.sort_values('trade_date')

            # 成长因子：
            closes = df['close'].values

            # 1. 250 日涨幅（年成长）
            if len(closes) >= 250:
                growth_250d = (closes[-1] / closes[-250] - 1) * 100
            else:
                growth_250d = (closes[-1] / closes[0] - 1) * 100

            # 2. 120 日涨幅（半年成长）
            if len(closes) >= 120:
                growth_120d = (closes[-1] / closes[-120] - 1) * 100
            else:
                growth_120d = None

            # 3. 60 日涨幅（季度成长）
            if len(closes) >= 60:
                growth_60d = (closes[-1] / closes[-60] - 1) * 100
            else:
                growth_60d = None

            # 4. 成长加速度（近期 vs 远期）
            if growth_120d and growth_60d:
                # 近 60 日相对于前 60 日的增速变化
                prev_60d = (closes[-60] / closes[-120] - 1) * 100 if len(closes) >= 120 else 0
                growth_acceleration = growth_60d - prev_60d
            else:
                growth_acceleration = 0

            factors.append({
                'ts_code': ts_code,
                'trade_date': date,
                # 成长因子
                'growth_250d': growth_250d,
                'growth_120d': growth_120d,
                'growth_60d': growth_60d,
                'growth_acceleration': growth_acceleration,
            })

        if factors:
            return pd.DataFrame(factors)
        return pd.DataFrame()

    def calculate_composite_score(
        self,
        factors: pd.DataFrame,
        weights: Dict[str, float] = None
    ) -> pd.DataFrame:
        """
        计算综合因子得分

        Args:
            factors: 因子 DataFrame
            weights: 各因子权重

        Returns:
            包含综合得分的 DataFrame
        """
        if factors.empty:
            return factors

        df = factors.copy()

        # 默认权重
        if weights is None:
            weights = {
                'momentum': 0.30,
                'quality': 0.25,
                'growth': 0.25,
                'value': 0.10,
                'liquidity': 0.10,
            }

        # 归一化各因子（0-100 分）
        def normalize(series, ascending=True):
            series = series.fillna(series.median())
            min_val, max_val = series.min(), series.max()
            if max_val == min_val:
                return pd.Series(50.0, index=series.index)
            if ascending:
                return (series - min_val) / (max_val - min_val) * 100
            else:
                return (max_val - series) / (max_val - min_val) * 100

        # 动量得分（使用 250 日涨幅）
        if 'growth_250d' in df.columns:
            df['momentum_score'] = normalize(df['growth_250d'], ascending=True)
        else:
            df['momentum_score'] = 50.0

        # 质量得分（低波动 + 高上涨占比）
        if 'quality_volatility' in df.columns:
            df['quality_score'] = (
                normalize(df['quality_volatility'], ascending=False) * 0.5 +
                normalize(df['quality_up_ratio'], ascending=True) * 0.5
            )
        else:
            df['quality_score'] = 50.0

        # 成长得分（年成长 + 成长加速度）
        if 'growth_250d' in df.columns and 'growth_acceleration' in df.columns:
            df['growth_score'] = (
                normalize(df['growth_250d'], ascending=True) * 0.5 +
                normalize(df['growth_acceleration'], ascending=True) * 0.5
            )
        else:
            df['growth_score'] = 50.0

        # 估值得分（低 PE 得分高）
        if 'pe_ttm' in df.columns:
            df['value_score'] = normalize(df['pe_ttm'], ascending=False)
        else:
            df['value_score'] = 50.0

        # 综合得分
        df['composite_score'] = (
            weights['momentum'] * df['momentum_score'] +
            weights['quality'] * df['quality_score'] +
            weights['growth'] * df['growth_score'] +
            weights['value'] * df['value_score'] +
            weights['liquidity'] * 50.0  # 流动性默认中间分
        )

        return df

    def get_top_stocks_by_factors(
        self,
        stock_codes: List[str],
        top_n: int = 20,
        date: str = None
    ) -> pd.DataFrame:
        """
        基于多因子模型筛选优质股票

        Args:
            stock_codes: 候选股票列表
            top_n: 返回数量
            date: 基准日期

        Returns:
            TOP N 股票 DataFrame
        """
        # 计算质量因子
        quality_factors = self.calculate_quality_factors(stock_codes, date)

        if quality_factors.empty:
            logger.warning("质量因子计算结果为空")
            return pd.DataFrame()

        # 计算成长因子
        growth_factors = self.calculate_growth_factors(stock_codes, date)

        # 合并因子
        factors = quality_factors.merge(
            growth_factors,
            on=['ts_code', 'trade_date'],
            how='left'
        )

        # 计算综合得分
        factors = self.calculate_composite_score(factors)

        # 排序返回 TOP N
        result = factors.nlargest(top_n, 'composite_score')

        logger.info(f"筛选完成：{len(result)} 只股票")
        return result


def main():
    """测试函数"""
    print("=" * 70)
    print("多因子模型测试")
    print("=" * 70)

    # 从数据库获取成分股
    conn = sqlite3.connect('data/stock.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT stock_code FROM concept_constituent")
    stock_codes = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"\n成分股数量：{len(stock_codes)}")

    # 测试多因子模型
    mfm = MultiFactorModel()

    # 获取综合得分
    result = mfm.get_top_stocks_by_factors(stock_codes, top_n=20)

    if not result.empty:
        print("\n【多因子选股 TOP10】")
        cols = ['ts_code', 'composite_score', 'momentum_score', 'quality_score',
                'growth_score', 'value_score', 'growth_250d', 'pe_ttm']
        available_cols = [c for c in cols if c in result.columns]
        print(result[available_cols].head(10).to_string())

        print("\n【因子统计】")
        print(f"  综合得分：{result['composite_score'].mean():.1f} (均值)")
        if 'growth_250d' in result.columns:
            print(f"  250 日成长：{result['growth_250d'].mean():.1f}% (均值)")
        if 'pe_ttm' in result.columns:
            print(f"  PE(TTM): {result['pe_ttm'].mean():.1f} (均值)")
    else:
        print("结果为空")


if __name__ == "__main__":
    import sqlite3
    main()
