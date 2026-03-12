#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略回测框架
统一管理和回测多个策略
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import sqlite3
from typing import Dict, List, Optional, Callable
from datetime import datetime
from loguru import logger

from strategies.mean_reversion import MeanReversionStrategy
from strategies.momentum import MomentumStrategy
from strategies.event_driven import EventDrivenStrategy


class StrategyBacktester:
    """
    策略回测框架

    功能：
    1. 多策略回测对比
    2. 参数优化
    3. 绩效分析
    4. 报告生成
    """

    def __init__(self, db_path: str = 'data/stock.db'):
        """
        初始化回测框架

        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.strategies = {}
        self.results = {}

        logger.info("策略回测框架初始化完成")

    def register_strategy(self, name: str, strategy):
        """
        注册策略

        Args:
            name: 策略名称
            strategy: 策略实例
        """
        self.strategies[name] = strategy
        logger.info(f"注册策略：{name}")

    def load_data(
        self,
        ts_code: str,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        从数据库加载股票数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with OHLCV data
        """
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT trade_date, open, high, low, close, vol
            FROM stock_daily
            WHERE ts_code = ?
        """
        params = [ts_code]

        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)

        query += " ORDER BY trade_date"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        logger.info(f"加载数据：{ts_code}, {len(df)} 条记录")
        return df

    def backtest_strategy(
        self,
        strategy_name: str,
        ts_code: str,
        initial_capital: float = 1000000,
        position_size: float = 0.95,
        start_date: str = None,
        end_date: str = None
    ) -> Dict:
        """
        回测单个策略

        Args:
            strategy_name: 策略名称
            ts_code: 股票代码
            initial_capital: 初始资金
            position_size: 仓位比例
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            回测结果
        """
        if strategy_name not in self.strategies:
            logger.error(f"策略 {strategy_name} 未注册")
            return {}

        strategy = self.strategies[strategy_name]
        df = self.load_data(ts_code, start_date, end_date)

        if df.empty:
            logger.warning(f"股票 {ts_code} 无数据")
            return {}

        # 执行回测
        results = strategy.backtest(
            df,
            initial_capital=initial_capital,
            position_size=position_size
        )

        # 添加元数据
        results['strategy_name'] = strategy_name
        results['ts_code'] = ts_code
        results['start_date'] = start_date or df['trade_date'].min()
        results['end_date'] = end_date or df['trade_date'].max()
        results['data_points'] = len(df)

        self.results[strategy_name] = results

        return results

    def backtest_all_strategies(
        self,
        ts_code: str,
        initial_capital: float = 1000000,
        position_size: float = 0.95
    ) -> Dict:
        """
        回测所有已注册策略

        Args:
            ts_code: 股票代码
            initial_capital: 初始资金
            position_size: 仓位比例

        Returns:
            所有策略的回测结果
        """
        all_results = {}

        for name in self.strategies:
            logger.info(f"回测策略：{name}")
            results = self.backtest_strategy(
                name,
                ts_code,
                initial_capital,
                position_size
            )
            all_results[name] = results

        return all_results

    def compare_strategies(self, results: Dict = None) -> pd.DataFrame:
        """
        对比策略表现

        Args:
            results: 回测结果字典

        Returns:
            对比 DataFrame
        """
        if results is None:
            results = self.results

        comparison_data = []

        for name, data in results.items():
            if not data:
                continue

            comparison_data.append({
                '策略名称': name,
                '总收益率 (%)': round(data.get('total_return', 0) * 100, 2),
                '年化收益 (%)': round(data.get('annual_return', 0) * 100, 2),
                '夏普比率': round(data.get('sharpe', 0), 4),
                '最大回撤 (%)': round(data.get('max_drawdown', 0) * 100, 2),
                '胜率 (%)': round(data.get('win_rate', 0) * 100, 2),
                '交易次数': data.get('total_trades', 0),
                '最终价值': data.get('final_value', 0)
            })

        df = pd.DataFrame(comparison_data)
        df = df.sort_values('夏普比率', ascending=False)

        return df

    def print_report(self, results: Dict = None):
        """打印回测报告"""
        if results is None:
            results = self.results

        print("\n" + "=" * 90)
        print("策略回测对比报告")
        print("=" * 90)

        df = self.compare_strategies(results)

        if df.empty:
            print("无回测数据")
            return

        # 打印表格
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(df.to_string(index=False))

        print("\n" + "=" * 90)

        # 找出最优策略
        if not df.empty:
            best_sharpe = df.iloc[0]
            best_return = df.loc[df['总收益率 (%)'].idxmax()]

            print(f"\n最优夏普比率：{best_sharpe['策略名称']} ({best_sharpe['夏普比率']:.4f})")
            print(f"最高收益：{best_return['策略名称']} ({best_return['总收益率 (%)']:.2f}%)")

        print("=" * 90)


def main():
    """测试函数"""
    print("=" * 90)
    print("策略回测框架测试")
    print("=" * 90)

    # 创建回测框架
    backtester = StrategyBacktester()

    # 注册策略
    backtester.register_strategy('均值回归', MeanReversionStrategy(
        bb_period=20,
        bb_std=2.0,
        rsi_period=14,
        rsi_oversold=30,
        rsi_overbought=70
    ))

    backtester.register_strategy('动量策略', MomentumStrategy(
        momentum_period=20,
        ma_short=5,
        ma_long=20
    ))

    backtester.register_strategy('事件驱动', EventDrivenStrategy(
        hold_period=5,
        stop_loss=0.08,
        take_profit=0.15
    ))

    # 获取一个有数据的股票
    conn = sqlite3.connect('data/stock.db')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ts_code, COUNT(*) as cnt
        FROM stock_daily
        GROUP BY ts_code
        HAVING cnt > 200
        ORDER BY cnt DESC
        LIMIT 3
    """)
    top_stocks = cursor.fetchall()
    conn.close()

    if not top_stocks:
        print("数据库中无符合条件的股票数据")
        return

    print(f"\n回测股票：{top_stocks[0][0]} ({top_stocks[0][1]} 条记录)")

    # 回测所有策略
    results = backtester.backtest_all_strategies(
        ts_code=top_stocks[0][0],
        initial_capital=1000000,
        position_size=0.95
    )

    # 打印报告
    backtester.print_report(results)

    # 如果有多个股票，进行对比
    if len(top_stocks) > 1:
        print("\n" + "=" * 90)
        print("多股票回测对比")
        print("=" * 90)

        all_stock_results = {}
        for ts_code, count in top_stocks[:3]:
            print(f"\n回测股票：{ts_code} ({count} 条记录)")
            results = backtester.backtest_all_strategies(ts_code)
            all_stock_results[ts_code] = results

            df = backtester.compare_strategies(results)
            print(f"\n{ts_code} 策略对比:")
            print(df.to_string(index=False))


if __name__ == "__main__":
    main()
