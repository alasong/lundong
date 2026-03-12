#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
资金流策略
基于北向资金、龙虎榜、主力资金的流向分析

策略逻辑：
1. 北向资金流向（沪股通/深股通）
2. 龙虎榜资金流向
3. 主力资金流向（大单净额）
4. 资金流向强度指标
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import warnings
warnings.filterwarnings('ignore')


class CapitalFlowStrategy:
    """
    资金流策略

    因子：
    1. 北向资金流向
    2. 龙虎榜资金流向
    3. 主力资金流向
    4. 资金流向强度
    """

    def __init__(self, db=None):
        """
        初始化策略

        Args:
            db: 数据库实例
        """
        self.db = db
        logger.info("资金流策略初始化完成")

    def compute_northbound_flow(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算北向资金流向因子

        北向资金（沪深股通）是重要的外资流向指标

        Args:
            stock_data: 股票数据（包含 northbound_hold 北向持股）
            lookback: 计算窗口

        Returns:
            北向资金流向得分
        """
        # 检查是否有北向持股数据
        if 'northbound_hold' in stock_data.columns:
            northbound = stock_data['northbound_hold']
            # 计算北向资金变化
            northbound_change = northbound.diff()
            # 滚动求和（净流入）
            northbound_flow = northbound_change.rolling(lookback).sum()
            # 标准化
            flow_score = (northbound_flow - northbound_flow.mean()) / (northbound_flow.std() + 1e-8)
            return flow_score.fillna(0)

        # 如果没有北向数据，返回 0
        return pd.Series(0, index=stock_data.index)

    def compute_block_trade_flow(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算龙虎榜/大宗交易资金流向

        Args:
            stock_data: 股票数据（包含 block_trade_amount 大宗交易额）
            lookback: 计算窗口

        Returns:
            大宗资金流向得分
        """
        if 'block_trade_amount' in stock_data.columns:
            block_amount = stock_data['block_trade_amount']
            # 滚动平均
            block_flow = block_amount.rolling(lookback).mean()
            # 标准化
            flow_score = (block_flow - block_flow.mean()) / (block_flow.std() + 1e-8)
            return flow_score.fillna(0)

        return pd.Series(0, index=stock_data.index)

    def compute_main_force_flow(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算主力资金流向（大单净额）

        Args:
            stock_data: 股票数据（包含 main_force_net 主力净额）
            lookback: 计算窗口

        Returns:
            主力资金流向得分
        """
        if 'main_force_net' in stock_data.columns:
            main_force = stock_data['main_force_net']
            # 滚动求和（净流入）
            main_flow = main_force.rolling(lookback).sum()
            # 标准化
            flow_score = (main_flow - main_flow.mean()) / (main_flow.std() + 1e-8)
            return flow_score.fillna(0)

        return pd.Series(0, index=stock_data.index)

    def compute_flow_intensity(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算资金流向强度

        综合考虑成交额和价格变动

        Args:
            stock_data: 股票数据（包含 amount 成交额）
            lookback: 计算窗口

        Returns:
            资金流向强度得分
        """
        if 'amount' not in stock_data.columns or 'pct_chg' not in stock_data.columns:
            return pd.Series(0, index=stock_data.index)

        amount = stock_data['amount']
        pct_chg = stock_data['pct_chg']

        # 资金流强度 = 成交额 × 涨跌幅
        # 上涨放量 = 正向资金流
        # 下跌放量 = 负向资金流
        flow_intensity = amount * pct_chg / 1e8  # 缩放

        # 滚动平均
        intensity = flow_intensity.rolling(lookback).mean()

        # 标准化
        intensity_score = (intensity - intensity.mean()) / (intensity.std() + 1e-8)

        return intensity_score.fillna(0)

    def compute_volume_price_correlation(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算量价相关性因子

        量价齐升 = 健康上涨
        量价背离 = 可能的顶部/底部

        Args:
            stock_data: 股票数据
            lookback: 计算窗口

        Returns:
            量价相关性得分
        """
        if 'vol' not in stock_data.columns or 'pct_chg' not in stock_data.columns:
            return pd.Series(0, index=stock_data.index)

        volume = stock_data['vol']
        price_change = stock_data['pct_chg']

        # 滚动相关系数
        correlation = pd.Series(index=stock_data.index)
        for i in range(lookback - 1, len(stock_data)):
            start_idx = i - lookback + 1
            vol_segment = volume.iloc[start_idx:i + 1]
            price_segment = price_change.iloc[start_idx:i + 1]

            if vol_segment.std() > 0 and price_segment.std() > 0:
                corr = vol_segment.corr(price_segment)
                correlation.iloc[i] = corr if not np.isnan(corr) else 0
            else:
                correlation.iloc[i] = 0

        return correlation.fillna(0)

    def compute_all_flow_factors(
        self,
        stock_data: pd.DataFrame,
        params: Dict = None
    ) -> pd.DataFrame:
        """
        计算所有资金流因子

        Args:
            stock_data: 股票数据
            params: 参数配置

        Returns:
            因子得分 DataFrame
        """
        if params is None:
            params = {}

        lookback = params.get('lookback', 20)

        factors = {}

        # 北向资金因子
        factors['northbound'] = self.compute_northbound_flow(stock_data, lookback)

        # 龙虎榜/大宗因子
        factors['block_trade'] = self.compute_block_trade_flow(stock_data, lookback)

        # 主力资金因子
        factors['main_force'] = self.compute_main_force_flow(stock_data, lookback)

        # 资金流向强度
        factors['flow_intensity'] = self.compute_flow_intensity(stock_data, lookback)

        # 量价相关性
        factors['volume_price_corr'] = self.compute_volume_price_correlation(stock_data, lookback)

        return pd.DataFrame(factors)

    def compute_composite_score(
        self,
        factor_df: pd.DataFrame,
        weights: Dict[str, float] = None
    ) -> pd.Series:
        """
        计算综合资金流得分

        Args:
            factor_df: 因子得分 DataFrame
            weights: 因子权重

        Returns:
            综合得分
        """
        if weights is None:
            weights = {
                'northbound': 0.25,      # 北向资金 25%
                'block_trade': 0.15,     # 大宗交易 15%
                'main_force': 0.25,      # 主力资金 25%
                'flow_intensity': 0.20,  # 资金强度 20%
                'volume_price_corr': 0.15  # 量价相关 15%
            }

        composite_score = pd.Series(0, index=factor_df.index)

        for factor_name, weight in weights.items():
            if factor_name in factor_df.columns:
                composite_score += factor_df[factor_name] * weight

        return composite_score

    def select_stocks(
        self,
        stock_data: Dict[str, pd.DataFrame],
        params: Dict = None,
        top_n: int = 10
    ) -> List[str]:
        """
        选股

        Args:
            stock_data: 股票数据字典
            params: 参数配置
            top_n: 选股数量

        Returns:
            选中的股票代码列表
        """
        if params is None:
            params = {}

        scores = {}

        for ts_code, data in stock_data.items():
            if len(data) < params.get('min_history', 30):
                continue

            # 计算资金流因子
            factor_df = self.compute_all_flow_factors(data, params)

            # 计算综合得分
            composite_score = self.compute_composite_score(
                factor_df,
                weights=params.get('factor_weights')
            )

            # 使用最新得分
            scores[ts_code] = composite_score.iloc[-1]

        # 排序选股
        score_series = pd.Series(scores)
        selected = score_series.nlargest(top_n).index.tolist()

        logger.info(f"资金流选股完成：选中 {len(selected)} 只股票")

        return selected

    def backtest(
        self,
        stock_data: Dict[str, pd.DataFrame],
        params: Dict = None,
        top_n: int = 10,
        rebalance_freq: int = 20,
        start_date: str = None,
        end_date: str = None,
        initial_capital: float = 1000000,
        commission: float = 0.0015
    ) -> Dict:
        """
        回测

        Args:
            stock_data: 股票数据字典
            params: 参数配置
            top_n: 持仓股票数
            rebalance_freq: 调仓频率
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            commission: 手续费率

        Returns:
            回测结果
        """
        logger.info("开始资金流策略回测...")

        if params is None:
            params = {}

        # 获取所有交易日期
        all_dates = set()
        for data in stock_data.values():
            if 'trade_date' in data.columns:
                all_dates.update(data['trade_date'].astype(str).tolist())
        all_dates = sorted(list(all_dates))

        if not all_dates:
            return {'error': '无可用数据'}

        # 日期筛选
        if start_date:
            all_dates = [d for d in all_dates if d >= start_date]
        if end_date:
            all_dates = [d for d in all_dates if d <= end_date]

        # 初始化
        capital = initial_capital
        positions = {}
        portfolio_values = []
        dates = []

        # 准备数据索引
        data_by_date = {}
        for ts_code, data in stock_data.items():
            if 'trade_date' in data.columns:
                for _, row in data.iterrows():
                    date = str(row['trade_date'])
                    if date not in data_by_date:
                        data_by_date[date] = {}
                    data_by_date[date][ts_code] = data[data['trade_date'] == date].iloc[0]

        # 回测主循环
        for i, date in enumerate(all_dates):
            # 调仓日
            if i % rebalance_freq == 0:
                daily_data = data_by_date.get(date, {})

                if len(daily_data) < top_n:
                    continue

                # 选股
                stock_series = {}
                for ts_code in daily_data.keys():
                    full_data = stock_data.get(ts_code)
                    if full_data is not None:
                        hist = full_data[full_data['trade_date'].astype(str) <= date]
                        if len(hist) >= params.get('min_history', 30):
                            stock_series[ts_code] = hist

                if len(stock_series) < top_n:
                    continue

                selected = self.select_stocks(stock_series, params, top_n)

                # 卖出不在选股列表的股票
                for ts_code in list(positions.keys()):
                    if ts_code not in selected:
                        if ts_code in daily_data:
                            price = daily_data[ts_code].get('close', 0)
                            shares = positions.pop(ts_code)
                            capital += shares * price * (1 - commission)

                # 买入新股
                target_weight = 1.0 / len(selected) if selected else 0

                for ts_code in selected:
                    if ts_code in daily_data:
                        price = daily_data[ts_code].get('close', 0)
                        target_value = capital * target_weight
                        shares = int(target_value / price / 100) * 100

                        if ts_code in positions:
                            diff = shares - positions[ts_code]
                            if diff > 0:
                                capital -= diff * price * (1 + commission)
                            elif diff < 0:
                                capital += abs(diff) * price * (1 - commission)
                            positions[ts_code] = shares
                        else:
                            if capital > price * shares * (1 + commission):
                                capital -= shares * price * (1 + commission)
                                positions[ts_code] = shares

            # 计算组合市值
            market_value = 0
            for ts_code, shares in positions.items():
                if ts_code in data_by_date.get(date, {}):
                    price = data_by_date[date][ts_code].get('close', 0)
                    market_value += shares * price

            portfolio_value = capital + market_value
            portfolio_values.append(portfolio_value)
            dates.append(date)

        # 计算收益
        if len(portfolio_values) < 2:
            return {'error': '回测数据不足'}

        portfolio_values = pd.Series(portfolio_values, index=dates)
        returns = portfolio_values.pct_change().dropna()

        # 计算指标
        total_return = (portfolio_values.iloc[-1] / portfolio_values.iloc[0] - 1) * 100
        n_years = len(portfolio_values) / 252
        annual_return = ((portfolio_values.iloc[-1] / portfolio_values.iloc[0]) ** (1 / n_years) - 1) * 100
        volatility = returns.std() * np.sqrt(252)
        sharpe = annual_return / volatility if volatility > 0 else 0

        # 最大回撤
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.cummax()
        drawdown = (cum_returns - running_max) / running_max
        max_drawdown = drawdown.min() * 100

        result = {
            'total_return': round(total_return, 2),
            'annual_return': round(annual_return, 2),
            'volatility': round(volatility, 4),
            'sharpe': round(sharpe, 4),
            'max_drawdown': round(max_drawdown, 2),
            'n_trades': len([d for d in all_dates[::rebalance_freq]]),
            'returns': returns,
            'portfolio_values': portfolio_values
        }

        logger.info(f"回测完成：总收益={total_return:.2f}%, 年化={annual_return:.2f}%, "
                   f"Sharpe={sharpe:.3f}, 最大回撤={max_drawdown:.2f}%")

        return result


def main():
    """测试函数"""
    print("=" * 60)
    print("资金流策略测试")
    print("=" * 60)

    # 生成模拟数据
    np.random.seed(42)
    n_days = 300

    stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']

    stock_data = {}
    for code in stock_codes:
        dates = pd.date_range('2023-01-01', periods=n_days, freq='D')
        close = 10 * np.cumprod(1 + np.random.normal(0.001, 0.03, n_days))

        data = pd.DataFrame({
            'ts_code': code,
            'trade_date': dates.strftime('%Y%m%d'),
            'close': close,
            'pct_chg': np.random.normal(0, 2, n_days),
            'vol': np.random.uniform(1e5, 1e6, n_days),
            'amount': np.random.uniform(1e7, 1e8, n_days),
            # 模拟资金流数据
            'northbound_hold': np.cumsum(np.random.normal(1000, 5000, n_days)),
            'main_force_net': np.random.normal(1e6, 5e6, n_days)
        })
        stock_data[code] = data

    # 初始化策略
    strategy = CapitalFlowStrategy()

    # 计算资金流因子
    print("\n[1] 资金流因子计算")
    print("-" * 50)
    for code in stock_codes[:2]:
        factor_df = strategy.compute_all_flow_factors(stock_data[code])
        composite = strategy.compute_composite_score(factor_df)
        print(f"{code}: 资金流得分={composite.iloc[-1]:.4f}")

    # 选股测试
    print("\n[2] 选股测试")
    print("-" * 50)
    selected = strategy.select_stocks(stock_data, top_n=3)
    print(f"选中的股票：{selected}")

    # 回测
    print("\n[3] 回测测试")
    print("-" * 50)
    result = strategy.backtest(stock_data, top_n=3, rebalance_freq=20)

    if 'error' not in result:
        print(f"总收益率：{result['total_return']:.2f}%")
        print(f"年化收益率：{result['annual_return']:.2f}%")
        print(f"夏普比率：{result['sharpe']:.4f}")
        print(f"最大回撤：{result['max_drawdown']:.2f}%")

    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
