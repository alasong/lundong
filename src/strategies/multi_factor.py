#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多因子选股策略
基于 Barra 模型的多因子选股框架

因子类别：
1. 市值因子
2. 估值因子（PE/PB/PS）
3. 动量因子
4. 波动率因子
5. 流动性因子
6. 成长因子
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger
from scipy import stats
import warnings
warnings.filterwarnings('ignore')


class MultiFactorStrategy:
    """
    多因子选股策略

    因子体系：
    1. 市值因子 - 总市值
    2. 估值因子 - PE/PB/PS
    3. 动量因子 - 20 日/60 日动量
    4. 波动率因子 - 20 日波动率
    5. 流动性因子 - 平均换手率/成交额
    6. 成长因子 - 营收/利润增长率
    """

    def __init__(self, db=None):
        """
        初始化策略

        Args:
            db: 数据库实例
        """
        self.db = db
        self.factor_names = []
        self.factor_weights = {}
        self.factor_directions = {}  # 因子方向（+1=正向，-1=负向）

        # 默认因子权重（等权重）
        self.default_factors = {
            'size': {'weight': 0.15, 'direction': -1},      # 小市值因子
            'value': {'weight': 0.20, 'direction': -1},     # 低估值因子
            'momentum': {'weight': 0.20, 'direction': 1},   # 动量因子
            'volatility': {'weight': 0.15, 'direction': -1}, # 低波动因子
            'liquidity': {'weight': 0.15, 'direction': 1},  # 高流动性因子
            'growth': {'weight': 0.15, 'direction': 1}      # 高成长因子
        }

        logger.info("多因子选股策略初始化完成")

    def compute_size_factor(self, stock_data: pd.DataFrame) -> pd.Series:
        """
        计算市值因子

        Args:
            stock_data: 包含 total_mv 列的 DataFrame

        Returns:
            市值因子得分（越小得分越高）
        """
        if 'total_mv' not in stock_data.columns:
            return pd.Series(0, index=stock_data.index)

        # 使用市值的对数
        size = np.log(stock_data['total_mv'])

        # Z-Score 标准化
        size_zscore = (size - size.mean()) / (size.std() + 1e-8)

        # 反向（小市值得分高）
        return -size_zscore

    def compute_value_factor(self, stock_data: pd.DataFrame) -> pd.Series:
        """
        计算估值因子

        Args:
            stock_data: 包含 pe/pb/ps 列的 DataFrame

        Returns:
            估值因子得分
        """
        factors = []

        if 'pe' in stock_data.columns:
            pe = stock_data['pe'].replace([np.inf, -np.inf], np.nan)
            pe = pe.fillna(pe.median())
            factors.append((pe - pe.mean()) / (pe.std() + 1e-8))

        if 'pb' in stock_data.columns:
            pb = stock_data['pb'].replace([np.inf, -np.inf], np.nan)
            pb = pb.fillna(pb.median())
            factors.append((pb - pb.mean()) / (pb.std() + 1e-8))

        if 'ps' in stock_data.columns:
            ps = stock_data['ps'].replace([np.inf, -np.inf], np.nan)
            ps = ps.fillna(ps.median())
            factors.append((ps - ps.mean()) / (ps.std() + 1e-8))

        if len(factors) == 0:
            return pd.Series(0, index=stock_data.index)

        # 多因子等权平均
        value_factor = pd.concat(factors, axis=1).mean(axis=1)

        return -value_factor  # 反向（低估值得分高）

    def compute_momentum_factor(
        self,
        stock_data: pd.DataFrame,
        lookback_short: int = 20,
        lookback_long: int = 60
    ) -> pd.Series:
        """
        计算动量因子

        Args:
            stock_data: 包含 close 列的 DataFrame
            lookback_short: 短期窗口
            lookback_long: 长期窗口

        Returns:
            动量因子得分
        """
        if 'close' not in stock_data.columns or len(stock_data) < lookback_long:
            return pd.Series(0, index=stock_data.index)

        close = stock_data['close']

        # 短期动量
        mom_short = (close / close.shift(lookback_short) - 1) * 100

        # 长期动量
        mom_long = (close / close.shift(lookback_long) - 1) * 100

        # 综合动量（短期 + 长期）
        momentum = 0.5 * mom_short + 0.5 * mom_long

        # 标准化
        momentum = (momentum - momentum.mean()) / (momentum.std() + 1e-8)

        return momentum

    def compute_volatility_factor(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算波动率因子

        Args:
            stock_data: 包含 pct_chg 列的 DataFrame
            lookback: 计算窗口

        Returns:
            波动率因子得分
        """
        if 'pct_chg' not in stock_data.columns or len(stock_data) < lookback:
            return pd.Series(0, index=stock_data.index)

        # 滚动波动率
        volatility = stock_data['pct_chg'].rolling(lookback).std() * np.sqrt(252)

        # 标准化
        volatility = (volatility - volatility.mean()) / (volatility.std() + 1e-8)

        return -volatility  # 反向（低波动得分高）

    def compute_liquidity_factor(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 20
    ) -> pd.Series:
        """
        计算流动性因子

        Args:
            stock_data: 包含 turnover_rate/amount 列的 DataFrame
            lookback: 计算窗口

        Returns:
            流动性因子得分
        """
        factors = []

        if 'turnover_rate' in stock_data.columns:
            turnover = stock_data['turnover_rate'].rolling(lookback).mean()
            turnover = (turnover - turnover.mean()) / (turnover.std() + 1e-8)
            factors.append(turnover)

        if 'amount' in stock_data.columns:
            amount = stock_data['amount'].rolling(lookback).mean()
            amount = (amount - amount.mean()) / (amount.std() + 1e-8)
            factors.append(amount)

        if len(factors) == 0:
            return pd.Series(0, index=stock_data.index)

        # 多因子等权平均
        liquidity = pd.concat(factors, axis=1).mean(axis=1)

        return liquidity

    def compute_growth_factor(
        self,
        stock_data: pd.DataFrame,
        lookback: int = 60
    ) -> pd.Series:
        """
        计算成长因子（使用价格变动代理）

        Args:
            stock_data: 包含 close 列的 DataFrame
            lookback: 计算窗口

        Returns:
            成长因子得分
        """
        if 'close' not in stock_data.columns or len(stock_data) < lookback:
            return pd.Series(0, index=stock_data.index)

        # 使用价格增长代理成长
        close = stock_data['close']
        growth = (close / close.shift(lookback) - 1) * 100

        # 标准化
        growth = (growth - growth.mean()) / (growth.std() + 1e-8)

        return growth

    def compute_all_factors(
        self,
        stock_data: pd.DataFrame,
        params: Dict = None
    ) -> pd.DataFrame:
        """
        计算所有因子

        Args:
            stock_data: 股票数据
            params: 参数配置

        Returns:
            因子得分 DataFrame
        """
        if params is None:
            params = {}

        factors = {}

        # 市值因子
        factors['size'] = self.compute_size_factor(stock_data)

        # 估值因子
        factors['value'] = self.compute_value_factor(stock_data)

        # 动量因子
        factors['momentum'] = self.compute_momentum_factor(
            stock_data,
            lookback_short=params.get('lookback_short', 20),
            lookback_long=params.get('lookback_long', 60)
        )

        # 波动率因子
        factors['volatility'] = self.compute_volatility_factor(
            stock_data,
            lookback=params.get('volatility_lookback', 20)
        )

        # 流动性因子
        factors['liquidity'] = self.compute_liquidity_factor(
            stock_data,
            lookback=params.get('liquidity_lookback', 20)
        )

        # 成长因子
        factors['growth'] = self.compute_growth_factor(
            stock_data,
            lookback=params.get('growth_lookback', 60)
        )

        # 转为 DataFrame
        factor_df = pd.DataFrame(factors)

        # 填充缺失值
        factor_df = factor_df.fillna(0)

        return factor_df

    def compute_composite_score(
        self,
        factor_df: pd.DataFrame,
        weights: Dict[str, float] = None
    ) -> pd.Series:
        """
        计算综合因子得分

        Args:
            factor_df: 因子得分 DataFrame
            weights: 因子权重

        Returns:
            综合得分
        """
        if weights is None:
            weights = {name: info['weight'] for name, info in self.default_factors.items()}

        # 加权求和
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
            stock_data: 股票数据字典 {ts_code: DataFrame}
            params: 参数配置
            top_n: 选股数量

        Returns:
            选中的股票代码列表
        """
        if params is None:
            params = {}

        scores = {}

        for ts_code, data in stock_data.items():
            if len(data) < params.get('min_history', 60):
                continue

            # 计算因子
            factor_df = self.compute_all_factors(data, params)

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

        logger.info(f"选股完成：选中 {len(selected)} 只股票")

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
            rebalance_freq: 调仓频率（交易日）
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            commission: 手续费率

        Returns:
            回测结果
        """
        logger.info("开始多因子策略回测...")

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
        positions = {}  # {ts_code: shares}
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

        turnover_list = []

        # 回测主循环
        for i, date in enumerate(all_dates):
            # 调仓日
            if i % rebalance_freq == 0:
                # 获取当日数据
                daily_data = data_by_date.get(date, {})

                if len(daily_data) < top_n:
                    continue

                # 选股
                # 转换为时间序列数据
                stock_series = {}
                for ts_code, row in daily_data.items():
                    # 获取该股票的完整历史数据用于计算因子
                    full_data = stock_data.get(ts_code)
                    if full_data is not None:
                        # 截取到当前日期的数据
                        hist = full_data[full_data['trade_date'].astype(str) <= date]
                        if len(hist) >= params.get('min_history', 60):
                            stock_series[ts_code] = hist

                if len(stock_series) < top_n:
                    continue

                selected = self.select_stocks(stock_series, params, top_n)

                # 卖出不在选股列表的股票
                for ts_code in list(positions.keys()):
                    if ts_code not in selected:
                        # 卖出
                        if ts_code in daily_data:
                            price = daily_data[ts_code].get('close', 0)
                            shares = positions.pop(ts_code)
                            capital += shares * price * (1 - commission)

                # 买入新股（等权重）
                target_weight = 1.0 / len(selected) if selected else 0
                n_selected = len(selected)

                for ts_code in selected:
                    if ts_code in daily_data:
                        price = daily_data[ts_code].get('close', 0)
                        target_value = capital * target_weight
                        shares = int(target_value / price / 100) * 100  # 整百股

                        if ts_code in positions:
                            # 调整持仓
                            diff = shares - positions[ts_code]
                            if diff > 0:
                                capital -= diff * price * (1 + commission)
                            elif diff < 0:
                                capital += abs(diff) * price * (1 - commission)
                            positions[ts_code] = shares
                        else:
                            # 新建仓
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

        # 计算换手率
        if turnover_list:
            avg_turnover = np.mean(turnover_list)
        else:
            avg_turnover = rebalance_freq * top_n / len(positions) if positions else 0

        result = {
            'total_return': round(total_return, 2),
            'annual_return': round(annual_return, 2),
            'volatility': round(volatility, 4),
            'sharpe': round(sharpe, 4),
            'max_drawdown': round(max_drawdown, 2),
            'n_trades': len([d for d in all_dates[::rebalance_freq]]),
            'avg_turnover': round(avg_turnover, 4),
            'returns': returns,
            'portfolio_values': portfolio_values
        }

        logger.info(f"回测完成：总收益={total_return:.2f}%, 年化={annual_return:.2f}%, "
                   f"Sharpe={sharpe:.3f}, 最大回撤={max_drawdown:.2f}%")

        return result


def main():
    """测试函数"""
    print("=" * 60)
    print("多因子选股策略测试")
    print("=" * 60)

    # 生成模拟数据
    np.random.seed(42)
    n_days = 500

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
            'total_mv': np.random.uniform(100, 1000, n_days) * 1e8,
            'pe': np.random.uniform(10, 50, n_days),
            'pb': np.random.uniform(1, 5, n_days),
            'ps': np.random.uniform(2, 10, n_days),
            'turnover_rate': np.random.uniform(1, 10, n_days),
            'amount': np.random.uniform(1e8, 1e9, n_days)
        })
        stock_data[code] = data

    # 初始化策略
    strategy = MultiFactorStrategy()

    # 计算因子
    print("\n[1] 因子计算测试")
    print("-" * 50)
    for code in stock_codes[:2]:
        factor_df = strategy.compute_all_factors(stock_data[code])
        composite = strategy.compute_composite_score(factor_df)
        print(f"{code}: 综合得分={composite.iloc[-1]:.4f}")

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
        print(f"交易次数：{result['n_trades']}")

    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
