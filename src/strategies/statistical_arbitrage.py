#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统计套利策略模块
实现基于配对交易的统计套利策略

策略原理：
1. 寻找高度相关的股票对
2. 计算价差的均值和标准差
3. 当价差偏离均值超过 2 倍标准差时开仓
4. 当价差回归均值时平仓
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
from scipy import stats
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class PairsTradingStrategy:
    """
    配对交易策略

    功能：
    1. 寻找相关股票对
    2. 计算价差序列
    3. 生成交易信号
    4. 回测绩效
    """

    def __init__(
        self,
        lookback_period: int = 60,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        stop_loss: float = 3.0,
        hold_period: int = 10,
        min_correlation: float = 0.7
    ):
        """
        初始化策略参数

        Args:
            lookback_period: 回溯期（用于计算均值和标准差）
            entry_threshold: 开仓阈值（标准差倍数）
            exit_threshold: 平仓阈值（标准差倍数）
            stop_loss: 止损阈值（标准差倍数）
            hold_period: 最短持有期
            min_correlation: 最小相关系数
        """
        self.lookback_period = lookback_period
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.stop_loss = stop_loss
        self.hold_period = hold_period
        self.min_correlation = min_correlation

        logger.info(f"配对交易策略初始化：Lookback({lookback_period}), Entry({entry_threshold}σ), Exit({exit_threshold}σ)")

    def find_pairs(
        self,
        price_data: Dict[str, pd.DataFrame],
        min_correlation: float = None
    ) -> List[Tuple[str, str, float]]:
        """
        寻找高度相关的股票对

        Args:
            price_data: 字典 {ts_code: price_dataframe}
            min_correlation: 最小相关系数

        Returns:
            股票对列表 [(ts_code1, ts_code2, correlation)]
        """
        min_correlation = min_correlation or self.min_correlation

        # 计算收益率
        returns = {}
        for ts_code, df in price_data.items():
            if len(df) < self.lookback_period:
                continue
            returns[ts_code] = df['close'].pct_change().dropna()

        # 计算相关系数矩阵
        returns_df = pd.DataFrame(returns)
        corr_matrix = returns_df.corr()

        # 找出高度相关的股票对
        pairs = []
        tickers = list(returns.keys())

        for i in range(len(tickers)):
            for j in range(i + 1, len(tickers)):
                ticker1, ticker2 = tickers[i], tickers[j]
                corr = corr_matrix.loc[ticker1, ticker2]

                if abs(corr) >= min_correlation:
                    pairs.append((ticker1, ticker2, corr))

        # 按相关系数排序
        pairs.sort(key=lambda x: abs(x[2]), reverse=True)

        logger.info(f"找到 {len(pairs)} 个高度相关的股票对")
        return pairs

    def compute_spread(
        self,
        price1: pd.Series,
        price2: pd.Series,
        hedge_ratio: float = None
    ) -> Tuple[pd.Series, float]:
        """
        计算价差序列

        Args:
            price1: 股票 1 价格序列
            price2: 股票 2 价格序列
            hedge_ratio: 对冲比率（None 表示用 OLS 计算）

        Returns:
            (spread_series, hedge_ratio)
        """
        # 对齐数据
        aligned = pd.concat([price1, price2], axis=1).dropna()
        if len(aligned) < self.lookback_period:
            return pd.Series(), 0

        # 计算对冲比率
        if hedge_ratio is None:
            model = stats.linregress(aligned.iloc[:, 0], aligned.iloc[:, 1])
            hedge_ratio = model.slope

        # 计算价差
        spread = aligned.iloc[:, 0] - hedge_ratio * aligned.iloc[:, 1]

        return spread, hedge_ratio

    def generate_signals(
        self,
        spread: pd.Series,
        entry_threshold: float = None,
        exit_threshold: float = None
    ) -> pd.DataFrame:
        """
        生成交易信号

        Args:
            spread: 价差序列
            entry_threshold: 开仓阈值
            exit_threshold: 平仓阈值

        Returns:
            包含信号的 DataFrame
        """
        entry_threshold = entry_threshold or self.entry_threshold
        exit_threshold = exit_threshold or self.exit_threshold

        signals_df = pd.DataFrame({'spread': spread})

        # 计算滚动均值和标准差
        signals_df['rolling_mean'] = spread.rolling(window=self.lookback_period).mean()
        signals_df['rolling_std'] = spread.rolling(window=self.lookback_period).std()

        # 计算 Z 分数
        signals_df['z_score'] = (spread - signals_df['rolling_mean']) / signals_df['rolling_std']

        # 生成信号
        signals_df['signal'] = 0
        signals_df['position'] = 0

        # 开仓信号
        long_entry = signals_df['z_score'] < -entry_threshold  # 价差过低，做多价差
        short_entry = signals_df['z_score'] > entry_threshold   # 价差过高，做空价差

        # 平仓信号
        long_exit = (signals_df['z_score'] > -exit_threshold) & (signals_df['z_score'].shift(1) <= -exit_threshold)
        short_exit = (signals_df['z_score'] < exit_threshold) & (signals_df['z_score'].shift(1) >= exit_threshold)

        # 止损信号
        stop_loss_long = signals_df['z_score'] < -self.stop_loss
        stop_loss_short = signals_df['z_score'] > self.stop_loss

        signals_df.loc[long_entry, 'signal'] = 1   # 做多价差
        signals_df.loc[short_entry, 'signal'] = -1  # 做空价差
        signals_df.loc[long_exit, 'signal'] = 0
        signals_df.loc[short_exit, 'signal'] = 0
        signals_df.loc[stop_loss_long | stop_loss_short, 'signal'] = 0

        # 计算持仓
        position = 0
        for idx in signals_df.index:
            if signals_df.loc[idx, 'signal'] == 1:
                position = 1
            elif signals_df.loc[idx, 'signal'] == -1:
                position = -1
            elif signals_df.loc[idx, 'signal'] == 0 and position != 0:
                position = 0
            signals_df.loc[idx, 'position'] = position

        return signals_df

    def backtest(
        self,
        price_data: Dict[str, pd.DataFrame],
        pairs: List[Tuple[str, str, float]] = None,
        initial_capital: float = 1000000,
        capital_per_pair: float = 0.1,
        commission: float = 0.0003,
        slippage: float = 0.001
    ) -> Dict:
        """
        回测策略

        Args:
            price_data: 价格数据字典
            pairs: 股票对列表（None 表示自动寻找）
            initial_capital: 初始资金
            capital_per_pair: 每对股票分配资金比例
            commission: 手续费率
            slippage: 滑点

        Returns:
            回测结果
        """
        logger.info(f"开始回测配对交易策略...")

        # 自动寻找股票对
        if pairs is None:
            pairs = self.find_pairs(price_data)

        if not pairs:
            logger.warning("未找到符合条件的股票对")
            return self._empty_result()

        # 回测每个股票对
        all_trades = []
        all_pnl = []
        pair_results = []

        for ticker1, ticker2, corr in pairs[:10]:  # 最多回测前 10 对
            df1 = price_data[ticker1].copy()
            df2 = price_data[ticker2].copy()

            # 对齐日期
            common_dates = pd.merge(
                df1[['trade_date']],
                df2[['trade_date']],
                on='trade_date'
            )['trade_date'].unique()

            if len(common_dates) < self.lookback_period + 10:
                continue

            df1 = df1[df1['trade_date'].isin(common_dates)].reset_index(drop=True)
            df2 = df2[df2['trade_date'].isin(common_dates)].reset_index(drop=True)

            # 计算价差
            spread, hedge_ratio = self.compute_spread(
                df1['close'].reset_index(drop=True),
                df2['close'].reset_index(drop=True)
            )

            if len(spread) == 0:
                continue

            # 生成信号
            signals = self.generate_signals(spread)

            # 计算收益
            trades, pnl, pair_pnl = self._calculate_pair_returns(
                signals,
                df1,
                df2,
                hedge_ratio,
                initial_capital * capital_per_pair,
                commission,
                slippage
            )

            all_trades.extend(trades)
            all_pnl.extend(pnl)
            pair_results.append({
                'ticker1': ticker1,
                'ticker2': ticker2,
                'correlation': corr,
                'hedge_ratio': hedge_ratio,
                'total_pnl': pair_pnl,
                'trades': len(trades)
            })

        if not all_pnl:
            logger.warning("回测无结果")
            return self._empty_result()

        # 计算总体指标
        results = self._calculate_metrics(
            all_pnl=all_pnl,
            all_trades=all_trades,
            pair_results=pair_results,
            initial_capital=initial_capital
        )

        logger.info(f"回测完成：Sharpe={results['sharpe']:.2f}, Total Trades={results['total_trades']}")
        return results

    def _calculate_pair_returns(
        self,
        signals: pd.DataFrame,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        hedge_ratio: float,
        capital: float,
        commission: float,
        slippage: float
    ) -> Tuple[List[Dict], List[float], float]:
        """计算单个股票对的收益"""
        trades = []
        daily_pnl = []

        position = 0  # 1=做多价差，-1=做空价差
        entry_spread = 0
        shares1 = 0
        shares2 = 0
        entry_price1 = 0
        entry_price2 = 0
        total_pnl = 0

        for idx in signals.index:
            if idx == 0:
                continue

            signal = signals.loc[idx, 'signal']
            price1 = df1.loc[idx, 'close']
            price2 = df2.loc[idx, 'close']
            trade_date = df1.loc[idx, 'trade_date']
            spread = signals.loc[idx, 'spread']

            # 平仓
            if position != 0 and signal == 0:
                if position == 1:  # 平多
                    proceeds1 = shares1 * price1 * (1 - commission - slippage)
                    cost2 = shares2 * price2 * (1 + commission + slippage)
                    pnl = proceeds1 - cost2 - (shares1 * entry_price1 - shares2 * entry_price2)
                else:  # 平空
                    cost1 = shares1 * price1 * (1 + commission + slippage)
                    proceeds2 = shares2 * price2 * (1 - commission - slippage)
                    pnl = proceeds2 - cost1 - (shares2 * entry_price2 - shares1 * entry_price1)

                total_pnl += pnl
                daily_pnl.append(pnl)

                trades.append({
                    'trade_date': trade_date,
                    'type': 'close',
                    'ticker1': df1.loc[idx, 'ts_code'] if 'ts_code' in df1.columns else 'A',
                    'ticker2': df2.loc[idx, 'ts_code'] if 'ts_code' in df2.columns else 'B',
                    'pnl': pnl,
                    'entry_spread': entry_spread,
                    'exit_spread': spread
                })

                position = 0
                shares1 = 0
                shares2 = 0

            # 开仓
            elif position == 0 and signal != 0:
                if signal == 1:  # 做多价差：买入 1，卖空 2
                    shares1 = int(capital * 0.5 / price1 / 100) * 100
                    shares2 = int(shares1 * hedge_ratio / 100) * 100

                    if shares1 > 0 and shares2 > 0:
                        cost1 = shares1 * price1 * (1 + commission + slippage)
                        proceeds2 = shares2 * price2 * (1 - commission - slippage)

                        position = 1
                        entry_spread = spread
                        entry_price1 = price1
                        entry_price2 = price2
                        shares1 = shares1
                        shares2 = shares2

                else:  # 做空价差：卖空 1，买入 2
                    shares2 = int(capital * 0.5 / price2 / 100) * 100
                    shares1 = int(shares2 / hedge_ratio / 100) * 100 if hedge_ratio > 0 else 0

                    if shares1 > 0 and shares2 > 0:
                        proceeds1 = shares1 * price1 * (1 - commission - slippage)
                        cost2 = shares2 * price2 * (1 + commission + slippage)

                        position = -1
                        entry_spread = spread
                        entry_price1 = price1
                        entry_price2 = price2
                        shares1 = shares1
                        shares2 = shares2

            # 计算当日未实现盈亏
            if position != 0:
                if position == 1:
                    unrealized_pnl = (shares1 * price1 - shares2 * price2) - (shares1 * entry_price1 - shares2 * entry_price2)
                else:
                    unrealized_pnl = (shares2 * entry_price2 - shares1 * entry_price1) - (shares2 * price2 - shares1 * price1)
                daily_pnl.append(unrealized_pnl)

        return trades, daily_pnl, total_pnl

    def _calculate_metrics(
        self,
        all_pnl: List[float],
        all_trades: List[Dict],
        pair_results: List[Dict],
        initial_capital: float
    ) -> Dict:
        """计算回测指标"""
        if not all_pnl:
            return self._empty_result()

        pnl_series = pd.Series(all_pnl)

        # 累计收益
        total_pnl = sum(all_pnl)
        total_return = total_pnl / initial_capital

        # 年化
        days = len(all_pnl)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0

        # 波动率
        daily_vol = pnl_series.std()
        annual_vol = daily_vol * np.sqrt(252) if daily_vol > 0 else 0

        # 夏普比率
        sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0

        # 最大回撤
        cum_pnl = pnl_series.cumsum()
        cummax = cum_pnl.cummax()
        drawdown = (cum_pnl - cummax) / (initial_capital + cummax)
        max_drawdown = drawdown.min()

        # 胜率
        winning_trades = len([t for t in all_trades if t.get('pnl', 0) > 0])
        win_rate = winning_trades / len(all_trades) if all_trades else 0

        return {
            'sharpe': round(sharpe, 4),
            'total_return': round(total_return, 4),
            'annual_return': round(annual_return, 4),
            'annual_volatility': round(annual_vol, 4),
            'max_drawdown': round(max_drawdown, 4),
            'win_rate': round(win_rate, 4),
            'total_trades': len(all_trades),
            'total_pnl': round(total_pnl, 2),
            'pair_results': pair_results,
            'trades': all_trades
        }

    def _empty_result(self) -> Dict:
        """返回空结果"""
        return {
            'sharpe': 0,
            'total_return': 0,
            'annual_return': 0,
            'max_drawdown': 0,
            'win_rate': 0,
            'total_trades': 0,
            'total_pnl': 0,
            'pair_results': [],
            'trades': []
        }


def main():
    """测试函数 - 使用模拟数据"""
    print("=" * 90)
    print("配对交易策略测试")
    print("=" * 90)

    # 创建模拟数据 - 两只高度相关的股票
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=252, freq='B')

    # 股票 A：基础价格序列
    returns_a = np.random.randn(252) * 0.02
    price_a = 100 * np.cumprod(1 + returns_a)

    # 股票 B：与 A 高度相关
    returns_b = 0.8 * returns_a + 0.2 * np.random.randn(252) * 0.02
    price_b = 50 * np.cumprod(1 + returns_b)

    # 创建 DataFrame
    df_a = pd.DataFrame({
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'close': price_a,
        'ts_code': 'STOCK_A'
    })
    df_b = pd.DataFrame({
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'close': price_b,
        'ts_code': 'STOCK_B'
    })

    price_data = {
        'STOCK_A': df_a,
        'STOCK_B': df_b
    }

    print(f"\n模拟数据：2 只股票，252 个交易日")
    print(f"股票 A 价格范围：{price_a.min():.2f} - {price_a.max():.2f}")
    print(f"股票 B 价格范围：{price_b.min():.2f} - {price_b.max():.2f}")

    # 计算实际相关系数
    ret_a = df_a['close'].pct_change().dropna()
    ret_b = df_b['close'].pct_change().dropna()
    corr = ret_a.corr(ret_b)
    print(f"收益率相关系数：{corr:.4f}")

    # 创建策略
    strategy = PairsTradingStrategy(
        lookback_period=60,
        entry_threshold=2.0,
        exit_threshold=0.5,
        stop_loss=3.0
    )

    # 寻找股票对
    pairs = strategy.find_pairs(price_data, min_correlation=0.5)
    print(f"\n找到 {len(pairs)} 个股票对:")
    for p in pairs:
        print(f"  {p[0]} - {p[1]}: {p[2]:.4f}")

    # 回测
    print("\n开始回测...")
    results = strategy.backtest(
        price_data,
        initial_capital=1000000,
        capital_per_pair=0.5
    )

    # 打印结果
    print("\n" + "=" * 90)
    print("回测结果")
    print("=" * 90)
    print(f"初始资金：¥1,000,000")
    if results['total_trades'] > 0:
        print(f"总盈亏：¥{results['total_pnl']:,.2f}")
        print(f"总收益率：{results['total_return']*100:.2f}%")
        print(f"年化收益：{results['annual_return']*100:.2f}%")
        print(f"夏普比率：{results['sharpe']:.2f}")
        print(f"最大回撤：{results['max_drawdown']*100:.2f}%")
        print(f"胜率：{results['win_rate']*100:.2f}%")
        print(f"交易次数：{results['total_trades']}")
    else:
        print("无交易信号")
    print("=" * 90)


if __name__ == "__main__":
    main()
