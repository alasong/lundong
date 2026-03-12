"""
动量策略模块
实现基于动量因子的趋势跟踪策略
"""
import pandas as pd
import numpy as np
from typing import Dict, List
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MomentumStrategy:
    """
    动量策略

    功能：
    1. 价格动量 - 基于历史收益率排序
    2. 相对强度 - 相对市场的超额收益
    3. 动量反转 - 识别动量衰竭信号
    """

    def __init__(
        self,
        momentum_period: int = 20,
        reversal_period: int = 5,
        ma_short: int = 5,
        ma_long: int = 20,
        stop_loss: float = 0.08,
        take_profit: float = 0.15
    ):
        """
        初始化动量策略

        Args:
            momentum_period: 动量周期
            reversal_period: 反转信号周期
            ma_short: 短期均线
            ma_long: 长期均线
            stop_loss: 止损比例
            take_profit: 止盈比例
        """
        self.momentum_period = momentum_period
        self.reversal_period = reversal_period
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.stop_loss = stop_loss
        self.take_profit = take_profit

        logger.info(f"动量策略初始化：Momentum({momentum_period}), MA({ma_short}/{ma_long})")

    def compute_momentum(self, df: pd.DataFrame, period: int = None) -> pd.DataFrame:
        """计算动量指标"""
        period = period or self.momentum_period
        df = df.copy()
        df['momentum'] = df['close'].pct_change(periods=period)
        return df

    def compute_ma_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算均线交叉信号"""
        df = df.copy()
        df['ma_short'] = df['close'].rolling(window=self.ma_short).mean()
        df['ma_long'] = df['close'].rolling(window=self.ma_long).mean()

        df['ma_signal'] = 0
        # 金叉买入
        golden_cross = (df['ma_short'] > df['ma_long']) & (df['ma_short'].shift(1) <= df['ma_long'].shift(1))
        df.loc[golden_cross, 'ma_signal'] = 1
        # 死叉卖出
        death_cross = (df['ma_short'] < df['ma_long']) & (df['ma_short'].shift(1) >= df['ma_long'].shift(1))
        df.loc[death_cross, 'ma_signal'] = -1

        return df

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成综合交易信号"""
        df = self.compute_momentum(df)
        df = self.compute_ma_signals(df)

        df = df.copy()
        df['signal'] = 0

        # 动量为正且金叉时买入
        buy_condition = (df['momentum'] > 0) & (df['ma_signal'] == 1)
        df.loc[buy_condition, 'signal'] = 1

        # 动量为负且死叉时卖出
        sell_condition = (df['momentum'] < 0) & (df['ma_signal'] == -1)
        df.loc[sell_condition, 'signal'] = -1

        return df

    def backtest(
        self,
        df: pd.DataFrame,
        initial_capital: float = 1000000,
        position_size: float = 0.1,
        commission: float = 0.0003,
        slippage: float = 0.001
    ) -> Dict:
        """回测策略"""
        logger.info(f"开始回测动量策略...")

        df = self.generate_signals(df)

        capital = initial_capital
        position = 0
        trades = []
        portfolio_values = []

        for idx in df.index:
            if idx == 0:
                continue

            current_price = df.loc[idx, 'close']
            signal = df.loc[idx, 'signal']

            if signal == 1 and position == 0:
                buy_price = current_price * (1 + slippage)
                available_capital = capital * position_size
                shares = int(available_capital / buy_price / 100) * 100

                if shares > 0:
                    cost = shares * buy_price * (1 + commission)
                    if cost <= capital:
                        capital -= cost
                        position = shares
                        trades.append({
                            'trade_date': df.loc[idx, 'trade_date'],
                            'type': 'buy',
                            'price': buy_price,
                            'shares': shares
                        })

            elif signal == -1 and position > 0:
                sell_price = current_price * (1 - slippage)
                proceeds = position * sell_price * (1 - commission)
                capital += proceeds
                trades.append({
                    'trade_date': df.loc[idx, 'trade_date'],
                    'type': 'sell',
                    'price': sell_price,
                    'shares': position
                })
                position = 0

            portfolio_value = capital + position * current_price
            portfolio_values.append({
                'trade_date': df.loc[idx, 'trade_date'],
                'value': portfolio_value
            })

        results = self._calculate_metrics(initial_capital, trades, portfolio_values, df)
        logger.info(f"回测完成：Sharpe={results['sharpe']:.2f}")
        return results

    def _calculate_metrics(
        self,
        initial_capital: float,
        trades: List[Dict],
        portfolio_values: List[Dict],
        df: pd.DataFrame
    ) -> Dict:
        """计算回测指标"""
        if not portfolio_values:
            return {'sharpe': 0, 'total_return': 0}

        pv_df = pd.DataFrame(portfolio_values)
        pv_df['daily_return'] = pv_df['value'].pct_change().fillna(0)

        final_value = pv_df['value'].iloc[-1]
        total_return = (final_value - initial_capital) / initial_capital

        days = len(pv_df)
        annual_return = (1 + total_return) ** (252 / days) - 1
        daily_vol = pv_df['daily_return'].std()
        annual_vol = daily_vol * np.sqrt(252)

        sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0

        pv_df['cummax'] = pv_df['value'].cummax()
        pv_df['drawdown'] = (pv_df['value'] - pv_df['cummax']) / pv_df['cummax']
        max_drawdown = pv_df['drawdown'].min()

        return {
            'sharpe': round(sharpe, 4),
            'total_return': round(total_return, 4),
            'annual_return': round(annual_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'total_trades': len(trades),
            'final_value': round(final_value, 2)
        }
