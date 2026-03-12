#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
均值回归策略模块
实现基于布林带和 RSI 的均值回归策略

策略原理：
1. 布林带策略：当价格触及下轨时买入，触及上轨时卖出
2. RSI 策略：当 RSI 进入超卖区时买入，进入超卖区时卖出
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MeanReversionStrategy:
    """
    均值回归策略

    功能：
    1. 布林带策略 - 价格触及下轨买入，触及上轨卖出
    2. RSI 策略 - RSI 超卖买入，超买卖出
    3. 双策略结合 - 综合信号生成
    """

    def __init__(
        self,
        bb_period: int = 20,
        bb_std: float = 2.0,
        rsi_period: int = 14,
        rsi_oversold: float = 30,
        rsi_overbought: float = 70,
        stop_loss: float = 0.08,
        take_profit: float = 0.15,
        use_bb: bool = True,
        use_rsi: bool = True,
        bb_weight: float = 0.5,
        rsi_weight: float = 0.5
    ):
        """
        初始化策略参数

        Args:
            bb_period: 布林带周期
            bb_std: 布林带标准差倍数
            rsi_period: RSI 周期
            rsi_oversold: RSI 超卖阈值
            rsi_overbought: RSI 超买阈值
            stop_loss: 止损比例
            take_profit: 止盈比例
            use_bb: 是否使用布林带策略
            use_rsi: 是否使用 RSI 策略
            bb_weight: 布林带信号权重
            rsi_weight: RSI 信号权重
        """
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.rsi_period = rsi_period
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.use_bb = use_bb
        self.use_rsi = use_rsi
        self.bb_weight = bb_weight
        self.rsi_weight = rsi_weight

        logger.info(f"均值回归策略初始化：BB({bb_period}, {bb_std}), RSI({rsi_period})")

    def compute_bollinger_bands(
        self,
        df: pd.DataFrame,
        period: int = None,
        std_dev: float = None
    ) -> pd.DataFrame:
        """
        计算布林带

        Args:
            df: 包含 close 列的 DataFrame
            period: 周期
            std_dev: 标准差倍数

        Returns:
            包含 boll_upper, boll_mid, boll_lower 的 DataFrame
        """
        period = period or self.bb_period
        std_dev = std_dev or self.bb_std

        df = df.copy()

        # 计算中轨（移动平均线）
        df['boll_mid'] = df['close'].rolling(window=period).mean()

        # 计算标准差
        rolling_std = df['close'].rolling(window=period).std()

        # 计算上下轨
        df['boll_upper'] = df['boll_mid'] + (std_dev * rolling_std)
        df['boll_lower'] = df['boll_mid'] - (std_dev * rolling_std)

        # 计算布林带带宽
        df['boll_bandwidth'] = (df['boll_upper'] - df['boll_lower']) / df['boll_mid'] * 100

        # 计算%B（价格在布林带中的位置）
        df['boll_pct'] = (df['close'] - df['boll_lower']) / (df['boll_upper'] - df['boll_lower'])

        return df

    def compute_rsi(
        self,
        df: pd.DataFrame,
        period: int = None
    ) -> pd.DataFrame:
        """
        计算 RSI 指标

        Args:
            df: 包含 close 列的 DataFrame
            period: RSI 周期

        Returns:
            包含 rsi 列的 DataFrame
        """
        period = period or self.rsi_period
        df = df.copy()

        # 计算价格变化
        delta = df['close'].diff()

        # 分离涨跌
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        # 计算平均涨跌
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()

        # 计算 RS 和 RSI
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # 填充 NaN
        df['rsi'] = df['rsi'].fillna(50)

        return df

    def generate_bb_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        生成布林带交易信号

        Args:
            df: 包含布林带数据的 DataFrame

        Returns:
            包含 bb_signal 列的 DataFrame（1=买入，-1=卖出，0=持有）
        """
        df = df.copy()
        df['bb_signal'] = 0

        # 当价格低于下轨时生成买入信号
        buy_condition = df['close'] < df['boll_lower']
        df.loc[buy_condition, 'bb_signal'] = 1

        # 当价格高于上轨时生成卖出信号
        sell_condition = df['close'] > df['boll_upper']
        df.loc[sell_condition, 'bb_signal'] = -1

        # 当价格穿越中轨时平仓
        cross_mid_buy = (df['close'] > df['boll_mid']) & (df['close'].shift(1) < df['boll_mid'].shift(1))
        cross_mid_sell = (df['close'] < df['boll_mid']) & (df['close'].shift(1) > df['boll_mid'].shift(1))

        return df

    def generate_rsi_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        生成 RSI 交易信号

        Args:
            df: 包含 RSI 数据的 DataFrame

        Returns:
            包含 rsi_signal 列的 DataFrame
        """
        df = df.copy()
        df['rsi_signal'] = 0

        # RSI 从超卖区回升时买入
        oversold_recovery = (
            (df['rsi'] < self.rsi_oversold) &
            (df['rsi'].shift(1) < self.rsi_oversold) &
            (df['rsi'] > df['rsi'].shift(1))
        )
        df.loc[oversold_recovery, 'rsi_signal'] = 1

        # RSI 从超买区回落时卖出
        overbought_decline = (
            (df['rsi'] > self.rsi_overbought) &
            (df['rsi'].shift(1) > self.rsi_overbought) &
            (df['rsi'] < df['rsi'].shift(1))
        )
        df.loc[overbought_decline, 'rsi_signal'] = -1

        # RSI 进入超卖区
        enter_oversold = (df['rsi'] < self.rsi_oversold) & (df['rsi'].shift(1) >= self.rsi_oversold)
        df.loc[enter_oversold, 'rsi_signal'] = 1

        # RSI 进入超买区
        enter_overbought = (df['rsi'] > self.rsi_overbought) & (df['rsi'].shift(1) <= self.rsi_overbought)
        df.loc[enter_overbought, 'rsi_signal'] = -1

        return df

    def generate_combined_signals(
        self,
        df: pd.DataFrame,
        min_bb_strength: float = 0.1,
        min_rsi_strength: float = 0.2
    ) -> pd.DataFrame:
        """
        生成综合交易信号

        Args:
            df: 包含指标数据的 DataFrame
            min_bb_strength: 最小布林带信号强度
            min_rsi_strength: 最小 RSI 信号强度

        Returns:
            包含 combined_signal 和 signal_strength 的 DataFrame
        """
        df = df.copy()

        # 计算各指标
        df = self.compute_bollinger_bands(df)
        df = self.compute_rsi(df)
        df = self.generate_bb_signals(df)
        df = self.generate_rsi_signals(df)

        # 综合信号
        df['combined_signal'] = 0
        df['signal_strength'] = 0.0

        for idx in df.index:
            signal_score = 0.0

            # 布林带信号
            if self.use_bb:
                bb_signal = df.loc[idx, 'bb_signal']
                bb_pct = df.loc[idx, 'boll_pct']

                if bb_signal == 1:  # 下轨买入
                    # 越接近下轨，信号越强
                    bb_strength = max(0, min_bb_strength + (0 - bb_pct))
                    signal_score += self.bb_weight * bb_strength
                elif bb_signal == -1:  # 上轨卖出
                    # 越接近上轨，信号越强
                    bb_strength = max(0, min_bb_strength + (bb_pct - 1))
                    signal_score -= self.bb_weight * bb_strength

            # RSI 信号
            if self.use_rsi:
                rsi_signal = df.loc[idx, 'rsi_signal']
                rsi_value = df.loc[idx, 'rsi']

                if rsi_signal == 1:  # 超卖买入
                    # RSI 越低，信号越强
                    rsi_strength = max(0, min_rsi_strength + (self.rsi_oversold - rsi_value) / 100)
                    signal_score += self.rsi_weight * rsi_strength
                elif rsi_signal == -1:  # 超买卖出
                    # RSI 越高，信号越强
                    rsi_strength = max(0, min_rsi_strength + (rsi_value - self.rsi_overbought) / 100)
                    signal_score -= self.rsi_weight * rsi_strength

            # 生成综合信号
            if signal_score > 0.15:
                df.loc[idx, 'combined_signal'] = 1  # 买入
            elif signal_score < -0.15:
                df.loc[idx, 'combined_signal'] = -1  # 卖出

            df.loc[idx, 'signal_strength'] = signal_score

        return df

    def backtest(
        self,
        df: pd.DataFrame,
        initial_capital: float = 1000000,
        position_size: float = 0.1,
        commission: float = 0.0003,
        slippage: float = 0.001
    ) -> Dict:
        """
        回测策略

        Args:
            df: 包含 OHLC 数据的 DataFrame
            initial_capital: 初始资金
            position_size: 单次交易仓位比例
            commission: 手续费率
            slippage: 滑点

        Returns:
            回测结果
        """
        logger.info(f"开始回测均值回归策略...")

        # 生成信号
        df = self.generate_combined_signals(df)

        # 初始化回测变量
        capital = initial_capital
        position = 0
        trades = []
        portfolio_values = []

        for idx in df.index:
            if idx == 0:
                continue

            current_price = df.loc[idx, 'close']
            signal = df.loc[idx, 'combined_signal']

            # 执行交易
            if signal == 1 and position == 0:  # 买入
                # 考虑滑点
                buy_price = current_price * (1 + slippage)
                # 计算可买数量
                available_capital = capital * position_size
                shares = int(available_capital / buy_price / 100) * 100  # 整百股

                if shares > 0:
                    cost = shares * buy_price * (1 + commission)
                    if cost <= capital:
                        capital -= cost
                        position = shares
                        trades.append({
                            'trade_date': df.loc[idx, 'trade_date'],
                            'type': 'buy',
                            'price': buy_price,
                            'shares': shares,
                            'cost': cost
                        })

            elif signal == -1 and position > 0:  # 卖出
                # 考虑滑点
                sell_price = current_price * (1 - slippage)
                proceeds = position * sell_price * (1 - commission)
                capital += proceeds

                trades.append({
                    'trade_date': df.loc[idx, 'trade_date'],
                    'type': 'sell',
                    'price': sell_price,
                    'shares': position,
                    'proceeds': proceeds
                })
                position = 0

            # 计算组合价值
            portfolio_value = capital + position * current_price
            portfolio_values.append({
                'trade_date': df.loc[idx, 'trade_date'],
                'value': portfolio_value,
                'capital': capital,
                'position_value': position * current_price
            })

        # 计算回测指标
        results = self._calculate_metrics(
            initial_capital=initial_capital,
            trades=trades,
            portfolio_values=portfolio_values,
            df=df
        )

        logger.info(f"回测完成：Sharpe={results['sharpe']:.2f}, Return={results['total_return']*100:.2f}%")

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
            return {'sharpe': 0, 'total_return': 0, 'max_drawdown': 0}

        pv_df = pd.DataFrame(portfolio_values)

        # 计算收益率
        pv_df['daily_return'] = pv_df['value'].pct_change().fillna(0)

        # 总收益率
        final_value = pv_df['value'].iloc[-1]
        total_return = (final_value - initial_capital) / initial_capital

        # 年化收益率
        days = len(pv_df)
        annual_return = (1 + total_return) ** (252 / days) - 1

        # 波动率
        daily_vol = pv_df['daily_return'].std()
        annual_vol = daily_vol * np.sqrt(252)

        # 夏普比率（假设无风险利率 3%）
        risk_free_rate = 0.03
        sharpe = (annual_return - risk_free_rate) / annual_vol if annual_vol > 0 else 0

        # 最大回撤
        pv_df['cummax'] = pv_df['value'].cummax()
        pv_df['drawdown'] = (pv_df['value'] - pv_df['cummax']) / pv_df['cummax']
        max_drawdown = pv_df['drawdown'].min()

        # 胜率
        buy_trades = [t for t in trades if t['type'] == 'buy']
        sell_trades = [t for t in trades if t['type'] == 'sell']

        winning_trades = 0
        total_trades = len(sell_trades)

        for i, sell in enumerate(sell_trades):
            if i < len(buy_trades):
                if sell['proceeds'] > buy_trades[i]['cost']:
                    winning_trades += 1

        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        # 交易次数
        total_trade_count = len(trades)

        return {
            'sharpe': round(sharpe, 4),
            'total_return': round(total_return, 4),
            'annual_return': round(annual_return, 4),
            'annual_volatility': round(annual_vol, 4),
            'max_drawdown': round(max_drawdown, 4),
            'win_rate': round(win_rate, 4),
            'total_trades': total_trade_count,
            'final_value': round(final_value, 2),
            'trades': trades,
            'portfolio_values': portfolio_values
        }

    def get_params(self) -> Dict:
        """获取策略参数"""
        return {
            'bb_period': self.bb_period,
            'bb_std': self.bb_std,
            'rsi_period': self.rsi_period,
            'rsi_oversold': self.rsi_oversold,
            'rsi_overbought': self.rsi_overbought,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'use_bb': self.use_bb,
            'use_rsi': self.use_rsi,
            'bb_weight': self.bb_weight,
            'rsi_weight': self.rsi_weight
        }

    def set_params(self, params: Dict):
        """设置策略参数"""
        for key, value in params.items():
            if hasattr(self, key):
                setattr(self, key, value)
        logger.info(f"策略参数已更新：{params}")


def main():
    """测试函数"""
    import sqlite3

    print("=" * 70)
    print("均值回归策略测试")
    print("=" * 70)

    # 从数据库加载测试数据
    conn = sqlite3.connect('data/stock.db')

    # 获取一个有足够数据的股票
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ts_code, COUNT(*) as cnt
        FROM stock_daily
        GROUP BY ts_code
        HAVING cnt > 100
        ORDER BY cnt DESC
        LIMIT 1
    """)
    result = cursor.fetchone()
    if result:
        ts_code = result[0]
        print(f"\n使用股票：{ts_code} ({result[1]} 条记录)")
    else:
        ts_code = '002049.SZ'  # 默认
        print(f"\n使用默认股票：{ts_code}")

    # 加载数据
    query = f"""
        SELECT trade_date, open, high, low, close, vol
        FROM stock_daily
        WHERE ts_code = '{ts_code}'
        ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"加载数据：{len(df)} 条记录")
    if not df.empty:
        print(f"日期范围：{df['trade_date'].min()} - {df['trade_date'].max()}")
    else:
        print("警告：未加载到数据")
        return

    # 创建策略
    strategy = MeanReversionStrategy(
        bb_period=20,
        bb_std=2.0,
        rsi_period=14,
        rsi_oversold=30,
        rsi_overbought=70
    )

    # 回测
    print("\n开始回测...")
    results = strategy.backtest(df, initial_capital=1000000, position_size=0.95)

    # 打印结果
    print("\n" + "=" * 70)
    print("回测结果")
    print("=" * 70)
    print(f"初始资金：¥1,000,000")
    if 'final_value' in results:
        print(f"最终价值：¥{results['final_value']:,.2f}")
        print(f"总收益率：{results['total_return']*100:.2f}%")
        print(f"年化收益：{results['annual_return']*100:.2f}%")
        print(f"夏普比率：{results['sharpe']:.2f}")
        print(f"最大回撤：{results['max_drawdown']*100:.2f}%")
        print(f"胜率：{results['win_rate']*100:.2f}%")
        print(f"交易次数：{results['total_trades']}")
    else:
        print("回测无交易或数据不足")
    print("=" * 70)

    # 显示部分交易记录
    if results['trades']:
        print("\n交易记录（前 10 条）:")
        for trade in results['trades'][:10]:
            date = trade.get('trade_date', 'N/A')
            type_ = trade.get('type', 'N/A')
            price = trade.get('price', 0)
            shares = trade.get('shares', 0)
            if type_ == 'buy':
                cost = trade.get('cost', 0)
                print(f"  {date} {type_.upper():4} {shares}股 @ ¥{price:.2f} (成本：¥{cost:,.2f})")
            else:
                proceeds = trade.get('proceeds', 0)
                print(f"  {date} {type_.upper():4} {shares}股 @ ¥{price:.2f} (收入：¥{proceeds:,.2f})")


if __name__ == "__main__":
    main()
