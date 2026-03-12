#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
事件驱动策略模块
实现基于财报、公告、调研等事件的策略

策略原理：
1. 财报事件 - 超预期财报买入，低于预期卖出
2. 公告事件 - 重大利好公告买入，利空公告卖出
3. 调研事件 - 机构调研活跃度买入信号
4. 高管增减持 - 高管增持买入，减持卖出
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class EventDrivenStrategy:
    """
    事件驱动策略

    功能：
    1. 财报事件策略 - 财报超预期买入
    2. 公告事件策略 - 重大公告买入
    3. 调研事件策略 - 机构调研买入
    4. 高管增减持策略 - 高管增持买入
    """

    def __init__(
        self,
        hold_period: int = 5,
        stop_loss: float = 0.08,
        take_profit: float = 0.15,
        earnings_weight: float = 0.4,
        announcement_weight: float = 0.3,
        survey_weight: float = 0.2,
        insider_weight: float = 0.1
    ):
        """
        初始化策略参数

        Args:
            hold_period: 持有期（交易日）
            stop_loss: 止损比例
            take_profit: 止盈比例
            earnings_weight: 财报事件权重
            announcement_weight: 公告事件权重
            survey_weight: 调研事件权重
            insider_weight: 高管增减持权重
        """
        self.hold_period = hold_period
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.earnings_weight = earnings_weight
        self.announcement_weight = announcement_weight
        self.survey_weight = survey_weight
        self.insider_weight = insider_weight

        logger.info(f"事件驱动策略初始化：Hold({hold_period}d), Weights(E:{earnings_weight}, A:{announcement_weight}, S:{survey_weight}, I:{insider_weight})")

    def process_earnings_events(
        self,
        earnings_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        处理财报事件

        Args:
            earnings_data: 财报数据 (ts_code, announce_date, report_type, revenue_growth, profit_growth, eps, eps_estimate)
            price_data: 价格数据

        Returns:
            包含 earnings_signal 的 DataFrame
        """
        if earnings_data is None or earnings_data.empty:
            price_data = price_data.copy()
            price_data['earnings_signal'] = 0
            return price_data

        price_data = price_data.copy()
        price_data['earnings_signal'] = 0
        price_data['earnings_score'] = 0.0

        for _, event in earnings_data.iterrows():
            announce_date = event.get('announce_date')
            if not announce_date:
                continue

            # 计算超预期分数
            score = 0.0

            # 营收超预期
            revenue_growth = event.get('revenue_growth', 0)
            if revenue_growth > 0.2:  # 增长超过 20%
                score += 0.4
            elif revenue_growth > 0.1:
                score += 0.2

            # 利润超预期
            profit_growth = event.get('profit_growth', 0)
            if profit_growth > 0.3:  # 增长超过 30%
                score += 0.4
            elif profit_growth > 0.15:
                score += 0.2

            # EPS 超预期
            eps = event.get('eps', 0)
            eps_estimate = event.get('eps_estimate', eps)
            if eps_estimate > 0:
                eps_surprise = (eps - eps_estimate) / eps_estimate
                if eps_surprise > 0.1:
                    score += 0.2
                elif eps_surprise > 0.05:
                    score += 0.1

            # 在公告日后一天买入
            mask = price_data['trade_date'] >= announce_date
            if mask.any():
                first_idx = price_data[mask].index[0]
                price_data.loc[first_idx:first_idx + self.hold_period, 'earnings_signal'] = 1 if score > 0.5 else 0
                price_data.loc[first_idx:first_idx + self.hold_period, 'earnings_score'] = score

        return price_data

    def process_announcement_events(
        self,
        announcement_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        处理公告事件

        Args:
            announcement_data: 公告数据 (ts_code, announce_date, type, sentiment)
            price_data: 价格数据

        Returns:
            包含 announcement_signal 的 DataFrame
        """
        if announcement_data is None or announcement_data.empty:
            price_data = price_data.copy()
            price_data['announcement_signal'] = 0
            return price_data

        price_data = price_data.copy()
        price_data['announcement_signal'] = 0
        price_data['announcement_score'] = 0.0

        # 利好公告类型
        positive_types = ['contract', 'acquisition', 'product_approval', 'upgrade', 'partnership']
        negative_types = ['investigation', 'penalty', 'litigation', 'downgrade']

        for _, event in announcement_data.iterrows():
            announce_date = event.get('announce_date')
            if not announce_date:
                continue

            event_type = event.get('type', '')
            sentiment = event.get('sentiment', 0)

            # 计算信号方向
            if event_type in positive_types or sentiment > 0.5:
                signal = 1
                score = abs(sentiment) if sentiment != 0 else 0.5
            elif event_type in negative_types or sentiment < -0.5:
                signal = -1
                score = abs(sentiment) if sentiment != 0 else 0.5
            else:
                signal = 0
                score = 0

            mask = price_data['trade_date'] >= announce_date
            if mask.any():
                first_idx = price_data[mask].index[0]
                price_data.loc[first_idx:first_idx + self.hold_period, 'announcement_signal'] = signal
                price_data.loc[first_idx:first_idx + self.hold_period, 'announcement_score'] = score

        return price_data

    def process_survey_events(
        self,
        survey_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        处理调研事件

        Args:
            survey_data: 调研数据 (ts_code, survey_date, institution_count, institution_type)
            price_data: 价格数据

        Returns:
            包含 survey_signal 的 DataFrame
        """
        if survey_data is None or survey_data.empty:
            price_data = price_data.copy()
            price_data['survey_signal'] = 0
            return price_data

        price_data = price_data.copy()
        price_data['survey_signal'] = 0
        price_data['survey_score'] = 0.0

        for _, event in survey_data.iterrows():
            survey_date = event.get('survey_date')
            if not survey_date:
                continue

            institution_count = event.get('institution_count', 0)
            institution_type = event.get('institution_type', '')

            # 计算调研强度分数
            score = 0.0
            if institution_count >= 10:
                score = 1.0
            elif institution_count >= 5:
                score = 0.7
            elif institution_count >= 3:
                score = 0.5
            elif institution_count >= 1:
                score = 0.3

            # 知名机构加成
            if 'fund' in str(institution_type).lower() or 'insurance' in str(institution_type).lower():
                score = min(1.0, score + 0.2)

            # 调研后买入
            mask = price_data['trade_date'] >= survey_date
            if mask.any():
                first_idx = price_data[mask].index[0]
                if score >= 0.5:
                    price_data.loc[first_idx:first_idx + self.hold_period, 'survey_signal'] = 1
                price_data.loc[first_idx:first_idx + self.hold_period, 'survey_score'] = score

        return price_data

    def process_insider_events(
        self,
        insider_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        处理高管增减持事件

        Args:
            insider_data: 增减持数据 (ts_code, trade_date, insider_name, change_type, change_ratio)
            price_data: 价格数据

        Returns:
            包含 insider_signal 的 DataFrame
        """
        if insider_data is None or insider_data.empty:
            price_data = price_data.copy()
            price_data['insider_signal'] = 0
            return price_data

        price_data = price_data.copy()
        price_data['insider_signal'] = 0
        price_data['insider_score'] = 0.0

        for _, event in insider_data.iterrows():
            trade_date = event.get('trade_date')
            if not trade_date:
                continue

            change_type = event.get('change_type', '')
            change_ratio = event.get('change_ratio', 0)

            # 增持股数占比
            if change_type == '增持':
                signal = 1
                score = min(1.0, change_ratio * 10)  # 增持 0.1% 以上得满分
            elif change_type == '减持':
                signal = -1
                score = min(1.0, change_ratio * 10)
            else:
                signal = 0
                score = 0

            mask = price_data['trade_date'] >= trade_date
            if mask.any():
                first_idx = price_data[mask].index[0]
                price_data.loc[first_idx:first_idx + self.hold_period, 'insider_signal'] = signal
                price_data.loc[first_idx:first_idx + self.hold_period, 'insider_score'] = score

        return price_data

    def generate_combined_signals(
        self,
        price_data: pd.DataFrame,
        earnings_data: pd.DataFrame = None,
        announcement_data: pd.DataFrame = None,
        survey_data: pd.DataFrame = None,
        insider_data: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        生成综合事件信号

        Args:
            price_data: 价格数据
            earnings_data: 财报事件数据
            announcement_data: 公告事件数据
            survey_data: 调研事件数据
            insider_data: 高管增减持数据

        Returns:
            包含 combined_signal 和 signal_strength 的 DataFrame
        """
        # 处理各类型事件
        df = self.process_earnings_events(earnings_data, price_data)
        df = self.process_announcement_events(announcement_data, df)
        df = self.process_survey_events(survey_data, df)
        df = self.process_insider_events(insider_data, df)

        # 计算综合信号
        df['combined_signal'] = 0
        df['signal_strength'] = 0.0

        for idx in df.index:
            signal_score = 0.0

            # 财报事件
            if 'earnings_signal' in df.columns and 'earnings_score' in df.columns:
                earnings_sig = df.loc[idx, 'earnings_signal']
                earnings_score = df.loc[idx, 'earnings_score']
                signal_score += self.earnings_weight * earnings_sig * earnings_score

            # 公告事件
            if 'announcement_signal' in df.columns and 'announcement_score' in df.columns:
                announce_sig = df.loc[idx, 'announcement_signal']
                announce_score = df.loc[idx, 'announcement_score']
                signal_score += self.announcement_weight * announce_sig * announce_score

            # 调研事件
            if 'survey_signal' in df.columns and 'survey_score' in df.columns:
                survey_sig = df.loc[idx, 'survey_signal']
                survey_score = df.loc[idx, 'survey_score']
                signal_score += self.survey_weight * survey_sig * survey_score

            # 高管增减持
            if 'insider_signal' in df.columns and 'insider_score' in df.columns:
                insider_sig = df.loc[idx, 'insider_signal']
                insider_score = df.loc[idx, 'insider_score']
                signal_score += self.insider_weight * insider_sig * insider_score

            # 生成交易信号
            if signal_score > 0.3:
                df.loc[idx, 'combined_signal'] = 1  # 买入
            elif signal_score < -0.3:
                df.loc[idx, 'combined_signal'] = -1  # 卖出

            df.loc[idx, 'signal_strength'] = signal_score

        return df

    def backtest(
        self,
        price_data: pd.DataFrame,
        earnings_data: pd.DataFrame = None,
        announcement_data: pd.DataFrame = None,
        survey_data: pd.DataFrame = None,
        insider_data: pd.DataFrame = None,
        initial_capital: float = 1000000,
        position_size: float = 0.1,
        commission: float = 0.0003,
        slippage: float = 0.001
    ) -> Dict:
        """
        回测策略

        Args:
            price_data: 价格数据
            earnings_data: 财报事件数据
            announcement_data: 公告事件数据
            survey_data: 调研事件数据
            insider_data: 高管增减持数据
            initial_capital: 初始资金
            position_size: 单次交易仓位比例
            commission: 手续费率
            slippage: 滑点

        Returns:
            回测结果
        """
        logger.info(f"开始回测事件驱动策略...")

        # 生成信号
        df = self.generate_combined_signals(
            price_data,
            earnings_data,
            announcement_data,
            survey_data,
            insider_data
        )

        # 初始化回测变量
        capital = initial_capital
        position = 0
        entry_price = 0
        trades = []
        portfolio_values = []

        for idx in df.index:
            if idx == 0:
                continue

            current_price = df.loc[idx, 'close']
            signal = df.loc[idx, 'combined_signal']

            # 检查止损止盈
            if position > 0:
                profit_loss_pct = (current_price - entry_price) / entry_price

                # 止损
                if profit_loss_pct <= -self.stop_loss:
                    sell_price = current_price * (1 - slippage)
                    proceeds = position * sell_price * (1 - commission)
                    capital += proceeds

                    trades.append({
                        'trade_date': df.loc[idx, 'trade_date'],
                        'type': 'stop_loss',
                        'price': sell_price,
                        'shares': position,
                        'proceeds': proceeds,
                        'pnl_pct': profit_loss_pct
                    })
                    position = 0

                # 止盈
                elif profit_loss_pct >= self.take_profit:
                    sell_price = current_price * (1 - slippage)
                    proceeds = position * sell_price * (1 - commission)
                    capital += proceeds

                    trades.append({
                        'trade_date': df.loc[idx, 'trade_date'],
                        'type': 'take_profit',
                        'price': sell_price,
                        'shares': position,
                        'proceeds': proceeds,
                        'pnl_pct': profit_loss_pct
                    })
                    position = 0

            # 执行交易信号
            if signal == 1 and position == 0:  # 买入
                buy_price = current_price * (1 + slippage)
                available_capital = capital * position_size
                shares = int(available_capital / buy_price / 100) * 100

                if shares > 0:
                    cost = shares * buy_price * (1 + commission)
                    if cost <= capital:
                        capital -= cost
                        position = shares
                        entry_price = buy_price
                        trades.append({
                            'trade_date': df.loc[idx, 'trade_date'],
                            'type': 'buy',
                            'price': buy_price,
                            'shares': shares,
                            'cost': cost
                        })

            elif signal == -1 and position > 0:  # 卖出
                sell_price = current_price * (1 - slippage)
                proceeds = position * sell_price * (1 - commission)

                pnl_pct = (sell_price - entry_price) / entry_price

                capital += proceeds
                trades.append({
                    'trade_date': df.loc[idx, 'trade_date'],
                    'type': 'sell',
                    'price': sell_price,
                    'shares': position,
                    'proceeds': proceeds,
                    'pnl_pct': pnl_pct
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
            return {
                'sharpe': 0,
                'total_return': 0,
                'max_drawdown': 0,
                'win_rate': 0,
                'total_trades': 0
            }

        pv_df = pd.DataFrame(portfolio_values)

        # 计算收益率
        pv_df['daily_return'] = pv_df['value'].pct_change().fillna(0)

        # 总收益率
        final_value = pv_df['value'].iloc[-1]
        total_return = (final_value - initial_capital) / initial_capital

        # 年化收益率
        days = len(pv_df)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0

        # 波动率
        daily_vol = pv_df['daily_return'].std()
        annual_vol = daily_vol * np.sqrt(252) if daily_vol > 0 else 0

        # 夏普比率
        risk_free_rate = 0.03
        sharpe = (annual_return - risk_free_rate) / annual_vol if annual_vol > 0 else 0

        # 最大回撤
        pv_df['cummax'] = pv_df['value'].cummax()
        pv_df['drawdown'] = (pv_df['value'] - pv_df['cummax']) / pv_df['cummax']
        max_drawdown = pv_df['drawdown'].min()

        # 胜率
        sell_trades = [t for t in trades if t.get('type') in ['sell', 'stop_loss', 'take_profit']]
        buy_trades = [t for t in trades if t.get('type') == 'buy']

        winning_trades = 0
        for sell in sell_trades:
            if sell.get('pnl_pct', 0) > 0:
                winning_trades += 1

        win_rate = winning_trades / len(sell_trades) if sell_trades else 0

        return {
            'sharpe': round(sharpe, 4),
            'total_return': round(total_return, 4),
            'annual_return': round(annual_return, 4),
            'annual_volatility': round(annual_vol, 4),
            'max_drawdown': round(max_drawdown, 4),
            'win_rate': round(win_rate, 4),
            'total_trades': len(trades),
            'final_value': round(final_value, 2),
            'trades': trades,
            'portfolio_values': portfolio_values
        }

    def get_params(self) -> Dict:
        """获取策略参数"""
        return {
            'hold_period': self.hold_period,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'earnings_weight': self.earnings_weight,
            'announcement_weight': self.announcement_weight,
            'survey_weight': self.survey_weight,
            'insider_weight': self.insider_weight
        }


def main():
    """测试函数 - 使用模拟数据"""
    print("=" * 90)
    print("事件驱动策略测试")
    print("=" * 90)

    # 创建模拟价格数据
    dates = pd.date_range('2023-01-01', periods=252, freq='B')
    np.random.seed(42)

    # 生成随机价格序列
    price_data = pd.DataFrame({
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'close': 100 * np.cumprod(1 + np.random.randn(252) * 0.02)
    })
    price_data['open'] = price_data['close'] * (1 + np.random.randn(252) * 0.01)
    price_data['high'] = price_data[['open', 'close']].max(axis=1) * (1 + np.random.rand(252) * 0.02)
    price_data['low'] = price_data[['open', 'close']].min(axis=1) * (1 - np.random.rand(252) * 0.02)

    # 创建模拟财报事件
    earnings_data = pd.DataFrame([
        {'ts_code': 'TEST', 'announce_date': '20230315', 'report_type': 'Q1',
         'revenue_growth': 0.25, 'profit_growth': 0.35, 'eps': 1.2, 'eps_estimate': 1.0},
        {'ts_code': 'TEST', 'announce_date': '20230615', 'report_type': 'H1',
         'revenue_growth': 0.18, 'profit_growth': 0.22, 'eps': 1.1, 'eps_estimate': 1.15},
        {'ts_code': 'TEST', 'announce_date': '20230915', 'report_type': 'Q3',
         'revenue_growth': 0.30, 'profit_growth': 0.45, 'eps': 1.5, 'eps_estimate': 1.2},
        {'ts_code': 'TEST', 'announce_date': '20231215', 'report_type': 'FY',
         'revenue_growth': 0.28, 'profit_growth': 0.40, 'eps': 1.8, 'eps_estimate': 1.6},
    ])

    # 创建模拟公告事件
    announcement_data = pd.DataFrame([
        {'ts_code': 'TEST', 'announce_date': '20230420', 'type': 'contract', 'sentiment': 0.8},
        {'ts_code': 'TEST', 'announce_date': '20230720', 'type': 'product_approval', 'sentiment': 0.6},
        {'ts_code': 'TEST', 'announce_date': '20231020', 'type': 'acquisition', 'sentiment': 0.7},
    ])

    # 创建模拟调研事件
    survey_data = pd.DataFrame([
        {'ts_code': 'TEST', 'survey_date': '20230301', 'institution_count': 15, 'institution_type': 'fund,insurance'},
        {'ts_code': 'TEST', 'survey_date': '20230601', 'institution_count': 8, 'institution_type': 'fund'},
        {'ts_code': 'TEST', 'survey_date': '20230901', 'institution_count': 12, 'institution_type': 'fund,securities'},
    ])

    # 创建模拟高管增减持事件
    insider_data = pd.DataFrame([
        {'ts_code': 'TEST', 'trade_date': '20230410', 'insider_name': '张三', 'change_type': '增持', 'change_ratio': 0.001},
        {'ts_code': 'TEST', 'trade_date': '20230810', 'insider_name': '李四', 'change_type': '增持', 'change_ratio': 0.002},
    ])

    print(f"\n价格数据：{len(price_data)} 条")
    print(f"财报事件：{len(earnings_data)} 条")
    print(f"公告事件：{len(announcement_data)} 条")
    print(f"调研事件：{len(survey_data)} 条")
    print(f"高管增减持：{len(insider_data)} 条")

    # 创建策略
    strategy = EventDrivenStrategy(
        hold_period=5,
        stop_loss=0.08,
        take_profit=0.15
    )

    # 回测
    print("\n开始回测...")
    results = strategy.backtest(
        price_data,
        earnings_data=earnings_data,
        announcement_data=announcement_data,
        survey_data=survey_data,
        insider_data=insider_data,
        initial_capital=1000000,
        position_size=0.95
    )

    # 打印结果
    print("\n" + "=" * 90)
    print("回测结果")
    print("=" * 90)
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
    print("=" * 90)


if __name__ == "__main__":
    main()
