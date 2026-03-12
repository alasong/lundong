"""
交易信号生成器
基于预测结果和风控规则生成明确的买入/卖出信号
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from risk.risk_manager import RiskManager
from risk.transaction_cost import TransactionCostModel


class SignalGenerator:
    """
    交易信号生成器

    信号类型：
    - STRONG_BUY: 强烈买入（预测涨幅>5%，无风险警告）
    - BUY: 买入（预测涨幅>2%，无风险警告）
    - HOLD: 持有（预测涨幅在 -2% 到 2% 之间）
    - REDUCE: 减仓（预测跌幅>2%，或触发布局止盈）
    - SELL: 卖出（预测跌幅>5%，或触发止损）
    """

    # 信号强度
    STRONG_BUY = 5
    BUY = 4
    HOLD = 3
    REDUCE = 2
    SELL = 1

    def __init__(
        self,
        risk_manager: RiskManager = None,
        cost_model: TransactionCostModel = None,
        stop_loss_pct: float = 0.08,
        take_profit_pct: float = 0.20
    ):
        """
        初始化信号生成器

        Args:
            risk_manager: 风险管理器
            cost_model: 交易成本模型
            stop_loss_pct: 止损比例（默认 8%）
            take_profit_pct: 止盈比例（默认 20%）
        """
        self.risk_manager = risk_manager or RiskManager()
        self.cost_model = cost_model or TransactionCostModel()
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct

        logger.info("交易信号生成器初始化完成")

    def generate_signals(
        self,
        predictions: pd.DataFrame,
        positions: Optional[List[Dict]] = None,
        current_prices: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        生成交易信号

        Args:
            predictions: 预测 DataFrame
                需包含：ts_code, pred_1d, pred_5d, combined_score
            positions: 当前持仓（可选）
            current_prices: 当前价格（可选）

        Returns:
            包含 signal 列的 DataFrame
        """
        df = predictions.copy()

        # 初始化信号为 HOLD
        df['signal'] = self.HOLD
        df['signal_reason'] = '持有 - 无明确信号'

        # 基于预测生成基础信号
        df.loc[df['pred_1d'] > 5.0, 'signal'] = self.STRONG_BUY
        df.loc[df['pred_1d'] > 5.0, 'signal_reason'] = '强烈买入 - 预测 1 日涨幅>5%'

        df.loc[(df['pred_1d'] > 2.0) & (df['pred_1d'] <= 5.0), 'signal'] = self.BUY
        df.loc[(df['pred_1d'] > 2.0) & (df['pred_1d'] <= 5.0), 'signal_reason'] = '买入 - 预测 1 日涨幅>2%'

        df.loc[(df['pred_1d'] < -5.0), 'signal'] = self.SELL
        df.loc[(df['pred_1d'] < -5.0), 'signal_reason'] = '卖出 - 预测 1 日跌幅>5%'

        df.loc[(df['pred_1d'] < -2.0) & (df['pred_1d'] >= -5.0), 'signal'] = self.REDUCE
        df.loc[(df['pred_1d'] < -2.0) & (df['pred_1d'] >= -5.0), 'signal_reason'] = '减仓 - 预测 1 日跌幅>2%'

        # 如果有持仓，检查止损止盈
        if positions is not None and current_prices is not None:
            # 检查止损
            stop_loss_signals = self.risk_manager.check_stop_loss(
                positions,
                current_prices,
                stop_loss_type='trailing',
                fixed_stop_loss_pct=self.stop_loss_pct,
                trailing_stop_loss_pct=0.10
            )

            # 覆盖为 SELL 信号
            for sl in stop_loss_signals:
                mask = df['ts_code'] == sl['ts_code']
                df.loc[mask, 'signal'] = self.SELL
                df.loc[mask, 'signal_reason'] = f'止损 - {sl["reason"]}'

        # 黑名单过滤
        blacklist = self.risk_manager.get_blacklist()
        if blacklist:
            mask = df['ts_code'].isin(blacklist)
            df.loc[mask, 'signal'] = self.SELL
            df.loc[mask, 'signal_reason'] = '卖出 - 黑名单股票'

        return df

    def generate_rebalance_signals(
        self,
        current_portfolio: pd.DataFrame,
        target_portfolio: pd.DataFrame,
        prices: pd.DataFrame,
        min_trade_value: float = 10000
    ) -> List[Dict]:
        """
        生成调仓交易指令

        Args:
            current_portfolio: 当前持仓 DataFrame
            target_portfolio: 目标持仓 DataFrame
            prices: 当前价格
            min_trade_value: 最小交易金额

        Returns:
            交易指令列表
        """
        trades = []

        # 构建索引
        current_dict = {row['ts_code']: row for _, row in current_portfolio.iterrows()}
        target_dict = {row['ts_code']: row for _, row in target_portfolio.iterrows()}

        all_codes = set(current_dict.keys()) | set(target_dict.keys())

        for ts_code in all_codes:
            current_weight = current_dict.get(ts_code, {}).get('weight', 0)
            target_weight = target_dict.get(ts_code, {}).get('weight', 0)

            # 获取价格
            price_row = prices[prices['ts_code'] == ts_code]
            if price_row.empty:
                continue
            price = price_row.iloc[0]['close']

            # 计算需要交易的金额
            total_value = current_portfolio['market_value'].sum() if 'market_value' in current_portfolio.columns else 0
            if total_value == 0:
                continue

            trade_value = (target_weight - current_weight) * total_value

            if abs(trade_value) < min_trade_value:
                continue

            # 生成交易指令
            action = 'buy' if trade_value > 0 else 'sell'
            shares = int(abs(trade_value) / price / 100) * 100  # 整百股

            if shares <= 0:
                continue

            # 计算成本
            cost = self.cost_model.calculate_cost(ts_code, action, price, shares)

            trades.append({
                'ts_code': ts_code,
                'action': action,
                'shares': shares,
                'price': price,
                'estimated_value': shares * price,
                'estimated_cost': cost['total_cost'],
                'reason': f'调仓：权重 {current_weight:.1%} → {target_weight:.1%}'
            })

        return trades

    def filter_by_risk(
        self,
        signals: pd.DataFrame,
        max_position_pct: float = 0.10,
        max_sector_pct: float = 0.25
    ) -> pd.DataFrame:
        """
        根据风险限制过滤信号

        Args:
            signals: 交易信号 DataFrame
            max_position_pct: 单股最大仓位
            max_sector_pct: 单板块最大仓位

        Returns:
            过滤后的信号
        """
        df = signals.copy()

        # 过滤掉 SELL 信号中的非黑名单股票（可能是正常调仓）
        # 这里简化处理，只保留 BUY 和 STRONG_BUY 信号的完整列表

        # 按板块分组，检查板块集中度
        if 'concept_code' in df.columns:
            sector_counts = df.groupby('concept_code').size()
            high_concentration_sectors = sector_counts[
                sector_counts > max_sector_pct / 0.02  # 假设等权，计算股票数
            ].index.tolist()

            # 降低高集中度板块的信号强度
            for sector in high_concentration_sectors:
                mask = (df['concept_code'] == sector) & (df['signal'] >= self.BUY)
                df.loc[mask, 'signal'] = df.loc[mask, 'signal'] - 1
                df.loc[mask, 'signal_reason'] += ' (板块集中度限制)'

        return df


def signals_to_dataframe(signals: List[Dict]) -> pd.DataFrame:
    """将信号列表转换为 DataFrame"""
    if not signals:
        return pd.DataFrame()
    return pd.DataFrame(signals)


def print_signals(signals_df: pd.DataFrame, title: str = "交易信号"):
    """打印交易信号"""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    if signals_df.empty:
        print("无信号")
        return

    # 按信号强度分组
    signal_names = {
        5: '强烈买入',
        4: '买入',
        3: '持有',
        2: '减仓',
        1: '卖出'
    }

    for signal_level in [5, 4, 3, 2, 1]:
        group = signals_df[signals_df['signal'] == signal_level]
        if not group.empty:
            print(f"\n【{signal_names[signal_level]}】({len(group)}只)")
            for _, row in group.iterrows():
                print(f"  {row['ts_code']}: {row.get('signal_reason', '')}")

    print("\n" + "=" * 70)


def main():
    """测试函数"""
    print("=" * 70)
    print("交易信号生成器测试")
    print("=" * 70)

    # 创建测试数据
    test_predictions = pd.DataFrame([
        {'ts_code': '000001.SZ', 'pred_1d': 3.5, 'pred_5d': 8.0, 'combined_score': 85},
        {'ts_code': '000002.SZ', 'pred_1d': 6.0, 'pred_5d': 12.0, 'combined_score': 92},
        {'ts_code': '600000.SH', 'pred_1d': 1.0, 'pred_5d': 2.0, 'combined_score': 55},
        {'ts_code': '600001.SH', 'pred_1d': -3.0, 'pred_5d': -5.0, 'combined_score': 30},
        {'ts_code': '300001.SZ', 'pred_1d': -7.0, 'pred_5d': -10.0, 'combined_score': 15},
    ])

    # 生成信号
    sg = SignalGenerator()
    signals = sg.generate_signals(test_predictions)

    # 打印结果
    print_signals(signals, "基于预测的交易信号")

    # 显示详细数据
    print("\n【信号详情】")
    print(signals[['ts_code', 'signal', 'signal_reason', 'pred_1d']].to_string())


if __name__ == "__main__":
    main()
