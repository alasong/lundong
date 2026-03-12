"""
交易成本模型
计算手续费、印花税、滑点等交易成本
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TransactionCostModel:
    """
    交易成本模型

    成本构成：
    1. 佣金：买卖双方收取，一般万 2.5，最低 5 元
    2. 印花税：仅卖方收取，0.05%（2023 年 8 月 28 日后）
    3. 过户费：买卖双方收取，万 0.1
    4. 滑点：市场冲击成本，与成交额和流动性相关
    """

    def __init__(
        self,
        commission_rate: float = 0.00025,  # 佣金率 万 2.5
        min_commission: float = 5.0,        # 最低佣金 5 元
        stamp_tax_rate: float = 0.0005,     # 印花税 0.05%（卖方）
        transfer_fee_rate: float = 0.00001, # 过户费 万 0.1
        slippage_model: str = "volume_based" # 滑点模型
    ):
        """
        初始化交易成本模型

        Args:
            commission_rate: 佣金率
            min_commission: 最低佣金
            stamp_tax_rate: 印花税率
            transfer_fee_rate: 过户费率
            slippage_model: 滑点模型类型
                - 'fixed': 固定滑点
                - 'volume_based': 基于成交量
                - 'volatility_based': 基于波动率
        """
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.stamp_tax_rate = stamp_tax_rate
        self.transfer_fee_rate = transfer_fee_rate
        self.slippage_model = slippage_model

        logger.info(f"交易成本模型初始化完成")
        logger.debug(f"佣金率：{commission_rate:.4f}, 印花税：{stamp_tax_rate:.4f}")

    def calculate_cost(
        self,
        ts_code: str,
        action: str,
        price: float,
        shares: int,
        daily_data: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        计算单笔交易成本

        Args:
            ts_code: 股票代码
            action: 买卖方向 ('buy' 或 'sell')
            price: 成交价格
            shares: 股数
            daily_data: 当日行情数据（用于计算滑点）

        Returns:
            成本明细字典
        """
        trade_value = price * shares

        # 1. 佣金（买卖双方）
        commission = max(trade_value * self.commission_rate, self.min_commission)

        # 2. 印花税（仅卖方）
        stamp_tax = trade_value * self.stamp_tax_rate if action == 'sell' else 0

        # 3. 过户费（买卖双方）
        transfer_fee = trade_value * self.transfer_fee_rate

        # 4. 滑点
        slippage = self._calculate_slippage(
            ts_code, action, price, shares, daily_data
        )

        # 总成本
        total_cost = commission + stamp_tax + transfer_fee + slippage

        # 成本率（相对于交易金额）
        cost_rate = total_cost / trade_value if trade_value > 0 else 0

        return {
            'ts_code': ts_code,
            'action': action,
            'price': price,
            'shares': shares,
            'trade_value': trade_value,
            'commission': commission,
            'stamp_tax': stamp_tax,
            'transfer_fee': transfer_fee,
            'slippage': slippage,
            'total_cost': total_cost,
            'cost_rate': cost_rate
        }

    def _calculate_slippage(
        self,
        ts_code: str,
        action: str,
        price: float,
        shares: int,
        daily_data: Optional[pd.DataFrame] = None
    ) -> float:
        """
        计算滑点成本

        Args:
            ts_code: 股票代码
            action: 买卖方向
            price: 价格
            shares: 股数
            daily_data: 行情数据

        Returns:
            滑点成本（元）
        """
        trade_value = price * shares

        if self.slippage_model == "fixed":
            # 固定滑点 0.1%
            slippage_rate = 0.001
            return trade_value * slippage_rate

        elif self.slippage_model == "volume_based":
            # 基于成交量的滑点模型
            # 假设：交易量占当日成交额比例越高，滑点越大
            if daily_data is not None and not daily_data.empty:
                row = daily_data[daily_data['ts_code'] == ts_code]
                if not row.empty:
                    daily_amount = row.iloc[0].get('amount', 0)  # 成交额（元）
                    if daily_amount > 0:
                        # 交易金额占日成交额的比例
                        volume_ratio = trade_value / daily_amount
                        # 滑点与比例成正比，系数 0.5
                        slippage_rate = 0.0005 * volume_ratio
                        # 设置上下限
                        slippage_rate = min(max(slippage_rate, 0.0001), 0.005)
                        return trade_value * slippage_rate

            # 默认滑点
            return trade_value * 0.0005

        elif self.slippage_model == "volatility_based":
            # 基于波动率的滑点模型
            # 波动率越高，滑点越大
            if daily_data is not None and not daily_data.empty:
                df = daily_data[daily_data['ts_code'] == ts_code].sort_values('trade_date')
                if len(df) >= 20:
                    volatility = df.tail(20)['pct_chg'].std() / 100  # 日化波动率
                    slippage_rate = volatility * 0.1  # 波动率的 10% 作为滑点
                    slippage_rate = min(max(slippage_rate, 0.0001), 0.01)
                    return trade_value * slippage_rate

            return trade_value * 0.0005

        return 0

    def adjust_price_for_slippage(
        self,
        price: float,
        action: str,
        slippage_rate: float = 0.001
    ) -> float:
        """
        计算考虑滑点后的实际成交价格

        Args:
            price: 名义价格
            action: 买卖方向
            slippage_rate: 滑点率

        Returns:
            实际成交价格
        """
        if action == 'buy':
            # 买入：滑点使成本更高
            return price * (1 + slippage_rate)
        else:
            # 卖出：滑点使收入更低
            return price * (1 - slippage_rate)

    def calculate_portfolio_turnover_cost(
        self,
        old_positions: List[Dict],
        new_positions: List[Dict],
        prices: pd.DataFrame,
        daily_data: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        计算组合调仓的总成本

        Args:
            old_positions: 原持仓
            new_positions: 新持仓
            prices: 当前价格 DataFrame
            daily_data: 行情数据

        Returns:
            调仓成本汇总
        """
        buy_costs = []
        sell_costs = []

        # 构建代码索引
        old_dict = {p['ts_code']: p for p in old_positions}
        new_dict = {p['ts_code']: p for p in new_positions}

        all_codes = set(old_dict.keys()) | set(new_dict.keys())

        for ts_code in all_codes:
            old_shares = old_dict.get(ts_code, {}).get('shares', 0)
            new_shares = new_dict.get(ts_code, {}).get('shares', 0)

            # 获取价格
            price_row = prices[prices['ts_code'] == ts_code]
            if price_row.empty:
                continue
            price = price_row.iloc[0]['close']

            if new_shares > old_shares:
                # 买入
                delta_shares = new_shares - old_shares
                cost = self.calculate_cost(ts_code, 'buy', price, delta_shares, daily_data)
                buy_costs.append(cost)
            elif new_shares < old_shares:
                # 卖出
                delta_shares = old_shares - new_shares
                cost = self.calculate_cost(ts_code, 'sell', price, delta_shares, daily_data)
                sell_costs.append(cost)

        # 汇总
        total_buy_cost = sum(c['total_cost'] for c in buy_costs)
        total_sell_cost = sum(c['total_cost'] for c in sell_costs)
        total_buy_value = sum(c['trade_value'] for c in buy_costs)
        total_sell_value = sum(c['trade_value'] for c in sell_costs)

        return {
            'buy_trades': len(buy_costs),
            'sell_trades': len(sell_costs),
            'buy_value': total_buy_value,
            'sell_value': total_sell_value,
            'buy_cost': total_buy_cost,
            'sell_cost': total_sell_cost,
            'total_cost': total_buy_cost + total_sell_cost,
            'cost_rate': (total_buy_cost + total_sell_cost) / (total_buy_value + total_sell_value)
                        if (total_buy_value + total_sell_value) > 0 else 0,
            'buy_details': buy_costs,
            'sell_details': sell_costs
        }


def estimate_impact_on_returns(
    annual_turnover: float,
    commission_rate: float = 0.00025,
    stamp_tax_rate: float = 0.0005,
    avg_slippage: float = 0.0005
) -> float:
    """
    估算交易成本对年化收益的影响

    Args:
        annual_turnover: 年换手率（如 5.0 表示 5 倍换手）
        commission_rate: 佣金率
        stamp_tax_rate: 印花税率
        avg_slippage: 平均滑点

    Returns:
        年化成本率（拖累收益的百分比）
    """
    # 双边佣金
    round_trip_commission = 2 * commission_rate

    # 单边印花税（仅卖出）
    # 假设换手率是单边计算的，卖出占一半
    effective_stamp = stamp_tax_rate * annual_turnover

    # 双边滑点
    round_trip_slippage = 2 * avg_slippage

    # 单次完整交易成本率
    one_way_cost = commission_rate + avg_slippage + (stamp_tax_rate / 2)

    # 年化成本 = 换手率 * 单次成本
    annual_cost = annual_turnover * one_way_cost

    return annual_cost * 100  # 返回百分比


def main():
    """测试函数"""
    print("=" * 70)
    print("交易成本模型测试")
    print("=" * 70)

    # 初始化模型
    tcm = TransactionCostModel()

    # 测试单笔交易成本
    print("\n【测试 1】单笔交易成本")
    print("-" * 50)

    test_cases = [
        ('buy', '000001.SZ', 10.5, 10000),   # 买入 10 万元
        ('sell', '000001.SZ', 10.5, 10000),  # 卖出 10 万元
        ('buy', '600000.SH', 8.2, 5000),     # 买入 4 万元（低于 5 元佣金下限测试）
    ]

    for action, code, price, shares in test_cases:
        cost = tcm.calculate_cost(code, action, price, shares)
        print(f"\n{action.upper()} {code}: {shares}股 @ ¥{price}")
        print(f"  交易金额：¥{cost['trade_value']:,.2f}")
        print(f"  佣金：¥{cost['commission']:.2f}")
        print(f"  印花税：¥{cost['stamp_tax']:.2f}")
        print(f"  滑点：¥{cost['slippage']:.2f}")
        print(f"  总成本：¥{cost['total_cost']:.2f} ({cost['cost_rate']:.2%})")

    # 测试对收益的影响
    print("\n【测试 2】交易成本对年化收益的影响")
    print("-" * 50)

    turnover_rates = [1, 2, 5, 10, 20]  # 不同换手率
    for tr in turnover_rates:
        impact = estimate_impact_on_returns(tr)
        print(f"年换手率 {tr}x: 成本拖累 {impact:.2f}%")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
