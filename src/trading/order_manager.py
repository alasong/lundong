"""
订单管理模块
生成、执行和跟踪交易订单
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from risk.risk_manager import RiskManager
from risk.transaction_cost import TransactionCostModel
from risk.signal_generator import SignalGenerator


class Order:
    """订单类"""

    # 订单状态
    PENDING = 'pending'      # 待执行
    FILLED = 'filled'        # 已成交
    PARTIALLY_FILLED = 'partial'  # 部分成交
    CANCELLED = 'cancelled'  # 已取消
    REJECTED = 'rejected'    # 已拒绝

    # 订单类型
    BUY = 'buy'
    SELL = 'sell'

    def __init__(
        self,
        ts_code: str,
        action: str,
        shares: int,
        price: float = None,
        order_type: str = 'market'
    ):
        """
        初始化订单

        Args:
            ts_code: 股票代码
            action: 买卖方向 ('buy' 或 'sell')
            shares: 股数
            price: 订单价格（限价单使用）
            order_type: 订单类型 ('market' 或 'limit')
        """
        self.ts_code = ts_code
        self.action = action
        self.shares = shares
        self.price = price
        self.order_type = order_type
        self.status = self.PENDING
        self.filled_shares = 0
        self.filled_price = 0
        self.created_at = datetime.now()
        self.updated_at = self.created_at
        self.order_id = self._generate_order_id()

    def _generate_order_id(self) -> str:
        """生成订单 ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"{self.action.upper()[:1]}{timestamp}{self.ts_code[:6]}"

    def fill(self, filled_shares: int, filled_price: float):
        """
        成交订单

        Args:
            filled_shares: 成交股数
            filled_price: 成交价格
        """
        self.filled_shares = filled_shares
        self.filled_price = filled_price

        if filled_shares >= self.shares:
            self.status = self.FILLED
        elif filled_shares > 0:
            self.status = self.PARTIALLY_FILLED
        else:
            self.status = self.REJECTED

        self.updated_at = datetime.now()

    def cancel(self):
        """取消订单"""
        self.status = self.CANCELLED
        self.updated_at = datetime.now()

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'order_id': self.order_id,
            'ts_code': self.ts_code,
            'action': self.action,
            'shares': self.shares,
            'price': self.price,
            'order_type': self.order_type,
            'status': self.status,
            'filled_shares': self.filled_shares,
            'filled_price': self.filled_price,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        }

    def __repr__(self):
        return f"Order({self.order_id}, {self.ts_code}, {self.action}, {self.shares}, status={self.status})"


class OrderManager:
    """
    订单管理器
    生成、执行和跟踪订单
    """

    def __init__(
        self,
        initial_capital: float = 1000000.0,
        commission_rate: float = 0.00025
    ):
        """
        初始化订单管理器

        Args:
            initial_capital: 初始资金
            commission_rate: 佣金率
        """
        self.initial_capital = initial_capital
        self.available_capital = initial_capital
        self.positions = {}  # 持仓 {ts_code: {'shares': x, 'cost_price': y, ...}}
        self.orders = {}  # 订单 {order_id: Order}
        self.cost_model = TransactionCostModel(commission_rate=commission_rate)
        self.risk_manager = RiskManager()

        logger.info(f"订单管理器初始化完成，初始资金：¥{initial_capital:,.2f}")

    def generate_orders_from_signals(
        self,
        signals_df: pd.DataFrame,
        prices: pd.DataFrame,
        target_weights: Dict[str, float] = None
    ) -> List[Order]:
        """
        根据交易信号生成订单

        Args:
            signals_df: 交易信号 DataFrame
            prices: 当前价格
            target_weights: 目标权重（可选）

        Returns:
            订单列表
        """
        orders = []

        for _, row in signals_df.iterrows():
            ts_code = row['ts_code']
            signal = row.get('signal', 3)

            # 获取价格
            price_row = prices[prices['ts_code'] == ts_code]
            if price_row.empty:
                continue
            price = price_row.iloc[0]['close']

            # 根据信号生成订单
            if signal >= 5:  # 强烈买入
                # 计算目标仓位（总资金的 10%）
                target_value = self.available_capital * 0.10
                shares = int(target_value / price / 100) * 100
                if shares > 0:
                    orders.append(Order(ts_code, 'buy', shares, price))

            elif signal >= 4:  # 买入
                target_value = self.available_capital * 0.05
                shares = int(target_value / price / 100) * 100
                if shares > 0:
                    orders.append(Order(ts_code, 'buy', shares, price))

            elif signal <= 1:  # 卖出
                # 卖出所有持仓
                if ts_code in self.positions:
                    shares = self.positions[ts_code]['shares']
                    if shares > 0:
                        orders.append(Order(ts_code, 'sell', shares, price))

            elif signal <= 2:  # 减仓
                # 卖出一半持仓
                if ts_code in self.positions:
                    shares = self.positions[ts_code]['shares'] // 2
                    if shares > 0:
                        orders.append(Order(ts_code, 'sell', shares, price))

        logger.info(f"生成 {len(orders)} 个订单")
        return orders

    def submit_order(self, order: Order) -> bool:
        """
        提交订单

        Args:
            order: 订单对象

        Returns:
            是否提交成功
        """
        # 检查资金是否充足（买单）
        if order.action == 'buy':
            estimated_cost = order.shares * order.price * 1.001  # 包含手续费
            if estimated_cost > self.available_capital:
                logger.warning(f"资金不足：需要¥{estimated_cost:,.2f}, 可用¥{self.available_capital:,.2f}")
                order.status = Order.REJECTED
                return False

        # 检查持仓是否充足（卖单）
        if order.action == 'sell':
            if ts_code not in self.positions or self.positions[ts_code]['shares'] < order.shares:
                logger.warning(f"持仓不足：{order.ts_code}")
                order.status = Order.REJECTED
                return False

        # 提交订单
        self.orders[order.order_id] = order
        logger.info(f"提交订单：{order}")
        return True

    def execute_order(
        self,
        order_id: str,
        execution_price: float = None
    ) -> Optional[Dict]:
        """
        执行订单（模拟成交）

        Args:
            order_id: 订单 ID
            execution_price: 成交价格（默认使用订单价格）

        Returns:
            成交回报
        """
        if order_id not in self.orders:
            logger.error(f"订单不存在：{order_id}")
            return None

        order = self.orders[order_id]

        if order.status != Order.PENDING:
            logger.warning(f"订单状态不正确：{order.status}")
            return None

        # 使用订单价格或传入价格
        price = execution_price or order.price

        # 计算成本
        cost_info = self.cost_model.calculate_cost(
            order.ts_code,
            order.action,
            price,
            order.shares
        )

        # 更新订单状态
        order.fill(order.shares, price)

        # 更新持仓
        if order.action == 'buy':
            # 买入：增加持仓
            if order.ts_code not in self.positions:
                self.positions[order.ts_code] = {
                    'shares': 0,
                    'cost_price': 0,
                    'market_value': 0
                }

            pos = self.positions[order.ts_code]
            total_cost = order.shares * price + cost_info['total_cost']

            # 更新平均成本
            if pos['shares'] > 0:
                pos['cost_price'] = (pos['shares'] * pos['cost_price'] + total_cost) / (pos['shares'] + order.shares)

            pos['shares'] += order.shares
            pos['market_value'] = pos['shares'] * price

            # 扣减资金
            self.available_capital -= (order.shares * price + cost_info['total_cost'])

        else:
            # 卖出：减少持仓
            if order.ts_code in self.positions:
                pos = self.positions[order.ts_code]
                sell_value = order.shares * price
                sell_cost = cost_info['total_cost']

                # 释放资金
                self.available_capital += (sell_value - sell_cost)

                pos['shares'] -= order.shares
                if pos['shares'] <= 0:
                    del self.positions[order.ts_code]
                else:
                    pos['market_value'] = pos['shares'] * price

        # 生成成交回报
        fill_report = {
            'order_id': order_id,
            'ts_code': order.ts_code,
            'action': order.action,
            'shares': order.shares,
            'price': price,
            'cost': cost_info['total_cost'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(f"订单成交：{order.action} {order.ts_code} {order.shares}股 @ ¥{price}")
        return fill_report

    def get_portfolio_summary(self) -> Dict:
        """获取组合汇总"""
        total_market_value = sum(
            pos['shares'] * pos.get('market_value', pos['shares'])
            for pos in self.positions.values()
        )

        # 实际上应该是当前市值
        total_market_value = sum(
            pos['market_value'] for pos in self.positions.values()
        )

        total_assets = self.available_capital + total_market_value

        # 计算持仓权重
        positions_detail = []
        for ts_code, pos in self.positions.items():
            weight = pos['market_value'] / total_assets if total_assets > 0 else 0
            positions_detail.append({
                'ts_code': ts_code,
                'shares': pos['shares'],
                'cost_price': pos['cost_price'],
                'market_value': pos['market_value'],
                'weight': weight,
                'unrealized_pnl': pos['market_value'] - (pos['shares'] * pos['cost_price'])
            })

        return {
            'total_assets': total_assets,
            'available_capital': self.available_capital,
            'market_value': total_market_value,
            'positions': positions_detail,
            'position_ratio': total_market_value / total_assets if total_assets > 0 else 0
        }

    def get_pending_orders(self) -> List[Order]:
        """获取待执行订单"""
        return [o for o in self.orders.values() if o.status == Order.PENDING]

    def get_order_history(self, days: int = 7) -> List[Dict]:
        """获取订单历史"""
        cutoff = datetime.now() - timedelta(days=days)
        return [
            o.to_dict() for o in self.orders.values()
            if o.created_at >= cutoff
        ]


def print_portfolio_summary(om: OrderManager):
    """打印组合汇总"""
    summary = om.get_portfolio_summary()

    print("\n" + "=" * 70)
    print("投资组合汇总")
    print("=" * 70)
    print(f"总资产：¥{summary['total_assets']:,.2f}")
    print(f"可用资金：¥{summary['available_capital']:,.2f}")
    print(f"持仓市值：¥{summary['market_value']:,.2f}")
    print(f"仓位：{summary['position_ratio']:.1%}")

    if summary['positions']:
        print("\n【持仓明细】")
        for pos in summary['positions']:
            pnl = pos['unrealized_pnl']
            pnl_str = f"+¥{pnl:,.2f}" if pnl >= 0 else f"-¥{abs(pnl):,.2f}"
            print(f"  {pos['ts_code']}: {pos['shares']}股，"
                  f"市值¥{pos['market_value']:,.2f} ({pos['weight']:.1%}), "
                  f"盈亏 {pnl_str}")
    else:
        print("\n无持仓")

    print("=" * 70)


def main():
    """测试函数"""
    print("=" * 70)
    print("订单管理模块测试")
    print("=" * 70)

    # 初始化
    om = OrderManager(initial_capital=1000000)

    # 模拟交易信号
    signals = pd.DataFrame([
        {'ts_code': '000001.SZ', 'signal': 5},  # 强烈买入
        {'ts_code': '000002.SZ', 'signal': 4},  # 买入
        {'ts_code': '600000.SH', 'signal': 3},  # 持有
        {'ts_code': '600001.SH', 'signal': 1},  # 卖出
    ])

    # 模拟价格
    prices = pd.DataFrame([
        {'ts_code': '000001.SZ', 'close': 10.5},
        {'ts_code': '000002.SZ', 'close': 25.8},
        {'ts_code': '600000.SH', 'close': 8.2},
        {'ts_code': '600001.SH', 'close': 15.3},
    ])

    # 生成订单
    print("\n【生成订单】")
    orders = om.generate_orders_from_signals(signals, prices)
    for order in orders:
        print(f"  {order}")

    # 提交并执行订单
    print("\n【执行订单】")
    for order in orders:
        if om.submit_order(order):
            om.execute_order(order.order_id)

    # 打印组合汇总
    print_portfolio_summary(om)

    # 订单历史
    print("\n【订单历史】")
    history = om.get_order_history()
    for h in history:
        print(f"  {h['order_id']}: {h['action']} {h['ts_code']} {h['shares']}股 @ ¥{h['price']}")


if __name__ == "__main__":
    main()
