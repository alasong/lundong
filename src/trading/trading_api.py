#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易接口模块
提供与券商 API 对接的抽象接口，支持模拟盘和实盘

支持接口：
1. 模拟交易接口 - PaperTradingAPI
2. 实盘交易接口抽象 - TradingAPI
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger
import pandas as pd


class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL_FILLED = "partial_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class Side(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class TradeOrder:
    """交易订单"""
    order_id: str
    ts_code: str
    side: Side
    order_type: str  # market, limit
    shares: int
    price: float = None  # 限价单价格
    status: OrderStatus = OrderStatus.PENDING
    filled_shares: int = 0
    filled_amount: float = 0
    avg_price: float = 0
    commission: float = 0
    submit_time: datetime = field(default_factory=datetime.now)
    fill_time: datetime = None

    def to_dict(self) -> Dict:
        return {
            "order_id": self.order_id,
            "ts_code": self.ts_code,
            "side": self.side.value,
            "order_type": self.order_type,
            "shares": self.shares,
            "price": self.price,
            "status": self.status.value,
            "filled_shares": self.filled_shares,
            "filled_amount": self.filled_amount,
            "avg_price": round(self.avg_price, 4),
            "commission": round(self.commission, 4),
            "submit_time": self.submit_time.isoformat(),
            "fill_time": self.fill_time.isoformat() if self.fill_time else None
        }


@dataclass
class Position:
    """持仓"""
    ts_code: str
    shares: int
    available_shares: int
    cost_basis: float
    current_price: float = 0
    market_value: float = 0
    unrealized_pnl: float = 0
    unrealized_pnl_pct: float = 0

    def to_dict(self) -> Dict:
        return {
            "ts_code": self.ts_code,
            "shares": self.shares,
            "available_shares": self.available_shares,
            "cost_basis": round(self.cost_basis, 4),
            "current_price": round(self.current_price, 4),
            "market_value": round(self.market_value, 4),
            "unrealized_pnl": round(self.unrealized_pnl, 4),
            "unrealized_pnl_pct": round(self.unrealized_pnl_pct, 4)
        }


@dataclass
class Account:
    """账户信息"""
    account_id: str
    initial_capital: float
    cash: float
    market_value: float
    total_value: float
    frozen_cash: float = 0  # 冻结资金

    def to_dict(self) -> Dict:
        return {
            "account_id": self.account_id,
            "initial_capital": round(self.initial_capital, 2),
            "cash": round(self.cash, 2),
            "market_value": round(self.market_value, 2),
            "total_value": round(self.total_value, 2),
            "frozen_cash": round(self.frozen_cash, 2),
            "available_cash": round(self.cash - self.frozen_cash, 2)
        }


class PaperTradingAPI:
    """
    模拟交易 API

    功能：
    1. 账户管理
    2. 订单提交
    3. 订单查询
    4. 持仓查询
    5. 成交回报
    """

    def __init__(
        self,
        account_id: str = None,
        initial_capital: float = 1000000,
        commission_rate: float = 0.0003,
        slippage_rate: float = 0.001
    ):
        """
        初始化模拟交易 API

        Args:
            account_id: 账户 ID
            initial_capital: 初始资金
            commission_rate: 手续费率
            slippage_rate: 滑点率
        """
        self.account_id = account_id or f"PAPER_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        # 账户状态
        self.account = Account(
            account_id=self.account_id,
            initial_capital=initial_capital,
            cash=initial_capital,
            market_value=0,
            total_value=initial_capital
        )

        # 订单和持仓
        self.orders: Dict[str, TradeOrder] = {}
        self.positions: Dict[str, Position] = {}
        self.trades: List[Dict] = []

        # 市场价格（模拟用）
        self.market_prices: Dict[str, float] = {}

        logger.info(f"模拟交易 API 初始化：{self.account_id}, 初始资金={initial_capital}")

    def set_market_price(self, ts_code: str, price: float):
        """设置市场价格（用于模拟）"""
        self.market_prices[ts_code] = price
        # 更新持仓市值
        if ts_code in self.positions:
            pos = self.positions[ts_code]
            pos.current_price = price
            pos.market_value = pos.shares * price
            pos.unrealized_pnl = (price - pos.cost_basis) * pos.shares
            pos.unrealized_pnl_pct = (price / pos.cost_basis - 1) * 100 if pos.cost_basis > 0 else 0

    def get_market_price(self, ts_code: str) -> float:
        """获取市场价格"""
        if ts_code in self.market_prices:
            return self.market_prices[ts_code]
        # 默认价格
        return 10.0

    def submit_order(
        self,
        ts_code: str,
        side: Side,
        shares: int,
        order_type: str = "market",
        price: float = None
    ) -> TradeOrder:
        """
        提交订单

        Args:
            ts_code: 股票代码
            side: 买卖方向
            shares: 股数
            order_type: 订单类型（market/limit）
            price: 限价单价格

        Returns:
            订单对象
        """
        # 生成订单 ID
        order_id = f"ORD_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        # 创建订单
        order = TradeOrder(
            order_id=order_id,
            ts_code=ts_code,
            side=side,
            order_type=order_type,
            shares=shares,
            price=price
        )

        # 检查资金/持仓
        if side == Side.BUY:
            exec_price = price if order_type == "limit" else self.get_market_price(ts_code)
            required_cash = shares * exec_price * (1 + self.commission_rate + self.slippage_rate)
            if required_cash > self.account.cash:
                order.status = OrderStatus.REJECTED
                logger.warning(f"订单拒绝：资金不足，需要{required_cash:.2f}, 可用{self.account.cash:.2f}")
            else:
                # 冻结资金
                self.account.frozen_cash += required_cash
                order.status = OrderStatus.SUBMITTED
        else:  # SELL
            if ts_code not in self.positions or self.positions[ts_code].available_shares < shares:
                order.status = OrderStatus.REJECTED
                logger.warning(f"订单拒绝：持仓不足，可用{self.positions.get(ts_code, Position(ts_code, 0, 0, 0)).available_shares}")
            else:
                order.status = OrderStatus.SUBMITTED

        # 保存订单
        self.orders[order_id] = order
        logger.info(f"订单提交：{order_id}, {ts_code}, {side.value}, {shares}股, 状态={order.status.value}")

        # 模拟成交
        if order.status == OrderStatus.SUBMITTED:
            self._simulate_fill(order)

        return order

    def _simulate_fill(self, order: TradeOrder):
        """模拟订单成交"""
        # 获取执行价格
        if order.order_type == "limit":
            exec_price = order.price
        else:
            exec_price = self.get_market_price(order.ts_code)

        # 应用滑点
        if order.side == Side.BUY:
            exec_price *= (1 + self.slippage_rate)
        else:
            exec_price *= (1 - self.slippage_rate)

        # 计算手续费
        exec_amount = order.shares * exec_price
        commission = exec_amount * self.commission_rate

        # 更新订单状态
        order.filled_shares = order.shares
        order.filled_amount = exec_amount
        order.avg_price = exec_price
        order.commission = commission
        order.status = OrderStatus.FILLED
        order.fill_time = datetime.now()

        # 更新账户
        if order.side == Side.BUY:
            # 买入：扣除资金，增加持仓
            total_cost = exec_amount + commission
            self.account.cash -= total_cost
            self.account.frozen_cash -= total_cost

            if order.ts_code in self.positions:
                pos = self.positions[order.ts_code]
                # 计算新的成本 basis
                old_cost = pos.cost_basis * pos.shares
                new_cost = old_cost + total_cost
                pos.shares += order.shares
                pos.available_shares += order.shares
                pos.cost_basis = new_cost / pos.shares
            else:
                self.positions[order.ts_code] = Position(
                    ts_code=order.ts_code,
                    shares=order.shares,
                    available_shares=order.shares,
                    cost_basis=exec_price + commission / order.shares
                )
        else:
            # 卖出：增加资金，减少持仓
            total_proceeds = exec_amount - commission
            self.account.cash += total_proceeds
            self.account.frozen_cash -= exec_amount * (1 + self.commission_rate + self.slippage_rate)

            pos = self.positions[order.ts_code]
            pos.shares -= order.shares
            pos.available_shares -= order.shares

            if pos.shares == 0:
                del self.positions[order.ts_code]

        # 更新账户市值
        self._update_account_value()

        # 记录成交
        self.trades.append({
            "trade_id": f"TRD_{uuid.uuid4().hex[:8]}",
            "order_id": order.order_id,
            "ts_code": order.ts_code,
            "side": order.side.value,
            "shares": order.filled_shares,
            "price": order.avg_price,
            "amount": order.filled_amount,
            "commission": commission,
            "time": order.fill_time.isoformat()
        })

        logger.info(f"订单成交：{order.order_id}, {order.ts_code}, {order.side.value}, "
                   f"{order.filled_shares}股 @ ¥{order.avg_price:.4f}")

    def _update_account_value(self):
        """更新账户总市值"""
        market_value = sum(
            pos.shares * self.get_market_price(pos.ts_code)
            for pos in self.positions.values()
        )
        self.account.market_value = market_value
        self.account.total_value = self.account.cash + market_value

    def cancel_order(self, order_id: str) -> bool:
        """取消订单"""
        if order_id not in self.orders:
            return False

        order = self.orders[order_id]
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            return False

        order.status = OrderStatus.CANCELLED

        # 解冻资金
        if order.side == Side.BUY:
            exec_price = order.price if order.price else self.get_market_price(order.ts_code)
            frozen = order.shares * exec_price * (1 + self.commission_rate + self.slippage_rate)
            self.account.frozen_cash -= frozen

        logger.info(f"订单取消：{order_id}")
        return True

    def get_order(self, order_id: str) -> Optional[TradeOrder]:
        """查询订单"""
        return self.orders.get(order_id)

    def get_orders(self, status: OrderStatus = None, ts_code: str = None) -> List[TradeOrder]:
        """查询订单列表"""
        orders = list(self.orders.values())

        if status:
            orders = [o for o in orders if o.status == status]
        if ts_code:
            orders = [o for o in orders if o.ts_code == ts_code]

        return orders

    def get_position(self, ts_code: str) -> Optional[Position]:
        """查询持仓"""
        return self.positions.get(ts_code)

    def get_positions(self) -> List[Position]:
        """查询所有持仓"""
        return list(self.positions.values())

    def get_account(self) -> Account:
        """查询账户"""
        self._update_account_value()
        return self.account

    def get_portfolio_value(self) -> Dict:
        """获取投资组合总览"""
        self._update_account_value()
        return {
            "account": self.account.to_dict(),
            "positions": [p.to_dict() for p in self.positions.values()],
            "total_value": round(self.account.total_value, 2),
            "total_pnl": round(self.account.total_value - self.initial_capital, 2),
            "total_pnl_pct": round((self.account.total_value / self.initial_capital - 1) * 100, 2)
        }


class TradingAPI:
    """
    实盘交易 API 抽象基类

    需要子类实现具体的券商 API 对接
    """

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.connected = False
        logger.info("实盘交易 API 初始化")

    def connect(self) -> bool:
        """连接到券商 API"""
        raise NotImplementedError

    def disconnect(self):
        """断开连接"""
        raise NotImplementedError

    def submit_order(self, ts_code: str, side: Side, shares: int,
                     order_type: str = "market", price: float = None) -> TradeOrder:
        """提交订单"""
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> bool:
        """取消订单"""
        raise NotImplementedError

    def get_orders(self, status: str = None) -> List[TradeOrder]:
        """查询订单"""
        raise NotImplementedError

    def get_positions(self) -> List[Position]:
        """查询持仓"""
        raise NotImplementedError

    def get_account(self) -> Account:
        """查询账户"""
        raise NotImplementedError


def main():
    """测试函数"""
    print("=" * 90)
    print("模拟交易 API 测试")
    print("=" * 90)

    # 初始化模拟 API
    api = PaperTradingAPI(initial_capital=1000000)

    print("\n[1] 账户信息")
    print("-" * 50)
    account = api.get_account()
    print(f"账户 ID: {account.account_id}")
    print(f"初始资金：¥{account.initial_capital:,.2f}")
    print(f"可用资金：¥{account.cash:,.2f}")
    print(f"总资产：¥{account.total_value:,.2f}")

    # 设置市场价格
    print("\n[2] 设置市场价格")
    print("-" * 50)
    api.set_market_price("000001.SZ", 12.50)
    api.set_market_price("600519.SH", 1800.00)
    print(f"000001.SZ: ¥12.50")
    print(f"600519.SH: ¥1800.00")

    # 买入测试
    print("\n[3] 买入测试")
    print("-" * 50)
    order1 = api.submit_order("000001.SZ", Side.BUY, 1000, "market")
    print(f"订单：{order1.to_dict()}")

    order2 = api.submit_order("600519.SH", Side.BUY, 100, "market")
    print(f"订单：{order2.to_dict()}")

    # 查询持仓
    print("\n[4] 查询持仓")
    print("-" * 50)
    positions = api.get_positions()
    for pos in positions:
        print(f"{pos.ts_code}: {pos.shares}股，成本¥{pos.cost_basis:.4f}")

    # 更新价格
    print("\n[5] 更新价格")
    print("-" * 50)
    api.set_market_price("000001.SZ", 13.00)
    api.set_market_price("600519.SH", 1750.00)
    print("000001.SZ: ¥12.50 -> ¥13.00")
    print("600519.SH: ¥1800.00 -> ¥1750.00")

    # 查询持仓盈亏
    print("\n[6] 查询持仓盈亏")
    print("-" * 50)
    positions = api.get_positions()
    for pos in positions:
        print(f"{pos.ts_code}:")
        print(f"  持仓：{pos.shares}股")
        print(f"  成本：¥{pos.cost_basis:.4f}")
        print(f"  现价：¥{pos.current_price:.4f}")
        print(f"  盈亏：¥{pos.unrealized_pnl:.2f} ({pos.unrealized_pnl_pct:.2f}%)")

    # 卖出测试
    print("\n[7] 卖出测试")
    print("-" * 50)
    order3 = api.submit_order("000001.SZ", Side.SELL, 500, "market")
    print(f"订单：{order3.to_dict()}")

    # 投资组合总览
    print("\n[8] 投资组合总览")
    print("-" * 50)
    portfolio = api.get_portfolio_value()
    print(f"总资产：¥{portfolio['total_value']:,.2f}")
    print(f"总盈亏：¥{portfolio['total_pnl']:,.2f} ({portfolio['total_pnl_pct']:.2f}%)")
    print("\n持仓明细:")
    for pos in portfolio['positions']:
        print(f"  {pos['ts_code']}: {pos['shares']}股，盈亏¥{pos['unrealized_pnl']:,.2f} ({pos['unrealized_pnl_pct']:.2f}%)")

    # 成交记录
    print("\n[9] 成交记录")
    print("-" * 50)
    for trade in api.trades:
        print(f"{trade['trade_id']}: {trade['ts_code']} {trade['side']} {trade['shares']}股 "
              f"@ ¥{trade['price']:.4f}")

    print("\n" + "=" * 90)
    print("模拟交易 API 测试完成!")
    print("=" * 90)


if __name__ == "__main__":
    main()
