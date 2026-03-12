#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
执行算法模块
实现智能订单执行算法，减少市场冲击成本

包含算法：
1. VWAP - 成交量加权平均价格执行
2. TWAP - 时间加权平均价格执行
3. Iceberg - 冰山订单（隐藏大单）
4. POV - 成交量参与率算法
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
from dataclasses import dataclass, field
from enum import Enum


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    VWAP = "vwap"
    TWAP = "twap"
    ICEBERG = "iceberg"
    POV = "pov"


@dataclass
class Order:
    """订单数据类"""
    ts_code: str
    side: OrderSide
    total_shares: int
    order_type: OrderType
    limit_price: float = None
    start_time: datetime = None
    end_time: datetime = None
    participation_rate: float = 0.1  # POV 参与率
    max_display_shares: int = None  # 冰山订单最大显示数量

    # 执行状态
    executed_shares: int = 0
    executed_amount: float = 0
    avg_price: float = 0
    status: str = "pending"

    # 执行记录
    child_orders: List[Dict] = field(default_factory=list)

    def is_complete(self) -> bool:
        return self.executed_shares >= self.total_shares

    def remaining_shares(self) -> int:
        return max(0, self.total_shares - self.executed_shares)


@dataclass
class ExecutionReport:
    """执行报告"""
    order: Order
    total_executed_shares: int
    total_executed_amount: float
    avg_execution_price: float
    benchmark_vwap: float = None
    slippage_bps: float = 0  # 滑点（基点）
    execution_rate: float = 0  # 执行率
    market_impact: float = 0  # 市场冲击成本
    timing_cost: float = 0  # 时机成本

    def to_dict(self) -> Dict:
        return {
            "ts_code": self.order.ts_code,
            "side": self.order.side.value,
            "order_type": self.order.order_type.value,
            "total_shares": self.order.total_shares,
            "executed_shares": self.total_executed_shares,
            "executed_amount": self.total_executed_amount,
            "avg_price": round(self.avg_execution_price, 4),
            "benchmark_vwap": round(self.benchmark_vwap, 4) if self.benchmark_vwap else None,
            "slippage_bps": round(self.slippage_bps, 2),
            "execution_rate": round(self.execution_rate, 4),
            "market_impact": round(self.market_impact, 4),
            "status": self.order.status
        }


class VWAPExecutor:
    """
    VWAP 执行算法

    原理：按照历史成交量分布执行订单，使执行价格接近市场 VWAP
    """

    def __init__(self, lookback_days: int = 20):
        """
        初始化 VWAP 执行器

        Args:
            lookback_days: 回溯天数（用于计算成交量分布）
        """
        self.lookback_days = lookback_days
        logger.info(f"VWAP 执行器初始化：lookback_days={lookback_days}")

    def calculate_volume_profile(self, historical_data: pd.DataFrame) -> pd.Series:
        """
        计算成交量分布

        Args:
            historical_data: 历史数据（包含 intraday_volume 列，按时间段）

        Returns:
            成交量占比序列
        """
        # 计算平均成交量分布
        if 'period_volume' in historical_data.columns and 'period' in historical_data.columns:
            volume_profile = historical_data.groupby('period')['period_volume'].mean()
        else:
            # 使用成交量模拟分布
            volumes = historical_data['vol'].dropna() if 'vol' in historical_data.columns else pd.Series([10000] * 20)
            # 分成 10 个时段
            n_periods = 10
            chunk_size = max(1, len(volumes) // n_periods)
            volume_profile = {}
            for i in range(n_periods):
                start = i * chunk_size
                end = start + chunk_size if i < n_periods - 1 else len(volumes)
                volume_profile[i] = volumes.iloc[start:end].mean()
            volume_profile = pd.Series(volume_profile)

        # 归一化
        if volume_profile.sum() > 0:
            volume_profile = volume_profile / volume_profile.sum()
        else:
            # 均匀分布
            volume_profile = pd.Series([1.0 / len(volume_profile)] * len(volume_profile))

        return volume_profile

    def generate_schedules(
        self,
        order: Order,
        historical_data: pd.DataFrame,
        current_price: float
    ) -> List[Dict]:
        """
        生成 VWAP 执行计划

        Args:
            order: 订单
            historical_data: 历史数据
            current_price: 当前价格

        Returns:
            子订单列表
        """
        # 计算成交量分布
        volume_profile = self.calculate_volume_profile(historical_data)

        # 生成执行计划
        schedules = []
        remaining = order.total_shares

        for i, (period, vol_pct) in enumerate(volume_profile.items()):
            if i == len(volume_profile) - 1:
                # 最后一个时段执行剩余所有
                shares = remaining
            else:
                shares = int(order.total_shares * vol_pct)
                shares = max(100, shares)  # 至少 100 股

            shares = min(shares, remaining)
            remaining -= shares

            if shares > 0:
                schedules.append({
                    "period": period,
                    "shares": shares,
                    "side": order.side.value,
                    "order_type": "market"
                })

        return schedules

    def execute(
        self,
        order: Order,
        market_data: pd.DataFrame,
        current_price: float
    ) -> ExecutionReport:
        """
        执行 VWAP 算法

        Args:
            order: 订单
            market_data: 市场数据
            current_price: 当前价格

        Returns:
            执行报告
        """
        logger.info(f"开始 VWAP 执行：{order.ts_code}, {order.total_shares}股, {order.side.value}")

        executed_shares = 0
        executed_amount = 0
        prices = []
        volumes = []

        # 获取成交量分布
        volume_profile = self.calculate_volume_profile(market_data)

        # 模拟执行
        for period, vol_pct in volume_profile.items():
            # 计算该时段应执行的数量
            target_shares = int(order.total_shares * vol_pct)
            target_shares = max(100, min(target_shares, order.remaining_shares() - executed_shares))

            if target_shares <= 0:
                continue

            # 获取该时段的市场价格（模拟）
            period_price = current_price * (1 + np.random.randn() * 0.001)

            # 执行
            exec_price = period_price
            if order.side == OrderSide.BUY:
                exec_price = period_price * (1 + 0.0005)  # 买入略有滑点
            else:
                exec_price = period_price * (1 - 0.0005)  # 卖出略有滑点

            exec_amount = target_shares * exec_price

            executed_shares += target_shares
            executed_amount += exec_amount
            prices.append(exec_price)
            volumes.append(target_shares)

            order.child_orders.append({
                "period": period,
                "shares": target_shares,
                "price": round(exec_price, 4),
                "amount": round(exec_amount, 2),
                "time": datetime.now().isoformat()
            })

        # 计算执行均价
        avg_price = executed_amount / executed_shares if executed_shares > 0 else 0

        # 计算基准 VWAP
        benchmark_vwap = sum(p * v for p, v in zip(prices, volumes)) / sum(volumes) if volumes else current_price

        # 计算滑点
        slippage_bps = (avg_price - benchmark_vwap) / benchmark_vwap * 10000
        if order.side == OrderSide.SELL:
            slippage_bps = -slippage_bps

        # 更新订单状态
        order.executed_shares = executed_shares
        order.executed_amount = executed_amount
        order.avg_price = avg_price
        order.status = "completed" if executed_shares >= order.total_shares else "partial"

        report = ExecutionReport(
            order=order,
            total_executed_shares=executed_shares,
            total_executed_amount=executed_amount,
            avg_execution_price=avg_price,
            benchmark_vwap=benchmark_vwap,
            slippage_bps=slippage_bps,
            execution_rate=executed_shares / order.total_shares,
            market_impact=abs(slippage_bps)
        )

        logger.info(f"VWAP 执行完成：执行{executed_shares}股，均价{avg_price:.4f}，滑点{slippage_bps:.2f}bps")
        return report


class TWAPExecutor:
    """
    TWAP 执行算法

    原理：在指定时间段内均匀执行订单
    """

    def __init__(self, num_slices: int = 12, slice_interval_minutes: int = 5):
        """
        初始化 TWAP 执行器

        Args:
            num_slices: 切片数量
            slice_interval_minutes: 切片间隔（分钟）
        """
        self.num_slices = num_slices
        self.slice_interval_minutes = slice_interval_minutes
        logger.info(f"TWAP 执行器初始化：num_slices={num_slices}, interval={slice_interval_minutes}min")

    def generate_schedules(self, order: Order) -> List[Dict]:
        """
        生成 TWAP 执行计划

        Args:
            order: 订单

        Returns:
            子订单列表
        """
        # 计算每片数量
        shares_per_slice = order.total_shares // self.num_slices
        remainder = order.total_shares % self.num_slices

        schedules = []
        for i in range(self.num_slices):
            shares = shares_per_slice
            if i == self.num_slices - 1:
                shares += remainder  # 最后一片包含余数

            # 计算执行时间
            if order.start_time:
                exec_time = order.start_time + timedelta(minutes=i * self.slice_interval_minutes)
            else:
                exec_time = datetime.now() + timedelta(minutes=i * self.slice_interval_minutes)

            schedules.append({
                "slice": i,
                "shares": shares,
                "side": order.side.value,
                "order_type": "market",
                "exec_time": exec_time.isoformat()
            })

        return schedules

    def execute(
        self,
        order: Order,
        market_data: pd.DataFrame,
        current_price: float
    ) -> ExecutionReport:
        """
        执行 TWAP 算法

        Args:
            order: 订单
            market_data: 市场数据
            current_price: 当前价格

        Returns:
            执行报告
        """
        logger.info(f"开始 TWAP 执行：{order.ts_code}, {order.total_shares}股，分{self.num_slices}片")

        # 计算每片数量
        shares_per_slice = order.total_shares // self.num_slices
        remainder = order.total_shares % self.num_slices

        executed_shares = 0
        executed_amount = 0
        prices = []

        for i in range(self.num_slices):
            # 计算本片数量
            shares = shares_per_slice
            if i == self.num_slices - 1:
                shares += remainder

            # 模拟价格变化（随机游走）
            price_change = np.random.randn() * 0.0005
            exec_price = current_price * (1 + price_change * (i - self.num_slices / 2) / self.num_slices)

            # 应用滑点
            if order.side == OrderSide.BUY:
                exec_price *= (1 + 0.0003)
            else:
                exec_price *= (1 - 0.0003)

            exec_amount = shares * exec_price

            executed_shares += shares
            executed_amount += exec_amount
            prices.append(exec_price)

            order.child_orders.append({
                "slice": i,
                "shares": shares,
                "price": round(exec_price, 4),
                "amount": round(exec_amount, 2),
                "time": datetime.now().isoformat()
            })

        # 计算执行均价
        avg_price = executed_amount / executed_shares if executed_shares > 0 else 0

        # 计算基准价格（期间平均价）
        benchmark_price = np.mean(prices)

        # 计算滑点
        slippage_bps = (avg_price - benchmark_price) / benchmark_price * 10000
        if order.side == OrderSide.SELL:
            slippage_bps = -slippage_bps

        # 更新订单状态
        order.executed_shares = executed_shares
        order.executed_amount = executed_amount
        order.avg_price = avg_price
        order.status = "completed"

        report = ExecutionReport(
            order=order,
            total_executed_shares=executed_shares,
            total_executed_amount=executed_amount,
            avg_execution_price=avg_price,
            benchmark_vwap=benchmark_price,
            slippage_bps=slippage_bps,
            execution_rate=1.0,
            market_impact=abs(slippage_bps),
            timing_cost=0.0
        )

        logger.info(f"TWAP 执行完成：执行{executed_shares}股，均价{avg_price:.4f}，滑点{slippage_bps:.2f}bps")
        return report


class IcebergExecutor:
    """
    冰山订单执行算法

    原理：将大单拆分为小单执行，隐藏真实订单规模
    """

    def __init__(self, display_ratio: float = 0.1, refresh_threshold: float = 0.5):
        """
        初始化冰山执行器

        Args:
            display_ratio: 显示比例（每次显示多少）
            refresh_threshold: 补单阈值（剩余多少时补单）
        """
        self.display_ratio = display_ratio
        self.refresh_threshold = refresh_threshold
        logger.info(f"冰山执行器初始化：display_ratio={display_ratio}")

    def execute(
        self,
        order: Order,
        market_data: pd.DataFrame,
        current_price: float
    ) -> ExecutionReport:
        """
        执行冰山算法

        Args:
            order: 订单
            market_data: 市场数据
            current_price: 当前价格

        Returns:
            执行报告
        """
        logger.info(f"开始冰山执行：{order.ts_code}, {order.total_shares}股，显示比例{self.display_ratio}")

        # 计算显示数量
        display_shares = max(100, int(order.total_shares * self.display_ratio))

        executed_shares = 0
        executed_amount = 0
        prices = []
        order_count = 0

        while executed_shares < order.total_shares:
            # 计算本片数量
            remaining = order.total_shares - executed_shares
            current_shares = min(display_shares, remaining)

            # 模拟执行价格
            price_impact = 0.0001 * order_count  # 订单越多，冲击越大
            exec_price = current_price * (1 + price_impact)

            if order.side == OrderSide.BUY:
                exec_price *= (1 + 0.0002)
            else:
                exec_price *= (1 - 0.0002)

            exec_amount = current_shares * exec_price

            executed_shares += current_shares
            executed_amount += exec_amount
            prices.append(exec_price)
            order_count += 1

            order.child_orders.append({
                "iceberg_slice": order_count,
                "shares": current_shares,
                "price": round(exec_price, 4),
                "amount": round(exec_amount, 2),
                "visible": True,
                "time": datetime.now().isoformat()
            })

        # 计算执行均价
        avg_price = executed_amount / executed_shares if executed_shares > 0 else 0

        # 计算基准价格
        benchmark_price = current_price

        # 计算滑点和市场冲击
        slippage_bps = (avg_price - benchmark_price) / benchmark_price * 10000
        if order.side == OrderSide.SELL:
            slippage_bps = -slippage_bps

        # 更新订单状态
        order.executed_shares = executed_shares
        order.executed_amount = executed_amount
        order.avg_price = avg_price
        order.status = "completed"

        report = ExecutionReport(
            order=order,
            total_executed_shares=executed_shares,
            total_executed_amount=executed_amount,
            avg_execution_price=avg_price,
            benchmark_vwap=benchmark_price,
            slippage_bps=slippage_bps,
            execution_rate=1.0,
            market_impact=abs(slippage_bps) * 0.5  # 冰山订单减少冲击
        )

        logger.info(f"冰山执行完成：执行{executed_shares}股，分{order_count}片，均价{avg_price:.4f}")
        return report


class POVExecutor:
    """
    POV 执行算法（成交量参与率）

    原理：按照市场成交量的一定比例参与交易
    """

    def __init__(self, participation_rate: float = 0.1, max_active_rate: float = 0.25):
        """
        初始化 POV 执行器

        Args:
            participation_rate: 目标参与率
            max_active_rate: 最大活跃率（不超过市场成交量的比例）
        """
        self.participation_rate = participation_rate
        self.max_active_rate = max_active_rate
        logger.info(f"POV 执行器初始化：participation_rate={participation_rate}")

    def execute(
        self,
        order: Order,
        market_data: pd.DataFrame,
        current_price: float,
        market_volumes: List[int] = None
    ) -> ExecutionReport:
        """
        执行 POV 算法

        Args:
            order: 订单
            market_data: 市场数据
            current_price: 当前价格
            market_volumes: 市场成交量列表

        Returns:
            执行报告
        """
        logger.info(f"开始 POV 执行：{order.ts_code}, {order.total_shares}股，参与率{self.participation_rate}")

        # 生成模拟市场成交量
        if market_volumes is None:
            market_volumes = [np.random.randint(10000, 100000) for _ in range(50)]

        executed_shares = 0
        executed_amount = 0
        prices = []

        for i, market_vol in enumerate(market_volumes):
            if executed_shares >= order.total_shares:
                break

            # 计算本片可执行数量
            max_participate = int(market_vol * self.max_active_rate)
            target_participate = int(market_vol * self.participation_rate)
            participate = min(target_participate, max_participate)

            # 不超过剩余数量
            remaining = order.total_shares - executed_shares
            participate = min(participate, remaining)

            if participate < 100:
                continue

            # 模拟执行价格
            price_change = np.random.randn() * 0.0003
            exec_price = current_price * (1 + price_change)

            if order.side == OrderSide.BUY:
                exec_price *= (1 + 0.0002)
            else:
                exec_price *= (1 - 0.0002)

            exec_amount = participate * exec_price

            executed_shares += participate
            executed_amount += exec_amount
            prices.append(exec_price)

            order.child_orders.append({
                "period": i,
                "market_volume": market_vol,
                "participate": participate,
                "participation_rate": round(participate / market_vol, 4),
                "price": round(exec_price, 4),
                "amount": round(exec_amount, 2),
                "time": datetime.now().isoformat()
            })

        # 计算执行均价
        avg_price = executed_amount / executed_shares if executed_shares > 0 else 0

        # 计算基准价格
        benchmark_price = np.mean(prices) if prices else current_price

        # 计算滑点
        slippage_bps = (avg_price - benchmark_price) / benchmark_price * 10000
        if order.side == OrderSide.SELL:
            slippage_bps = -slippage_bps

        # 更新订单状态
        order.executed_shares = executed_shares
        order.executed_amount = executed_amount
        order.avg_price = avg_price
        order.status = "completed" if executed_shares >= order.total_shares else "partial"

        report = ExecutionReport(
            order=order,
            total_executed_shares=executed_shares,
            total_executed_amount=executed_amount,
            avg_execution_price=avg_price,
            benchmark_vwap=benchmark_price,
            slippage_bps=slippage_bps,
            execution_rate=executed_shares / order.total_shares,
            market_impact=abs(slippage_bps) * 0.7  # POV 适度减少冲击
        )

        logger.info(f"POV 执行完成：执行{executed_shares}股，均价{avg_price:.4f}，滑点{slippage_bps:.2f}bps")
        return report


class SmartOrderExecutor:
    """
    智能订单执行器

    根据订单特征自动选择最优执行算法
    """

    def __init__(self):
        """初始化智能执行器"""
        self.vwap_executor = VWAPExecutor()
        self.twap_executor = TWAPExecutor()
        self.iceberg_executor = IcebergExecutor()
        self.pov_executor = POVExecutor()
        logger.info("智能订单执行器初始化完成")

    def select_algorithm(self, order: Order, avg_daily_volume: int) -> str:
        """
        根据订单特征选择执行算法

        Args:
            order: 订单
            avg_daily_volume: 日均成交量

        Returns:
            算法名称
        """
        # 计算订单占日均成交量的比例
        volume_ratio = order.total_shares / avg_daily_volume if avg_daily_volume > 0 else 0

        if order.order_type != OrderType.MARKET:
            return order.order_type.value

        # 小单：直接市价单
        if volume_ratio < 0.01:
            return "market"

        # 中等订单：TWAP
        if volume_ratio < 0.05:
            return "twap"

        # 大单：VWAP
        if volume_ratio < 0.2:
            return "vwap"

        # 超大单：冰山订单
        if volume_ratio < 0.5:
            return "iceberg"

        # 巨大单：POV
        return "pov"

    def execute(
        self,
        order: Order,
        market_data: pd.DataFrame,
        current_price: float,
        avg_daily_volume: int = None
    ) -> ExecutionReport:
        """
        智能执行订单

        Args:
            order: 订单
            market_data: 市场数据
            current_price: 当前价格
            avg_daily_volume: 日均成交量

        Returns:
            执行报告
        """
        # 自动选择算法
        if avg_daily_volume is None:
            avg_daily_volume = market_data['vol'].mean() * 3 if 'vol' in market_data.columns else 100000

        algorithm = self.select_algorithm(order, avg_daily_volume)
        logger.info(f"智能选择执行算法：{algorithm}")

        # 执行
        if algorithm == "vwap":
            return self.vwap_executor.execute(order, market_data, current_price)
        elif algorithm == "twap":
            return self.twap_executor.execute(order, market_data, current_price)
        elif algorithm == "iceberg":
            return self.iceberg_executor.execute(order, market_data, current_price)
        elif algorithm == "pov":
            return self.pov_executor.execute(order, market_data, current_price)
        else:
            # 市价单：立即执行
            order.executed_shares = order.total_shares
            order.executed_amount = order.total_shares * current_price
            order.avg_price = current_price
            order.status = "completed"

            return ExecutionReport(
                order=order,
                total_executed_shares=order.total_shares,
                total_executed_amount=order.executed_amount,
                avg_execution_price=current_price,
                benchmark_vwap=current_price,
                slippage_bps=0,
                execution_rate=1.0,
                market_impact=0
            )


def main():
    """测试函数"""
    print("=" * 90)
    print("执行算法测试")
    print("=" * 90)

    # 创建测试数据
    np.random.seed(42)
    dates = pd.date_range('2026-03-01', periods=50, freq='5min')
    market_data = pd.DataFrame({
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'close': 100 * np.cumprod(1 + np.random.randn(50) * 0.001),
        'vol': np.random.randint(10000, 100000, 50)
    })
    current_price = 100.0

    # 1. VWAP 测试
    print("\n[1] VWAP 算法测试")
    print("-" * 50)
    vwap_order = Order(
        ts_code="000001.SZ",
        side=OrderSide.BUY,
        total_shares=10000,
        order_type=OrderType.VWAP
    )
    vwap_executor = VWAPExecutor()
    vwap_report = vwap_executor.execute(vwap_order, market_data, current_price)
    print(f"执行结果：{vwap_report.to_dict()}")

    # 2. TWAP 测试
    print("\n[2] TWAP 算法测试")
    print("-" * 50)
    twap_order = Order(
        ts_code="000001.SZ",
        side=OrderSide.SELL,
        total_shares=10000,
        order_type=OrderType.TWAP
    )
    twap_executor = TWAPExecutor(num_slices=10)
    twap_report = twap_executor.execute(twap_order, market_data, current_price)
    print(f"执行结果：{twap_report.to_dict()}")

    # 3. 冰山订单测试
    print("\n[3] 冰山订单测试")
    print("-" * 50)
    iceberg_order = Order(
        ts_code="000001.SZ",
        side=OrderSide.BUY,
        total_shares=50000,
        order_type=OrderType.ICEBERG
    )
    iceberg_executor = IcebergExecutor(display_ratio=0.1)
    iceberg_report = iceberg_executor.execute(iceberg_order, market_data, current_price)
    print(f"执行结果：{iceberg_report.to_dict()}")
    print(f"子订单数：{len(iceberg_order.child_orders)}")

    # 4. POV 测试
    print("\n[4] POV 算法测试")
    print("-" * 50)
    pov_order = Order(
        ts_code="000001.SZ",
        side=OrderSide.SELL,
        total_shares=30000,
        order_type=OrderType.POV,
        participation_rate=0.15
    )
    pov_executor = POVExecutor(participation_rate=0.15)
    pov_report = pov_executor.execute(pov_order, market_data, current_price)
    print(f"执行结果：{pov_report.to_dict()}")

    # 5. 智能执行器测试
    print("\n[5] 智能执行器测试")
    print("-" * 50)
    smart_executor = SmartOrderExecutor()

    # 测试不同规模的订单
    for shares, name in [(500, "小单"), (5000, "中单"), (20000, "大单"), (100000, "超大单")]:
        order = Order(
            ts_code="000001.SZ",
            side=OrderSide.BUY,
            total_shares=shares,
            order_type=OrderType.MARKET
        )
        algo = smart_executor.select_algorithm(order, avg_daily_volume=500000)
        report = smart_executor.execute(order, market_data, current_price, avg_daily_volume=500000)
        print(f"{name} ({shares}股) -> 算法：{algo}, 滑点：{report.slippage_bps:.2f}bps")

    print("\n" + "=" * 90)
    print("执行算法测试完成!")
    print("=" * 90)


if __name__ == "__main__":
    main()
