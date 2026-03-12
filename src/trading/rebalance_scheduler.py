#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自动调仓调度器
定期执行再平衡和风险管理
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger
import os
import sys
import json

# 添加 src 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import get_database
from risk.risk_manager import RiskManager
from risk.transaction_cost import TransactionCostModel
from risk.signal_generator import SignalGenerator
from trading.order_manager import Order, OrderManager, print_portfolio_summary
from strategy.market_regime import MarketRegimeDetector
from models.stock_predictor import StockPredictor
from data.stock_screener import StockScreener


class RebalanceScheduler:
    """
    自动调仓调度器

    功能：
    1. 定期再平衡（每周/每月）
    2. 触发式调仓（止损/止盈）
    3. 市场状态自适应
    """

    def __init__(
        self,
        initial_capital: float = 1000000.0,
        rebalance_frequency: str = 'weekly'  # 'daily', 'weekly', 'monthly'
    ):
        """
        初始化调度器

        Args:
            initial_capital: 初始资金
            rebalance_frequency: 再平衡频率
        """
        self.initial_capital = initial_capital
        self.rebalance_frequency = rebalance_frequency
        self.order_manager = OrderManager(initial_capital)
        self.risk_manager = RiskManager()
        self.signal_generator = SignalGenerator(self.risk_manager)
        self.market_detector = MarketRegimeDetector()
        self.screener = StockScreener()

        self.last_rebalance = None
        self.rebalance_count = 0

        logger.info(f"调仓调度器初始化完成，再平衡频率：{rebalance_frequency}")

    def should_rebalance(self) -> bool:
        """判断是否应该再平衡"""
        now = datetime.now()

        if self.last_rebalance is None:
            return True

        if self.rebalance_frequency == 'daily':
            return (now - self.last_rebalance).days >= 1
        elif self.rebalance_frequency == 'weekly':
            # 每周再平衡（假设每 5 个交易日）
            return (now - self.last_rebalance).days >= 5
        elif self.rebalance_frequency == 'monthly':
            return (now - self.last_rebalance).days >= 21

        return False

    def run_rebalance(
        self,
        concept_codes: List[str] = None,
        top_n: int = 10
    ) -> Dict:
        """
        执行再平衡

        Args:
            concept_codes: 看好的板块列表
            top_n: 持仓股票数量

        Returns:
            再平衡结果
        """
        logger.info("开始执行再平衡...")

        # Step 1: 获取当前市场状态
        market_regime = self.market_detector.identify_regime()
        regime = market_regime.get('regime', 'sideways')

        logger.info(f"当前市场状态：{market_regime.get('regime_name', '未知')}")

        # Step 2: 根据市场状态调整策略
        strategy_suggestion = self.market_detector.get_strategy_suggestion(regime)
        logger.info(f"策略建议：{strategy_suggestion['strategy']}")

        # Step 3: 筛选股票
        if concept_codes is None:
            # 使用默认板块
            concept_codes = ['885311.TI', '885368.TI', '885394.TI']

        screened_stocks = self.screener.get_top_stocks(concept_codes, top_n=top_n * 2)

        if screened_stocks.empty:
            logger.warning("筛选结果为空，跳过再平衡")
            return {'success': False, 'reason': 'no_stocks'}

        logger.info(f"筛选出 {len(screened_stocks)} 只股票")

        # Step 4: 生成目标权重
        # 简化：等权重配置
        target_weight = 1.0 / top_n
        target_positions = screened_stocks.head(top_n).copy()
        target_positions['target_weight'] = target_weight

        # 列名映射：stock_code -> ts_code, stock_score -> combined_score
        if 'stock_code' in target_positions.columns:
            target_positions = target_positions.rename(columns={'stock_code': 'ts_code'})
        if 'stock_score' in target_positions.columns:
            target_positions = target_positions.rename(columns={'stock_score': 'combined_score'})

        # 构造预测数据（使用综合得分作为预测代理）
        if 'pred_1d' not in target_positions.columns:
            # 使用 stock_score 作为预测代理
            target_positions['pred_1d'] = target_positions['combined_score'] * 0.05
            target_positions['pred_5d'] = target_positions['combined_score'] * 0.15

        # Step 5: 生成交易信号
        signals = self.signal_generator.generate_signals(
            target_positions[['ts_code', 'pred_1d', 'pred_5d', 'combined_score']]
        )

        # Step 6: 生成订单
        # 获取当前价格（从筛选结果中）
        if 'close' in target_positions.columns:
            prices = target_positions[['ts_code', 'close']].copy()
        else:
            # 使用数据库获取最新价格
            prices = self._get_current_prices(target_positions['ts_code'].tolist())

        orders = self.order_manager.generate_orders_from_signals(signals, prices)

        # Step 7: 执行订单
        fills = []
        for order in orders:
            if self.order_manager.submit_order(order):
                fill = self.order_manager.execute_order(order.order_id)
                if fill:
                    fills.append(fill)

        # Step 8: 更新再平衡时间
        self.last_rebalance = datetime.now()
        self.rebalance_count += 1

        # Step 9: 生成报告
        summary = self.order_manager.get_portfolio_summary()

        result = {
            'success': True,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'rebalance_count': self.rebalance_count,
            'market_regime': regime,
            'stocks_selected': len(target_positions),
            'orders_generated': len(orders),
            'orders_filled': len(fills),
            'portfolio_summary': summary
        }

        logger.info(f"再平衡完成：{len(fills)} 笔成交")
        return result

    def _get_current_prices(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取当前价格"""
        db = get_database()
        latest_date = db.get_latest_date()

        if latest_date is None:
            return pd.DataFrame()

        all_data = []
        for code in stock_codes:
            df = db.get_stock_data(code, latest_date, latest_date)
            if not df.empty:
                all_data.append(df[['ts_code', 'trade_date', 'close']])

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def run_stop_loss_check(self) -> List[Dict]:
        """
        执行止损检查

        Returns:
            止损订单列表
        """
        logger.info("执行止损检查...")

        # 获取当前持仓
        summary = self.order_manager.get_portfolio_summary()

        if not summary['positions']:
            logger.info("无持仓，跳过止损检查")
            return []

        # 获取当前价格
        stock_codes = [p['ts_code'] for p in summary['positions']]
        prices = self._get_current_prices(stock_codes)

        if prices.empty:
            logger.warning("无法获取价格数据")
            return []

        # 构建持仓列表
        positions = []
        for pos in summary['positions']:
            price_row = prices[prices['ts_code'] == pos['ts_code']]
            current_price = price_row.iloc[0]['close'] if not price_row.empty else pos['market_value'] / pos['shares']

            positions.append({
                'ts_code': pos['ts_code'],
                'shares': pos['shares'],
                'cost_price': pos['cost_price'],
                'current_price': current_price,
                'highest_price': current_price  # 简化处理
            })

        # 检查止损
        stop_loss_orders = self.risk_manager.check_stop_loss(
            positions,
            prices,
            stop_loss_type='trailing',
            trailing_stop_loss_pct=0.10
        )

        # 生成止损订单
        orders = []
        for sl in stop_loss_orders:
            order = Order(sl['ts_code'], 'sell', sl['shares'], sl['current_price'])
            orders.append(order)
            logger.info(f"触发止损：{sl['ts_code']} ({sl['reason']})")

        return orders

    def get_status(self) -> Dict:
        """获取调度器状态"""
        return {
            'initial_capital': self.initial_capital,
            'rebalance_frequency': self.rebalance_frequency,
            'last_rebalance': self.last_rebalance.strftime('%Y-%m-%d %H:%M:%S') if self.last_rebalance else None,
            'rebalance_count': self.rebalance_count,
            'current_portfolio': self.order_manager.get_portfolio_summary()
        }


def print_rebalance_report(result: Dict):
    """打印再平衡报告"""
    print("\n" + "=" * 70)
    print("再平衡执行报告")
    print("=" * 70)

    if not result.get('success'):
        print(f"再平衡失败：{result.get('reason', '未知原因')}")
        return

    print(f"执行时间：{result['timestamp']}")
    print(f"再平衡次数：{result['rebalance_count']}")
    print(f"市场状态：{result['market_regime']}")
    print(f"选股数量：{result['stocks_selected']}")
    print(f"订单数量：{result['orders_generated']} 生成，{result['orders_filled']} 成交")

    # 组合汇总
    summary = result.get('portfolio_summary', {})
    print(f"\n总资产：¥{summary.get('total_assets', 0):,.2f}")
    print(f"可用资金：¥{summary.get('available_capital', 0):,.2f}")
    print(f"持仓市值：¥{summary.get('market_value', 0):,.2f}")
    print(f"仓位：{summary.get('position_ratio', 0):.1%}")

    if summary.get('positions'):
        print("\n【持仓明细】")
        for pos in summary['positions']:
            print(f"  {pos['ts_code']}: {pos['shares']}股，"
                  f"权重{pos['weight']:.1%}, "
                  f"市值¥{pos['market_value']:,.2f}")

    print("\n" + "=" * 70)


def main():
    """主函数"""
    print("=" * 70)
    print("自动调仓调度器测试")
    print("=" * 70)

    # 初始化调度器
    scheduler = RebalanceScheduler(
        initial_capital=1000000,
        rebalance_frequency='weekly'
    )

    # 执行再平衡
    print("\n【执行再平衡】")
    result = scheduler.run_rebalance(
        concept_codes=['885311.TI', '885368.TI', '885394.TI'],
        top_n=10
    )

    # 打印报告
    print_rebalance_report(result)

    # 显示状态
    print("\n【调度器状态】")
    status = scheduler.get_status()
    print(f"再平衡频率：{status['rebalance_frequency']}")
    print(f"上次再平衡：{status['last_rebalance']}")
    print(f"累计再平衡：{status['rebalance_count']} 次")

    # 测试止损检查
    print("\n【止损检查】")
    stop_loss = scheduler.run_stop_loss_check()
    if stop_loss:
        print(f"触发 {len(stop_loss)} 只股票止损")
    else:
        print("无止损触发")


if __name__ == "__main__":
    main()
