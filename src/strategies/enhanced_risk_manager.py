"""
增强风险管理模块
实现动态止损止盈、风险预警、风险预算分配
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger
from dataclasses import dataclass
from enum import Enum
import sys
import os

from ..data.database import SQLiteDatabase, get_database


class AlertType(Enum):
    """预警类型"""
    STOP_LOSS = 'stop_loss'
    TAKE_PROFIT = 'take_profit'
    DRAWDOWN = 'drawdown'
    CONCENTRATION = 'concentration'
    VOLATILITY = 'volatility'


@dataclass
class Alert:
    """风险预警"""
    type: AlertType
    position_code: str
    position_name: str = ''
    current_price: float = 0.0
    target_price: float = 0.0
    pnl_pct: float = 0.0
    message: str = ''
    severity: str = 'warning'  # warning, critical


class EnhancedRiskManager:
    """
    增强风险管理器

    功能：
    1. 动态止损止盈计算
    2. 移动止损（盈利后调整止损线）
    3. 波动率调整止损
    4. 风险预警检查
    """

    # 止损配置
    STOP_LOSS_CONFIGS = {
        'conservative': {'stop_loss': -0.05, 'take_profit': 0.10},
        'default': {'stop_loss': -0.08, 'take_profit': 0.15},
        'aggressive': {'stop_loss': -0.12, 'take_profit': 0.25},
    }

    def __init__(
        self,
        db: SQLiteDatabase = None,
        default_stop_loss: float = -0.08,
        default_take_profit: float = 0.15,
        trailing_stop_enabled: bool = True,
        profit_trailing_threshold: float = 0.05,
        high_vol_threshold: float = 0.30
    ):
        """
        初始化增强风险管理器

        Args:
            db: 数据库实例
            default_stop_loss: 默认止损比例
            default_take_profit: 默认止盈比例
            trailing_stop_enabled: 是否启用移动止损
            profit_trailing_threshold: 启动移动止损的盈利阈值
            high_vol_threshold: 高波动率阈值
        """
        self.db = db or get_database()
        self.default_stop_loss = default_stop_loss
        self.default_take_profit = default_take_profit
        self.trailing_stop_enabled = trailing_stop_enabled
        self.profit_trailing_threshold = profit_trailing_threshold
        self.high_vol_threshold = high_vol_threshold

        logger.info(f"增强风险管理器初始化: 止损={default_stop_loss:.0%}, 止盈={default_take_profit:.0%}")

    def calculate_stop_loss(
        self,
        position: Dict,
        market_state: str = 'SIDEWAYS',
        volatility: float = 0.20
    ) -> Dict:
        """
        动态止损计算

        规则:
        1. 基础止损 = 配置比例（根据市场状态）
        2. 盈利后移动止损:
           - 盈利 > 5% → 止损移到成本价
           - 盈利 > 10% → 止损移到 +5%
        3. 高波动市场放宽止损幅度

        Args:
            position: 持仓信息
                - ts_code, cost_price, current_price, shares
                - highest_price (可选，用于移动止损)
            market_state: 市场状态
            volatility: 波动率

        Returns:
            止损信息字典
        """
        cost_price = position.get('cost_price', 0)
        current_price = position.get('current_price', cost_price)
        highest_price = position.get('highest_price', current_price)

        if cost_price <= 0:
            return {'stop_price': 0, 'stop_pct': self.default_stop_loss}

        # 计算盈亏比例
        profit_pct = (current_price - cost_price) / cost_price

        # 根据市场状态选择止损配置
        if market_state == 'BULL':
            config = self.STOP_LOSS_CONFIGS['aggressive']
        elif market_state == 'BEAR':
            config = self.STOP_LOSS_CONFIGS['conservative']
        else:
            config = self.STOP_LOSS_CONFIGS['default']

        base_stop_pct = config['stop_loss']

        # 移动止损计算
        if self.trailing_stop_enabled:
            if profit_pct > 0.10:
                # 盈利超过 10%，止损移到 +5%
                stop_price = cost_price * 1.05
                stop_type = 'trailing_high_profit'
            elif profit_pct > 0.05:
                # 盈利超过 5%，止损移到成本价
                stop_price = cost_price
                stop_type = 'trailing_profit'
            elif profit_pct > 0:
                # 小幅盈利，适当收紧止损
                adjusted_stop_pct = base_stop_pct * (1 + profit_pct)
                stop_price = cost_price * (1 + adjusted_stop_pct)
                stop_type = 'adjusted_profit'
            else:
                # 亏损状态，使用基础止损
                stop_price = cost_price * (1 + base_stop_pct)
                stop_type = 'base'
        else:
            stop_price = cost_price * (1 + base_stop_pct)
            stop_type = 'base'

        # 波动率调整
        if volatility > self.high_vol_threshold:
            # 高波动放宽止损 2%
            stop_price *= 0.98
            stop_type += '_vol_adjusted'

        # 计算实际止损比例
        actual_stop_pct = (stop_price - cost_price) / cost_price if cost_price > 0 else 0

        return {
            'stop_price': round(stop_price, 2),
            'stop_pct': round(actual_stop_pct, 4),
            'stop_type': stop_type,
            'take_profit_price': round(cost_price * (1 + config['take_profit']), 2),
            'take_profit_pct': config['take_profit']
        }

    def calculate_take_profit(
        self,
        position: Dict,
        market_state: str = 'SIDEWAYS',
        volatility: float = 0.20
    ) -> Dict:
        """
        动态止盈计算

        规则:
        1. 基础止盈 = 配置比例
        2. 高波动市场提高止盈目标
        3. 分批止盈建议

        Args:
            position: 持仓信息
            market_state: 市场状态
            volatility: 波动率

        Returns:
            止盈信息字典
        """
        cost_price = position.get('cost_price', 0)
        current_price = position.get('current_price', cost_price)

        if cost_price <= 0:
            return {'take_profit_price': 0, 'take_profit_pct': self.default_take_profit}

        # 根据市场状态选择止盈配置
        if market_state == 'BULL':
            base_take_profit = self.STOP_LOSS_CONFIGS['aggressive']['take_profit']
        elif market_state == 'BEAR':
            base_take_profit = self.STOP_LOSS_CONFIGS['conservative']['take_profit']
        else:
            base_take_profit = self.STOP_LOSS_CONFIGS['default']['take_profit']

        # 波动率调整
        if volatility > self.high_vol_threshold:
            # 高波动提高止盈目标
            take_profit_pct = base_take_profit * 1.2
        else:
            take_profit_pct = base_take_profit

        take_profit_price = cost_price * (1 + take_profit_pct)

        # 分批止盈建议
        batch_suggestions = self._generate_batch_take_profit(
            cost_price, take_profit_price, current_price
        )

        return {
            'take_profit_price': round(take_profit_price, 2),
            'take_profit_pct': round(take_profit_pct, 4),
            'batch_suggestions': batch_suggestions
        }

    def _generate_batch_take_profit(
        self,
        cost_price: float,
        target_price: float,
        current_price: float
    ) -> List[Dict]:
        """生成分批止盈建议"""
        if cost_price <= 0 or target_price <= cost_price:
            return []

        suggestions = []

        # 第一批：盈利 10%
        price_1 = cost_price * 1.10
        if current_price < price_1:
            suggestions.append({
                'batch': 1,
                'price': round(price_1, 2),
                'pct': 0.10,
                'sell_ratio': 0.3,  # 卖出 30%
                'description': '减仓 30%，锁定部分利润'
            })

        # 第二批：盈利 15%
        price_2 = cost_price * 1.15
        if current_price < price_2:
            suggestions.append({
                'batch': 2,
                'price': round(price_2, 2),
                'pct': 0.15,
                'sell_ratio': 0.3,
                'description': '再减仓 30%'
            })

        # 第三批：目标止盈
        if current_price < target_price:
            suggestions.append({
                'batch': 3,
                'price': round(target_price, 2),
                'pct': (target_price - cost_price) / cost_price,
                'sell_ratio': 0.4,
                'description': '清仓止盈'
            })

        return suggestions

    def check_risk_alert(
        self,
        positions: List[Dict],
        current_prices: Dict[str, float],
        market_state: str = 'SIDEWAYS'
    ) -> List[Alert]:
        """
        检查风险预警

        Args:
            positions: 持仓列表
                [{'ts_code', 'stock_name', 'cost_price', 'shares', ...}]
            current_prices: 当前价格 {'ts_code': price}
            market_state: 市场状态

        Returns:
            风险预警列表
        """
        alerts = []

        for pos in positions:
            ts_code = pos.get('ts_code', '')
            cost_price = pos.get('cost_price', 0)
            current_price = current_prices.get(ts_code, cost_price)
            stock_name = pos.get('stock_name', '')

            if cost_price <= 0:
                continue

            # 计算盈亏
            pnl_pct = (current_price - cost_price) / cost_price

            # 获取止损止盈价格
            stop_info = self.calculate_stop_loss(pos, market_state)
            take_profit_info = self.calculate_take_profit(pos, market_state)

            stop_price = stop_info['stop_price']
            take_profit_price = take_profit_info['take_profit_price']

            # 检查止损
            if current_price <= stop_price:
                alerts.append(Alert(
                    type=AlertType.STOP_LOSS,
                    position_code=ts_code,
                    position_name=stock_name,
                    current_price=current_price,
                    target_price=stop_price,
                    pnl_pct=pnl_pct,
                    message=f"触发止损: 当前价 {current_price:.2f} <= 止损价 {stop_price:.2f}",
                    severity='critical'
                ))

            # 检查止盈
            elif current_price >= take_profit_price:
                alerts.append(Alert(
                    type=AlertType.TAKE_PROFIT,
                    position_code=ts_code,
                    position_name=stock_name,
                    current_price=current_price,
                    target_price=take_profit_price,
                    pnl_pct=pnl_pct,
                    message=f"触发止盈: 当前价 {current_price:.2f} >= 止盈价 {take_profit_price:.2f}",
                    severity='warning'
                ))

            # 检查接近止损 (预警)
            elif current_price < cost_price * (1 + stop_info['stop_pct'] * 0.5):
                alerts.append(Alert(
                    type=AlertType.DRAWDOWN,
                    position_code=ts_code,
                    position_name=stock_name,
                    current_price=current_price,
                    target_price=stop_price,
                    pnl_pct=pnl_pct,
                    message=f"接近止损: 亏损 {pnl_pct:.1%}",
                    severity='warning'
                ))

        return alerts

    def calculate_position_var(
        self,
        position: Dict,
        confidence: float = 0.95,
        days: int = 60
    ) -> float:
        """
        计算持仓 VaR

        Args:
            position: 持仓信息
            confidence: 置信度
            days: 历史数据天数

        Returns:
            VaR 值（金额）
        """
        ts_code = position.get('ts_code', '')
        shares = position.get('shares', 0)
        current_price = position.get('current_price', 0)

        if shares <= 0 or current_price <= 0:
            return 0

        # 获取历史数据计算波动率
        try:
            end_date = self.db.get_latest_date()
            if end_date is None:
                return 0

            from datetime import timedelta
            start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=days)).strftime("%Y%m%d")

            df = self.db.get_stock_data(ts_code, start_date, end_date)

            if df.empty or len(df) < 20:
                return 0

            returns = df['pct_chg'].values / 100
            daily_vol = np.std(returns)
            annual_vol = daily_vol * np.sqrt(252)

            # VaR 计算
            from scipy import stats
            z_score = stats.norm.ppf(1 - confidence)

            position_value = shares * current_price
            var = abs(z_score) * daily_vol * position_value

            return var

        except Exception as e:
            logger.error(f"计算 VaR 失败: {e}")
            return 0

    def get_risk_report(
        self,
        positions: List[Dict],
        current_prices: Dict[str, float],
        market_state: str = 'SIDEWAYS'
    ) -> Dict:
        """
        生成风险报告

        Args:
            positions: 持仓列表
            current_prices: 当前价格
            market_state: 市场状态

        Returns:
            风险报告字典
        """
        alerts = self.check_risk_alert(positions, current_prices, market_state)

        # 统计
        critical_alerts = [a for a in alerts if a.severity == 'critical']
        warning_alerts = [a for a in alerts if a.severity == 'warning']

        # 计算组合风险指标
        total_value = 0
        total_pnl = 0
        positions_info = []

        for pos in positions:
            ts_code = pos.get('ts_code', '')
            cost_price = pos.get('cost_price', 0)
            shares = pos.get('shares', 0)
            current_price = current_prices.get(ts_code, cost_price)

            value = shares * current_price
            pnl = (current_price - cost_price) * shares

            total_value += value
            total_pnl += pnl

            # 获取止损止盈信息
            stop_info = self.calculate_stop_loss(pos, market_state)
            take_profit_info = self.calculate_take_profit(pos, market_state)

            positions_info.append({
                'ts_code': ts_code,
                'stock_name': pos.get('stock_name', ''),
                'cost_price': cost_price,
                'current_price': current_price,
                'shares': shares,
                'market_value': value,
                'pnl': pnl,
                'pnl_pct': (current_price - cost_price) / cost_price if cost_price > 0 else 0,
                'stop_price': stop_info['stop_price'],
                'stop_type': stop_info['stop_type'],
                'take_profit_price': take_profit_info['take_profit_price']
            })

        return {
            'total_value': total_value,
            'total_pnl': total_pnl,
            'total_pnl_pct': total_pnl / total_value if total_value > 0 else 0,
            'alert_count': len(alerts),
            'critical_count': len(critical_alerts),
            'warning_count': len(warning_alerts),
            'alerts': [{
                'type': a.type.value,
                'position': a.position_code,
                'position_name': a.position_name,
                'message': a.message,
                'severity': a.severity
            } for a in alerts],
            'positions': positions_info,
            'market_state': market_state
        }


def main():
    """测试函数"""
    print("=" * 70)
    print("增强风险管理测试")
    print("=" * 70)

    rm = EnhancedRiskManager()

    # 测试动态止损
    print("\n【动态止损测试】")
    positions = [
        {'ts_code': '000001.SZ', 'cost_price': 10.0, 'current_price': 9.0, 'shares': 1000},
        {'ts_code': '600519.SH', 'cost_price': 100.0, 'current_price': 106.0, 'shares': 100},
        {'ts_code': '300750.SZ', 'cost_price': 50.0, 'current_price': 58.0, 'shares': 200},
    ]

    for pos in positions:
        stop_info = rm.calculate_stop_loss(pos, market_state='SIDEWAYS', volatility=0.22)
        print(f"\n{pos['ts_code']}:")
        print(f"  成本价: {pos['cost_price']:.2f}")
        print(f"  当前价: {pos['current_price']:.2f}")
        print(f"  止损价: {stop_info['stop_price']:.2f} ({stop_info['stop_pct']:.1%})")
        print(f"  止损类型: {stop_info['stop_type']}")

    # 测试风险预警
    print("\n【风险预警测试】")
    current_prices = {p['ts_code']: p['current_price'] for p in positions}
    # 调整一个价格触发止损
    current_prices['000001.SZ'] = 8.5

    alerts = rm.check_risk_alert(positions, current_prices, 'SIDEWAYS')

    for alert in alerts:
        print(f"\n[{alert.severity.upper()}] {alert.position_code}:")
        print(f"  类型: {alert.type.value}")
        print(f"  消息: {alert.message}")

    # 测试风险报告
    print("\n【风险报告】")
    report = rm.get_risk_report(positions, current_prices, 'SIDEWAYS')
    print(f"组合总市值: {report['total_value']:,.2f}")
    print(f"总盈亏: {report['total_pnl']:,.2f} ({report['total_pnl_pct']:.1%})")
    print(f"预警数: {report['alert_count']} (严重: {report['critical_count']}, 警告: {report['warning_count']})")


if __name__ == "__main__":
    main()