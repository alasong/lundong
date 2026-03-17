#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
打板策略监控仪表盘
Real-time monitoring dashboard for 打板 strategies
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from strategies.daban_version import DabanStrategyVersion
from data.database import get_database


class RiskMonitor:
    """风险监控仪表盘"""

    def __init__(self):
        self.db = get_database()
        self.version = DabanStrategyVersion.get_current_version()

    def check_daily_risk_limits(self):
        """检查日风险限制"""
        risk_limits = {
            "daily_loss_limit": -0.05,  # 日最大亏损5%
            "consecutive_losses": 3,  # 连续3次亏损暂停
            "market_volatility": 0.08,  # 市场波动率>8%降低仓位
            "sector_concentration": 0.4,  # 单板块≤40%仓位
        }
        return risk_limits

    def monitor_market_conditions(self):
        """监控市场条件"""
        latest_date = self.db.get_latest_date()
        if not latest_date:
            return {"status": "error", "message": "无法获取最新日期"}

        # 获取市场数据
        market_data = self.db.get_all_stock_data(latest_date)
        if market_data.empty:
            return {"status": "error", "message": "无市场数据"}

        # 计算市场波动率
        volatility = (
            float(market_data["pct_chg"].std() / 100.0)
            if not market_data.empty
            else 0.0
        )

        # 统计涨停股票数量
        limit_ups = (
            int(len(market_data[market_data["pct_chg"] > 9.0]))
            if not market_data.empty
            else 0
        )

        # 统计跌停股票数量
        limit_downs = (
            int(len(market_data[market_data["pct_chg"] < -9.0]))
            if not market_data.empty
            else 0
        )

        market_status = {
            "date": latest_date,
            "volatility": volatility,
            "limit_up_count": limit_ups,
            "limit_down_count": limit_downs,
            "market_sentiment": "bullish" if limit_ups > limit_downs else "bearish",
            "trading_opportunity": "high" if limit_ups >= 3 else "low",
        }

        return market_status

    def generate_risk_report(self):
        """生成风险报告"""
        print("🛡️  打板策略风险监控仪表盘")
        print(f"版本: {self.version}")
        print("-" * 50)

        # 风险限制
        limits = self.check_daily_risk_limits()
        print("📊 风险限制:")
        print(f"  日最大亏损: {limits['daily_loss_limit']:.1%}")
        print(f"  连续亏损暂停: {limits['consecutive_losses']}次")
        print(f"  市场波动率阈值: {limits['market_volatility']:.1%}")
        print(f"  板块集中度: {limits['sector_concentration']:.1%}")

        # 市场状态
        market = self.monitor_market_conditions()
        if (
            isinstance(market, dict)
            and "status" in market
            and market["status"] == "error"
        ):
            print(f"⚠️  {market['message']}")
        else:
            print("\n📈 市场状态:")
            print(f"  日期: {market['date']}")
            print(f"  市场波动率: {market['volatility']:.2%}")
            print(f"  涨停数量: {market['limit_up_count']}")
            print(f"  跌停数量: {market['limit_down_count']}")
            print(f"  市场情绪: {market['market_sentiment']}")
            print(f"  交易机会: {market['trading_opportunity']}")

            # 风险建议
            if market["volatility"] > 0.08:
                print("  ⚠️  建议: 市场波动率过高，降低仓位")
            if market["limit_up_count"] == 0:
                print("  ⚠️  建议: 无涨停股票，暂停交易")
            if market["limit_down_count"] > 10:
                print("  ⚠️  建议: 市场情绪悲观，谨慎交易")

        return market, limits


def main():
    """主监控函数"""
    monitor = RiskMonitor()
    market_status, risk_limits = monitor.generate_risk_report()

    # 应急处理建议
    print("\n🚨 应急处理预案:")
    print("  • 技术故障: 自动切换到保守模式或暂停交易")
    print("  • 数据异常: 触发人工审核机制")
    print("  • 市场异常: 启动熔断保护，暂停所有策略")

    return market_status, risk_limits


if __name__ == "__main__":
    main()
