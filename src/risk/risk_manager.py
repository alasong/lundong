"""
风险管理模块
包含止损策略、仓位管理、风险预算、黑名单等功能
"""
import pandas as pd
import numpy as np
import sqlite3
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import SQLiteDatabase, get_database


class RiskManager:
    """风险管理器"""

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化风险管理器

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        self.blacklist_cache = None
        self.blacklist_cache_time = None
        logger.info("风险管理器初始化完成")

    # ==================== 止损策略 ====================

    def check_stop_loss(
        self,
        positions: List[Dict],
        current_prices: pd.DataFrame,
        stop_loss_type: str = "trailing",
        fixed_stop_loss_pct: float = 0.08,
        trailing_stop_loss_pct: float = 0.10
    ) -> List[Dict]:
        """
        检查止损条件

        Args:
            positions: 当前持仓列表
                [{'ts_code': 'xxx', 'cost_price': 10.5, 'shares': 1000, 'highest_price': 11.0}, ...]
            current_prices: 当前价格 DataFrame
                [{'ts_code': 'xxx', 'close': 10.2}, ...]
            stop_loss_type: 止损类型
                - 'fixed': 固定比例止损
                - 'trailing': 移动止损（从最高点回撤）
            fixed_stop_loss_pct: 固定止损比例（默认 8%）
            trailing_stop_loss_pct: 移动止损比例（默认 10%）

        Returns:
            需要卖出的持仓列表
        """
        sell_list = []

        for pos in positions:
            ts_code = pos['ts_code']
            cost_price = pos['cost_price']
            shares = pos['shares']

            # 获取当前价格
            price_row = current_prices[current_prices['ts_code'] == ts_code]
            if price_row.empty:
                continue

            current_price = price_row.iloc[0]['close']

            # 计算盈亏比例
            profit_loss_pct = (current_price - cost_price) / cost_price

            should_sell = False
            reason = ""

            if stop_loss_type == "fixed":
                # 固定比例止损
                if profit_loss_pct <= -fixed_stop_loss_pct:
                    should_sell = True
                    reason = f"固定止损：亏损{profit_loss_pct:.1%} > {fixed_stop_loss_pct:.1%}"

            elif stop_loss_type == "trailing":
                # 移动止损：从最高点回撤超过阈值
                highest_price = pos.get('highest_price', cost_price)
                highest_price = max(highest_price, current_price)

                # 更新最高价
                pos['highest_price'] = highest_price

                # 从最高点回撤
                drawdown_from_peak = (highest_price - current_price) / highest_price

                if drawdown_from_peak >= trailing_stop_loss_pct:
                    should_sell = True
                    reason = f"移动止损：回撤{drawdown_from_peak:.1%} > {trailing_stop_loss_pct:.1%}"

                # 也有固定止损保护
                elif profit_loss_pct <= -fixed_stop_loss_pct:
                    should_sell = True
                    reason = f"固定止损保护：亏损{profit_loss_pct:.1%}"

            if should_sell:
                sell_list.append({
                    'ts_code': ts_code,
                    'shares': shares,
                    'reason': reason,
                    'current_price': current_price,
                    'cost_price': cost_price,
                    'profit_loss_pct': profit_loss_pct
                })

        return sell_list

    def calculate_position_risk(
        self,
        ts_code: str,
        days: int = 20
    ) -> Dict:
        """
        计算单个股票的风险指标

        Args:
            ts_code: 股票代码
            days: 计算天数

        Returns:
            风险指标字典
        """
        from datetime import timedelta
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

        df = self.db.get_stock_data(ts_code, start_date, end_date)

        if df.empty:
            return {}

        # 计算风险指标
        returns = df['pct_chg'] / 100
        volatility = returns.std() * np.sqrt(252)  # 年化波动率
        max_drawdown = self._calculate_max_drawdown(df['close'])

        # VaR (95% 置信度)
        var_95 = returns.quantile(0.05)

        # CVaR (预期亏损)
        cvar_95 = returns[returns <= var_95].mean()

        return {
            'ts_code': ts_code,
            'volatility': volatility,
            'max_drawdown': max_drawdown,
            'var_95': var_95,
            'cvar_95': cvar_95 if not np.isnan(cvar_95) else var_95,
            'avg_return': returns.mean() * 252,
        }

    def _calculate_max_drawdown(self, prices: pd.Series) -> float:
        """计算最大回撤"""
        rolling_max = prices.cummax()
        drawdown = (prices - rolling_max) / rolling_max
        return drawdown.min()

    # ==================== 黑名单管理 ====================

    def get_blacklist(self) -> List[str]:
        """
        获取黑名单股票列表

        包括：
        - ST、*ST 股票
        - 财务造假嫌疑
        - 重大违规
        - 长期停牌
        """
        # 缓存 1 小时
        now = datetime.now()
        if (self.blacklist_cache is not None and
            self.blacklist_cache_time is not None and
            now - self.blacklist_cache_time < timedelta(hours=1)):
            return self.blacklist_cache

        blacklist = []

        try:
            conn = sqlite3.connect('data/stock.db')
            cursor = conn.cursor()

            # 查询股票名称包含 ST 的（从成分股表中）
            cursor.execute("""
                SELECT DISTINCT stock_code FROM concept_constituent
                WHERE stock_name LIKE '%ST%' OR stock_name LIKE '%*ST%'
            """)
            st_stocks = [row[0] for row in cursor.fetchall()]
            blacklist.extend(st_stocks)

            # 查询停牌超过 30 天的股票
            cursor.execute("""
                SELECT ts_code, MAX(trade_date) as last_trade
                FROM stock_daily
                GROUP BY ts_code
                HAVING julianday('now') - julianday(MAX(trade_date)) > 30
            """)
            suspended = [row[0] for row in cursor.fetchall()]
            blacklist.extend(suspended)

            conn.close()

            # 去重
            blacklist = list(set(blacklist))

        except Exception as e:
            logger.error(f"获取黑名单失败：{e}")
            return []

        self.blacklist_cache = blacklist
        self.blacklist_cache_time = now

        logger.info(f"黑名单股票：{len(blacklist)} 只")
        return blacklist

    def filter_blacklist(
        self,
        stock_list: List[str]
    ) -> List[str]:
        """
        过滤黑名单股票

        Args:
            stock_list: 待过滤的股票列表

        Returns:
            过滤后的股票列表
        """
        blacklist = self.get_blacklist()
        filtered = [s for s in stock_list if s not in blacklist]

        if len(filtered) < len(stock_list):
            removed = len(stock_list) - len(filtered)
            logger.info(f"过滤掉 {removed} 只黑名单股票")

        return filtered

    def refresh_blacklist(self):
        """强制刷新黑名单缓存"""
        self.blacklist_cache = None
        self.blacklist_cache_time = None
        logger.info("黑名单缓存已刷新")

    # ==================== 仓位管理 ====================

    def calculate_position_size(
        self,
        total_capital: float,
        ts_code: str,
        current_price: float,
        volatility: float,
        target_risk: float = 0.15,
        max_position_pct: float = 0.10,
        min_position_pct: float = 0.02
    ) -> int:
        """
        计算单只股票的合理仓位（基于波动率调整）

        Args:
            total_capital: 总资金
            ts_code: 股票代码
            current_price: 当前价格
            volatility: 该股年化波动率
            target_risk: 目标组合波动率
            max_position_pct: 单股最大仓位比例
            min_position_pct: 单股最小仓位比例

        Returns:
            建议买入股数
        """
        # 基于波动率调整仓位：波动率越高，仓位越低
        # 基准仓位 = 1 / 波动率
        base_risk_weight = 1.0 / max(volatility, 0.01)

        # 归一化到目标风险
        risk_adjusted_weight = (base_risk_weight * target_risk)

        # 计算目标仓位比例
        position_pct = min(max(risk_adjusted_weight, min_position_pct), max_position_pct)

        # 计算金额和股数
        position_value = total_capital * position_pct
        shares = int(position_value / current_price / 100) * 100  # 整百股

        logger.debug(f"{ts_code}: 波动率={volatility:.1%}, "
                    f"仓位比例={position_pct:.1%}, 股数={shares}")

        return shares

    def calculate_portfolio_var(
        self,
        positions: List[Dict],
        current_prices: pd.DataFrame,
        confidence: float = 0.95,
        days: int = 1
    ) -> float:
        """
        计算组合 VaR（Value at Risk）

        Args:
            positions: 持仓列表
            current_prices: 当前价格
            confidence: 置信度
            days: 持有期天数

        Returns:
            组合 VaR（金额）
        """
        # 获取各股票的日波动率
        volatilities = {}
        for pos in positions:
            ts_code = pos['ts_code']
            risk_metrics = self.calculate_position_risk(ts_code, days=60)
            if risk_metrics:
                # 年化转日化
                daily_vol = risk_metrics['volatility'] / np.sqrt(252)
                volatilities[ts_code] = daily_vol

        if not volatilities:
            return 0

        # 计算组合波动率（简化：假设相关性为 0.5）
        total_value = sum(
            pos['shares'] * current_prices[
                current_prices['ts_code'] == pos['ts_code']
            ].iloc[0]['close'] if not current_prices[
                current_prices['ts_code'] == pos['ts_code']
            ].empty else 0
            for pos in positions
        )

        if total_value <= 0:
            return 0

        # 平均波动率
        avg_vol = np.mean(list(volatilities.values()))

        # 组合波动率（简化）
        portfolio_vol = avg_vol * np.sqrt(len(volatilities)) * 0.7  # 分散化折扣

        # VaR = 组合价值 * Z 分数 * 波动率 * sqrt(天数)
        from scipy import stats
        z_score = stats.norm.ppf(1 - confidence)

        var = abs(z_score) * portfolio_vol * np.sqrt(days) * total_value

        return var

    def check_concentration_risk(
        self,
        positions: List[Dict],
        max_sector_pct: float = 0.25
    ) -> Dict:
        """
        检查集中度风险

        Args:
            positions: 持仓列表（需包含 concept_code）
            max_sector_pct: 单板块最大比例

        Returns:
            风险分析结果
        """
        if not positions:
            return {'risk_level': 'low', 'details': []}

        # 计算板块权重
        sector_weights = {}
        total_value = sum(pos.get('market_value', 0) for pos in positions)

        if total_value <= 0:
            return {'risk_level': 'low', 'details': []}

        for pos in positions:
            sector = pos.get('concept_code', 'unknown')
            sector_weights[sector] = sector_weights.get(sector, 0) + pos.get('market_value', 0)

        # 转换为比例
        sector_pcts = {k: v / total_value for k, v in sector_weights.items()}

        # 检查是否超限
        warnings = []
        for sector, pct in sector_pcts.items():
            if pct > max_sector_pct:
                warnings.append({
                    'sector': sector,
                    'current': pct,
                    'limit': max_sector_pct,
                    'excess': pct - max_sector_pct
                })

        risk_level = 'high' if warnings else ('medium' if max(sector_pcts.values()) > 0.2 else 'low')

        return {
            'risk_level': risk_level,
            'sector_weights': sector_pcts,
            'warnings': warnings,
            'max_concentration': max(sector_pcts.values()) if sector_pcts else 0
        }


def main():
    """测试函数"""
    rm = RiskManager()

    # 测试黑名单
    print("\n=== 黑名单测试 ===")
    blacklist = rm.get_blacklist()
    print(f"黑名单股票数量：{len(blacklist)}")
    if blacklist:
        print(f"示例：{blacklist[:5]}")

    # 测试个股风险
    print("\n=== 个股风险测试 ===")
    test_stocks = ['000001.SZ', '600519.SH', '300750.SZ']
    for stock in test_stocks:
        metrics = rm.calculate_position_risk(stock, days=60)
        if metrics:
            print(f"{stock}:")
            print(f"  波动率：{metrics['volatility']:.1%}")
            print(f"  最大回撤：{metrics['max_drawdown']:.1%}")
            print(f"  VaR(95%): {metrics['var_95']:.2%}")


if __name__ == "__main__":
    main()
