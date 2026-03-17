"""
成长策略
基于营收和净利润高增长的成长股选股
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class GrowthStrategy(BaseStrategy):
    """成长策略 - 高营收/净利润增长"""

    def __init__(self, name: str = "growth", params: Optional[Dict] = None):
        default_params = {
            "min_revenue_growth": 0.20,  # 最小营收增长 20%
            "min_profit_growth": 0.25,  # 最小利润增长 25%
            "min_roe": 0.10,  # 最小 ROE 10%
            "max_pe": 50,  # 最大 PE 50
            "min_market_cap": 30,  # 最小市值 30 亿
            "top_n_stocks": 20,  # 选股数量
        }
        default_params.update(params or {})

        super().__init__(name, default_params)
        self.db = None

    def _init_db(self):
        if self.db is None:
            from data.database import get_database

            self.db = get_database()

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "concept_data": False,
            "stock_data": True,
            "history_days": 1,
            "features": ["revenue_growth", "profit_growth", "roe", "pe", "market_cap"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("成长策略：生成信号...")
        self._init_db()

        signals = []
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            return []

        latest_date = self.db.get_latest_date()
        if not latest_date:
            return []

        # 获取基本面数据
        logger.info("获取基本面数据...")
        basic_data = self.db.get_stock_basics()

        if basic_data.empty:
            logger.warning("无基本面数据")
            return []

        for ts_code in all_stocks["ts_code"].unique():
            stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
            stock_name = stock_info["name"].iloc[0] if not stock_info.empty else ts_code

            stock_basic = basic_data[basic_data["ts_code"] == ts_code]
            if stock_basic.empty:
                continue

            row = stock_basic.iloc[0]
            revenue_growth = row.get("revenue_growth", 0)
            profit_growth = row.get("profit_growth", 0)
            roe = row.get("roe", 0)
            pe = row.get("pe", 999)
            market_cap = row.get("market_cap", 0)

            # 筛选条件
            if revenue_growth < self.params["min_revenue_growth"]:
                continue
            if profit_growth < self.params["min_profit_growth"]:
                continue
            if roe < self.params["min_roe"]:
                continue
            if pe > self.params["max_pe"] or pe < 0:
                continue
            if market_cap < self.params["min_market_cap"]:
                continue

            # 计算评分
            score = self._calculate_score(revenue_growth, profit_growth, roe)

            signal = StrategySignal(
                ts_code=ts_code,
                stock_name=stock_name,
                strategy_type="growth",
                signal_type="buy",
                weight=0.5,
                score=score,
                reason=f"营收增长={revenue_growth:.1%}, 利润增长={profit_growth:.1%}",
                metadata={
                    "revenue_growth": revenue_growth,
                    "profit_growth": profit_growth,
                    "roe": roe,
                    "pe": pe,
                },
            )
            signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"成长策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_score(
        self, revenue_growth: float, profit_growth: float, roe: float
    ) -> float:
        """
        计算综合评分

        评分 = 营收增长 (40%) + 利润增长 (40%) + ROE(20%)
        """
        # 营收增长评分
        rev_score = min(100, revenue_growth * 200)

        # 利润增长评分
        profit_score = min(100, profit_growth * 200)

        # ROE 评分
        roe_score = min(100, roe * 500)

        total = rev_score * 0.40 + profit_score * 0.40 + roe_score * 0.20
        return min(100, max(0, total))
