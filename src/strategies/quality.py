"""
质量策略
基于高 ROE、低负债、稳定现金流的质量选股
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from strategies.base_strategy import BaseStrategy, StrategySignal


class QualityStrategy(BaseStrategy):
    """质量策略 - 高 ROE+ 低负债 + 稳定现金流"""

    def __init__(self, name: str = "quality", params: Optional[Dict] = None):
        default_params = {
            "min_roe": 0.15,  # 最小 ROE 15%
            "max_debt_ratio": 0.5,  # 最大资产负债率 50%
            "min_cash_flow": 0,  # 最小经营现金流
            "min_gross_margin": 0.20,  # 最小毛利率 20%
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
            "features": ["roe", "debt_ratio", "cash_flow", "gross_margin"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("质量策略：生成信号...")
        self._init_db()

        signals = []
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            return []

        latest_date = self.db.get_latest_date()
        if not latest_date:
            return []

        basic_data = self.db.get_stock_basics()
        if basic_data.empty:
            return []

        for ts_code in all_stocks["ts_code"].unique():
            stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
            stock_name = stock_info["name"].iloc[0] if not stock_info.empty else ts_code

            stock_basic = basic_data[basic_data["ts_code"] == ts_code]
            if stock_basic.empty:
                continue

            row = stock_basic.iloc[0]
            roe = row.get("roe", 0)
            debt_ratio = row.get("debt_ratio", 1)
            gross_margin = row.get("gross_margin", 0)

            # 筛选条件
            if roe < self.params["min_roe"]:
                continue
            if debt_ratio > self.params["max_debt_ratio"]:
                continue
            if gross_margin < self.params["min_gross_margin"]:
                continue

            score = self._calculate_score(roe, debt_ratio, gross_margin)

            signal = StrategySignal(
                ts_code=ts_code,
                stock_name=stock_name,
                strategy_type="quality",
                signal_type="buy",
                weight=0.5,
                score=score,
                reason=f"ROE={roe:.1%}, 负债率={debt_ratio:.1%}, 毛利率={gross_margin:.1%}",
                metadata={
                    "roe": roe,
                    "debt_ratio": debt_ratio,
                    "gross_margin": gross_margin,
                },
            )
            signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"质量策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_score(
        self, roe: float, debt_ratio: float, gross_margin: float
    ) -> float:
        """计算综合评分"""
        roe_score = min(100, roe * 400)
        debt_score = max(0, 100 - debt_ratio * 100)
        margin_score = min(100, gross_margin * 200)

        total = roe_score * 0.50 + debt_score * 0.25 + margin_score * 0.25
        return min(100, max(0, total))
