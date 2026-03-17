"""
高股息策略
基于股息率和分红稳定性的防守型策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class DividendStrategy(BaseStrategy):
    """高股息策略 - 基于股息率和分红稳定性"""

    def __init__(self, name: str = "dividend", params: Optional[Dict] = None):
        default_params = {
            "min_dividend_yield": 0.03,  # 最小股息率 3%
            "min_dividend_years": 3,  # 最小连续分红年数
            "max_pe": 20,  # 最大 PE
            "min_roe": 0.08,  # 最小 ROE 8%
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
            "features": ["dividend_yield", "pe", "roe", "dividend_years"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("高股息策略：生成信号...")
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
            logger.warning("无基本面数据")
            return []

        for ts_code in all_stocks["ts_code"].unique():
            stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
            stock_name = stock_info["name"].iloc[0] if not stock_info.empty else ts_code

            stock_basic = basic_data[basic_data["ts_code"] == ts_code]
            if stock_basic.empty:
                continue

            row = stock_basic.iloc[0]

            # 获取指标
            dividend_yield = row.get("dividend_yield", 0)
            pe = row.get("pe", 999)
            roe = row.get("roe", 0)
            dividend_years = row.get("dividend_years", 0)

            # 筛选条件
            if dividend_yield < self.params["min_dividend_yield"]:
                continue
            if pe > self.params["max_pe"] or pe < 0:
                continue
            if roe < self.params["min_roe"]:
                continue
            if dividend_years < self.params["min_dividend_years"]:
                continue

            # 计算评分
            score = self._calculate_score(dividend_yield, pe, roe, dividend_years)

            signal = StrategySignal(
                ts_code=ts_code,
                stock_name=stock_name,
                strategy_type="dividend",
                signal_type="buy",
                weight=0.5,
                score=score,
                reason=f"股息率={dividend_yield:.1%}, PE={pe:.1f}, ROE={roe:.1%}",
                metadata={
                    "dividend_yield": dividend_yield,
                    "pe": pe,
                    "roe": roe,
                    "dividend_years": dividend_years,
                },
            )
            signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"高股息策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_score(
        self, dividend_yield: float, pe: float, roe: float, dividend_years: int
    ) -> float:
        """
        计算综合评分

        评分 = 股息率 (40%) + PE (20%) + ROE (30%) + 分红年限 (10%)
        """
        # 股息率评分（越高越好）
        yield_score = min(100, dividend_yield * 2000)

        # PE 评分（越低越好）
        pe_score = max(0, 100 - pe * 5)

        # ROE 评分
        roe_score = min(100, roe * 400)

        # 分红年限评分（连续分红越长越好）
        years_score = min(100, dividend_years * 20)

        total = (
            yield_score * 0.40 + pe_score * 0.20 + roe_score * 0.30 + years_score * 0.10
        )

        return min(100, max(0, total))
