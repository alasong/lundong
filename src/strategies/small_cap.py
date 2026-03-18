"""
小市值策略
基于市值因子的选股策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class SmallCapStrategy(BaseStrategy):
    """小市值策略 - 选择小市值股票"""

    def __init__(self, name: str = "small_cap", params: Optional[Dict] = None):
        default_params = {
            "max_market_cap": 100,  # 最大市值 100 亿
            "min_market_cap": 10,  # 最小市值 10 亿 (排除壳股)
            "min_roe": 0.05,  # 最小 ROE 5% (保证质量)
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
            "features": ["market_cap", "roe"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("小市值策略：生成信号...")
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

        # 按市值排序选股
        candidates = []
        for ts_code in all_stocks["ts_code"].unique():
            stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
            stock_name = stock_info["name"].iloc[0] if not stock_info.empty else ts_code

            stock_basic = basic_data[basic_data["ts_code"] == ts_code]
            if stock_basic.empty:
                continue

            row = stock_basic.iloc[0]
            market_cap = row.get("market_cap", 999)
            roe = row.get("roe", 0)

            # 筛选条件
            if market_cap < self.params["min_market_cap"]:
                continue
            if market_cap > self.params["max_market_cap"]:
                continue
            if roe < self.params["min_roe"]:
                continue

            # 市值越小评分越高
            score = self._calculate_score(market_cap)

            signal = StrategySignal(
                ts_code=ts_code,
                stock_name=stock_name,
                strategy_type="small_cap",
                signal_type="buy",
                weight=0.5,
                score=score,
                reason=f"市值={market_cap:.1f}亿，ROE={roe:.1%}",
                metadata={"market_cap": market_cap, "roe": roe},
            )
            candidates.append((signal, market_cap))

        # 按市值排序 (越小越好)
        candidates.sort(key=lambda x: x[1])
        signals = [c[0] for c in candidates[: self.params["top_n_stocks"]]]
        self.signals = signals
        logger.info(f"小市值策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_score(self, market_cap: float) -> float:
        """计算评分：市值越小分数越高"""
        # 线性映射：min_cap->100, max_cap->0
        min_cap = self.params["min_market_cap"]
        max_cap = self.params["max_market_cap"]

        if market_cap <= min_cap:
            return 100
        elif market_cap >= max_cap:
            return 0
        else:
            return 100 * (max_cap - market_cap) / (max_cap - min_cap)
