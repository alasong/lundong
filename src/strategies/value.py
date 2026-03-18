"""
价值策略
基于低估值 (PE/PB) 和高 ROE 的价值投资选股
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class ValueStrategy(BaseStrategy):
    """价值策略 - 低 PE/PB+高 ROE"""

    def __init__(self, name: str = "value", params: Optional[Dict] = None):
        default_params = {
            "max_pe": 20,  # 最大 PE
            "max_pb": 2,  # 最大 PB
            "min_roe": 0.10,  # 最小 ROE 10%
            "min_market_cap": 50,  # 最小市值 50 亿
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
            "features": ["pe", "pb", "roe", "market_cap"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("价值策略：生成信号...")
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

            # 获取基本面指标
            stock_basic = basic_data[basic_data["ts_code"] == ts_code]
            if stock_basic.empty:
                continue

            row = stock_basic.iloc[0]
            pe = row.get("pe", 999)
            pb = row.get("pb", 999)
            roe = row.get("roe", 0)
            market_cap = row.get("market_cap", 0)

            # 筛选条件
            if pe < 0 or pb < 0:  # 排除负值
                continue
            if pe > self.params["max_pe"]:
                continue
            if pb > self.params["max_pb"]:
                continue
            if roe < self.params["min_roe"]:
                continue
            if market_cap < self.params["min_market_cap"]:
                continue

            # 计算评分
            score = self._calculate_score(pe, pb, roe)

            signal = StrategySignal(
                ts_code=ts_code,
                stock_name=stock_name,
                strategy_type="value",
                signal_type="buy",
                weight=0.5,
                score=score,
                reason=f"PE={pe:.1f}, PB={pb:.1f}, ROE={roe:.1%}",
                metadata={"pe": pe, "pb": pb, "roe": roe, "market_cap": market_cap},
            )
            signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"价值策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_score(self, pe: float, pb: float, roe: float) -> float:
        """
        计算综合评分

        评分 = PE 评分 (30%) + PB 评分 (30%) + ROE 评分 (40%)
        """
        # PE 评分：越低越好
        pe_score = max(0, 100 - pe * 5)

        # PB 评分：越低越好
        pb_score = max(0, 100 - pb * 25)

        # ROE 评分：越高越好
        roe_score = min(100, roe * 500)

        total = pe_score * 0.30 + pb_score * 0.30 + roe_score * 0.40
        return min(100, max(0, total))
