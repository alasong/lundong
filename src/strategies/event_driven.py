"""
事件驱动策略 (兼容多策略框架版本)
基于财报、公告、调研等事件的策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from strategies.base_strategy import BaseStrategy, StrategySignal


class EventDrivenStrategy(BaseStrategy):
    """事件驱动策略 - 财报/公告/调研"""

    def __init__(self, name: str = "event_driven", params: Optional[Dict] = None):
        default_params = {
            "hold_period": 5,
            "earnings_weight": 0.4,
            "announcement_weight": 0.3,
            "survey_weight": 0.2,
            "insider_weight": 0.1,
            "top_n_stocks": 20,
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
            "history_days": 30,
            "features": ["close", "events"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("事件驱动策略：生成信号...")
        self._init_db()

        signals = []
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            return []

        latest_date = self.db.get_latest_date()
        if not latest_date:
            return []

        # 获取事件数据（示例：使用基本面数据模拟）
        basic_data = self.db.get_stock_basics()

        for ts_code in all_stocks["ts_code"].unique():
            stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
            stock_name = stock_info["name"].iloc[0] if not stock_info.empty else ts_code

            # 模拟事件信号（实际应从事件表获取）
            event_score = self._check_recent_events(ts_code, latest_date)

            if event_score > 0.5:
                signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=stock_name,
                    strategy_type="event_driven",
                    signal_type="buy",
                    weight=event_score,
                    score=event_score * 100,
                    reason=f"事件评分：{event_score:.1f}",
                    metadata={"event_score": event_score},
                )
                signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"事件驱动策略：生成 {len(signals)} 个信号")
        return signals

    def _check_recent_events(self, ts_code: str, date: str) -> float:
        """检查最近事件（简化实现）"""
        # TODO: 实际应从事件表获取财报、公告、调研等数据
        import random

        return random.uniform(0.3, 0.9)
