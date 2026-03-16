"""
资金流策略 (兼容多策略框架版本)
基于北向资金、龙虎榜、主力资金的流向分析
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from strategies.base_strategy import BaseStrategy, StrategySignal


class CapitalFlowStrategy(BaseStrategy):
    """资金流策略 - 北向/主力/龙虎榜"""

    def __init__(self, name: str = "capital_flow", params: Optional[Dict] = None):
        default_params = {
            "lookback": 20,
            "min_flow_score": 0.5,
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
            "features": ["close", "volume", "moneyflow"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("资金流策略：生成信号...")
        self._init_db()

        signals = []
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            return []

        latest_date = self.db.get_latest_date()
        if not latest_date:
            return []

        start_date = self._get_n_days_before(latest_date, 30)
        stock_data = self.db.get_daily_batch(
            stock_codes=all_stocks["ts_code"].tolist(),
            start_date=start_date,
            end_date=latest_date,
        )

        if stock_data.empty:
            return []

        for ts_code in all_stocks["ts_code"].unique():
            stock_df = stock_data[stock_data["ts_code"] == ts_code].sort_values(
                "trade_date"
            )
            if len(stock_df) < self.params["lookback"]:
                continue

            # 计算资金流信号
            flow_score = self._compute_flow_score(stock_df)

            if flow_score >= self.params["min_flow_score"]:
                stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
                stock_name = (
                    stock_info["name"].iloc[0] if not stock_info.empty else ts_code
                )

                signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=stock_name,
                    strategy_type="capital_flow",
                    signal_type="buy",
                    weight=flow_score,
                    score=flow_score * 100,
                    reason=f"资金流评分：{flow_score:.1f}",
                    metadata={"flow_score": flow_score},
                )
                signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"资金流策略：生成 {len(signals)} 个信号")
        return signals

    def _compute_flow_score(self, df: pd.DataFrame) -> float:
        """计算资金流评分"""
        # 简化实现：使用成交量和价格变化模拟
        if "amount" not in df.columns:
            return 0.5

        latest_amount = df["amount"].iloc[-1]
        avg_amount = df["amount"].iloc[-self.params["lookback"] :].mean()

        if avg_amount == 0:
            return 0.5

        # 放量且上涨
        amount_ratio = latest_amount / avg_amount
        price_change = (df["close"].iloc[-1] - df["close"].iloc[-1]) / df["close"].iloc[
            -1
        ]

        score = min(1.0, (amount_ratio - 1) * 0.3 + max(0, price_change) * 0.7)
        return score

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        from datetime import timedelta, datetime

        date = datetime.strptime(date_str, "%Y%m%d")
        prev_date = date - timedelta(days=n_days + n_days // 5)
        return prev_date.strftime("%Y%m%d")
