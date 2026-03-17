"""
均值回归策略 (兼容多策略框架版本)
基于布林带和 RSI 的均值回归策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class MeanReversionStrategy(BaseStrategy):
    """均值回归策略 - 布林带+RSI"""

    def __init__(self, name: str = "mean_reversion", params: Optional[Dict] = None):
        default_params = {
            "bb_period": 20,
            "bb_std": 2.0,
            "rsi_period": 14,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
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
            "history_days": 60,
            "features": ["close", "volume"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("均值回归策略：生成信号...")
        self._init_db()

        signals = []
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            return []

        latest_date = self.db.get_latest_date()
        if not latest_date:
            return []

        start_date = self._get_n_days_before(latest_date, 60)
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
            if len(stock_df) < self.params["bb_period"]:
                continue

            # 计算布林带和 RSI
            bb_signal = self._compute_bb_signal(stock_df)
            rsi_signal = self._compute_rsi_signal(stock_df)

            # 综合信号
            if bb_signal > 0 or rsi_signal > 0:
                stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
                stock_name = (
                    stock_info["name"].iloc[0] if not stock_info.empty else ts_code
                )

                score = (bb_signal + rsi_signal) / 2 * 50
                signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=stock_name,
                    strategy_type="mean_reversion",
                    signal_type="buy",
                    weight=0.5,
                    score=min(100, score),
                    reason=f"布林带：{bb_signal:.1f}, RSI: {rsi_signal:.1f}",
                    metadata={"bb_signal": bb_signal, "rsi_signal": rsi_signal},
                )
                signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals
        logger.info(f"均值回归策略：生成 {len(signals)} 个信号")
        return signals

    def _compute_bb_signal(self, df: pd.DataFrame) -> float:
        """布林带信号：0-1，越接近 1 越超卖"""
        period = self.params["bb_period"]
        std = self.params["bb_std"]

        if len(df) < period:
            return 0

        close = df["close"].iloc[-1]
        sma = df["close"].iloc[-period:].mean()
        std_val = df["close"].iloc[-period:].std()

        upper = sma + std * std_val
        lower = sma - std * std_val

        if lower == 0:
            return 0

        # 价格在下轨附近为超卖
        if close <= lower:
            return 1.0
        elif close >= upper:
            return 0.0
        else:
            return (upper - close) / (upper - lower)

    def _compute_rsi_signal(self, df: pd.DataFrame) -> float:
        """RSI 信号：0-1，越接近 1 越超卖"""
        period = self.params["rsi_period"]
        oversold = self.params["rsi_oversold"]

        if len(df) < period + 1:
            return 0

        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).iloc[-period:].mean()
        loss = -delta.where(delta < 0, 0).iloc[-period:].mean()

        if loss == 0:
            rsi = 100
        else:
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))

        # RSI 越低越超卖
        if rsi <= oversold:
            return 1.0
        elif rsi >= self.params["rsi_overbought"]:
            return 0.0
        else:
            return (self.params["rsi_overbought"] - rsi) / (
                self.params["rsi_overbought"] - oversold
            )

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        from datetime import timedelta, datetime

        date = datetime.strptime(date_str, "%Y%m%d")
        prev_date = date - timedelta(days=n_days + n_days // 5)
        return prev_date.strftime("%Y%m%d")
