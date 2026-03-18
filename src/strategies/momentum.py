"""
动量策略 (符合多策略框架版本)
基于价格动量和成交量突破的趋势跟踪策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class MomentumStrategy(BaseStrategy):
    """动量策略 - 跟随趋势"""

    def __init__(self, name: str = "momentum", params: Optional[Dict] = None):
        default_params = {
            "momentum_window": 20,  # 动量计算周期
            "volume_window": 20,  # 成交量计算周期
            "min_momentum": 0.05,  # 最小动量阈值 5%
            "min_volume_ratio": 1.5,  # 最小成交量比率
            "top_n_stocks": 20,  # 选股数量
        }
        default_params.update(params or {})

        super().__init__(name, default_params)
        self.db = None

    def _init_db(self):
        """懒加载数据库"""
        if self.db is None:
            from data.database import get_database

            self.db = get_database()

    def get_required_data(self) -> Dict[str, Any]:
        """需要个股日线数据"""
        return {
            "concept_data": False,
            "stock_data": True,
            "history_days": max(
                self.params["momentum_window"] * 2, self.params["volume_window"] * 2
            ),
            "features": ["close", "volume", "amount"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """生成动量信号"""
        logger.info("动量策略：生成信号...")
        self._init_db()

        signals = []

        # 获取所有股票代码
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            logger.warning("无股票数据")
            return []

        # 获取最新日期
        latest_date = self.db.get_latest_date()
        if not latest_date:
            logger.warning("无法获取最新日期")
            return []

        # 计算起始日期
        history_days = self.get_required_data()["history_days"]
        start_date = self._get_n_days_before(latest_date, history_days)

        # 获取所有股票的日线数据
        logger.info(f"获取 {len(all_stocks)} 只股票的历史数据...")
        stock_data = self.db.get_daily_batch(
            stock_codes=all_stocks["ts_code"].tolist(),
            start_date=start_date,
            end_date=latest_date,
        )

        if stock_data.empty:
            logger.warning("无法获取股票数据")
            return []

        # 按股票分组计算动量
        logger.info("计算动量指标...")

        for ts_code in all_stocks["ts_code"].unique():
            stock_df = stock_data[stock_data["ts_code"] == ts_code].sort_values(
                "trade_date"
            )

            if len(stock_df) < self.params["momentum_window"]:
                continue

            # 计算动量
            momentum = self._calculate_momentum(stock_df)

            # 计算成交量比率
            volume_ratio = self._calculate_volume_ratio(stock_df)

            # 判断是否满足买入条件
            if (
                momentum >= self.params["min_momentum"]
                and volume_ratio >= self.params["min_volume_ratio"]
            ):
                stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
                stock_name = (
                    stock_info["name"].iloc[0] if not stock_info.empty else ts_code
                )

                # 计算评分（动量 + 成交量）
                score = self._calculate_score(momentum, volume_ratio)

                signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=stock_name,
                    strategy_type="momentum",
                    signal_type="buy",
                    weight=min(1.0, momentum / 0.2),
                    score=score,
                    reason=f"20 日动量：{momentum:.1%}, 成交量比：{volume_ratio:.1f}x",
                    metadata={
                        "momentum_20d": momentum,
                        "volume_ratio": volume_ratio,
                        "latest_close": stock_df["close"].iloc[-1]
                        if len(stock_df) > 0
                        else 0,
                    },
                )
                signals.append(signal)

        # 按评分排序，取 TOP N
        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals

        logger.info(f"动量策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_momentum(self, stock_df: pd.DataFrame) -> float:
        """计算动量（N 日涨幅）"""
        window = self.params["momentum_window"]
        if len(stock_df) < window:
            return 0.0

        latest_close = stock_df["close"].iloc[-1]
        prev_close = stock_df["close"].iloc[-window]

        if prev_close == 0:
            return 0.0

        momentum = (latest_close - prev_close) / prev_close
        return momentum

    def _calculate_volume_ratio(self, stock_df: pd.DataFrame) -> float:
        """计算成交量比率"""
        window = self.params["volume_window"]
        if len(stock_df) < window:
            return 1.0

        latest_volume = stock_df["volume"].iloc[-1]
        avg_volume = stock_df["volume"].iloc[-window:-1].mean()

        if avg_volume == 0:
            return 1.0

        return latest_volume / avg_volume

    def _calculate_score(self, momentum: float, volume_ratio: float) -> float:
        """计算综合评分"""
        momentum_score = min(100, max(0, momentum * 200))
        volume_score = min(100, max(0, (volume_ratio - 1) * 50))
        total_score = momentum_score * 0.7 + volume_score * 0.3
        return min(100, max(0, total_score))

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        """获取 N 个交易日前的日期"""
        from datetime import timedelta
        from datetime import datetime

        date = datetime.strptime(date_str, "%Y%m%d")
        prev_date = date - timedelta(days=n_days + n_days // 5)
        return prev_date.strftime("%Y%m%d")
