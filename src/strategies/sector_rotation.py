"""
行业轮动策略
基于行业景气度和资金流入的行业轮动选股
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class SectorRotationStrategy(BaseStrategy):
    """行业轮动策略 - 基于行业景气度和资金流向"""

    def __init__(self, name: str = "sector_rotation", params: Optional[Dict] = None):
        default_params = {
            "lookback_days": 20,  # 回看天数
            "min_sector_momentum": 0.05,  # 最小行业动量 5%
            "top_n_sectors": 5,  # 选择 TOP N 行业
            "stocks_per_sector": 3,  # 每个行业选股数量
            "use_moneyflow": True,  # 是否使用资金流
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
            "concept_data": True,
            "stock_data": True,
            "history_days": 60,
            "features": ["close", "volume", "moneyflow", "industry"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        logger.info("行业轮动策略：生成信号...")
        self._init_db()

        signals = []

        # 获取最新日期
        latest_date = self.db.get_latest_date()
        if not latest_date:
            logger.warning("无法获取最新日期")
            return []

        # 获取行业数据（使用板块数据模拟）
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            logger.warning("无股票数据")
            return []

        # 计算各行业动量
        sector_momentum = self._calculate_sector_momentum(latest_date)

        if not sector_momentum:
            logger.warning("无法计算行业动量")
            return []

        # 选择强势行业
        top_sectors = sorted(sector_momentum.items(), key=lambda x: x[1], reverse=True)[
            : self.params["top_n_sectors"]
        ]

        logger.info(f"强势行业：{[s[0] for s in top_sectors]}")

        # 从强势行业中选股
        for sector_name, momentum in top_sectors:
            if momentum < self.params["min_sector_momentum"]:
                continue

            # 获取该行业股票
            sector_stocks = self._get_sector_stocks(sector_name)

            for ts_code in sector_stocks[: self.params["stocks_per_sector"]]:
                stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
                stock_name = (
                    stock_info["name"].iloc[0] if not stock_info.empty else ts_code
                )

                score = self._calculate_score(momentum)

                signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=stock_name,
                    strategy_type="sector_rotation",
                    signal_type="buy",
                    weight=0.5,
                    score=score,
                    reason=f"行业：{sector_name}，动量：{momentum:.1%}",
                    metadata={"sector": sector_name, "sector_momentum": momentum},
                )
                signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        self.signals = signals
        logger.info(f"行业轮动策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_sector_momentum(self, latest_date: str) -> Dict[str, float]:
        """计算各行业动量"""
        # 简化实现：使用板块数据计算动量
        start_date = self._get_n_days_before(latest_date, self.params["lookback_days"])

        try:
            concept_data = self.db.get_all_concept_data()
            if concept_data.empty:
                return {}

            momentum = {}
            for concept_code in concept_data["ts_code"].unique():
                concept_df = concept_data[concept_data["ts_code"] == concept_code]
                if len(concept_df) < 2:
                    continue

                concept_df = concept_df.sort_values("trade_date")
                latest = concept_df.iloc[-1]
                prev = (
                    concept_df.iloc[-self.params["lookback_days"]]
                    if len(concept_df) > self.params["lookback_days"]
                    else concept_df.iloc[0]
                )

                if prev["close"] > 0:
                    m = (latest["close"] - prev["close"]) / prev["close"]
                    momentum[concept_code] = m

            return momentum
        except Exception as e:
            logger.error(f"计算行业动量失败：{e}")
            return {}

    def _get_sector_stocks(self, sector_name: str) -> List[str]:
        """获取行业内的股票"""
        # 简化实现：返回所有股票（实际应根据行业映射）
        all_stocks = self.db.get_all_stocks()
        if all_stocks.empty:
            return []
        return all_stocks["ts_code"].tolist()[:20]

    def _calculate_score(self, momentum: float) -> float:
        """计算评分"""
        score = min(100, max(0, 50 + momentum * 500))
        return score

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        from datetime import timedelta, datetime

        date = datetime.strptime(date_str, "%Y%m%d")
        prev_date = date - timedelta(days=n_days + n_days // 5)
        return prev_date.strftime("%Y%m%d")
