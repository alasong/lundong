"""
热点识别模块
计算板块热点强度，识别市场热点
"""
import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from loguru import logger
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories


class HotspotDetector:
    """热点识别器"""

    def __init__(self):
        ensure_directories()
        self.weights = settings.hotspot_weights

    def compute_hotspot_score(
        self,
        concept_data: pd.DataFrame,
        moneyflow_data: Optional[pd.DataFrame] = None,
        limit_data: Optional[pd.DataFrame] = None,
        historical_data: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        计算热点强度综合评分

        Args:
            concept_data: 概念板块行情数据（同花顺格式：concept_code, trade_date, pct_chg, name）
            moneyflow_data: 资金流向数据
            limit_data: 涨跌停数据
            historical_data: 历史数据（用于计算持续性）
        """
        results = []

        for trade_date in concept_data["trade_date"].unique():
            day_data = concept_data[concept_data["trade_date"] == trade_date]
            day_moneyflow = moneyflow_data[moneyflow_data["trade_date"] == trade_date] if moneyflow_data is not None else None
            day_limit = limit_data[limit_data["trade_date"] == trade_date] if limit_data is not None else None

            for _, row in day_data.iterrows():
                scores = {
                    "trade_date": trade_date,
                    "concept_code": row.get("concept_code", ""),
                    "concept_name": row.get("name", ""),
                }

                # 1. 涨幅强度（百分位排名）
                scores["price_strength"] = self._compute_price_strength(row, day_data)

                # 2. 资金强度
                scores["money_strength"] = self._compute_money_strength(row, day_moneyflow)

                # 3. 情绪强度
                scores["sentiment_strength"] = self._compute_sentiment_strength(row, day_limit)

                # 4. 持续性
                scores["persistence"] = self._compute_persistence(row, historical_data)

                # 5. 市场地位
                scores["market_position"] = self._compute_market_position(row, day_data)

                # 计算综合评分
                scores["hotspot_score"] = (
                    self.weights["price_strength"] * scores["price_strength"] +
                    self.weights["money_strength"] * scores["money_strength"] +
                    self.weights["sentiment_strength"] * scores["sentiment_strength"] +
                    self.weights["persistence"] * scores["persistence"] +
                    self.weights["market_position"] * scores["market_position"]
                )

                results.append(scores)

        return pd.DataFrame(results)

    def _compute_price_strength(self, row: pd.Series, day_data: pd.DataFrame) -> float:
        """计算涨幅强度（百分位排名 0-100）"""
        pct_chg = row.get("pct_chg", 0)
        percentile = (day_data["pct_chg"] <= pct_chg).mean() * 100
        return round(percentile, 2)

    def _compute_money_strength(
        self,
        row: pd.Series,
        moneyflow_data: Optional[pd.DataFrame]
    ) -> float:
        """计算资金强度"""
        if moneyflow_data is None or moneyflow_data.empty:
            return 50.0  # 默认中等强度

        # 计算资金净流入占比
        if "main_net_ratio" in moneyflow_data.columns:
            net_ratio = row.get("main_net_ratio", 0)
            # 归一化到0-100
            max_ratio = moneyflow_data["main_net_ratio"].abs().max()
            if max_ratio > 0:
                return min(100, max(0, 50 + net_ratio / max_ratio * 50))
        return 50.0

    def _compute_sentiment_strength(
        self,
        row: pd.Series,
        limit_data: Optional[pd.DataFrame]
    ) -> float:
        """计算情绪强度"""
        if limit_data is None or limit_data.empty:
            return 50.0

        # 基于板块内涨停股数量计算情绪强度
        # 需要板块成分股数据，这里简化处理
        # 实际应该计算该板块内涨停股数量占比

        pct_chg = row.get("pct_chg", 0)
        # 涨幅越大，情绪越强
        if pct_chg > 5:
            return min(100, 70 + pct_chg * 2)
        elif pct_chg > 3:
            return 60 + pct_chg * 3
        elif pct_chg > 0:
            return 50 + pct_chg * 3
        else:
            return max(0, 50 + pct_chg * 2)

    def _compute_persistence(
        self,
        row: pd.Series,
        historical_data: Optional[pd.DataFrame]
    ) -> float:
        """计算持续性"""
        if historical_data is None or historical_data.empty:
            return 50.0

        concept_code = row.get("concept_code", "")

        # 获取该概念的历史数据
        concept_history = historical_data[historical_data["concept_code"] == concept_code]

        if len(concept_history) < 2:
            return 50.0

        # 计算连续上涨天数
        recent = concept_history.tail(5)
        up_days = (recent["pct_chg"] > 0).sum()

        # 连续上涨越多，分数越高
        return min(100, 40 + up_days * 15)

    def _compute_market_position(self, row: pd.Series, day_data: pd.DataFrame) -> float:
        """计算市场地位（成交额占比百分位）"""
        amount = row.get("amount", 0)
        if amount <= 0:
            return 50.0

        # 成交额百分位排名
        percentile = (day_data["amount"] <= amount).mean() * 100
        return round(percentile, 2)

    def identify_hotspots(
        self,
        scores_df: pd.DataFrame,
        top_n: int = 10,
        min_score: float = 60.0
    ) -> pd.DataFrame:
        """
        识别热点板块

        Args:
            scores_df: 热点评分数据
            top_n: 返回前N个热点
            min_score: 最低评分阈值
        """
        # 按评分排序
        hotspots = scores_df[scores_df["hotspot_score"] >= min_score].copy()
        hotspots = hotspots.sort_values("hotspot_score", ascending=False)

        return hotspots.head(top_n)

    def compute_hotspot_ranking(
        self,
        scores_df: pd.DataFrame,
        group_by_date: bool = True
    ) -> pd.DataFrame:
        """
        计算热点排名

        Args:
            scores_df: 热点评分数据
            group_by_date: 是否按日期分组排名
        """
        if group_by_date:
            scores_df["rank"] = scores_df.groupby("trade_date")["hotspot_score"].rank(
                ascending=False, method="min"
            )
        else:
            scores_df["rank"] = scores_df["hotspot_score"].rank(
                ascending=False, method="min"
            )

        return scores_df.sort_values(["trade_date", "rank"])

    def detect_hotspot_emergence(
        self,
        scores_df: pd.DataFrame,
        threshold_rank: int = 5,
        threshold_score_diff: float = 10.0
    ) -> pd.DataFrame:
        """
        检测新出现的热点

        Args:
            scores_df: 热点评分数据
            threshold_rank: 排名阈值
            threshold_score_diff: 评分提升阈值
        """
        emergence = []

        for concept_code in scores_df["concept_code"].unique():
            concept_data = scores_df[scores_df["concept_code"] == concept_code].sort_values("trade_date")

            if len(concept_data) < 2:
                continue

            # 获取最近两天的数据
            recent = concept_data.tail(2)

            if len(recent) == 2:
                today = recent.iloc[-1]
                yesterday = recent.iloc[-2]

                # 检查是否是新热点
                if (
                    today["rank"] <= threshold_rank and
                    today["hotspot_score"] - yesterday["hotspot_score"] >= threshold_score_diff
                ):
                    emergence.append({
                        "concept_code": concept_code,
                        "concept_name": today["concept_name"],
                        "trade_date": today["trade_date"],
                        "current_rank": today["rank"],
                        "previous_rank": yesterday["rank"],
                        "rank_change": yesterday["rank"] - today["rank"],
                        "score_change": today["hotspot_score"] - yesterday["hotspot_score"]
                    })

        return pd.DataFrame(emergence)


def main():
    """主函数"""
    detector = HotspotDetector()
    logger.info("热点识别模块已就绪")

    # 示例：使用模拟数据测试
    # 实际使用时需要加载真实数据


if __name__ == "__main__":
    main()
