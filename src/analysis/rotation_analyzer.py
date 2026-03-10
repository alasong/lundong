"""
轮动分析模块
分析板块轮动规律，识别轮动信号
"""
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Tuple
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories


class RotationAnalyzer:
    """轮动分析器"""

    def __init__(self):
        ensure_directories()

    def compute_correlation_matrix(
        self,
        price_data: pd.DataFrame,
        window: int = 20
    ) -> pd.DataFrame:
        """
        计算板块相关性矩阵

        Args:
            price_data: 板块价格数据，需包含 trade_date, concept_code, pct_chg
            window: 滚动窗口
        """
        # 去重：保留每个 trade_date + concept_code 组合的第一条记录
        price_data = price_data.drop_duplicates(subset=["trade_date", "concept_code"], keep="first")

        # 转换为宽格式
        pivot = price_data.pivot(
            index="trade_date",
            columns="concept_code",
            values="pct_chg"
        )

        # 计算滚动相关性
        if window:
            rolling_corr = pivot.rolling(window=window).corr()
            # 取最近一天的相关性
            if len(rolling_corr) > 0:
                latest_date = rolling_corr.index[-1][0]
                corr_matrix = rolling_corr.xs(latest_date, level=0)
            else:
                corr_matrix = pd.DataFrame(index=pivot.columns, columns=pivot.columns)
        else:
            corr_matrix = pivot.corr()

        return corr_matrix

    def compute_lead_lag_matrix(
        self,
        price_data: pd.DataFrame,
        max_lag: int = 5
    ) -> pd.DataFrame:
        """
        计算领涨-滞后关系矩阵

        Args:
            price_data: 板块价格数据
            max_lag: 最大滞后期数
        """
        # 去重：保留每个 trade_date + concept_code 组合的第一条记录
        price_data = price_data.drop_duplicates(subset=["trade_date", "concept_code"], keep="first")

        pivot = price_data.pivot(
            index="trade_date",
            columns="concept_code",
            values="pct_chg"
        )

        concepts = pivot.columns.tolist()
        lead_lag = pd.DataFrame(index=concepts, columns=concepts, dtype=float)

        for leader in concepts:
            for lagger in concepts:
                if leader == lagger:
                    lead_lag.loc[leader, lagger] = 0
                    continue

                # 计算交叉相关性
                leader_series = pivot[leader]
                lagger_series = pivot[lagger]

                best_corr = 0
                best_lag = 0

                for lag in range(1, max_lag + 1):
                    corr = leader_series.shift(lag).corr(lagger_series)
                    if abs(corr) > abs(best_corr):
                        best_corr = corr
                        best_lag = lag

                lead_lag.loc[leader, lagger] = best_corr if best_corr > 0 else 0

        return lead_lag

    def compute_money_transfer_matrix(
        self,
        moneyflow_data: pd.DataFrame,
        concept_mapping: pd.DataFrame
    ) -> pd.DataFrame:
        """
        计算资金流向转移矩阵

        Args:
            moneyflow_data: 资金流向数据
            concept_mapping: 股票-概念映射关系
        """
        # 合并资金流向和概念映射
        merged = moneyflow_data.merge(
            concept_mapping,
            on="ts_code",
            how="left"
        )

        # 计算每个概念的资金净流入
        concept_flow = merged.groupby(["trade_date", "concept_code"])["main_net_inflow"].sum().reset_index()

        # 转换为宽格式
        pivot = concept_flow.pivot(
            index="trade_date",
            columns="concept_code",
            values="main_net_inflow"
        )

        # 计算资金流向变化
        flow_change = pivot.diff()

        return flow_change

    def compute_rotation_strength_index(
        self,
        price_data: pd.DataFrame,
        window: int = 20
    ) -> pd.Series:
        """
        计算轮动强度指数（RSI）

        衡量市场热点的轮动程度

        Args:
            price_data: 板块价格数据
            window: 计算窗口
        """
        pivot = price_data.pivot(
            index="trade_date",
            columns="concept_code",
            values="pct_chg"
        )

        # 计算每个板块的排名变化
        ranks = pivot.rank(axis=1, ascending=False)

        # 计算排名变化幅度
        rank_change = ranks.diff().abs().sum(axis=1)

        # 归一化
        max_change = len(pivot.columns) * (len(pivot.columns) - 1) / 2
        rsi = rank_change / max_change * 100

        return rsi.rolling(window=window).mean()

    def identify_rotation_signal(
        self,
        hotspot_scores: pd.DataFrame,
        correlation_matrix: pd.DataFrame,
        lead_lag_matrix: pd.DataFrame,
        threshold: float = 0.3
    ) -> List[Dict]:
        """
        识别轮动信号

        Args:
            hotspot_scores: 热点评分数据
            correlation_matrix: 相关性矩阵
            lead_lag_matrix: 领涨滞后矩阵
            threshold: 相关性阈值
        """
        signals = []

        # 获取当前热点
        current_hotspots = hotspot_scores.sort_values("hotspot_score", ascending=False).head(10)
        hot_concepts = current_hotspots["concept_code"].tolist()

        for concept in correlation_matrix.columns:
            if concept in hot_concepts:
                continue

            # 检查与当前热点的相关性
            related_hotspots = []

            for hot_concept in hot_concepts:
                corr = correlation_matrix.loc[concept, hot_concept]
                lead_lag = lead_lag_matrix.loc[hot_concept, concept]

                # 如果相关且滞后，可能是下一个轮动目标
                if corr > threshold and lead_lag > 0:
                    related_hotspots.append({
                        "hot_concept": hot_concept,
                        "correlation": corr,
                        "lead_lag_score": lead_lag
                    })

            if related_hotspots:
                signals.append({
                    "target_concept": concept,
                    "related_hotspots": related_hotspots,
                    "rotation_probability": np.mean([r["correlation"] for r in related_hotspots]),
                    "signal_type": "potential_next"
                })

        # 按轮动概率排序
        signals = sorted(signals, key=lambda x: x["rotation_probability"], reverse=True)

        return signals

    def compute_rotation_path(
        self,
        hotspot_scores: pd.DataFrame,
        min_periods: int = 3
    ) -> pd.DataFrame:
        """
        计算历史轮动路径

        Args:
            hotspot_scores: 热点评分数据
            min_periods: 最小周期数
        """
        # 获取每日热点排名
        rankings = hotspot_scores.pivot(
            index="trade_date",
            columns="concept_code",
            values="rank"
        )

        # 识别每个热点的生命周期
        paths = []

        for concept in rankings.columns:
            concept_ranks = rankings[concept]

            # 找到进入热点的时间点（排名<=5）
            in_hotspot = concept_ranks <= 5

            # 识别连续的热点期
            hotspot_periods = []
            start = None

            for i, (date, is_hot) in enumerate(in_hotspot.items()):
                if is_hot and start is None:
                    start = date
                elif not is_hot and start is not None:
                    hotspot_periods.append((start, date))
                    start = None

            if start is not None:
                hotspot_periods.append((start, in_hotspot.index[-1]))

            for start_date, end_date in hotspot_periods:
                duration = len(in_hotspot[start_date:end_date])
                if duration >= min_periods:
                    paths.append({
                        "concept_code": concept,
                        "start_date": start_date,
                        "end_date": end_date,
                        "duration": duration,
                        "peak_rank": concept_ranks[start_date:end_date].min(),
                        "avg_rank": concept_ranks[start_date:end_date].mean()
                    })

        return pd.DataFrame(paths)

    def compute_rotation_patterns(
        self,
        rotation_paths: pd.DataFrame
    ) -> Dict:
        """
        分析轮动模式

        Args:
            rotation_paths: 轮动路径数据
        """
        patterns = {
            "avg_duration": rotation_paths["duration"].mean(),
            "max_duration": rotation_paths["duration"].max(),
            "concepts_by_frequency": rotation_paths["concept_code"].value_counts().to_dict(),
            "duration_distribution": rotation_paths["duration"].value_counts().sort_index().to_dict()
        }

        # 分析概念之间的轮动顺序
        # 找出经常连续出现的概念对
        concepts_by_date = rotation_paths.sort_values("start_date").groupby("start_date")["concept_code"].apply(list).to_dict()

        transition_pairs = {}
        dates = sorted(concepts_by_date.keys())

        for i in range(len(dates) - 1):
            current = concepts_by_date[dates[i]]
            next_concepts = concepts_by_date[dates[i + 1]]

            for c in current:
                for n in next_concepts:
                    if c != n:
                        pair = (c, n)
                        transition_pairs[pair] = transition_pairs.get(pair, 0) + 1

        patterns["common_transitions"] = dict(
            sorted(transition_pairs.items(), key=lambda x: x[1], reverse=True)[:20]
        )

        return patterns


def main():
    """主函数"""
    analyzer = RotationAnalyzer()
    logger.info("轮动分析模块已就绪")


if __name__ == "__main__":
    main()
