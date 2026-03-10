"""
规律学习模块
从历史数据中学习热点轮动规律
"""
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Tuple
from loguru import logger
from datetime import datetime
from collections import defaultdict
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories


class PatternLearner:
    """规律学习器"""

    def __init__(self):
        ensure_directories()
        self.patterns_dir = os.path.join(settings.data_dir, "patterns")
        os.makedirs(self.patterns_dir, exist_ok=True)

    def learn_rotation_rules(
        self,
        hotspot_scores: pd.DataFrame,
        rotation_paths: pd.DataFrame,
        lookback_days: int = 60
    ) -> Dict:
        """
        学习轮动规则

        Args:
            hotspot_scores: 热点评分数据
            rotation_paths: 轮动路径数据
            lookback_days: 回溯天数
        """
        rules = {
            "concept_sequences": {},  # 概念序列规律
            "duration_patterns": {},   # 持续时间规律
            "intensity_patterns": {},  # 强度变化规律
            "market_context": {}       # 市场环境规律
        }

        # 1. 学习概念序列规律
        rules["concept_sequences"] = self._learn_concept_sequences(hotspot_scores)

        # 2. 学习持续时间规律
        rules["duration_patterns"] = self._learn_duration_patterns(rotation_paths)

        # 3. 学习强度变化规律
        rules["intensity_patterns"] = self._learn_intensity_patterns(hotspot_scores)

        return rules

    def _learn_concept_sequences(
        self,
        hotspot_scores: pd.DataFrame,
        top_n: int = 5
    ) -> Dict:
        """学习概念序列规律"""
        # 获取每日Top N热点
        daily_hotspots = hotspot_scores.groupby("trade_date").apply(
            lambda x: x.nlargest(top_n, "hotspot_score")["concept_code"].tolist()
        )

        # 分析序列转换概率
        transitions = defaultdict(lambda: defaultdict(int))
        dates = sorted(daily_hotspots.index)

        for i in range(len(dates) - 1):
            today_hotspots = daily_hotspots[dates[i]]
            tomorrow_hotspots = daily_hotspots[dates[i + 1]]

            for today_concept in today_hotspots:
                for tomorrow_concept in tomorrow_hotspots:
                    if today_concept != tomorrow_concept:
                        transitions[today_concept][tomorrow_concept] += 1

        # 转换为概率
        sequence_probs = {}
        for from_concept, to_concepts in transitions.items():
            total = sum(to_concepts.values())
            sequence_probs[from_concept] = {
                k: v / total for k, v in sorted(
                    to_concepts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]  # 保留前10个
            }

        return sequence_probs

    def _learn_duration_patterns(
        self,
        rotation_paths: pd.DataFrame
    ) -> Dict:
        """学习持续时间规律"""
        patterns = {}

        # 按概念统计平均持续时间
        concept_duration = rotation_paths.groupby("concept_code")["duration"].agg(["mean", "std", "max", "min"])

        patterns["by_concept"] = concept_duration.to_dict("index")

        # 整体持续时间分布
        patterns["distribution"] = {
            "avg": rotation_paths["duration"].mean(),
            "std": rotation_paths["duration"].std(),
            "median": rotation_paths["duration"].median(),
            "quartiles": rotation_paths["duration"].quantile([0.25, 0.5, 0.75]).to_dict()
        }

        return patterns

    def _learn_intensity_patterns(
        self,
        hotspot_scores: pd.DataFrame
    ) -> Dict:
        """学习强度变化规律"""
        patterns = {}

        # 分析热点强度随时间的变化
        for concept in hotspot_scores["concept_code"].unique():
            concept_data = hotspot_scores[hotspot_scores["concept_code"] == concept].sort_values("trade_date")

            if len(concept_data) < 5:
                continue

            # 计算强度变化率
            concept_data["score_change"] = concept_data["hotspot_score"].diff()

            # 统计强度变化特征
            patterns[concept] = {
                "avg_score": concept_data["hotspot_score"].mean(),
                "avg_change": concept_data["score_change"].mean(),
                "volatility": concept_data["score_change"].std(),
                "peak_to_decay": self._analyze_peak_decay(concept_data)
            }

        return patterns

    def _analyze_peak_decay(self, concept_data: pd.DataFrame) -> Dict:
        """分析热点从高峰到衰退的模式"""
        # 找到评分最高的点
        peak_idx = concept_data["hotspot_score"].idxmax()

        if peak_idx is None:
            return {}

        peak_data = concept_data.loc[peak_idx]

        # 找到高峰后的数据
        post_peak = concept_data[concept_data.index > peak_idx]

        if len(post_peak) < 3:
            return {"peak_score": peak_data["hotspot_score"]}

        # 计算衰退速度
        decay_rate = (post_peak["hotspot_score"] - peak_data["hotspot_score"]) / peak_data["hotspot_score"]

        return {
            "peak_score": peak_data["hotspot_score"],
            "avg_decay_rate": decay_rate.mean(),
            "days_to_half": self._find_decay_days(decay_rate, -0.5)
        }

    def _find_decay_days(self, decay_rate: pd.Series, threshold: float) -> int:
        """找到衰退到阈值的天数"""
        for i, rate in enumerate(decay_rate):
            if rate <= threshold:
                return i + 1
        return -1  # 未找到

    def learn_market_context_rules(
        self,
        hotspot_scores: pd.DataFrame,
        market_data: pd.DataFrame
    ) -> Dict:
        """
        学习市场环境与热点的关系

        Args:
            hotspot_scores: 热点评分数据
            market_data: 市场数据（指数涨跌、成交量等）
        """
        rules = {}

        # 合并数据
        merged = hotspot_scores.merge(
            market_data,
            on="trade_date",
            how="left"
        )

        # 按市场涨跌分组分析
        if "index_pct_chg" in merged.columns:
            up_days = merged[merged["index_pct_chg"] > 0]
            down_days = merged[merged["index_pct_chg"] <= 0]

            rules["market_up_hotspots"] = (
                up_days.groupby("concept_code")["hotspot_score"]
                .mean()
                .sort_values(ascending=False)
                .head(20)
                .to_dict()
            )

            rules["market_down_hotspots"] = (
                down_days.groupby("concept_code")["hotspot_score"]
                .mean()
                .sort_values(ascending=False)
                .head(20)
                .to_dict()
            )

        # 按成交量变化分析
        if "vol_change" in merged.columns:
            high_vol = merged[merged["vol_change"] > merged["vol_change"].median()]
            low_vol = merged[merged["vol_change"] <= merged["vol_change"].median()]

            rules["high_volume_hotspots"] = (
                high_vol.groupby("concept_code")["hotspot_score"]
                .mean()
                .sort_values(ascending=False)
                .head(20)
                .to_dict()
            )

        return rules

    def build_knowledge_graph(
        self,
        correlation_matrix: pd.DataFrame,
        lead_lag_matrix: pd.DataFrame,
        transition_probs: Dict
    ) -> Dict:
        """
        构建概念知识图谱

        Args:
            correlation_matrix: 相关性矩阵
            lead_lag_matrix: 领涨滞后矩阵
            transition_probs: 转换概率
        """
        nodes = []
        edges = []

        # 创建节点
        for concept in correlation_matrix.columns:
            nodes.append({
                "id": concept,
                "type": "concept"
            })

        # 创建边（相关性）
        for i, concept1 in enumerate(correlation_matrix.columns):
            for concept2 in correlation_matrix.columns[i + 1:]:
                corr = correlation_matrix.loc[concept1, concept2]
                if abs(corr) > 0.3:  # 只保留显著相关
                    edges.append({
                        "source": concept1,
                        "target": concept2,
                        "type": "correlation",
                        "weight": corr
                    })

        # 创建边（领涨滞后）
        for concept1 in lead_lag_matrix.index:
            for concept2 in lead_lag_matrix.columns:
                if concept1 != concept2:
                    lead_lag = lead_lag_matrix.loc[concept1, concept2]
                    if lead_lag > 0.2:  # 只保留显著关系
                        edges.append({
                            "source": concept1,
                            "target": concept2,
                            "type": "lead_lag",
                            "weight": lead_lag
                        })

        # 创建边（转换概率）
        for from_concept, to_concepts in transition_probs.items():
            for to_concept, prob in to_concepts.items():
                if prob > 0.1:  # 只保留显著概率
                    edges.append({
                        "source": from_concept,
                        "target": to_concept,
                        "type": "transition",
                        "weight": prob
                    })

        return {
            "nodes": nodes,
            "edges": edges
        }

    def save_patterns(self, patterns: Dict, filename: str):
        """保存规律数据"""
        filepath = os.path.join(self.patterns_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(patterns, f, ensure_ascii=False, indent=2)
        logger.info(f"规律数据已保存: {filepath}")

    def load_patterns(self, filename: str) -> Dict:
        """加载规律数据"""
        filepath = os.path.join(self.patterns_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}


def main():
    """主函数"""
    learner = PatternLearner()
    logger.info("规律学习模块已就绪")


if __name__ == "__main__":
    main()
