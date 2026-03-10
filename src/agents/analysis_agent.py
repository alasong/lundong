"""
分析Agent
负责热点识别和轮动分析
"""
import pandas as pd
from typing import Optional, Dict, Any, List
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.base_agent import BaseAgent, AgentResult
from analysis.hotspot_detector import HotspotDetector
from analysis.rotation_analyzer import RotationAnalyzer
from analysis.pattern_learner import PatternLearner
from config import settings


class AnalysisAgent(BaseAgent):
    """分析Agent"""

    def __init__(self):
        super().__init__("AnalysisAgent")
        self.hotspot_detector = HotspotDetector()
        self.rotation_analyzer = RotationAnalyzer()
        self.pattern_learner = PatternLearner()

    def run(
        self,
        task: str = "hotspot",
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行分析任务

        Args:
            task: 任务类型 hotspot/rotation/pattern/all
            data: 输入数据字典
        """
        if task == "hotspot":
            return self._analyze_hotspot(data, **kwargs)
        elif task == "rotation":
            return self._analyze_rotation(data, **kwargs)
        elif task == "pattern":
            return self._learn_patterns(data, **kwargs)
        elif task == "all":
            return self._full_analysis(data, **kwargs)
        else:
            raise ValueError(f"未知任务类型: {task}")

    def _analyze_hotspot(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """分析热点"""
        logger.info("开始热点分析")

        if data is None:
            data = self._load_latest_data()

        concept_data = data.get("concept")
        moneyflow_data = data.get("moneyflow")
        limit_data = data.get("limit")

        if concept_data is None or concept_data.empty:
            return {"error": "无概念板块数据"}

        # 计算热点评分
        scores = self.hotspot_detector.compute_hotspot_score(
            concept_data=concept_data,
            moneyflow_data=moneyflow_data,
            limit_data=limit_data,
            historical_data=data.get("concept_history")
        )

        # 识别热点
        hotspots = self.hotspot_detector.identify_hotspots(
            scores_df=scores,
            top_n=10,
            min_score=60.0
        )

        # 检测新出现的热点
        emergence = self.hotspot_detector.detect_hotspot_emergence(scores)

        return {
            "hotspot_scores": scores.to_dict("records"),
            "top_hotspots": hotspots.to_dict("records"),
            "emerging_hotspots": emergence.to_dict("records") if not emergence.empty else []
        }

    def _analyze_rotation(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """分析轮动"""
        logger.info("开始轮动分析")

        if data is None:
            data = self._load_latest_data()

        concept_data = data.get("concept")

        if concept_data is None or concept_data.empty:
            return {"error": "无概念板块数据"}

        # 计算相关性矩阵
        corr_matrix = self.rotation_analyzer.compute_correlation_matrix(
            price_data=concept_data,
            window=20
        )

        # 计算领涨滞后矩阵
        lead_lag_matrix = self.rotation_analyzer.compute_lead_lag_matrix(
            price_data=concept_data,
            max_lag=5
        )

        # 计算轮动强度指数
        rsi = self.rotation_analyzer.compute_rotation_strength_index(
            price_data=concept_data,
            window=20
        )

        # 计算轮动路径
        hotspot_scores = data.get("hotspot_scores")
        if hotspot_scores is not None:
            rotation_paths = self.rotation_analyzer.compute_rotation_path(hotspot_scores)
            rotation_patterns = self.rotation_analyzer.compute_rotation_patterns(rotation_paths)
        else:
            rotation_paths = pd.DataFrame()
            rotation_patterns = {}

        # 识别轮动信号
        signals = []
        if hotspot_scores is not None:
            signals = self.rotation_analyzer.identify_rotation_signal(
                hotspot_scores=hotspot_scores,
                correlation_matrix=corr_matrix,
                lead_lag_matrix=lead_lag_matrix
            )

        return {
            "correlation_matrix": corr_matrix.to_dict(),
            "lead_lag_matrix": lead_lag_matrix.to_dict(),
            "rotation_strength": rsi.to_dict() if isinstance(rsi, pd.Series) else rsi,
            "rotation_paths": rotation_paths.to_dict("records") if not rotation_paths.empty else [],
            "rotation_patterns": rotation_patterns,
            "rotation_signals": signals
        }

    def _learn_patterns(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """学习规律"""
        logger.info("开始规律学习")

        if data is None:
            data = self._load_latest_data()

        hotspot_scores = data.get("hotspot_scores")
        rotation_paths = data.get("rotation_paths")

        if hotspot_scores is None:
            return {"error": "无热点评分数据"}

        # 学习轮动规则
        rules = self.pattern_learner.learn_rotation_rules(
            hotspot_scores=hotspot_scores,
            rotation_paths=rotation_paths or pd.DataFrame()
        )

        # 学习市场环境规则
        market_data = data.get("market")
        if market_data is not None:
            market_rules = self.pattern_learner.learn_market_context_rules(
                hotspot_scores=hotspot_scores,
                market_data=market_data
            )
            rules.update(market_rules)

        # 保存规律
        self.pattern_learner.save_patterns(rules, "rotation_rules.json")

        return rules

    def _full_analysis(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """完整分析"""
        logger.info("开始完整分析")

        if data is None:
            data = self._load_latest_data()

        results = {}

        # 热点分析
        hotspot_result = self._analyze_hotspot(data)
        results["hotspot"] = hotspot_result

        # 更新数据
        if "hotspot_scores" in hotspot_result:
            data["hotspot_scores"] = pd.DataFrame(hotspot_result["hotspot_scores"])

        # 轮动分析
        rotation_result = self._analyze_rotation(data)
        results["rotation"] = rotation_result

        # 规律学习
        pattern_result = self._learn_patterns(data)
        results["patterns"] = pattern_result

        return results

    def _load_latest_data(self) -> Dict[str, pd.DataFrame]:
        """加载最新数据"""
        data = {}

        # 查找最新的数据文件
        raw_dir = settings.raw_data_dir

        # 概念板块数据
        concept_files = sorted([f for f in os.listdir(raw_dir) if f.startswith("concept_daily_")])
        if concept_files:
            latest_concept = concept_files[-1]
            data["concept"] = pd.read_csv(os.path.join(raw_dir, latest_concept))

        # 资金流向数据
        moneyflow_files = sorted([f for f in os.listdir(raw_dir) if f.startswith("moneyflow_")])
        if moneyflow_files:
            latest_moneyflow = moneyflow_files[-1]
            data["moneyflow"] = pd.read_csv(os.path.join(raw_dir, latest_moneyflow))

        # 涨跌停数据
        limit_up_files = sorted([f for f in os.listdir(raw_dir) if f.startswith("limit_up_")])
        limit_down_files = sorted([f for f in os.listdir(raw_dir) if f.startswith("limit_down_")])

        if limit_up_files and limit_down_files:
            limit_up = pd.read_csv(os.path.join(raw_dir, limit_up_files[-1]))
            limit_down = pd.read_csv(os.path.join(raw_dir, limit_down_files[-1]))
            limit_up["limit_type"] = "U"
            limit_down["limit_type"] = "D"
            data["limit"] = pd.concat([limit_up, limit_down], ignore_index=True)

        return data


def main():
    """主函数"""
    agent = AnalysisAgent()
    result = agent.execute(task="all")
    print(f"分析结果: {result}")


if __name__ == "__main__":
    main()
