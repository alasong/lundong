"""
热点和轮动分析测试
"""
import os
import pytest
import pandas as pd
import numpy as np

from analysis.hotspot_detector import HotspotDetector
from analysis.rotation_analyzer import RotationAnalyzer
from analysis.pattern_learner import PatternLearner
from agents.analysis_agent import AnalysisAgent


class TestHotspotDetector:
    """热点识别器测试类"""

    def test_detector_init(self):
        """测试热点识别器初始化"""
        detector = HotspotDetector()
        assert detector is not None
        assert detector.weights is not None

    def test_compute_price_strength(self):
        """测试涨幅强度计算"""
        detector = HotspotDetector()

        day_data = pd.DataFrame({
            "pct_chg": [1.0, 2.0, 3.0, 4.0, 5.0]
        })

        row = pd.Series({"pct_chg": 3.0})
        strength = detector._compute_price_strength(row, day_data)

        assert 0 <= strength <= 100
        assert strength == 60.0  # 3.0 是第 3 大的，百分位是 60%

    def test_compute_money_strength_no_data(self):
        """测试资金强度计算（无资金数据）"""
        detector = HotspotDetector()
        row = pd.Series({})
        strength = detector._compute_money_strength(row, None)
        assert strength == 50.0  # 默认中等强度

    def test_compute_money_strength_with_data(self):
        """测试资金强度计算（有资金数据）"""
        detector = HotspotDetector()

        moneyflow_data = pd.DataFrame({
            "main_net_ratio": [-5.0, -2.0, 0.0, 2.0, 5.0]
        })

        row = pd.Series({"main_net_ratio": 2.0})
        strength = detector._compute_money_strength(row, moneyflow_data)

        assert 0 <= strength <= 100
        assert strength > 50.0  # 正流入应该高于平均水平

    def test_compute_sentiment_strength_no_data(self):
        """测试情绪强度计算（无限跌数据）"""
        detector = HotspotDetector()
        row = pd.Series({})
        strength = detector._compute_sentiment_strength(row, None)
        assert strength == 50.0

    def test_compute_sentiment_strength_high_gain(self):
        """测试情绪强度计算（高涨幅）"""
        detector = HotspotDetector()
        row = pd.Series({"pct_chg": 6.0})  # 涨幅 6%
        strength = detector._compute_sentiment_strength(row, pd.DataFrame({"dummy": [1]}))
        assert strength > 70.0

    def test_compute_persistence_no_data(self):
        """测试持续性计算（无历史数据）"""
        detector = HotspotDetector()
        row = pd.Series({"ts_code": "test"})
        persistence = detector._compute_persistence(row, None)
        assert persistence == 50.0

    def test_compute_market_position(self):
        """测试市场地位计算"""
        detector = HotspotDetector()

        day_data = pd.DataFrame({
            "amount": [100, 200, 300, 400, 500]
        })

        row = pd.Series({"amount": 300})
        position = detector._compute_market_position(row, day_data)

        assert 0 <= position <= 100
        assert position == 60.0  # 300 是第 3 大的

    def test_compute_hotspot_score(self, sample_concept_data):
        """测试热点综合评分计算"""
        detector = HotspotDetector()

        # 准备测试数据 - 确保列名正确
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        scores = detector.compute_hotspot_score(
            concept_data=concept_data,
            moneyflow_data=None,
            limit_data=None,
            historical_data=concept_data  # 使用相同数据作为历史数据
        )

        assert not scores.empty
        assert "hotspot_score" in scores.columns
        assert "trade_date" in scores.columns
        assert "concept_code" in scores.columns

    def test_identify_hotspots(self, sample_concept_data):
        """测试识别热点板块"""
        detector = HotspotDetector()

        concept_data = sample_concept_data.copy()

        # 计算评分
        scores = detector.compute_hotspot_score(concept_data=concept_data)

        # 识别热点
        hotspots = detector.identify_hotspots(scores, top_n=5, min_score=60.0)

        assert isinstance(hotspots, pd.DataFrame)
        assert len(hotspots) <= 5

    def test_detect_hotspot_emergence(self, sample_concept_data):
        """测试热点涌现检测"""
        detector = HotspotDetector()

        concept_data = sample_concept_data.copy()
        scores = detector.compute_hotspot_score(concept_data=concept_data)
        scores["rank"] = scores.groupby("trade_date")["hotspot_score"].rank(ascending=False, method="min")

        emergence = detector.detect_hotspot_emergence(scores)

        assert isinstance(emergence, pd.DataFrame)


class TestRotationAnalyzer:
    """轮动分析器测试类"""

    def test_analyzer_init(self):
        """测试轮动分析器初始化"""
        analyzer = RotationAnalyzer()
        assert analyzer is not None

    def test_compute_correlation_matrix(self, sample_concept_data):
        """测试相关性矩阵计算"""
        analyzer = RotationAnalyzer()

        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        # 使用较大的 window 值以适应测试数据
        corr_matrix = analyzer.compute_correlation_matrix(concept_data, window=5)

        assert isinstance(corr_matrix, pd.DataFrame)
        assert not corr_matrix.empty

    def test_compute_lead_lag_matrix(self, sample_concept_data):
        """测试领涨 - 滞后矩阵计算"""
        analyzer = RotationAnalyzer()

        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        lead_lag = analyzer.compute_lead_lag_matrix(concept_data, max_lag=5)

        assert isinstance(lead_lag, pd.DataFrame)
        assert lead_lag.shape[0] == lead_lag.shape[1]

    def test_compute_rotation_strength_index(self, sample_concept_data):
        """测试轮动强度指数计算"""
        analyzer = RotationAnalyzer()

        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        rsi = analyzer.compute_rotation_strength_index(concept_data, window=20)

        assert isinstance(rsi, pd.Series)
        assert not rsi.empty

    def test_compute_rotation_path(self, sample_concept_data):
        """测试轮动路径计算"""
        analyzer = RotationAnalyzer()
        detector = HotspotDetector()

        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        # 先计算热点评分
        scores = detector.compute_hotspot_score(concept_data=concept_data)

        # 添加 rank 列
        if not scores.empty:
            scores["rank"] = scores.groupby("trade_date")["hotspot_score"].rank(ascending=False, method="min")

            rotation_path = analyzer.compute_rotation_path(scores, min_periods=3)

            assert isinstance(rotation_path, pd.DataFrame)
        else:
            # 如果评分为空，跳过测试
            pytest.skip("无法生成热点评分")

    def test_compute_rotation_patterns(self):
        """测试轮动模式计算"""
        analyzer = RotationAnalyzer()

        rotation_paths = pd.DataFrame({
            "concept_code": ["A", "B", "A", "C"],
            "start_date": ["20240101", "20240105", "20240110", "20240115"],
            "end_date": ["20240104", "20240109", "20240114", "20240119"],
            "duration": [3, 4, 4, 4],
        })

        patterns = analyzer.compute_rotation_patterns(rotation_paths)

        assert isinstance(patterns, dict)
        assert "avg_duration" in patterns
        assert "max_duration" in patterns


class TestPatternLearner:
    """模式学习器测试类"""

    def test_learner_init(self):
        """测试模式学习器初始化"""
        learner = PatternLearner()
        assert learner is not None

    def test_learn_rotation_rules(self, sample_concept_data):
        """测试轮动规则学习"""
        learner = PatternLearner()
        detector = HotspotDetector()

        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        scores = detector.compute_hotspot_score(concept_data=concept_data)

        if not scores.empty:
            scores["rank"] = scores.groupby("trade_date")["hotspot_score"].rank(ascending=False, method="min")
            rotation_paths = pd.DataFrame({
                "concept_code": scores["concept_code"].unique()[:3],
                "start_date": ["20240101", "20240105", "20240110"],
                "end_date": ["20240104", "20240109", "20240114"],
                "duration": [3, 4, 4],
            })
            rules = learner.learn_rotation_rules(hotspot_scores=scores, rotation_paths=rotation_paths)

            assert isinstance(rules, dict)
        else:
            pytest.skip("无法生成热点评分")


class TestAnalysisAgent:
    """分析 Agent 测试类"""

    def test_agent_init(self):
        """测试分析 Agent 初始化"""
        agent = AnalysisAgent()
        assert agent is not None
        assert agent.hotspot_detector is not None
        assert agent.rotation_analyzer is not None

    def test_agent_run_hotspot(self, sample_concept_data):
        """测试热点分析任务"""
        agent = AnalysisAgent()

        data = {"concept": sample_concept_data}
        result = agent.run(task="hotspot", data=data)

        assert result is not None
        assert isinstance(result, dict)

    def test_agent_run_rotation(self, sample_concept_data):
        """测试轮动分析任务"""
        agent = AnalysisAgent()

        data = {"concept": sample_concept_data.rename(columns={"ts_code": "concept_code"})}
        result = agent.run(task="rotation", data=data)

        assert result is not None
        assert isinstance(result, dict)

    def test_agent_run_all(self, sample_concept_data):
        """测试完整分析任务"""
        agent = AnalysisAgent()

        concept_data = sample_concept_data.rename(columns={"ts_code": "concept_code"})

        # 先计算热点评分
        detector = HotspotDetector()
        scores = detector.compute_hotspot_score(concept_data=concept_data)

        if not scores.empty:
            scores["rank"] = scores.groupby("trade_date")["hotspot_score"].rank(ascending=False, method="min")

            # 构造 rotation_paths
            from analysis.rotation_analyzer import RotationAnalyzer
            analyzer = RotationAnalyzer()
            rotation_paths = analyzer.compute_rotation_path(scores)

            data = {
                "concept": concept_data,
                "hotspot_scores": scores,
                "rotation_paths": rotation_paths if rotation_paths is not None and not rotation_paths.empty else pd.DataFrame()
            }
            result = agent.run(task="all", data=data)

            assert result is not None
            assert isinstance(result, dict)
        else:
            pytest.skip("无法生成热点评分")
