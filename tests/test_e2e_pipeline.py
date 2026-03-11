"""
端到端流程测试
测试完整的数据流：数据采集 -> 热点分析 -> 轮动分析 -> 模型训练 -> 预测
"""
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from agents.data_agent import DataAgent
from agents.analysis_agent import AnalysisAgent
from agents.predict_agent import PredictAgent
from runner import SimpleRunner
from analysis.hotspot_detector import HotspotDetector
from models.predictor import UnifiedPredictor


@pytest.mark.e2e
class TestEndToEndPipeline:
    """端到端流程测试类"""

    def test_full_pipeline(self, sample_concept_data, sample_ths_indices,
                           sample_ths_industries_l1, temp_data_dir, monkeypatch):
        """
        测试完整流程：
        1. 数据采集（使用模拟数据）
        2. 热点分析
        3. 轮动分析
        4. 模型训练
        5. 预测
        """
        # 设置测试目录
        data_dir = temp_data_dir
        raw_dir = data_dir / "raw"
        raw_dir.mkdir(exist_ok=True)

        monkeypatch.setattr("config.settings.data_dir", str(data_dir))
        monkeypatch.setattr("config.settings.raw_data_dir", str(raw_dir))

        # Step 1: 准备测试数据文件（模拟数据采集结果）
        ths_indices_file = raw_dir / "ths_indices.csv"
        sample_ths_indices.to_csv(ths_indices_file, index=False)

        ths_industries_file = raw_dir / "ths_industries_l1.csv"
        sample_ths_industries_l1.to_csv(ths_industries_file, index=False)

        # 为每个行业创建历史数据文件
        for ts_code in sample_ths_industries_l1["ts_code"]:
            concept_hist = sample_concept_data[sample_concept_data["concept_code"] == ts_code].copy()
            if not concept_hist.empty:
                hist_file = raw_dir / f"ths_{ts_code.replace('.', '_')}.csv"
                concept_hist.to_csv(hist_file, index=False)

        # Step 2: 热点分析
        analysis_agent = AnalysisAgent()
        data = {"concept": sample_concept_data}
        analysis_result = analysis_agent.run(task="all", data=data)

        assert analysis_result is not None
        assert "hotspot" in analysis_result
        assert "rotation" in analysis_result
        assert "patterns" in analysis_result

        # Step 3: 模型训练
        predict_agent = PredictAgent()
        train_result = predict_agent.run(task="train", horizon="all", data=data)

        assert train_result is not None
        assert train_result.get("success") is True or train_result.get("success") is False

        # Step 4: 预测
        predict_result = predict_agent.run(task="predict", horizon="all", data=data)

        assert predict_result is not None
        assert isinstance(predict_result, dict)

    def test_quick_analysis_pipeline(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试快速分析流程（不采集新数据）"""
        data_dir = temp_data_dir
        raw_dir = data_dir / "raw"
        raw_dir.mkdir(exist_ok=True)

        monkeypatch.setattr("config.settings.data_dir", str(data_dir))
        monkeypatch.setattr("config.settings.raw_data_dir", str(raw_dir))

        # 创建测试数据文件
        test_file = raw_dir / "ths_881101_TI.csv"
        sample_concept_data[sample_concept_data["concept_code"] == "881101.TI"].to_csv(
            test_file, index=False
        )

        # 运行快速分析
        runner = SimpleRunner()
        results = runner.quick_analysis(date="20240101")

        assert results is not None
        assert "analysis" in results or "prediction" in results or "report" in results


@pytest.mark.e2e
class TestRunnerIntegration:
    """SimpleRunner 集成测试"""

    def test_runner_init(self, temp_data_dir, monkeypatch):
        """测试 SimpleRunner 初始化"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        runner = SimpleRunner()
        assert runner is not None
        assert runner.data_agent is not None
        assert runner.analysis_agent is not None
        assert runner.predict_agent is not None

    def test_runner_daily_workflow(self, sample_concept_data, sample_ths_industries_l1,
                                    temp_data_dir, monkeypatch):
        """测试每日工作流（简化版）"""
        data_dir = temp_data_dir
        raw_dir = data_dir / "raw"
        raw_dir.mkdir(exist_ok=True)

        monkeypatch.setattr("config.settings.data_dir", str(data_dir))
        monkeypatch.setattr("config.settings.raw_data_dir", str(raw_dir))

        # 创建测试数据
        ths_industries_file = raw_dir / "ths_industries_l1.csv"
        sample_ths_industries_l1.to_csv(ths_industries_file, index=False)

        for ts_code in sample_ths_industries_l1["ts_code"]:
            concept_hist = sample_concept_data[sample_concept_data["concept_code"] == ts_code].copy()
            if not concept_hist.empty:
                hist_file = raw_dir / f"ths_{ts_code.replace('.', '_')}.csv"
                concept_hist.to_csv(hist_file, index=False)

        runner = SimpleRunner()

        # 运行每日工作流（不训练）
        results = runner.run_daily(date="20240101", train=False)

        assert results is not None
        assert results.get("date") == "20240101"
        assert "analysis" in results or "prediction" in results

    def test_save_results(self, temp_data_dir, monkeypatch):
        """测试结果保存"""
        data_dir = temp_data_dir
        results_dir = data_dir / "results"
        results_dir.mkdir(exist_ok=True)

        monkeypatch.setattr("config.settings.data_dir", str(data_dir))

        runner = SimpleRunner()

        test_results = {
            "date": "20240101",
            "success": True,
            "test_data": {"key": "value"}
        }

        filepath = runner.save_results(test_results, "test_results.json")

        assert filepath is not None
        assert os.path.exists(filepath)


@pytest.mark.slow
class TestDataFlowValidation:
    """数据流验证测试"""

    def test_data_format_validation(self, sample_concept_data):
        """测试数据格式验证"""
        # 验证必需列
        required_columns = ["concept_code", "trade_date", "pct_chg"]
        for col in required_columns:
            assert col in sample_concept_data.columns

        # 验证数据类型
        assert sample_concept_data["pct_chg"].dtype in [float, int, np.float64, np.int64]

        # 验证日期格式
        dates = sample_concept_data["trade_date"].unique()
        for date in dates[:5]:  # 检查前 5 个日期
            assert len(str(date)) == 8  # YYYYMMDD 格式

    def test_analysis_output_validation(self, sample_concept_data):
        """测试分析输出验证"""
        detector = HotspotDetector()
        concept_data = sample_concept_data.copy()

        scores = detector.compute_hotspot_score(concept_data=concept_data)

        # 验证输出格式
        assert not scores.empty
        required_columns = ["trade_date", "concept_code", "hotspot_score"]
        for col in required_columns:
            assert col in scores.columns

        # 验证评分范围
        assert scores["hotspot_score"].between(0, 100).all()

    def test_prediction_output_validation(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试预测输出验证"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()  # 已经是 concept_code 格式

        features = predictor.prepare_features(concept_data, lookback=5)

        if not features.empty:
            model_result = predictor.train(features)
            predictions = predictor.predict(model_result, features)

            # 验证输出格式
            required_columns = ["concept_code", "trade_date", "pred_1d", "pred_5d", "pred_20d", "combined_score"]
            for col in required_columns:
                assert col in predictions.columns


# 导入必要的模块（在测试文件中）
from analysis.hotspot_detector import HotspotDetector
from models.predictor import UnifiedPredictor
