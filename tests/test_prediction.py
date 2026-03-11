"""
预测模型测试
"""
import os
import pytest
import pandas as pd
import numpy as np
import pickle

from models.predictor import UnifiedPredictor
from agents.predict_agent import PredictAgent


class TestUnifiedPredictor:
    """统一预测器测试类"""

    def test_predictor_init(self, temp_data_dir, monkeypatch):
        """测试预测器初始化"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        assert predictor is not None
        assert predictor.models_dir is not None

    def test_prepare_features(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试特征准备"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        features = predictor.prepare_features(concept_data, lookback=5)

        assert isinstance(features, pd.DataFrame)
        assert not features.empty
        assert "concept_code" in features.columns
        assert "trade_date" in features.columns

    def test_prepare_features_insufficient_data(self, temp_data_dir, monkeypatch):
        """测试数据不足时的特征准备"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()

        # 创建数据量不足的测试数据
        insufficient_data = pd.DataFrame({
            "concept_code": ["A", "A", "A"],
            "trade_date": ["20240101", "20240102", "20240103"],
            "pct_chg": [1.0, 2.0, 3.0],
            "vol": [1000, 2000, 3000],
        })

        features = predictor.prepare_features(insufficient_data, lookback=10)

        assert features.empty

    def test_train_model(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试模型训练"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        # 准备特征
        features = predictor.prepare_features(concept_data, lookback=5)

        if not features.empty:
            # 训练模型
            result = predictor.train(features, model_type="xgboost")

            assert result is not None
            assert "models" in result
            assert "metrics" in result
            assert "feature_cols" in result

            # 验证模型已保存
            model_path = os.path.join(predictor.models_dir, "unified_model.pkl")
            assert os.path.exists(model_path)

    def test_train_model_random_forest(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试随机森林模型训练"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        features = predictor.prepare_features(concept_data, lookback=5)

        if not features.empty:
            result = predictor.train(features, model_type="random_forest")

            assert result is not None
            assert "models" in result

    def test_load_model(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试加载模型"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        # 先训练模型
        features = predictor.prepare_features(concept_data, lookback=5)
        if not features.empty:
            predictor.train(features)

            # 加载模型
            model_result = predictor.load_model()

            assert model_result is not None
            assert "models" in model_result
            assert "feature_cols" in model_result

    def test_predict(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试模型预测"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        # 准备特征
        features = predictor.prepare_features(concept_data, lookback=5)

        if not features.empty:
            # 训练模型
            model_result = predictor.train(features)

            # 预测
            predictions = predictor.predict(model_result, features)

            assert isinstance(predictions, pd.DataFrame)
            assert not predictions.empty
            assert "pred_1d" in predictions.columns
            assert "pred_5d" in predictions.columns
            assert "pred_20d" in predictions.columns
            assert "combined_score" in predictions.columns

    def test_predict_no_model(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试无模型时的预测"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        features = predictor.prepare_features(concept_data, lookback=5)

        if not features.empty:
            # 不训练模型，直接预测
            predictions = predictor.predict(None, features)

            # 应该返回空 DataFrame（因为没有模型）
            assert isinstance(predictions, pd.DataFrame)

    def test_process_single_concept(self, temp_data_dir, monkeypatch):
        """测试单个概念处理"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()

        concept_df = pd.DataFrame({
            "concept_code": ["A"] * 50,
            "trade_date": pd.date_range(start="2024-01-01", periods=50, freq="D").strftime("%Y%m%d"),
            "pct_chg": np.random.randn(50) * 2,
            "vol": np.random.randint(1000, 5000, 50),
        })

        result = predictor._process_single_concept_vectorized("A", concept_df, lookback=10)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "target_1d" in result.columns
        assert "target_5d" in result.columns
        assert "target_20d" in result.columns


class TestPredictAgent:
    """预测 Agent 测试类"""

    def test_agent_init(self, temp_data_dir, monkeypatch):
        """测试预测 Agent 初始化"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        agent = PredictAgent()
        assert agent is not None
        assert agent.predictor is not None

    def test_agent_run_train(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试训练任务"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        # Mock _load_training_data
        def mock_load_training_data(self):
            data = sample_concept_data.copy()
            data = data.rename(columns={"ts_code": "concept_code"})
            return {"concept": data}

        from unittest.mock import patch

        with patch.object(PredictAgent, '_load_training_data', mock_load_training_data):
            agent = PredictAgent()
            result = agent.run(task="train", horizon="all")

            assert result is not None
            assert isinstance(result, dict)
            assert result.get("success") is True

    def test_agent_run_predict(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试预测任务"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))
        monkeypatch.setattr("config.settings.raw_data_dir", str(temp_data_dir / "raw"))

        # 创建测试数据文件
        raw_dir = temp_data_dir / "raw"
        raw_dir.mkdir(exist_ok=True)

        test_file = raw_dir / "ths_881101_TI.csv"
        sample_concept_data.to_csv(test_file, index=False)

        agent = PredictAgent()
        result = agent.run(task="predict", horizon="all")

        assert result is not None
        assert isinstance(result, dict)

    def test_agent_simple_prediction(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试简化预测（基于近期动量）"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        agent = PredictAgent()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        result = agent._simple_prediction(concept_data)

        assert result is not None
        assert "predictions" in result
        # predictions 可能是 DataFrame 或 list
        assert result["predictions"] is not None

    def test_format_predictions(self, sample_concept_data, temp_data_dir, monkeypatch):
        """测试预测结果格式化"""
        monkeypatch.setattr("config.settings.data_dir", str(temp_data_dir))

        predictor = UnifiedPredictor()
        concept_data = sample_concept_data.copy()
        concept_data = concept_data.rename(columns={"ts_code": "concept_code"})

        features = predictor.prepare_features(concept_data, lookback=5)

        if not features.empty:
            model_result = predictor.train(features)
            predictions = predictor.predict(model_result, features)

            agent = PredictAgent()
            result = agent._format_predictions(predictions)

            assert result is not None
            assert "predictions" in result
            assert "top_10" in result
