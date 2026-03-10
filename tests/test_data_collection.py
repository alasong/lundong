"""
数据采集测试
"""
import os
import pytest
import pandas as pd
from datetime import datetime, timedelta

from agents.data_agent import DataAgent
from data.data_collector import DataCollector
from data.tushare_ths_client import TushareTHSClient


class TestDataAgent:
    """DataAgent 测试类"""

    def test_data_agent_init(self, mock_ths_client):
        """测试 DataAgent 初始化"""
        agent = DataAgent(ths_client=mock_ths_client)
        assert agent is not None
        assert agent.ths_client is not None
        assert agent.collector is not None

    def test_data_agent_run_lists(self, mock_ths_client, temp_data_dir, monkeypatch):
        """测试采集列表数据"""
        # Mock settings
        monkeypatch.setattr("config.settings.raw_data_dir", str(temp_data_dir / "raw"))

        agent = DataAgent(ths_client=mock_ths_client)
        result = agent.run(task="lists")

        assert result is not None
        assert "ths_indices" in result or "ths_industries_l1" in result or "ths_industries_l2" in result

    def test_data_agent_run_invalid_task(self, mock_ths_client):
        """测试无效任务类型"""
        agent = DataAgent(ths_client=mock_ths_client)

        with pytest.raises(ValueError, match="未知任务类型"):
            agent.run(task="invalid_task")

    def test_data_agent_check_data_availability(self, mock_ths_client, temp_data_dir, monkeypatch):
        """测试数据可用性检查"""
        monkeypatch.setattr("config.settings.raw_data_dir", str(temp_data_dir / "raw"))

        agent = DataAgent(ths_client=mock_ths_client)

        # 目录不存在时
        availability = agent.check_data_availability("20240101")
        assert isinstance(availability, dict)

    def test_data_agent_get_data_summary(self, mock_ths_client, temp_data_dir, monkeypatch):
        """测试数据摘要"""
        monkeypatch.setattr("config.settings.raw_data_dir", str(temp_data_dir / "raw"))

        agent = DataAgent(ths_client=mock_ths_client)
        summary = agent.get_data_summary()

        assert "lists" in summary
        assert "history_files" in summary
        assert "total_records" in summary


class TestDataCollector:
    """DataCollector 测试类"""

    def test_collector_init(self, mock_ths_client):
        """测试数据采集器初始化"""
        collector = DataCollector(ths_client=mock_ths_client)
        assert collector is not None
        assert collector.ths_client is not None

    def test_collect_basic_data(self, mock_ths_client, temp_data_dir, monkeypatch):
        """测试采集基础数据"""
        monkeypatch.setattr("config.settings.raw_data_dir", str(temp_data_dir / "raw"))

        collector = DataCollector(ths_client=mock_ths_client)
        collector.collect_basic_data()

        # 验证文件是否创建
        raw_dir = temp_data_dir / "raw"
        files = list(raw_dir.glob("*.csv"))
        assert len(files) > 0

    def test_collect_history_data(self, mock_ths_client, temp_data_dir, monkeypatch):
        """测试采集历史数据"""
        monkeypatch.setattr("config.settings.raw_data_dir", str(temp_data_dir / "raw"))

        # 先采集列表数据
        collector = DataCollector(ths_client=mock_ths_client)
        collector.collect_basic_data()

        # 再采集历史数据
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        collector.collect_history_data(start_date, end_date, data_types=["ths_industry"])


class TestTushareTHSClient:
    """TushareTHSClient 测试类（集成测试）"""

    @pytest.mark.integration
    @pytest.mark.skip(reason="需要有效的 TUSHARE_TOKEN")
    def test_real_client_init(self):
        """测试真实客户端初始化"""
        from core.settings import settings as core_settings

        if core_settings.tushare_token and core_settings.tushare_token != "your_token_here":
            client = TushareTHSClient(token=core_settings.tushare_token)
            assert client is not None
        else:
            pytest.skip("TUSHARE_TOKEN 未配置")

    @pytest.mark.integration
    @pytest.mark.skip(reason="需要有效的 TUSHARE_TOKEN")
    def test_get_ths_indices(self):
        """测试获取同花顺指数列表"""
        from core.settings import settings as core_settings

        if core_settings.tushare_token and core_settings.tushare_token != "your_token_here":
            client = TushareTHSClient(token=core_settings.tushare_token)
            indices = client.get_ths_indices()
            assert isinstance(indices, pd.DataFrame)
        else:
            pytest.skip("TUSHARE_TOKEN 未配置")
