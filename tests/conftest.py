"""
pytest 配置和 fixtures
"""
import os
import sys
import pytest
import pandas as pd
from datetime import datetime, timedelta

# 添加 src 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings, ensure_directories


@pytest.fixture(scope="session")
def test_data_dir():
    """测试数据目录"""
    return os.path.join(os.path.dirname(__file__), "test_data")


@pytest.fixture(scope="session")
def sample_concept_data():
    """样本概念板块数据"""
    dates = pd.date_range(start="2024-01-01", periods=60, freq="D")
    dates = [d.strftime("%Y%m%d") for d in dates if d.weekday() < 5]  # 工作日

    concepts = [
        ("881101.TI", "半导体"),
        ("881102.TI", "人工智能"),
        ("881103.TI", "新能源"),
        ("881104.TI", "医药生物"),
        ("881105.TI", "消费电子"),
    ]

    data = []
    for ts_code, name in concepts:
        for i, date in enumerate(dates):
            pct_chg = (i % 10 - 5) * 0.5 + (hash(ts_code + date) % 100 - 50) * 0.1
            close = 1000 + (i % 10 - 5) * 10 + (hash(ts_code) % 100)
            vol = 1000000 + (hash(ts_code + date) % 100000)
            amount = vol * close / 100

            data.append({
                "concept_code": ts_code,  # 使用 concept_code 而不是 ts_code
                "name": name,
                "trade_date": date,
                "close": close,
                "pct_chg": round(pct_chg, 2),
                "vol": vol,
                "amount": amount,
            })

    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def sample_ths_indices():
    """样本同花顺指数列表"""
    data = [
        {"ts_code": "881101.TI", "name": "半导体", "count": 150, "exchange": "SZSE", "type": "行业"},
        {"ts_code": "881102.TI", "name": "人工智能", "count": 200, "exchange": "SZSE", "type": "行业"},
        {"ts_code": "881103.TI", "name": "新能源", "count": 180, "exchange": "SZSE", "type": "行业"},
        {"ts_code": "881104.TI", "name": "医药生物", "count": 120, "exchange": "SZSE", "type": "行业"},
        {"ts_code": "881105.TI", "name": "消费电子", "count": 90, "exchange": "SZSE", "type": "行业"},
    ]
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def sample_ths_industries_l1():
    """样本同花顺一级行业"""
    data = [
        {"ts_code": "881101.TI", "name": "半导体", "count": 150},
        {"ts_code": "881102.TI", "name": "人工智能", "count": 200},
        {"ts_code": "881103.TI", "name": "新能源", "count": 180},
        {"ts_code": "881104.TI", "name": "医药生物", "count": 120},
        {"ts_code": "881105.TI", "name": "消费电子", "count": 90},
    ]
    return pd.DataFrame(data)


@pytest.fixture
def mock_ths_client(sample_ths_indices, sample_ths_industries_l1, sample_concept_data):
    """模拟同花顺客户端"""

    class MockTHSClient:
        def get_ths_indices(self):
            return sample_ths_indices

        def get_ths_industries(self, level=1):
            return sample_ths_industries_l1

        def get_ths_history(self, ts_code, start_date, end_date):
            data = sample_concept_data[sample_concept_data["ts_code"] == ts_code].copy()
            if data.empty:
                return None
            # 按日期筛选
            data = data[(data["trade_date"] >= start_date) & (data["trade_date"] <= end_date)]
            return data

    return MockTHSClient()


@pytest.fixture
def temp_data_dir(tmp_path):
    """临时数据目录"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "raw").mkdir()
    (data_dir / "processed").mkdir()
    (data_dir / "features").mkdir()
    (data_dir / "models").mkdir()
    (data_dir / "results").mkdir()
    return data_dir


@pytest.fixture(scope="session")
def hotspot_weights():
    """热点权重配置"""
    return {
        "price_strength": 0.30,
        "money_strength": 0.25,
        "sentiment_strength": 0.20,
        "persistence": 0.15,
        "market_position": 0.10,
    }


@pytest.fixture(scope="session")
def prediction_horizons():
    """预测周期配置"""
    return {
        "short_term": 1,
        "mid_term": 5,
        "long_term": 20,
    }


def pytest_configure(config):
    """pytest 配置钩子"""
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
