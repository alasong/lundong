"""
配置模块
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    """系统配置"""

    # Tushare
    tushare_token: str = ""

    # 通义千问
    dashscope_api_key: str = ""

    # Database
    database_url: str = "sqlite:///data/stock.db"
    redis_url: str = "redis://localhost:6379/0"

    # Logging
    log_level: str = "INFO"

    # 数据存储路径
    data_dir: str = "data"
    raw_data_dir: str = "data/raw"
    processed_data_dir: str = "data/processed"
    features_dir: str = "data/features"
    cache_dir: str = "data/cache"

    # 热点识别权重
    hotspot_weights: dict = {
        "price_strength": 0.30,      # 涨幅强度
        "money_strength": 0.25,      # 资金强度
        "sentiment_strength": 0.20,  # 情绪强度
        "persistence": 0.15,         # 持续性
        "market_position": 0.10,     # 市场地位
    }

    # 预测周期
    prediction_horizons: dict = {
        "short_term": 1,    # 日内
        "mid_term": 5,      # 周级
        "long_term": 20,    # 月级
    }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# 全局配置实例
settings = Settings()


def ensure_directories():
    """确保数据目录存在"""
    dirs = [
        settings.data_dir,
        settings.raw_data_dir,
        settings.processed_data_dir,
        settings.features_dir,
        settings.cache_dir,
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
