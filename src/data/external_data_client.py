"""
External Data Client for Real-time Market Sentiment and News Analysis
集成外部数据源，提供实时市场情绪、新闻情感分析和社交媒体数据
"""

import requests
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd


class ExternalDataClient:
    """外部数据客户端 - 获取实时市场情绪和新闻数据"""

    def __init__(self, api_keys: Dict[str, str] = None):
        """
        初始化外部数据客户端

        Args:
            api_keys: API密钥字典
                {
                    "news_api": "your_news_api_key",
                    "social_api": "your_social_api_key",
                    "sentiment_api": "your_sentiment_api_key"
                }
        """
        self.api_keys = api_keys or {}
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

        logger.info("外部数据客户端初始化完成")

    def get_market_sentiment(self, date: str = None) -> Dict[str, Any]:
        """
        获取市场情绪数据

        Args:
            date: 日期字符串，格式 YYYYMMDD

        Returns:
            市场情绪数据字典
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        # 模拟市场情绪数据（实际实现需要连接真实API）
        sentiment_data = {
            "date": date,
            "market_sentiment_score": 65.0,  # 0-100
            "market_phase": "启动期",  # 启动期/高潮期/衰退期/冰点期
            "bullish_ratio": 0.65,  # 看涨比例
            "fear_greed_index": 55,  # 恐惧贪婪指数
            "volatility_index": 22.5,  # 波动率指数
            "advance_decline_ratio": 1.2,  # 上涨/下跌家数比
            "new_highs_count": 45,
            "new_lows_count": 18,
            "volume_ratio": 1.15,  # 成交量比率
        }

        logger.info(f"获取市场情绪数据: {date}")
        return sentiment_data

    def get_stock_news_sentiment(self, ts_code: str, days: int = 1) -> Dict[str, Any]:
        """
        获取个股新闻情感分析

        Args:
            ts_code: 股票代码
            days: 获取最近N天的新闻

        Returns:
            新闻情感分析结果
        """
        # 模拟新闻情感数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        sentiment_result = {
            "ts_code": ts_code,
            "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "news_count": 12,
            "positive_news": 8,
            "negative_news": 2,
            "neutral_news": 2,
            "sentiment_score": 72.5,  # 0-100, 越高越正面
            "key_topics": ["业绩增长", "行业利好", "政策支持"],
            "sentiment_trend": "上升",  # 上升/下降/稳定
        }

        logger.info(f"获取个股新闻情感: {ts_code}")
        return sentiment_result

    def get_sector_news_sentiment(
        self, concept_code: str, days: int = 1
    ) -> Dict[str, Any]:
        """
        获取板块新闻情感分析

        Args:
            concept_code: 板块代码
            days: 获取最近N天的新闻

        Returns:
            板块新闻情感分析结果
        """
        # 模拟板块新闻情感数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        sentiment_result = {
            "concept_code": concept_code,
            "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "news_count": 25,
            "positive_news": 18,
            "negative_news": 3,
            "neutral_news": 4,
            "sentiment_score": 78.0,  # 0-100
            "key_topics": ["行业景气", "政策利好", "技术创新"],
            "sentiment_trend": "上升",
        }

        logger.info(f"获取板块新闻情感: {concept_code}")
        return sentiment_result

    def get_social_media_sentiment(
        self, ts_code: str, platform: str = "weibo"
    ) -> Dict[str, Any]:
        """
        获取社交媒体情感分析

        Args:
            ts_code: 股票代码
            platform: 社交媒体平台 (weibo, xueqiu, eastmoney)

        Returns:
            社交媒体情感分析结果
        """
        # 模拟社交媒体情感数据
        sentiment_result = {
            "ts_code": ts_code,
            "platform": platform,
            "post_count": 156,
            "positive_posts": 98,
            "negative_posts": 32,
            "neutral_posts": 26,
            "engagement_score": 85.0,  # 互动热度评分
            "sentiment_score": 68.5,  # 情感评分
            "trending_keywords": ["涨停", "龙头", "强势"],
            "sentiment_momentum": "增强",  # 情感动量
        }

        logger.info(f"获取社交媒体情感: {ts_code} on {platform}")
        return sentiment_result

    def get_realtime_market_data(self) -> Dict[str, Any]:
        """
        获取实时市场数据

        Returns:
            实时市场数据
        """
        # 模拟实时市场数据
        current_time = datetime.now().strftime("%H:%M:%S")

        market_data = {
            "timestamp": current_time,
            "limit_up_count": 68,
            "limit_down_count": 12,
            "seal_success_rate": 0.65,
            "max_consecutive_boards": 4,
            "market_return": 0.015,
            "volume_ratio": 1.2,
            "advance_decline_ratio": 1.3,
            "new_highs_count": 52,
            "new_lows_count": 15,
        }

        logger.info(f"获取实时市场数据: {current_time}")
        return market_data

    def integrate_external_features(
        self, stock_data: Dict, concept_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        集成外部特征到股票数据中

        Args:
            stock_data: 原始股票数据
            concept_data: 板块数据

        Returns:
            增强后的股票数据
        """
        ts_code = stock_data.get("ts_code", "")
        concept_codes = stock_data.get("concept_codes", [])

        # 获取个股新闻情感
        news_sentiment = self.get_stock_news_sentiment(ts_code)

        # 获取社交媒体情感
        social_sentiment = self.get_social_media_sentiment(ts_code)

        # 获取板块新闻情感（取第一个板块）
        sector_sentiment = {}
        if concept_codes:
            sector_sentiment = self.get_sector_news_sentiment(concept_codes[0])

        # 获取实时市场数据
        market_data = self.get_realtime_market_data()

        # 获取市场情绪
        market_sentiment = self.get_market_sentiment()

        # 整合所有外部特征
        enhanced_data = {
            **stock_data,
            "news_sentiment_score": news_sentiment.get("sentiment_score", 50.0),
            "social_sentiment_score": social_sentiment.get("sentiment_score", 50.0),
            "sector_sentiment_score": sector_sentiment.get("sentiment_score", 50.0),
            "market_sentiment_score": market_sentiment.get(
                "market_sentiment_score", 50.0
            ),
            "news_count": news_sentiment.get("news_count", 0),
            "social_engagement": social_sentiment.get("engagement_score", 0.0),
            "sector_news_count": sector_sentiment.get("news_count", 0),
            "market_phase": market_sentiment.get("market_phase", "normal"),
            "bullish_ratio": market_sentiment.get("bullish_ratio", 0.5),
            "fear_greed_index": market_sentiment.get("fear_greed_index", 50),
            "advance_decline_ratio": market_sentiment.get("advance_decline_ratio", 1.0),
            "new_highs_count": market_sentiment.get("new_highs_count", 0),
            "new_lows_count": market_sentiment.get("new_lows_count", 0),
            "volume_ratio": market_sentiment.get("volume_ratio", 1.0),
            "realtime_limit_up_count": market_data.get("limit_up_count", 0),
            "realtime_limit_down_count": market_data.get("limit_down_count", 0),
            "realtime_seal_success_rate": market_data.get("seal_success_rate", 0.5),
            "realtime_max_consecutive_boards": market_data.get(
                "max_consecutive_boards", 1
            ),
            "realtime_market_return": market_data.get("market_return", 0.0),
        }

        logger.info(f"集成外部特征完成: {ts_code}")
        return enhanced_data


# 便捷函数
def get_enhanced_market_sentiment(date: str = None) -> Dict[str, Any]:
    """获取增强版市场情绪数据的便捷函数"""
    client = ExternalDataClient()
    return client.get_market_sentiment(date)


def integrate_external_features_for_daban(
    stock_data: Dict, concept_data: pd.DataFrame
) -> Dict[str, Any]:
    """为打板策略集成外部特征的便捷函数"""
    client = ExternalDataClient()
    return client.integrate_external_features(stock_data, concept_data)
