"""
External Integration for Daban Strategy
集成外部数据源到打板策略中
"""

from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger

from .daban_features import DabanFeatureEngine, analyze_limit_up_stock
from data.external_data_client import ExternalDataClient


class DabanExternalIntegration:
    """打板策略外部数据集成器"""

    def __init__(self, api_keys: Optional[Dict[str, str]] = None):
        """
        初始化外部数据集成器

        Args:
            api_keys: API密钥字典
        """
        self.external_client = ExternalDataClient(api_keys)
        self.feature_engine = DabanFeatureEngine()
        logger.info("打板策略外部数据集成器初始化完成")

    def enhance_stock_data_with_external_features(
        self, stock_data: Dict[str, Any], concept_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        使用外部特征增强股票数据

        Args:
            stock_data: 原始股票数据
            concept_data: 板块数据

        Returns:
            增强后的股票数据
        """
        try:
            enhanced_data = self.external_client.integrate_external_features(
                stock_data, concept_data
            )
            logger.info(f"成功增强股票数据: {stock_data.get('ts_code', 'unknown')}")
            return enhanced_data
        except Exception as e:
            logger.error(f"增强股票数据失败: {e}")
            return stock_data

    def get_enhanced_market_sentiment(self) -> Dict[str, Any]:
        """
        获取增强版市场情绪数据

        Returns:
            增强的市场情绪数据
        """
        try:
            # 获取基础市场情绪
            market_sentiment = self.external_client.get_market_sentiment()

            # 获取实时市场数据
            realtime_data = self.external_client.get_realtime_market_data()

            # 合并数据
            enhanced_sentiment = {**market_sentiment, **realtime_data, "enhanced": True}

            logger.info("成功获取增强版市场情绪数据")
            return enhanced_sentiment

        except Exception as e:
            logger.error(f"获取增强市场情绪失败: {e}")
            # 返回默认值
            return {
                "date": "20260319",
                "market_sentiment_score": 50.0,
                "market_phase": "normal",
                "bullish_ratio": 0.5,
                "fear_greed_index": 50,
                "volatility_index": 20.0,
                "advance_decline_ratio": 1.0,
                "new_highs_count": 25,
                "new_lows_count": 25,
                "volume_ratio": 1.0,
                "limit_up_count": 30,
                "limit_down_count": 30,
                "seal_success_rate": 0.5,
                "max_consecutive_boards": 2,
                "market_return": 0.0,
                "enhanced": False,
            }

    def analyze_stock_with_external_features(
        self, stock_data: Dict[str, Any], concept_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        使用外部特征分析个股打板机会

        Args:
            stock_data: 股票数据
            concept_data: 板块数据

        Returns:
            综合分析结果
        """
        try:
            # 增强股票数据
            enhanced_stock_data = self.enhance_stock_data_with_external_features(
                stock_data, concept_data
            )

            # 获取增强市场情绪
            enhanced_market_sentiment = self.get_enhanced_market_sentiment()

            # 使用增强数据进行分析
            analysis_result = analyze_limit_up_stock(
                enhanced_stock_data, concept_data, enhanced_market_sentiment
            )

            # 添加外部特征信息
            analysis_result["external_features"] = {
                "news_sentiment_score": enhanced_stock_data.get(
                    "news_sentiment_score", 50.0
                ),
                "social_sentiment_score": enhanced_stock_data.get(
                    "social_sentiment_score", 50.0
                ),
                "sector_sentiment_score": enhanced_stock_data.get(
                    "sector_sentiment_score", 50.0
                ),
                "market_sentiment_score": enhanced_stock_data.get(
                    "market_sentiment_score", 50.0
                ),
                "external_data_source": "simulated",
            }

            logger.info(f"成功分析个股打板机会: {stock_data.get('ts_code', 'unknown')}")
            return analysis_result

        except Exception as e:
            logger.error(f"分析个股打板机会失败: {e}")
            # 返回基础分析结果
            basic_result = analyze_limit_up_stock(
                stock_data, concept_data, {"score": 50, "phase": "normal"}
            )
            basic_result["external_features"] = {"external_data_source": "none"}
            return basic_result


# 便捷函数
def analyze_stock_with_external_features(
    stock_data: Dict[str, Any],
    concept_data: pd.DataFrame,
    api_keys: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    分析个股打板机会的便捷函数（包含外部特征）

    Args:
        stock_data: 股票数据
        concept_data: 板块数据
        api_keys: API密钥字典

    Returns:
        综合分析结果
    """
    integrator = DabanExternalIntegration(api_keys)
    return integrator.analyze_stock_with_external_features(stock_data, concept_data)


def get_enhanced_market_sentiment(
    api_keys: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    获取增强版市场情绪的便捷函数

    Args:
        api_keys: API密钥字典

    Returns:
        增强的市场情绪数据
    """
    integrator = DabanExternalIntegration(api_keys)
    return integrator.get_enhanced_market_sentiment()
