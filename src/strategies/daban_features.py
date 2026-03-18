"""
打板策略高级特征模块
包含封单强度、情绪周期、板块联动、涨停形态等特征计算
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from loguru import logger
from datetime import datetime, timedelta


class DabanFeatureEngine:
    """打板策略特征引擎"""

    # 涨停阈值配置
    LIMIT_UP_THRESHOLD = {
        "main": 0.095,  # 主板/中小板 9.5%
        "star_gem": 0.195,  # 科创板/创业板 19.5%
        "st": 0.045,  # ST 股 4.5%
    }

    # 情绪周期阈值
    SENTIMENT_THRESHOLDS = {
        "启动期": {"min_limit_up": 20, "max_limit_up": 50, "seal_rate": 0.6},
        "高潮期": {"min_limit_up": 50, "max_limit_up": 100, "seal_rate": 0.7},
        "衰退期": {"min_limit_up": 20, "max_limit_up": 50, "seal_rate": 0.4},
        "冰点期": {"min_limit_up": 0, "max_limit_up": 20, "seal_rate": 0.3},
    }

    def __init__(self, db=None):
        """初始化特征引擎"""
        self.db = db

    def calculate_seal_order_strength(
        self,
        current_price: float,
        limit_up_price: float,
        seal_amount: float,
        circulating_cap: float,
        limit_up_time: Optional[str] = None,
    ) -> float:
        """
        计算封单强度

        封单强度 = 封单量 / 流通股本
        封单金额超过5000万的涨停板成功率更高

        Args:
            current_price: 当前价格
            limit_up_price: 涨停价
            seal_amount: 封单金额（万元）
            circulating_cap: 流通市值（万元）
            limit_up_time: 封板时间（HH:MM 格式）

        Returns:
            封单强度评分 (0-100)
        """
        # 基础封单强度
        if circulating_cap <= 0:
            return 0.0

        seal_ratio = seal_amount / circulating_cap

        # 时间因子（早盘封板更强）
        time_factor = 1.0
        if limit_up_time:
            try:
                hour, minute = map(int, limit_up_time.split(":"))
                minutes_from_open = (hour - 9) * 60 + minute - 30

                if minutes_from_open <= 30:  # 9:30-10:00
                    time_factor = 1.3
                elif minutes_from_open <= 60:  # 10:00-10:30
                    time_factor = 1.2
                elif minutes_from_open <= 120:  # 10:30-11:30
                    time_factor = 1.0
                elif minutes_from_open <= 210:  # 13:00-14:30
                    time_factor = 0.9
                else:  # 14:30 之后
                    time_factor = 0.7
            except:
                pass

        # 封单金额评分
        amount_score = min(100, seal_amount / 5000 * 50)  # 5000万为基准

        # 封单比例评分
        ratio_score = min(100, seal_ratio * 1000)  # 10% 封单比例为满分

        # 综合评分
        final_score = (amount_score * 0.5 + ratio_score * 0.5) * time_factor

        return min(100, max(0, final_score))

    def calculate_sentiment_cycle(
        self,
        limit_up_count: int,
        limit_down_count: int,
        seal_success_rate: float,
        max_consecutive_boards: int,
        market_return: float = 0.0,
    ) -> Dict:
        """
        计算情绪周期

        Args:
            limit_up_count: 涨停家数
            limit_down_count: 跌停家数
            seal_success_rate: 封板成功率
            max_consecutive_boards: 最高连板数
            market_return: 大盘涨幅

        Returns:
            {
                "phase": "启动期/高潮期/衰退期/冰点期",
                "score": 0-100,
                "metrics": {...}
            }
        """
        # 计算各项指标
        up_down_ratio = limit_up_count / max(1, limit_down_count)
        heat_score = min(100, limit_up_count / 100 * 100)  # 100家涨停为满分

        # 判断周期阶段
        if limit_up_count >= 80 and seal_success_rate >= 0.7:
            phase = "高潮期"
        elif limit_up_count >= 40 and seal_success_rate >= 0.5:
            phase = "启动期"
        elif limit_up_count < 20 or seal_success_rate < 0.3:
            phase = "冰点期"
        elif seal_success_rate < 0.5 or up_down_ratio < 1:
            phase = "衰退期"
        else:
            phase = "启动期"

        # 计算综合评分
        phase_scores = {
            "高潮期": 85,
            "启动期": 70,
            "衰退期": 35,
            "冰点期": 20,
        }

        base_score = phase_scores.get(phase, 50)

        # 根据细节调整评分
        score_adjustment = 0
        if max_consecutive_boards >= 5:
            score_adjustment += 10
        elif max_consecutive_boards >= 3:
            score_adjustment += 5

        if market_return > 0.02:
            score_adjustment += 5
        elif market_return < -0.02:
            score_adjustment -= 10

        final_score = min(100, max(0, base_score + score_adjustment))

        return {
            "phase": phase,
            "score": final_score,
            "metrics": {
                "limit_up_count": limit_up_count,
                "limit_down_count": limit_down_count,
                "up_down_ratio": round(up_down_ratio, 2),
                "seal_success_rate": round(seal_success_rate, 2),
                "max_consecutive_boards": max_consecutive_boards,
                "heat_score": round(heat_score, 1),
            },
        }

    def calculate_sector_resonance(
        self,
        ts_code: str,
        concept_codes: List[str],
        concept_data: pd.DataFrame,
        limit_up_stocks_in_sector: int = 0,
    ) -> float:
        """
        计算板块联动效应评分

        Args:
            ts_code: 个股代码
            concept_codes: 所属板块代码列表
            concept_data: 板块行情数据 DataFrame
            limit_up_stocks_in_sector: 板块内涨停家数

        Returns:
            板块联动评分 (0-100)
        """
        if not concept_codes or concept_data.empty:
            return 50.0

        scores = []

        for concept_code in concept_codes:
            concept_row = concept_data[concept_data["concept_code"] == concept_code]
            if concept_row.empty:
                continue

            row = concept_row.iloc[-1]
            pct_chg = row.get("pct_chg", 0) or row.get("pct_change", 0)

            # 板块涨幅评分
            pct_score = min(100, max(0, 50 + pct_chg * 10))

            # 板块内涨停家数评分
            limit_score = min(100, limit_up_stocks_in_sector * 20)

            # 成交额评分（放量）
            amount = row.get("amount", 0)
            amount_score = min(100, amount / 1e8 * 10) if amount > 0 else 0

            # 综合板块评分
            sector_score = pct_score * 0.5 + limit_score * 0.3 + amount_score * 0.2
            scores.append(sector_score)

        return round(sum(scores) / len(scores), 2) if scores else 50.0

    def recognize_limit_pattern(
        self,
        open_price: float,
        close_price: float,
        high_price: float,
        low_price: float,
        limit_up_price: float,
        intraday_high_time: Optional[str] = None,
    ) -> Dict:
        """
        识别涨停形态

        Args:
            open_price: 开盘价
            close_price: 收盘价
            high_price: 最高价
            low_price: 最低价
            limit_up_price: 涨停价
            intraday_high_time: 盘中最高价时间

        Returns:
            {
                "pattern": "一字板/T字板/实体板/尾盘板",
                "strength": 0-100,
                "description": "描述"
            }
        """
        # 判断是否涨停
        is_limit_up = abs(close_price - limit_up_price) < 0.01

        if not is_limit_up:
            return {
                "pattern": "未涨停",
                "strength": 0,
                "description": "股票未封涨停板",
            }

        # 判断涨停形态
        if (
            abs(open_price - limit_up_price) < 0.01
            and abs(high_price - low_price) < 0.01
        ):
            # 一字板：开盘即涨停且全天无波动
            pattern = "一字板"
            strength = 95
            description = "开盘即涨停，全天无波动，最强形态"
        elif abs(open_price - limit_up_price) < 0.01:
            # T字板：开盘涨停但盘中打开过
            pattern = "T字板"
            strength = 85
            description = "开盘涨停，盘中打开后回封"
        elif intraday_high_time:
            hour, minute = map(int, intraday_high_time.split(":"))
            minutes = (hour - 9) * 60 + minute - 30

            if minutes >= 270:  # 14:00 之后
                pattern = "尾盘板"
                strength = 50
                description = "尾盘封板，力度较弱，需谨慎"
            elif minutes >= 180:  # 13:00 之后
                pattern = "下午板"
                strength = 65
                description = "下午封板，力度中等"
            elif minutes >= 120:  # 11:00 之后
                pattern = "上午板"
                strength = 75
                description = "上午封板，力度较好"
            else:
                pattern = "早盘板"
                strength = 90
                description = "早盘封板，力度强劲"
        else:
            # 默认实体板
            pattern = "实体板"
            strength = 70
            description = "盘中涨停，力度中等"

        return {
            "pattern": pattern,
            "strength": strength,
            "description": description,
        }

    def predict_consecutive_limit(
        self,
        ts_code: str,
        current_limit_count: int,
        historical_limit_records: List[int],
        market_sentiment: Dict,
        seal_strength: float,
        sector_resonance: float,
    ) -> float:
        """
        预测连板概率

        Args:
            ts_code: 个股代码
            current_limit_count: 当前连板数
            historical_limit_records: 历史连板记录
            market_sentiment: 市场情绪数据
            seal_strength: 封单强度
            sector_resonance: 板块联动评分

        Returns:
            连板概率 (0-1)
        """
        # 基础概率（连板数越高，概率越低）
        base_prob = max(0.1, 0.8 - current_limit_count * 0.15)

        # 历史连板能力
        if historical_limit_records:
            avg_historical = sum(historical_limit_records) / len(
                historical_limit_records
            )
            history_factor = min(1.2, avg_historical / 3)
        else:
            history_factor = 1.0

        # 市场情绪因子
        sentiment_score = market_sentiment.get("score", 50)
        sentiment_factor = sentiment_score / 50  # 50分为基准

        # 封单强度因子
        seal_factor = seal_strength / 100

        # 板块联动因子
        sector_factor = sector_resonance / 100

        # 综合概率计算
        probability = (
            base_prob
            * history_factor
            * sentiment_factor
            * 0.8
            * (1 + seal_factor * 0.3)
            * (1 + sector_factor * 0.2)
        )

        # 限制范围
        return min(0.95, max(0.05, probability))

    def calculate_comprehensive_score(
        self,
        seal_strength: float,
        sentiment_score: float,
        sector_resonance: float,
        pattern_strength: float,
        consecutive_prob: float,
        weights: Optional[Dict[str, float]] = None,
    ) -> float:
        """
        计算综合评分

        Args:
            seal_strength: 封单强度 (0-100)
            sentiment_score: 情绪周期评分 (0-100)
            sector_resonance: 板块联动评分 (0-100)
            pattern_strength: 涨停形态强度 (0-100)
            consecutive_prob: 连板概率 (0-1)

        Returns:
            综合评分 (0-100)
        """
        default_weights = {
            "seal_strength": 0.25,
            "sentiment": 0.15,
            "sector_resonance": 0.15,
            "pattern": 0.15,
            "consecutive_prob": 0.30,
        }

        weights = weights or default_weights

        # 连板概率转换为 0-100
        prob_score = consecutive_prob * 100

        # 加权计算
        comprehensive_score = (
            seal_strength * weights["seal_strength"]
            + sentiment_score * weights["sentiment"]
            + sector_resonance * weights["sector_resonance"]
            + pattern_strength * weights["pattern"]
            + prob_score * weights["consecutive_prob"]
        )

        return round(comprehensive_score, 2)

    def get_limit_up_price(self, pre_close: float, ts_code: str) -> float:
        """
        计算涨停价

        Args:
            pre_close: 昨收价
            ts_code: 股票代码

        Returns:
            涨停价
        """
        # 判断股票类型
        if ts_code.startswith(("688", "300")):
            # 科创板/创业板
            threshold = self.LIMIT_UP_THRESHOLD["star_gem"]
        elif ts_code.startswith("ST") or "ST" in ts_code:
            # ST 股
            threshold = self.LIMIT_UP_THRESHOLD["st"]
        else:
            # 主板/中小板
            threshold = self.LIMIT_UP_THRESHOLD["main"]

        # 四舍五入到分
        limit_up = round(pre_close * (1 + threshold), 2)

        return limit_up

    def analyze_stock_for_daban(
        self,
        stock_data: Dict,
        concept_data: pd.DataFrame,
        market_sentiment: Dict,
    ) -> Dict:
        """
        综合分析个股打板机会

        Args:
            stock_data: 个股数据字典
            concept_data: 板块数据
            market_sentiment: 市场情绪

        Returns:
            综合分析结果
        """
        ts_code = stock_data.get("ts_code", "")
        pre_close = stock_data.get("pre_close", 0)
        open_price = stock_data.get("open", 0)
        close_price = stock_data.get("close", 0)
        high_price = stock_data.get("high", 0)
        low_price = stock_data.get("low", 0)
        circulating_cap = stock_data.get("circ_mv", 0)  # 流通市值（万元）
        seal_amount = stock_data.get("seal_amount", 0)  # 封单金额（万元）
        concept_codes = stock_data.get("concept_codes", [])
        current_limit_count = stock_data.get("limit_count", 1)

        # 计算涨停价
        limit_up_price = self.get_limit_up_price(pre_close, ts_code)

        # 1. 封单强度
        seal_strength = self.calculate_seal_order_strength(
            current_price=close_price,
            limit_up_price=limit_up_price,
            seal_amount=seal_amount,
            circulating_cap=circulating_cap,
            limit_up_time=stock_data.get("limit_time"),
        )

        # 2. 板块联动
        sector_resonance = self.calculate_sector_resonance(
            ts_code=ts_code,
            concept_codes=concept_codes,
            concept_data=concept_data,
            limit_up_stocks_in_sector=stock_data.get("limit_up_in_sector", 0),
        )

        # 3. 涨停形态
        pattern_result = self.recognize_limit_pattern(
            open_price=open_price,
            close_price=close_price,
            high_price=high_price,
            low_price=low_price,
            limit_up_price=limit_up_price,
            intraday_high_time=stock_data.get("high_time"),
        )

        # 4. 连板概率
        consecutive_prob = self.predict_consecutive_limit(
            ts_code=ts_code,
            current_limit_count=current_limit_count,
            historical_limit_records=stock_data.get("historical_limits", []),
            market_sentiment=market_sentiment,
            seal_strength=seal_strength,
            sector_resonance=sector_resonance,
        )

        # 5. 综合评分
        comprehensive_score = self.calculate_comprehensive_score(
            seal_strength=seal_strength,
            sentiment_score=market_sentiment.get("score", 50),
            sector_resonance=sector_resonance,
            pattern_strength=pattern_result["strength"],
            consecutive_prob=consecutive_prob,
        )

        return {
            "ts_code": ts_code,
            "comprehensive_score": comprehensive_score,
            "features": {
                "seal_strength": seal_strength,
                "sector_resonance": sector_resonance,
                "pattern": pattern_result["pattern"],
                "pattern_strength": pattern_result["strength"],
                "consecutive_prob": round(consecutive_prob, 3),
            },
            "recommendation": self._get_recommendation(comprehensive_score),
            "pattern_description": pattern_result["description"],
        }

    def _get_recommendation(self, score: float) -> str:
        """根据评分给出建议"""
        if score >= 80:
            return "强烈推荐"
        elif score >= 65:
            return "推荐"
        elif score >= 50:
            return "观望"
        else:
            return "不推荐"


# 便捷函数
def analyze_limit_up_stock(
    stock_data: Dict, concept_data: pd.DataFrame, market_sentiment: Dict
) -> Dict:
    """分析涨停股打板机会的便捷函数"""
    engine = DabanFeatureEngine()
    return engine.analyze_stock_for_daban(stock_data, concept_data, market_sentiment)
