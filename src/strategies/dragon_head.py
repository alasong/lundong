"""
龙头打板策略 (Dragon Head Limit-Up Strategy)
基于A股市场特点，识别潜在的龙头股和涨停机会
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class DragonHeadStrategy(BaseStrategy):
    """龙头打板策略 - 识别潜在涨停龙头股"""

    def __init__(self, name: str = "dragon_head", params: Optional[Dict] = None):
        default_params = {
            "limit_up_threshold": 0.095,  # 涨停阈值 9.5%（考虑误差）
            "volume_multiplier": 2.0,  # 成交量倍数阈值
            "consecutive_limit_ups": 1,  # 连续涨停次数
            "top_n_stocks": 20,  # 选股数量
            "min_market_cap": 50,  # 最小市值（亿）
            "max_market_cap": 500,  # 最大市值（亿）
            "sector_hotness_weight": 0.3,  # 板块热度权重
            "momentum_weight": 0.4,  # 动量权重
            "volume_weight": 0.3,  # 成交量权重
        }
        default_params.update(params or {})

        super().__init__(name, default_params)
        self.db = None

    def _init_db(self):
        """懒加载数据库"""
        if self.db is None:
            from data.database import get_database

            self.db = get_database()

    def get_required_data(self) -> Dict[str, Any]:
        """需要个股日线数据和板块数据"""
        return {
            "concept_data": True,
            "stock_data": True,
            "history_days": 60,  # 需要60天历史数据计算各项指标
            "features": ["close", "volume", "amount", "pct_chg", "total_mv"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """生成龙头打板信号"""
        logger.info("龙头打板策略：生成信号...")
        self._init_db()

        signals = []

        # 获取最新日期
        latest_date = self.db.get_latest_date()
        if not latest_date:
            logger.warning("无法获取最新日期")
            return []

        # 计算起始日期
        history_days = self.get_required_data()["history_days"]
        start_date = self._get_n_days_before(latest_date, history_days)

        # 获取所有股票的日线数据
        logger.info("获取股票历史数据...")
        all_stock_data = self.db.get_all_stock_data()
        if all_stock_data.empty:
            logger.warning("无股票数据")
            return []

        # 获取唯一的股票代码列表
        all_stocks = all_stock_data[["ts_code"]].drop_duplicates()

        stock_data = self.db.get_daily_batch(
            stock_codes=all_stocks["ts_code"].tolist(),
            start_date=start_date,
            end_date=latest_date,
        )

        if stock_data.empty:
            logger.warning("无法获取股票数据")
            return []

        # 获取板块成分股关系
        logger.info("获取板块成分股关系...")
        all_constituents = self.db.get_all_constituents()
        if all_constituents:
            constituent_df = pd.DataFrame(all_constituents)
            # 获取板块数据
            concept_data = self.db.get_all_concept_data(latest_date)
        else:
            logger.warning("无板块成分股数据，仅基于个股特征选股")
            constituent_df = pd.DataFrame()
            concept_data = pd.DataFrame()

        # 按股票分组计算龙头特征
        logger.info("计算龙头特征...")

        for ts_code in all_stocks["ts_code"].unique():
            stock_df = stock_data[stock_data["ts_code"] == ts_code].sort_values(
                "trade_date"
            )

            if len(stock_df) < 5:  # 至少需要5天数据
                continue

            # 获取最新数据
            latest_row = stock_df.iloc[-1]

            # 检查是否接近涨停
            pct_chg = latest_row.get("pct_chg", 0) or 0

            # 检查市值范围
            market_cap = latest_row.get("total_mv", 0) or 0
            market_cap_billion = market_cap / 10000  # 转换为亿

            if not (
                self.params["min_market_cap"]
                <= market_cap_billion
                <= self.params["max_market_cap"]
            ):
                continue

            # 计算各项指标
            volume_ratio = self._calculate_volume_ratio(stock_df)
            momentum_score = self._calculate_momentum_score(stock_df)
            limit_up_potential = self._calculate_limit_up_potential(
                pct_chg, volume_ratio
            )

            # 计算板块热度（如果有板块数据）
            sector_hotness = self._calculate_sector_hotness(
                ts_code, concept_data, constituent_df
            )

            # 综合评分
            composite_score = (
                limit_up_potential * 0.4
                + momentum_score * self.params["momentum_weight"] * 0.6
                + volume_ratio * self.params["volume_weight"] * 0.3
                + sector_hotness * self.params["sector_hotness_weight"] * 0.2
            ) * 100

            # 如果接近涨停或有涨停潜力，则生成信号
            if pct_chg >= self.params["limit_up_threshold"] or (
                composite_score > 60 and volume_ratio > self.params["volume_multiplier"]
            ):
                stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
                stock_name = (
                    stock_info["name"].iloc[0] if not stock_info.empty else ts_code
                )

                # 计算权重（基于综合评分）
                weight = min(1.0, composite_score / 100.0)

                # 确定信号类型
                signal_type = (
                    "buy" if pct_chg >= self.params["limit_up_threshold"] else "watch"
                )

                # 获取所属板块信息
                concept_info = ""
                if not constituent_df.empty:
                    concept_row = constituent_df[
                        constituent_df["stock_code"] == ts_code
                    ]
                    if not concept_row.empty:
                        concept_info = (
                            f"所属板块: {concept_row.iloc[0]['concept_code']}"
                        )

                signal = StrategySignal(
                    ts_code=ts_code,
                    stock_name=stock_name,
                    strategy_type="dragon_head",
                    signal_type=signal_type,
                    weight=weight,
                    score=composite_score,
                    reason=f"涨幅:{pct_chg:.2%}, 成交量比:{volume_ratio:.1f}x, 板块热度:{sector_hotness:.1f}, 综合评分:{composite_score:.1f}",
                    metadata={
                        "pct_chg": pct_chg,
                        "volume_ratio": volume_ratio,
                        "momentum_score": momentum_score,
                        "sector_hotness": sector_hotness,
                        "market_cap_billion": market_cap_billion,
                        "latest_close": latest_row["close"]
                        if "close" in latest_row
                        else 0,
                        "concept_info": concept_info,
                    },
                )
                signals.append(signal)

        # 按评分排序，取 TOP N
        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[: self.params["top_n_stocks"]]
        self.signals = signals

        logger.info(f"龙头打板策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_volume_ratio(self, stock_df: pd.DataFrame) -> float:
        """计算成交量比率（当日成交量 / 历史平均成交量）"""
        if len(stock_df) < 10:
            return 1.0

        latest_volume = (
            stock_df["vol"].iloc[-1]
            if "vol" in stock_df.columns
            else stock_df["volume"].iloc[-1]
        )
        # 计算过去10天平均成交量（排除当天）
        historical_avg = (
            stock_df["vol"].iloc[-11:-1].mean()
            if "vol" in stock_df.columns
            else stock_df["volume"].iloc[-11:-1].mean()
        )

        if historical_avg == 0:
            return 1.0

        return latest_volume / historical_avg

    def _calculate_momentum_score(self, stock_df: pd.DataFrame) -> float:
        """计算动量评分（基于短期和中期涨幅）"""
        if len(stock_df) < 20:
            return 0.5

        # 短期动量（5日）
        short_mom = 0.0
        if len(stock_df) >= 5:
            short_start = stock_df["close"].iloc[-5]
            short_end = stock_df["close"].iloc[-1]
            if short_start != 0:
                short_mom = (short_end - short_start) / short_start

        # 中期动量（20日）
        med_mom = 0.0
        if len(stock_df) >= 20:
            med_start = stock_df["close"].iloc[-20]
            med_end = stock_df["close"].iloc[-1]
            if med_start != 0:
                med_mom = (med_end - med_start) / med_start

        # 综合动量评分（0-1之间）
        combined_mom = short_mom * 0.6 + med_mom * 0.4
        # 标准化到0-1区间
        score = (np.tanh(combined_mom * 10) + 1) / 2  # 使用tanh函数平滑并映射到[0,1]
        return max(0.0, min(1.0, score))

    def _calculate_limit_up_potential(
        self, pct_chg: float, volume_ratio: float
    ) -> float:
        """计算涨停潜力评分"""
        # 涨幅越接近涨停线，潜力越大
        limit_up_threshold = self.params["limit_up_threshold"]
        if pct_chg >= limit_up_threshold:
            # 已涨停，潜力最高
            return 1.0
        elif pct_chg >= limit_up_threshold * 0.8:
            # 接近涨停，潜力较高
            return (
                0.8
                + (pct_chg - limit_up_threshold * 0.8)
                / (limit_up_threshold * 0.2)
                * 0.2
            )
        else:
            # 涨幅一般，根据成交量判断潜力
            potential_from_volume = min(
                0.5, (volume_ratio - 1.0) / 4.0
            )  # 假设最大成交量是平时4倍
            return max(0.0, min(0.5, potential_from_volume))

    def _calculate_sector_hotness(
        self, ts_code: str, concept_data: pd.DataFrame, constituent_df: pd.DataFrame
    ) -> float:
        """计算股票所属板块的热度"""
        if constituent_df.empty or concept_data.empty:
            return 0.5  # 无板块数据时返回中等热度

        # 找到该股票所属的板块
        concept_row = constituent_df[constituent_df["stock_code"] == ts_code]
        if concept_row.empty:
            return 0.5  # 无板块归属时返回中等热度

        concept_code = concept_row.iloc[0]["concept_code"]

        # 获取该板块的最新表现
        concept_perf = concept_data[concept_data["ts_code"] == concept_code]
        if concept_perf.empty:
            return 0.5  # 无板块数据时返回中等热度

        latest_concept_row = concept_perf.iloc[-1]
        concept_chg = latest_concept_row.get("pct_change", 0) or 0

        # 根据板块涨幅计算热度（0-1之间）
        # 假设板块涨幅在-5%到+10%之间正常，超出部分按比例计算
        if concept_chg >= 0.05:  # 板块涨幅大于5%
            return min(1.0, 0.5 + concept_chg * 5)  # 线性增长到1.0
        elif concept_chg >= -0.03:  # 板块跌幅小于3%
            return max(0.2, 0.5 + concept_chg * 10)  # 线性下降到0.2
        else:  # 板块跌幅较大
            return max(0.0, 0.2 + concept_chg * 20)  # 线性下降到0.0

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        """获取 N 个交易日之前的日期"""
        from datetime import timedelta
        from datetime import datetime

        date = datetime.strptime(date_str, "%Y%m%d")
        # 估算交易日，考虑周末
        approx_business_days = n_days + n_days // 5 * 2  # 大约加上周末天数
        prev_date = date - timedelta(days=approx_business_days)
        return prev_date.strftime("%Y%m%d")
