"""
打板策略 V1
结合首板策略和一进二策略的组合策略
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from loguru import logger

import sys
import os

sys.path.insert(
    0,
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
)

from ...base_strategy import BaseStrategy
from data.database import get_database


class DabanV1Strategy(BaseStrategy):
    """
    打板策略 V1 - 首板+一进二+龙头股组合策略
    """

    def __init__(self, strategy_config: Optional[Dict] = None):
        super().__init__("daban_v1", strategy_config)
        self.db = get_database()

        # 默认参数配置
        self.default_config = {
            "first_limit": {
                "limit_up_threshold": 0.095,  # 涨停阈值
                "min_volume_ratio": 3.0,  # 最小量比
                "top_n_stocks": 8,  # 选股数量
            },
            "one_to_two": {
                "gap_open_min": 0.01,  # 最小高开幅度
                "gap_open_max": 0.06,  # 最大高开幅度
                "top_n_stocks": 6,  # 选股数量
            },
            "risk_management": {
                "stop_loss_pct": -0.03,  # 止损比例
                "take_profit_pct": 0.02,  # 止盈比例
                "max_position_per_stock": 0.10,  # 单股最大仓位
            },
        }

        # 合并配置
        if strategy_config:
            self.config = self._merge_configs(self.default_config, strategy_config)
        else:
            self.config = self.default_config

    def _merge_configs(self, default: Dict, override: Dict) -> Dict:
        """合并默认配置和覆盖配置"""
        result = default.copy()
        for key, value in override.items():
            if isinstance(value, dict) and key in result:
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result

    def execute(self, date: Optional[str] = None) -> Dict:
        """
        执行打板策略
        :param date: 日期字符串，格式YYYYMMDD
        :return: 策略执行结果
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        try:
            # 获取首板股票
            first_limit_stocks = self._get_first_limit_stocks(date)

            # 获取一进二股票
            one_to_two_stocks = self._get_one_to_two_stocks(date)

            # 合并并排序
            combined_signals = self._combine_signals(
                first_limit_stocks, one_to_two_stocks
            )

            # 风控处理
            final_signals = self._apply_risk_management(combined_signals)

            return {
                "date": date,
                "signals": final_signals,
                "summary": {
                    "total_signals": len(final_signals),
                    "first_limit_count": len(first_limit_stocks),
                    "one_to_two_count": len(one_to_two_stocks),
                    "combined_count": len(combined_signals),
                },
            }

        except Exception as e:
            logger.error(f"Error executing DabanV1Strategy: {e}")
            return {
                "date": date,
                "signals": [],
                "summary": {"total_signals": 0, "error": str(e)},
            }

    def _get_first_limit_stocks(self, date: str) -> List[Dict]:
        """
        获取首板股票信号
        """
        try:
            # 获取涨停股票
            limit_up_stocks = self.db.get_limit_up_stocks(date)

            if not limit_up_stocks:
                return []

            # 过滤首板（排除连续涨停的股票）
            first_limit_stocks = []
            for stock in limit_up_stocks:
                ts_code = stock["ts_code"]

                # 检查是否为首板（前一日未涨停）
                prev_date = self._get_prev_trading_day(date)
                if prev_date:
                    prev_limit_status = self.db.get_limit_status(ts_code, prev_date)
                    if not prev_limit_status or not prev_limit_status.get(
                        "is_limit_up", False
                    ):
                        # 计算量比
                        volume_ratio = self._calculate_volume_ratio(ts_code, date)

                        if (
                            volume_ratio
                            >= self.config["first_limit"]["min_volume_ratio"]
                        ):
                            first_limit_stocks.append(
                                {
                                    "ts_code": ts_code,
                                    "stock_name": stock.get("stock_name", ""),
                                    "close_price": stock.get("close", 0),
                                    "pct_change": stock.get("pct_chg", 0),
                                    "volume_ratio": volume_ratio,
                                    "signal_strength": volume_ratio
                                    * stock.get("pct_chg", 0)
                                    / 100,
                                    "strategy": "first_limit",
                                }
                            )

            # 按信号强度排序，取前N只
            first_limit_stocks.sort(key=lambda x: x["signal_strength"], reverse=True)
            return first_limit_stocks[: self.config["first_limit"]["top_n_stocks"]]

        except Exception as e:
            logger.error(f"Error getting first limit stocks: {e}")
            return []

    def _get_one_to_two_stocks(self, date: str) -> List[Dict]:
        """
        获取一进二股票信号
        """
        try:
            # 获取昨日涨停股票
            prev_date = self._get_prev_trading_day(date)
            if not prev_date:
                return []

            prev_limit_stocks = self.db.get_limit_up_stocks(prev_date)
            if not prev_limit_stocks:
                return []

            one_to_two_stocks = []

            for stock in prev_limit_stocks:
                ts_code = stock["ts_code"]

                # 获取当日数据
                current_data = self.db.get_stock_daily(ts_code, date)
                if not current_data:
                    continue

                # 计算开盘涨幅
                open_pct = (
                    current_data.get("open", 0) - current_data.get("pre_close", 1)
                ) / current_data.get("pre_close", 1)

                # 检查是否符合一进二条件（高开幅度）
                if (
                    self.config["one_to_two"]["gap_open_min"]
                    <= open_pct
                    <= self.config["one_to_two"]["gap_open_max"]
                ):
                    # 检查是否继续涨停
                    if current_data.get("pct_chg", 0) > 9.5:  # 接近涨停
                        one_to_two_stocks.append(
                            {
                                "ts_code": ts_code,
                                "stock_name": stock.get("stock_name", ""),
                                "close_price": current_data.get("close", 0),
                                "pct_change": current_data.get("pct_chg", 0),
                                "open_pct": open_pct,
                                "signal_strength": open_pct
                                * current_data.get("pct_chg", 0)
                                / 100,
                                "strategy": "one_to_two",
                            }
                        )

            # 按信号强度排序，取前N只
            one_to_two_stocks.sort(key=lambda x: x["signal_strength"], reverse=True)
            return one_to_two_stocks[: self.config["one_to_two"]["top_n_stocks"]]

        except Exception as e:
            logger.error(f"Error getting one to two stocks: {e}")
            return []

    def _combine_signals(
        self, first_limit_stocks: List[Dict], one_to_two_stocks: List[Dict]
    ) -> List[Dict]:
        """
        合并首板和一进二信号
        """
        # 合并两个列表
        all_signals = first_limit_stocks + one_to_two_stocks

        # 去重（同一个股票可能在两个列表中出现）
        seen_codes = set()
        unique_signals = []

        for signal in all_signals:
            if signal["ts_code"] not in seen_codes:
                unique_signals.append(signal)
                seen_codes.add(signal["ts_code"])

        # 按信号强度排序
        unique_signals.sort(key=lambda x: x["signal_strength"], reverse=True)

        return unique_signals

    def _apply_risk_management(self, signals: List[Dict]) -> List[Dict]:
        """
        应用风控规则
        """
        filtered_signals = []

        for signal in signals:
            # 这里可以添加更多的风控逻辑
            # 当前只是简单的过滤
            if (
                abs(signal["signal_strength"]) > 0.01  # 信号强度阈值
                and signal["pct_change"] > 5.0
            ):  # 涨幅阈值
                filtered_signals.append(signal)

        return filtered_signals

    def _calculate_volume_ratio(self, ts_code: str, date: str) -> float:
        """
        计算量比
        """
        try:
            # 获取最近10个交易日的平均成交量
            end_date = date
            start_date = self._get_nth_trading_day(date, -10)

            if not start_date:
                return 0.0

            hist_data = self.db.get_stock_daily_hist(ts_code, start_date, end_date)
            if len(hist_data) < 5:  # 至少需要5天数据
                return 0.0

            avg_volume = np.mean([d.get("vol", 0) for d in hist_data[:-1]])  # 排除当天
            if avg_volume == 0:
                return 0.0

            # 获取当天成交量
            current_data = hist_data[-1] if hist_data else {}
            current_volume = current_data.get("vol", 0)

            return current_volume / avg_volume if avg_volume > 0 else 0.0

        except Exception as e:
            logger.error(f"Error calculating volume ratio for {ts_code}: {e}")
            return 0.0

    def _get_prev_trading_day(self, date: str) -> Optional[str]:
        """
        获取前一个交易日
        """
        try:
            trading_days = self.db.get_trading_calendar()
            current_idx = trading_days.index(date) if date in trading_days else -1

            if current_idx > 0:
                return trading_days[current_idx - 1]
            else:
                return None
        except Exception as e:
            logger.error(f"Error getting previous trading day for {date}: {e}")
            return None

    def _get_nth_trading_day(self, date: str, n: int) -> Optional[str]:
        """
        获取第n个交易日（n为负数表示往前，正数表示往后）
        """
        try:
            trading_days = self.db.get_trading_calendar()
            current_idx = trading_days.index(date) if date in trading_days else -1

            if current_idx != -1:
                target_idx = current_idx + n
                if 0 <= target_idx < len(trading_days):
                    return trading_days[target_idx]

            return None
        except Exception as e:
            logger.error(f"Error getting {n}th trading day for {date}: {e}")
            return None
