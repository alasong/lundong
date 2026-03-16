"""
分析师预期数据采集器
从 Tushare 获取研报评级、业绩预测等数据
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from loguru import logger
import time
import tushare as ts

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class AnalystCollector:
    """分析师预期数据采集器"""

    # API 限流配置
    API_LIMIT = 480
    REQUEST_TIMES: List[float] = []

    def __init__(self):
        """初始化"""
        ts.set_token(settings.tushare_token)
        self.pro = ts.pro_api()
        logger.info("分析师预期数据采集器初始化完成")

    def _check_rate_limit(self):
        """检查 API 限流"""
        now = time.time()
        self.REQUEST_TIMES = [t for t in self.REQUEST_TIMES if now - t < 60]

        if len(self.REQUEST_TIMES) >= self.API_LIMIT * 0.9:
            wait_time = 60 - (now - self.REQUEST_TIMES[0]) + 1
            logger.warning(f"接近 API 限流，等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
            self.REQUEST_TIMES = []

        self.REQUEST_TIMES.append(time.time())

    def get_report_rc(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取研报评级

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 研报评级
        """
        self._check_rate_limit()

        try:
            df = self.pro.report_rc(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,report_date,rc_rating,rating_change,"
                "analyst_num,target_price,close_price",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 研报评级：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取研报评级失败：{str(e)[:80]}")
            return None

    def get_forecast(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取业绩预告

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 业绩预告
        """
        self._check_rate_limit()

        try:
            df = self.pro.forecast(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,end_date,profit_forecast,profit_lastyear,"
                "profit_surprise,sales_forecast,sales_surprise",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 业绩预告：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取业绩预告失败：{str(e)[:80]}")
            return None

    def get_express(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取业绩快报

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 业绩快报
        """
        self._check_rate_limit()

        try:
            df = self.pro.express(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,end_date,revenue,revenue_yoy,profit,profit_yoy,"
                "eps,roe,bps",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 业绩快报：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取业绩快报失败：{str(e)[:80]}")
            return None

    def collect_all_analyst(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        采集所有分析师预期数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            合并后的分析师预期数据
        """
        logger.info(f"采集 {ts_code} 分析师预期数据...")

        # 获取研报评级
        report_rc = self.get_report_rc(ts_code, start_date, end_date)
        if report_rc is None:
            return None

        result = report_rc.copy()

        # 获取业绩预告
        forecast = self.get_forecast(ts_code, start_date, end_date)
        if forecast is not None and not forecast.empty:
            forecast = forecast.rename(columns={"end_date": "trade_date"})
            # 只保留需要的列
            forecast_cols = [
                "trade_date",
                "profit_forecast",
                "profit_surprise",
                "sales_forecast",
                "sales_surprise",
            ]
            available_cols = [c for c in forecast_cols if c in forecast.columns]
            result = result.merge(forecast[available_cols], on="trade_date", how="left")

        # 获取业绩快报
        express = self.get_express(ts_code, start_date, end_date)
        if express is not None and not express.empty:
            express = express.rename(columns={"end_date": "trade_date"})
            express_cols = ["trade_date", "revenue_yoy", "profit_yoy", "eps", "roe"]
            available_cols = [c for c in express_cols if c in express.columns]
            result = result.merge(express[available_cols], on="trade_date", how="left")

        # 计算衍生指标
        # 评级量化 (1=买入，2=增持，3=中性，4=减持，5=卖出)
        if "rc_rating" in result.columns:
            result["rating_score"] = (
                result["rc_rating"]
                .map(
                    {
                        "买入": 1,
                        "增持": 2,
                        "推荐": 2,
                        "中性": 3,
                        "减持": 4,
                        "卖出": 5,
                        "未知": 3,
                    }
                )
                .fillna(3)
            )

        # 评级变化方向
        if "rating_change" in result.columns:
            result["rating_change_dir"] = (
                result["rating_change"]
                .map({"上调": 1, "下调": -1, "维持": 0})
                .fillna(0)
            )

        # 超预期幅度
        if "profit_surprise" in result.columns:
            result["surprise_ratio"] = result["profit_surprise"]

        logger.info(f"{ts_code} 分析师预期数据采集完成：{len(result)} 条")
        return result


def main():
    """测试"""
    collector = AnalystCollector()

    # 测试平安银行
    df = collector.collect_all_analyst(
        ts_code="000001.SZ", start_date="20250101", end_date="20260316"
    )

    if df is not None:
        print(f"\n采集成功：{len(df)} 条记录")
        print(f"字段：{df.columns.tolist()}")
        print(f"\n最新数据:")
        print(df.tail(1).to_string())


if __name__ == "__main__":
    main()
