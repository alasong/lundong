"""
基本面数据采集器
从 Tushare 获取 PE/PB/ROE 等财务指标
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from loguru import logger
import time
import tushare as ts

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class FundamentalCollector:
    """基本面数据采集器"""

    # API 限流配置
    API_LIMIT = 480
    REQUEST_TIMES: List[float] = []

    def __init__(self):
        """初始化"""
        ts.set_token(settings.tushare_token)
        self.pro = ts.pro_api()
        logger.info("基本面数据采集器初始化完成")

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

    def get_daily_basic(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取每日基本面数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with PE/PB/PS/市值等
        """
        self._check_rate_limit()

        try:
            df = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv,"
                "turnover_rate,turnover_rate_f,dv_ratio,close",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 每日基本面：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取每日基本面失败：{str(e)[:80]}")
            return None

    def get_fina_indicator(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取财务指标

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with ROE/ROA/毛利率等
        """
        self._check_rate_limit()

        try:
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,end_date,roa,roe,gross_margin,net_margin,"
                "debt_to_assets,current_ratio,quick_ratio,asset_turnover,"
                "inventory_turnover,accounts_receivable_turnover",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 财务指标：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取财务指标失败：{str(e)[:80]}")
            return None

    def get_income(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取利润表

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 营收/利润等
        """
        self._check_rate_limit()

        try:
            df = self.pro.income(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,end_date,total_revenue,operate_profit,profit_statements,"
                "net_profit,operating_cost,selling_expense,admin_expense",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 利润表：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取利润表失败：{str(e)[:80]}")
            return None

    def get_balance(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取资产负债表

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 资产/负债等
        """
        self._check_rate_limit()

        try:
            df = self.pro.balance(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,end_date,total_assets,total_liab,total_hldr_eqy,"
                "retained_profit,cash_equivalents,accounts_receivable,inventory",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 资产负债表：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取资产负债表失败：{str(e)[:80]}")
            return None

    def get_cashflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取现金流量表

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 经营/投资/筹资现金流
        """
        self._check_rate_limit()

        try:
            df = self.pro.cashflow(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,end_date,oper_cf,invest_cf,financing_cf,"
                "free_cash_flow,cash_equivalents_end",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 现金流量表：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取现金流量表失败：{str(e)[:80]}")
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
                fields="ts_code,end_date,profit_forecast,sales_forecast,"
                "profit_surprise,sales_surprise",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 业绩预告：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取业绩预告失败：{str(e)[:80]}")
            return None

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
                "analyst_num,target_price",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 研报评级：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取研报评级失败：{str(e)[:80]}")
            return None

    def collect_all_fundamental(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        采集所有基本面数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            合并后的基本面数据
        """
        logger.info(f"采集 {ts_code} 基本面数据...")

        # 获取每日基本面
        daily_basic = self.get_daily_basic(ts_code, start_date, end_date)
        if daily_basic is None:
            return None

        # 获取财务指标 (季度)
        fina_indicator = self.get_fina_indicator(ts_code, start_date, end_date)

        # 获取业绩预告
        forecast = self.get_forecast(ts_code, start_date, end_date)

        # 获取研报评级
        report_rc = self.get_report_rc(ts_code, start_date, end_date)

        # 合并数据
        result = daily_basic.copy()

        # 合并季度财务指标 (需要日期对齐)
        if fina_indicator is not None and not fina_indicator.empty:
            fina_indicator = fina_indicator.rename(columns={"end_date": "trade_date"})
            # 只合并存在的列
            available_cols = ["trade_date"]
            for col in [
                "roa",
                "roe",
                "gross_margin",
                "net_margin",
                "debt_to_assets",
                "current_ratio",
            ]:
                if col in fina_indicator.columns:
                    available_cols.append(col)
            if len(available_cols) > 1:
                result = result.merge(
                    fina_indicator[available_cols], on="trade_date", how="left"
                )

        # 合并业绩预告
        if forecast is not None and not forecast.empty:
            forecast = forecast.rename(columns={"end_date": "trade_date"})
            available_cols = ["trade_date"]
            for col in ["profit_forecast", "profit_surprise"]:
                if col in forecast.columns:
                    available_cols.append(col)
            if len(available_cols) > 1:
                result = result.merge(
                    forecast[available_cols], on="trade_date", how="left"
                )

        # 合并研报评级
        if report_rc is not None and not report_rc.empty:
            available_cols = ["trade_date"]
            for col in ["rc_rating", "rating_change", "analyst_num"]:
                if col in report_rc.columns:
                    available_cols.append(col)
            if len(available_cols) > 1:
                result = result.merge(
                    report_rc[available_cols], on="trade_date", how="left"
                )

        logger.info(
            f"{ts_code} 基本面数据采集完成：{len(result)} 条，{len(result.columns)} 个字段"
        )
        return result


def main():
    """测试"""
    collector = FundamentalCollector()

    # 测试平安银行
    df = collector.collect_all_fundamental(
        ts_code="000001.SZ", start_date="20250101", end_date="20260316"
    )

    if df is not None:
        print(f"\n采集成功：{len(df)} 条记录")
        print(f"字段：{df.columns.tolist()}")
        print(f"\n最新数据:")
        print(df.tail(1).to_string())


if __name__ == "__main__":
    main()
