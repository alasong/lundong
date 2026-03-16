"""
资金流向数据采集器
从 Tushare 获取个股/板块资金流向数据
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


class MoneyflowCollector:
    """资金流向数据采集器"""

    # API 限流配置
    API_LIMIT = 480
    REQUEST_TIMES: List[float] = []

    def __init__(self):
        """初始化"""
        ts.set_token(settings.tushare_token)
        self.pro = ts.pro_api()
        logger.info("资金流向数据采集器初始化完成")

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

    def get_moneyflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取个股资金流向

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 主力/散户资金流向
        """
        self._check_rate_limit()

        try:
            df = self.pro.moneyflow(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,buy_sm_amount,buy_md_amount,buy_lg_amount,"
                "buy_elg_amount,sell_elg_amount,sell_lg_amount,sell_md_amount,"
                "sell_sm_amount,net_mf_amount,net_lg_amount,net_md_amount,"
                "net_sm_amount,net_elg_amount",
            )

            if df is not None and not df.empty:
                logger.debug(f"{ts_code} 资金流向：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"{ts_code} 获取资金流向失败：{str(e)[:80]}")
            return None

    def get_moneyflow_hsgt(
        self, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取沪深港通资金流向

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 北向/南向资金
        """
        self._check_rate_limit()

        try:
            df = self.pro.moneyflow_hsgt(
                start_date=start_date,
                end_date=end_date,
                fields="trade_date,gts_net_buy,gts_buy_amt,gts_sell_amt,"
                "gh_net_buy,gh_buy_amt,gh_sell_amt,total_net_buy",
            )

            if df is not None and not df.empty:
                logger.debug(f"沪深港通资金流向：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"获取沪深港通资金流向失败：{str(e)[:80]}")
            return None

    def get_moneyflow_ind(
        self, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取行业资金流向

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 行业资金流
        """
        self._check_rate_limit()

        try:
            df = self.pro.moneyflow_ind(
                start_date=start_date,
                end_date=end_date,
                fields="trade_date,ind_code,ind_name,net_in_amount,net_out_amount,"
                "net_big_amount,net_mid_amount,net_small_amount",
            )

            if df is not None and not df.empty:
                logger.debug(f"行业资金流向：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"获取行业资金流向失败：{str(e)[:80]}")
            return None

    def get_northbound(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取北向资金详情

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with 北向资金持仓
        """
        self._check_rate_limit()

        try:
            df = self.pro.northbound(
                start_date=start_date,
                end_date=end_date,
                fields="trade_date,ts_code,name,hold_shares,hold_ratio,change_shares",
            )

            if df is not None and not df.empty:
                logger.debug(f"北向资金持仓：{len(df)} 条")
                return df
            return None

        except Exception as e:
            logger.warning(f"获取北向资金持仓失败：{str(e)[:80]}")
            return None

    def collect_all_moneyflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        采集所有资金流向数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            合并后的资金流向数据
        """
        logger.info(f"采集 {ts_code} 资金流向数据...")

        # 获取个股资金流向
        moneyflow = self.get_moneyflow(ts_code, start_date, end_date)
        if moneyflow is None:
            return None

        result = moneyflow.copy()

        # 计算衍生指标
        # 主力净流入 (大单 + 超大单)
        result["main_force_net"] = (
            result["buy_lg_amount"]
            + result["buy_elg_amount"]
            - result["sell_lg_amount"]
            - result["sell_elg_amount"]
        )

        # 散户净流入
        result["retail_net"] = result["buy_sm_amount"] - result["sell_sm_amount"]

        # 主力/散户比
        result["main_retail_ratio"] = result["main_force_net"] / (
            result["retail_net"].abs() + 1e-8
        )

        # 资金流向强度 (净流入/成交额)
        if "amount" in result.columns:
            result["mf_strength"] = result["net_mf_amount"] / (result["amount"] + 1e-8)
        else:
            result["mf_strength"] = result["net_mf_amount"]

        logger.info(f"{ts_code} 资金流向数据采集完成：{len(result)} 条")
        return result


def main():
    """测试"""
    collector = MoneyflowCollector()

    # 测试平安银行
    df = collector.collect_all_moneyflow(
        ts_code="000001.SZ", start_date="20250101", end_date="20260316"
    )

    if df is not None:
        print(f"\n采集成功：{len(df)} 条记录")
        print(f"字段：{df.columns.tolist()}")
        print(f"\n最新数据:")
        print(df.tail(1).to_string())


if __name__ == "__main__":
    main()
