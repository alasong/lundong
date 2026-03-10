"""
特征工程模块
计算各类技术指标和特征
"""
import pandas as pd
import numpy as np
from typing import Optional, List
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories


class FeatureEngineer:
    """特征工程"""

    def __init__(self):
        ensure_directories()

    # ==================== 量价特征 ====================

    def compute_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算价格相关特征

        Args:
            df: 日线数据，需包含 open, high, low, close, vol, amount
        """
        df = df.copy()

        # 涨跌幅
        df["pct_chg"] = df["close"].pct_change() * 100

        # 振幅
        df["amplitude"] = (df["high"] - df["low"]) / df["pre_close"] * 100

        # 换手率 (需要流通股本数据)
        if "turnover_rate" not in df.columns:
            df["turnover_rate"] = df["vol"] / df["amount"] * 100

        # 均线
        for window in [5, 10, 20, 60]:
            df[f"ma{window}"] = df["close"].rolling(window=window).mean()
            df[f"ma{window}_vol"] = df["vol"].rolling(window=window).mean()

        # 价格相对位置
        for window in [5, 10, 20]:
            df[f"price_position_{window}"] = (
                (df["close"] - df["low"].rolling(window).min()) /
                (df["high"].rolling(window).max() - df["low"].rolling(window).min())
            )

        # 连续涨跌天数
        df["up_days"] = (df["pct_chg"] > 0).astype(int)
        df["down_days"] = (df["pct_chg"] < 0).astype(int)

        # 计算连续上涨/下跌天数
        df["consecutive_up"] = self._count_consecutive(df["up_days"])
        df["consecutive_down"] = self._count_consecutive(df["down_days"])

        return df

    def _count_consecutive(self, series: pd.Series) -> pd.Series:
        """计算连续出现的次数"""
        groups = (series != series.shift(1)).cumsum()
        return series.groupby(groups).cumsum()

    def compute_momentum_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算动量相关特征"""
        df = df.copy()

        # RSI
        for window in [6, 14, 24]:
            df[f"rsi_{window}"] = self._compute_rsi(df["close"], window)

        # MACD
        macd, signal, hist = self._compute_macd(df["close"])
        df["macd"] = macd
        df["macd_signal"] = signal
        df["macd_hist"] = hist

        # KDJ
        k, d, j = self._compute_kdj(df)
        df["kdj_k"] = k
        df["kdj_d"] = d
        df["kdj_j"] = j

        # 布林带
        upper, middle, lower = self._compute_bollinger(df["close"])
        df["boll_upper"] = upper
        df["boll_middle"] = middle
        df["boll_lower"] = lower
        df["boll_width"] = (upper - lower) / middle

        return df

    def _compute_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        """计算RSI"""
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)

        avg_gain = gain.rolling(window=window).mean()
        avg_loss = loss.rolling(window=window).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _compute_macd(
        self,
        prices: pd.Series,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ):
        """计算MACD"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal).mean()
        hist = macd - signal_line
        return macd, signal_line, hist

    def _compute_kdj(
        self,
        df: pd.DataFrame,
        n: int = 9,
        m1: int = 3,
        m2: int = 3
    ):
        """计算KDJ"""
        low_min = df["low"].rolling(window=n).min()
        high_max = df["high"].rolling(window=n).max()

        rsv = (df["close"] - low_min) / (high_max - low_min) * 100
        rsv = rsv.fillna(50)

        k = rsv.ewm(alpha=1/m1).mean()
        d = k.ewm(alpha=1/m2).mean()
        j = 3 * k - 2 * d

        return k, d, j

    def _compute_bollinger(
        self,
        prices: pd.Series,
        window: int = 20,
        num_std: float = 2.0
    ):
        """计算布林带"""
        middle = prices.rolling(window=window).mean()
        std = prices.rolling(window=window).std()
        upper = middle + num_std * std
        lower = middle - num_std * std
        return upper, middle, lower

    # ==================== 资金特征 ====================

    def compute_moneyflow_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算资金流向特征

        Args:
            df: 资金流向数据
        """
        df = df.copy()

        # 主力资金净流入
        df["main_net_inflow"] = df.get("buy_elg_vol", 0) - df.get("sell_elg_vol", 0)

        # 主力资金净流入占比
        df["main_net_ratio"] = df["main_net_inflow"] / df["amount"] * 100

        # 大单净流入
        df["big_net_inflow"] = (
            df.get("buy_elg_vol", 0) + df.get("buy_lg_vol", 0) -
            df.get("sell_elg_vol", 0) - df.get("sell_lg_vol", 0)
        )

        # 散户资金净流入
        df["retail_net_inflow"] = (
            df.get("buy_sm_vol", 0) - df.get("sell_sm_vol", 0)
        )

        return df

    # ==================== 板块特征 ====================

    def compute_sector_features(
        self,
        sector_df: pd.DataFrame,
        stock_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        计算板块特征

        Args:
            sector_df: 板块行情数据
            stock_df: 个股数据
        """
        features = []

        for trade_date in sector_df["trade_date"].unique():
            date_data = {"trade_date": trade_date}
            day_sector = sector_df[sector_df["trade_date"] == trade_date]
            day_stock = stock_df[stock_df["trade_date"] == trade_date]

            # 板块涨跌分布
            date_data["sector_up_ratio"] = (day_sector["pct_chg"] > 0).mean()

            # 板块平均涨幅
            date_data["sector_avg_pct"] = day_sector["pct_chg"].mean()

            # 板块最高涨幅
            date_data["sector_max_pct"] = day_sector["pct_chg"].max()

            # 板块成交额占比
            date_data["sector_amount_ratio"] = (
                day_sector["amount"].sum() / day_stock["amount"].sum()
            )

            features.append(date_data)

        return pd.DataFrame(features)

    # ==================== 热点特征 ====================

    def compute_hotspot_features(
        self,
        concept_df: pd.DataFrame,
        limit_df: pd.DataFrame,
        moneyflow_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        计算热点特征

        Args:
            concept_df: 概念板块数据
            limit_df: 涨跌停数据
            moneyflow_df: 资金流向数据
        """
        features = []

        for trade_date in concept_df["trade_date"].unique():
            date_data = {"trade_date": trade_date}

            # 概念板块数据
            day_concept = concept_df[concept_df["trade_date"] == trade_date]

            # 涨幅强度
            date_data["concept_avg_pct"] = day_concept["pct_chg"].mean()
            date_data["concept_max_pct"] = day_concept["pct_chg"].max()

            # 活跃概念数量（涨幅>3%）
            date_data["active_concept_count"] = (day_concept["pct_chg"] > 3).sum()

            # 涨跌停数据
            day_limit = limit_df[limit_df["trade_date"] == trade_date] if not limit_df.empty else pd.DataFrame()

            if not day_limit.empty:
                date_data["limit_up_count"] = len(day_limit[day_limit.get("limit_type", "U") == "U"])
                date_data["limit_down_count"] = len(day_limit[day_limit.get("limit_type", "D") == "D"])

                # 连板股数量（需要额外处理）
                if "limit_times" in day_limit.columns:
                    date_data["multi_limit_count"] = (day_limit["limit_times"] >= 2).sum()

            # 资金流向
            day_moneyflow = moneyflow_df[moneyflow_df["trade_date"] == trade_date] if not moneyflow_df.empty else pd.DataFrame()

            if not day_moneyflow.empty:
                date_data["total_main_inflow"] = day_moneyflow.get("buy_elg_vol", pd.Series([0])).sum()
                date_data["total_main_outflow"] = day_moneyflow.get("sell_elg_vol", pd.Series([0])).sum()

            features.append(date_data)

        return pd.DataFrame(features)

    def save_features(self, df: pd.DataFrame, filename: str):
        """保存特征数据"""
        filepath = os.path.join(settings.features_dir, filename)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        logger.info(f"特征已保存: {filepath}")
        return filepath


def main():
    """主函数"""
    engineer = FeatureEngineer()

    # 示例：加载日线数据并计算特征
    # 实际使用时需要加载数据
    logger.info("特征工程模块已就绪")


if __name__ == "__main__":
    main()
