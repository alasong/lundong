"""
个股筛选器
从板块成分股中优选个股，基于流动性、估值、技术面等维度
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import SQLiteDatabase, get_database


class StockScreener:
    """个股筛选器 - 从板块成分股中优选个股"""

    # 筛选条件配置
    SCREENING_RULES = {
        # 流动性要求
        'min_avg_amount': 5000,        # 日均成交额≥5000 万
        'min_avg_turnover': 1.0,       # 日均换手率≥1%

        # 市值要求
        'min_market_cap': 50,          # 市值≥50 亿
        'max_market_cap': 2000,        # 市值≤2000 亿

        # 估值要求
        'max_pe': 100,                 # PE<100 (排除极端高估)
        'min_pb': 0.3,                 # PB>0.3 (排除问题股)
        'max_pb': 30,                  # PB<30 (排除过度炒作)

        # 技术面要求
        'max_volatility': 0.25,        # 20 日波动率<25%
    }

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化筛选器

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        logger.info("个股筛选器初始化完成")

    def screen_stocks(
        self,
        concept_codes: List[str],
        concept_ranking: pd.DataFrame = None,
        date: str = None,
        lookback_days: int = 20,
        top_n_per_concept: int = None
    ) -> pd.DataFrame:
        """
        从目标板块中筛选优质个股

        Args:
            concept_codes: 看好的板块代码列表
            concept_ranking: 板块预测排名 (可选，用于增强排序)
            date: 筛选基准日期
            lookback_days: 回看天数
            top_n_per_concept: 每个板块选取的个股数量

        Returns:
            DataFrame with columns:
            - stock_code, stock_name
            - concept_code (所属板块)
            - concept_pred (板块预测涨幅)
            - stock_score (个股得分)
            - liquidity_score, momentum_score, value_score, size_score (子分数)
        """
        if date is None:
            date = self.db.get_latest_date()
            if date is None:
                logger.warning("无法获取最新日期")
                return pd.DataFrame()

        logger.info(f"开始筛选个股 (基准日期：{date})...")

        # Step 1: 获取成分股
        constituents = self.db.get_constituent_stocks(concept_codes)

        if constituents.empty:
            logger.warning("未找到成分股")
            return pd.DataFrame()

        logger.info(f"获取到 {len(constituents)} 只成分股")

        # Step 2: 获取个股历史数据用于计算因子
        stock_data = self._get_stock_history(constituents, date, lookback_days)

        if stock_data.empty:
            logger.warning("无法获取个股历史数据")
            return pd.DataFrame()

        # Step 3: 计算筛选因子
        factors = self._calculate_factors(stock_data, date)

        # Step 4: 应用筛选规则
        filtered = self._apply_rules(factors)

        if filtered.empty:
            logger.warning("所有股票都被筛除")
            return pd.DataFrame()

        logger.info(f"筛选后剩余 {len(filtered)} 只股票")

        # Step 5: 计算综合得分
        filtered = self._calculate_scores(filtered)

        # Step 6: 合并板块信息
        if concept_ranking is not None and not concept_ranking.empty:
            filtered = self._merge_concept_prediction(filtered, concept_ranking)

        # Step 7: 每个板块选 TOP N
        if top_n_per_concept:
            # 使用 sort_values + groupby + head 保留 concept_code 列
            filtered = filtered.sort_values('stock_score', ascending=False)
            filtered = filtered.groupby('concept_code', group_keys=False).head(top_n_per_concept)

        # 排序输出
        result = filtered.sort_values('stock_score', ascending=False)

        logger.info(f"筛选完成，共 {len(result)} 只股票")
        return result

    def _get_stock_history(
        self,
        constituents: pd.DataFrame,
        date: str,
        lookback_days: int
    ) -> pd.DataFrame:
        """获取个股历史数据"""
        # 计算起始日期
        start_date = self._calc_start_date(date, lookback_days)

        # 获取所有成分股的历史数据
        stock_codes = constituents['stock_code'].unique().tolist()

        all_data = []
        for code in stock_codes:
            df = self.db.get_stock_data(code, start_date, date)
            if not df.empty:
                df['concept_code'] = constituents[
                    constituents['stock_code'] == code
                ]['concept_code'].iloc[0]
                df['stock_name'] = constituents[
                    constituents['stock_code'] == code
                ]['stock_name'].iloc[0]
                all_data.append(df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _calc_start_date(self, date: str, lookback_days: int) -> str:
        """计算起始日期"""
        from datetime import timedelta
        trade_date = datetime.strptime(date, "%Y%m%d")
        # 考虑交易日，多预留一些天数
        start = trade_date - timedelta(days=int(lookback_days * 1.5))
        return start.strftime("%Y%m%d")

    def _calculate_factors(
        self,
        stock_data: pd.DataFrame,
        date: str
    ) -> pd.DataFrame:
        """
        计算筛选因子

        返回:
            DataFrame with factors per stock
        """
        factors_list = []

        for stock_code in stock_data['ts_code'].unique():
            stock_df = stock_data[stock_data['ts_code'] == stock_code].sort_values('trade_date')

            if len(stock_df) < 10:
                continue

            # 获取最新数据
            latest = stock_df.iloc[-1]
            stock_name = stock_df['stock_name'].iloc[0] if 'stock_name' in stock_df.columns else ''
            concept_code = stock_df['concept_code'].iloc[0] if 'concept_code' in stock_df.columns else ''

            # 从数据库获取基本面数据 (PE/PB/市值)
            pe_ttm = None
            pb_ttm = None
            market_cap = None  # 亿元

            try:
                import sqlite3
                conn = sqlite3.connect('data/stock.db')
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT pe_ttm, pb, total_mv
                    FROM stock_daily_basic
                    WHERE ts_code = ? AND trade_date = ?
                ''', (stock_code, date))
                row = cursor.fetchone()
                if row:
                    # PE 直接使用该值
                    pe_ttm = row[0]
                    # PB 直接使用该值
                    pb_ttm = row[1]
                    # 市值：从万元转换为亿元 (除以 10000)
                    if row[2] is not None:
                        market_cap = row[2] / 10000.0
                conn.close()
            except Exception as e:
                logger.debug(f"获取 {stock_code} 基本面数据失败：{e}")

            # 流动性因子
            # amount 单位是千元，除以 100 转换为万元
            avg_amount_20d = stock_df.tail(20)['amount'].mean() / 100  # 转换为万元
            avg_turnover_20d = stock_df.tail(20)['turnover_rate'].mean() if 'turnover_rate' in stock_df.columns else None

            # 动量因子
            close_prices = stock_df['close'].values
            if len(close_prices) >= 20:
                momentum_20d = (close_prices[-1] / close_prices[-20] - 1) * 100
            else:
                momentum_20d = 0

            # 波动率因子
            if len(close_prices) >= 20:
                daily_returns = stock_df['pct_chg'].values[-20:]
                volatility_20d = np.std(daily_returns) * np.sqrt(252) / 100  # 年化波动率
            else:
                volatility_20d = 0

            factors_list.append({
                'stock_code': stock_code,
                'stock_name': stock_name,
                'concept_code': concept_code,
                'trade_date': date,
                # 流动性
                'avg_amount_20d': avg_amount_20d,
                'avg_turnover_20d': avg_turnover_20d,
                # 市值
                'market_cap': market_cap,
                # 估值
                'pe_ttm': pe_ttm,
                'pb_ttm': pb_ttm,
                # 动量
                'momentum_20d': momentum_20d,
                # 波动率
                'volatility_20d': volatility_20d,
            })

        return pd.DataFrame(factors_list)

    def _apply_rules(self, factors: pd.DataFrame) -> pd.DataFrame:
        """应用筛选规则"""
        df = factors.copy()

        rules = self.SCREENING_RULES

        # 流动性过滤
        if 'min_avg_amount' in rules:
            df = df[df['avg_amount_20d'] >= rules['min_avg_amount']]
        # 注意：turnover_rate 可能为空，跳过此过滤
        # if 'min_avg_turnover' in rules and df['avg_turnover_20d'].notna().any():
        #     df = df[df['avg_turnover_20d'] >= rules['min_avg_turnover']]

        # 市值过滤 - 如果数据为空，跳过
        if 'min_market_cap' in rules and df['market_cap'].notna().any():
            df = df[df['market_cap'] >= rules['min_market_cap']]
        if 'max_market_cap' in rules and df['market_cap'].notna().any():
            df = df[df['market_cap'] <= rules['max_market_cap']]

        # 估值过滤 - 如果数据为空，跳过
        if 'max_pe' in rules and df['pe_ttm'].notna().any():
            df = df[(df['pe_ttm'].isna()) | (df['pe_ttm'] <= rules['max_pe'])]
        if 'min_pb' in rules and df['pb_ttm'].notna().any():
            df = df[(df['pb_ttm'].isna()) | (df['pb_ttm'] >= rules['min_pb'])]
        if 'max_pb' in rules and df['pb_ttm'].notna().any():
            df = df[(df['pb_ttm'].isna()) | (df['pb_ttm'] <= rules['max_pb'])]

        # 波动率过滤
        if 'max_volatility' in rules:
            df = df[df['volatility_20d'] <= rules['max_volatility']]

        return df

    def _calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算综合得分

        得分构成:
        - 流动性得分 (30%)
        - 动量得分 (30%)
        - 估值得分 (20%)
        - 市值得分 (20%)
        """
        # 流动性得分 (成交额越大得分越高)
        df['liquidity_score'] = self._normalize_score(df['avg_amount_20d'], ascending=True)

        # 动量得分 (动量越大得分越高)
        df['momentum_score'] = self._normalize_score(df['momentum_20d'], ascending=True)

        # 估值得分 (PE 越小得分越高) - 如果 PE 为空，使用中间值
        if df['pe_ttm'].notna().any():
            df['value_score'] = self._normalize_score(df['pe_ttm'], ascending=False)
        else:
            df['value_score'] = 50.0  # 默认中间分

        # 市值得分 (中等市值得分高，避免过大过小) - 如果市值为空，使用中间值
        if df['market_cap'].notna().any():
            df['size_score'] = self._normalize_mid_score(df['market_cap'])
        else:
            df['size_score'] = 50.0  # 默认中间分

        # 综合得分
        df['stock_score'] = (
            0.30 * df['liquidity_score'] +
            0.30 * df['momentum_score'] +
            0.20 * df['value_score'] +
            0.20 * df['size_score']
        )

        return df

    def _normalize_score(
        self,
        series: pd.Series,
        ascending: bool = True
    ) -> pd.Series:
        """
        归一化得分到 0-100

        Args:
            series: 原始数据
            ascending: True=越大越好，False=越小越好
        """
        series = series.fillna(series.median())

        if len(series) == 0:
            return pd.Series(dtype=float)

        min_val = series.min()
        max_val = series.max()

        if max_val == min_val:
            return pd.Series(50.0, index=series.index)

        if ascending:
            score = (series - min_val) / (max_val - min_val) * 100
        else:
            score = (max_val - series) / (max_val - min_val) * 100

        return score

    def _normalize_mid_score(
        self,
        series: pd.Series,
        target_min: float = 100,
        target_max: float = 500
    ) -> pd.Series:
        """
        中间值最优的归一化 (中等市值得分最高)

        Args:
            series: 原始数据
            target_min: 理想下限
            target_max: 理想上限
        """
        series = series.fillna(series.median())

        score = pd.Series(50.0, index=series.index)

        # 在目标区间内得高分
        mask_low = series < target_min
        mask_high = series > target_max
        mask_mid = ~mask_low & ~mask_high

        score[mask_mid] = 80 + 20 * (1 - abs(series[mask_mid] - (target_min + target_max) / 2) / ((target_max - target_min) / 2))
        score[mask_low] = 50 + 30 * (series[mask_low] / target_min)
        score[mask_high] = 50 + 30 * (target_max / series[mask_high])

        return score.clip(0, 100)

    def _merge_concept_prediction(
        self,
        stocks: pd.DataFrame,
        concept_ranking: pd.DataFrame
    ) -> pd.DataFrame:
        """合并板块预测信息"""
        # 提取板块预测数据
        concept_pred = concept_ranking[['concept_code', 'pred_1d', 'pred_5d', 'pred_20d', 'combined_score']].copy()
        concept_pred = concept_pred.rename(columns={
            'combined_score': 'concept_strength'
        })

        # 合并
        result = stocks.merge(concept_pred, on='concept_code', how='left')

        # 填充缺失值
        if 'concept_strength' in result.columns:
            result['concept_strength'] = result['concept_strength'].fillna(50)
        else:
            result['concept_strength'] = 50

        return result

    def get_top_stocks(
        self,
        concept_codes: List[str],
        top_n: int = 20,
        **kwargs
    ) -> pd.DataFrame:
        """
        获取优选股票 TOP N

        Args:
            concept_codes: 板块代码列表
            top_n: 返回数量
            **kwargs: 传递给 screen_stocks 的参数

        Returns:
            TOP N 股票 DataFrame
        """
        result = self.screen_stocks(concept_codes, **kwargs)

        if result.empty:
            return result

        return result.head(top_n)


def main():
    """测试函数"""
    from data.database import get_database

    screener = StockScreener()

    # 测试筛选
    print("\n[测试] 筛选股票...")
    test_concepts = ['881101.TI', '881102.TI']  # 测试板块代码

    result = screener.get_top_stocks(test_concepts, top_n=10)

    if not result.empty:
        print(f"\n筛选结果：{len(result)} 只股票")
        print(result[['stock_code', 'stock_name', 'concept_code', 'stock_score']].to_string())
    else:
        print("筛选结果为空")


if __name__ == "__main__":
    main()
