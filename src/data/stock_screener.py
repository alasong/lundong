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
        'min_avg_amount': 3000,        # 日均成交额≥3000 万 (降低门槛覆盖更多中小盘)
        'min_avg_turnover': 1.0,       # 日均换手率≥1%

        # 市值要求
        'min_market_cap': 50,          # 市值≥50 亿
        'max_market_cap': 5000,        # 市值≤5000 亿 (提高上限避免错过龙头)

        # 估值要求
        'max_pe': 100,                 # PE<100 (排除极端高估)
        'min_pb': 0.3,                 # PB>0.3 (排除问题股)
        'max_pb': 30,                  # PB<30 (排除过度炒作)

        # 技术面要求
        'max_volatility': 0.35,        # 20 日波动率<35% (提高门槛允许高波动成长股)

        # 综合得分权重配置
        'score_weights': {
            'liquidity': 0.30,
            'momentum': 0.30,
            'value': 0.20,
            'size': 0.20,
        }
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
        top_n_per_concept: int = None,
        industry_neutral: bool = False,
        use_vectorized: bool = True
    ) -> pd.DataFrame:
        """
        从目标板块中筛选优质个股

        Args:
            concept_codes: 看好的板块代码列表
            concept_ranking: 板块预测排名 (可选，用于增强排序)
            date: 筛选基准日期
            lookback_days: 回看天数
            top_n_per_concept: 每个板块选取的个股数量
            industry_neutral: 是否使用行业中性化评分
            use_vectorized: 是否使用向量化计算 (性能更好)

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
        factors = self._calculate_factors(stock_data, date, use_vectorized=use_vectorized)

        # Step 4: 应用筛选规则
        filtered = self._apply_rules(factors)

        if filtered.empty:
            logger.warning("所有股票都被筛除")
            return pd.DataFrame()

        logger.info(f"筛选后剩余 {len(filtered)} 只股票")

        # Step 5: 计算综合得分 (可选行业中性化)
        if industry_neutral:
            logger.info("使用行业中性化评分...")
            filtered = self._calculate_scores_industry_neutral(filtered)
        else:
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
        date: str,
        use_vectorized: bool = True
    ) -> pd.DataFrame:
        """
        计算筛选因子

        Args:
            stock_data: 股票历史数据
            date: 基准日期
            use_vectorized: 是否使用向量化优化（性能更好）

        返回:
            DataFrame with factors per stock
        """
        if use_vectorized:
            return self._calculate_factors_vectorized(stock_data, date)
        else:
            return self._calculate_factors_legacy(stock_data, date)

    def _calculate_factors_legacy(
        self,
        stock_data: pd.DataFrame,
        date: str
    ) -> pd.DataFrame:
        """
        legacy 方法：逐只股票循环计算（保留用于调试）
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

            # 从数据库获取基本面数据 (PE/PB/市值) - 使用 self.db 统一连接
            pe_ttm = None
            pb_ttm = None
            market_cap = None  # 亿元

            try:
                # 使用 self.db 统一查询，避免直接连接
                result = self.db.query('''
                    SELECT pe_ttm, pb, total_mv
                    FROM stock_daily_basic
                    WHERE ts_code = ? AND trade_date = ?
                ''', (stock_code, date))
                if result:
                    row = result[0]
                    # PE 直接使用该值
                    pe_ttm = row[0]
                    # PB 直接使用该值
                    pb_ttm = row[1]
                    # 市值：从万元转换为亿元 (除以 10000)
                    if row[2] is not None:
                        market_cap = row[2] / 10000.0
            except Exception as e:
                logger.debug(f"获取 {stock_code} 基本面数据失败：{e}")

            # 流动性因子
            # amount 单位是千元，除以 100 转换为万元
            avg_amount_20d = stock_df.tail(20)['amount'].mean() / 100  # 转换为万元
            avg_turnover_20d = stock_df.tail(20)['turnover_rate'].mean() if 'turnover_rate' in stock_df.columns else None

            # 动量因子 - 多周期动量 (5 日/10 日/20 日)
            close_prices = stock_df['close'].values
            momentum_5d = 0.0
            momentum_10d = 0.0
            momentum_20d = 0.0

            if len(close_prices) >= 5:
                momentum_5d = (close_prices[-1] / close_prices[-5] - 1) * 100
            if len(close_prices) >= 10:
                momentum_10d = (close_prices[-1] / close_prices[-10] - 1) * 100
            if len(close_prices) >= 20:
                momentum_20d = (close_prices[-1] / close_prices[-20] - 1) * 100

            # 综合动量得分 (加权平均：短期 40% + 中期 30% + 长期 30%)
            momentum_score = momentum_5d * 0.4 + momentum_10d * 0.3 + momentum_20d * 0.3

            # 波动率因子 - 修复：pct_chg 已是百分比格式，不需要除以 100
            if len(close_prices) >= 20:
                daily_returns = stock_df['pct_chg'].values[-20:]
                # pct_chg 是百分比格式 (如 2.5 表示 2.5%)
                # 转换为小数后计算年化波动率
                volatility_20d = np.std(daily_returns / 100) * np.sqrt(252)
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
                # 动量 (多周期)
                'momentum_5d': momentum_5d,
                'momentum_10d': momentum_10d,
                'momentum_20d': momentum_20d,
                'momentum_score': momentum_score,  # 综合动量得分
                # 波动率
                'volatility_20d': volatility_20d,
            })

        return pd.DataFrame(factors_list)

    def _calculate_factors_vectorized(
        self,
        stock_data: pd.DataFrame,
        date: str
    ) -> pd.DataFrame:
        """
        向量化方法：批量计算所有股票因子（性能优化）

        使用 groupby + agg 向量化操作，避免 Python 循环
        """
        logger.debug("使用向量化方法计算因子...")

        # 按股票分组
        grouped = stock_data.groupby('ts_code')

        # 检查每组数据量
        valid_stocks = grouped.filter(lambda x: len(x) >= 10)['ts_code'].unique()
        stock_data = stock_data[stock_data['ts_code'].isin(valid_stocks)]
        grouped = stock_data.groupby('ts_code')

        # 1. 计算流动性因子 (20 日平均成交额/换手率)
        liquidity = grouped.apply(
            lambda df: pd.Series({
                'avg_amount_20d': df.tail(20)['amount'].mean() / 100,
                'avg_turnover_20d': df.tail(20)['turnover_rate'].mean() if 'turnover_rate' in df.columns else None
            })
        ).reset_index()

        # 2. 计算动量因子 (向量化计算多周期动量)
        def calc_momentum(df):
            df = df.sort_values('trade_date')
            close = df['close'].values
            n = len(close)
            result = {'momentum_5d': 0.0, 'momentum_10d': 0.0, 'momentum_20d': 0.0}
            if n >= 5:
                result['momentum_5d'] = (close[-1] / close[-5] - 1) * 100
            if n >= 10:
                result['momentum_10d'] = (close[-1] / close[-10] - 1) * 100
            if n >= 20:
                result['momentum_20d'] = (close[-1] / close[-20] - 1) * 100
            result['momentum_score'] = (
                result['momentum_5d'] * 0.4 +
                result['momentum_10d'] * 0.3 +
                result['momentum_20d'] * 0.3
            )
            return pd.Series(result)

        momentum = grouped.apply(calc_momentum).reset_index()

        # 3. 计算波动率因子
        def calc_volatility(df):
            df = df.sort_values('trade_date')
            if len(df) >= 20:
                returns = df['pct_chg'].values[-20:] / 100  # 转换为小数
                vol = np.std(returns) * np.sqrt(252)
            else:
                vol = 0
            return pd.Series({'volatility_20d': vol})

        volatility = grouped.apply(calc_volatility).reset_index()

        # 4. 获取基本面数据 (批量查询)
        basic_data = self._get_basic_data_batch(list(valid_stocks), date)

        # 5. 获取股票基本信息 (名称/板块)
        stock_info = grouped.apply(
            lambda df: pd.Series({
                'stock_name': df['stock_name'].iloc[0] if 'stock_name' in df.columns else '',
                'concept_code': df['concept_code'].iloc[0] if 'concept_code' in df.columns else ''
            })
        ).reset_index()

        # 合并所有因子
        factors = stock_info.merge(liquidity, on='ts_code', how='left')
        factors = factors.merge(momentum, on='ts_code', how='left')
        factors = factors.merge(volatility, on='ts_code', how='left')
        factors = factors.merge(basic_data, on='ts_code', how='left')
        factors['trade_date'] = date

        # 添加行业标签用于中性化 (使用板块代码作为代理)
        factors['industry'] = factors['concept_code']

        logger.debug(f"向量化因子计算完成：{len(factors)} 只股票")
        return factors

    def _get_basic_data_batch(
        self,
        stock_codes: List[str],
        date: str
    ) -> pd.DataFrame:
        """
        批量获取基本面数据 (PE/PB/市值)

        使用单个 SQL 查询代替多次单独查询
        """
        if not stock_codes:
            return pd.DataFrame()

        # 构建 IN 子句
        placeholders = ','.join(['?' for _ in stock_codes])
        query = f'''
            SELECT ts_code, pe_ttm, pb, total_mv
            FROM stock_daily_basic
            WHERE ts_code IN ({placeholders}) AND trade_date = ?
        '''

        try:
            # 添加日期参数
            params = stock_codes + [date]
            result = self.db.query(query, params)

            if not result:
                return pd.DataFrame(columns=['ts_code', 'pe_ttm', 'pb_ttm', 'market_cap'])

            # 转换为 DataFrame
            df = pd.DataFrame(result, columns=['ts_code', 'pe_ttm', 'pb_ttm', 'total_mv'])
            # 市值从万元转换为亿元
            df['market_cap'] = df['total_mv'] / 10000.0
            df = df.drop(columns=['total_mv'])

            return df

        except Exception as e:
            logger.debug(f"批量获取基本面数据失败：{e}")
            return pd.DataFrame(columns=['ts_code', 'pe_ttm', 'pb_ttm', 'market_cap'])

    def _apply_rules(self, factors: pd.DataFrame) -> pd.DataFrame:
        """应用筛选规则"""
        df = factors.copy()

        rules = self.SCREENING_RULES

        # 流动性过滤
        if 'min_avg_amount' in rules:
            df = df[df['avg_amount_20d'] >= rules['min_avg_amount']]

        # 换手率过滤 - 如果有数据则应用
        if 'min_avg_turnover' in rules:
            # 检查是否有换手率数据
            if df['avg_turnover_20d'].notna().any():
                # 对于换手率为空的股票，使用成交额作为替代判断
                low_turnover_mask = df['avg_turnover_20d'].isna() & (df['avg_amount_20d'] < rules['min_avg_amount'] * 2)
                low_turnover_mask |= df['avg_turnover_20d'] < rules['min_avg_turnover']
                df = df[~low_turnover_mask]

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

        # 波动率过滤 - 使用修复后的计算
        if 'max_volatility' in rules:
            df = df[df['volatility_20d'] <= rules['max_volatility']]

        return df

    def _calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算综合得分

        得分构成:
        - 流动性得分 (30%)
        - 动量得分 (30%) - 使用多周期综合动量
        - 估值得分 (20%)
        - 市值得分 (20%)
        """
        weights = self.SCREENING_RULES.get('score_weights', {
            'liquidity': 0.30,
            'momentum': 0.30,
            'value': 0.20,
            'size': 0.20,
        })

        # 流动性得分 (成交额越大得分越高)
        df['liquidity_score'] = self._normalize_score(df['avg_amount_20d'], ascending=True)

        # 动量得分 (使用综合动量得分，动量越大得分越高)
        df['momentum_score'] = self._normalize_score(df['momentum_score'], ascending=True)

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

        # 综合得分 - 使用配置化权重
        df['stock_score'] = (
            weights['liquidity'] * df['liquidity_score'] +
            weights['momentum'] * df['momentum_score'] +
            weights['value'] * df['value_score'] +
            weights['size'] * df['size_score']
        )

        return df

    def _calculate_scores_industry_neutral(
        self,
        df: pd.DataFrame,
        industry_col: str = 'industry'
    ) -> pd.DataFrame:
        """
        计算行业中性化后的综合得分

        在每个行业内部分别计算排名，避免行业偏差

        Args:
            df: 因子 DataFrame
            industry_col: 行业列名

        Returns:
            包含中性化得分的 DataFrame
        """
        weights = self.SCREENING_RULES.get('score_weights', {
            'liquidity': 0.30,
            'momentum': 0.30,
            'value': 0.20,
            'size': 0.20,
        })

        df = df.copy()

        # 在每个行业内计算排名得分
        def rank_within_industry(group, col, ascending):
            """组内排名归一化到 0-100"""
            if len(group) < 2:
                return pd.Series(50.0, index=group.index)
            # 使用 rank 方法，返回 0-1 的百分位，再乘以 100
            return group[col].rank(ascending=ascending, pct=True) * 100

        # 按行业分组
        if industry_col not in df.columns or df[industry_col].isna().all():
            # 如果没有行业数据，使用普通方法
            return self._calculate_scores(df)

        def rank_within_group(group, col, ascending):
            """组内排名归一化到 0-100"""
            if len(group) < 2:
                return pd.Series(50.0, index=group.index, name=col)
            result = group[col].rank(ascending=ascending, pct=True) * 100
            result.name = col
            return result

        def size_score_group(group):
            """计算市值得分"""
            result = self._normalize_mid_score(group['market_cap'])
            result.name = 'size_score'
            return result

        # 行业内流动性得分
        liquidity_scores = []
        for name, group in df.groupby(industry_col):
            liquidity_scores.append(rank_within_group(group, 'avg_amount_20d', ascending=False))
        df['liquidity_score'] = pd.concat(liquidity_scores)

        # 行业内动量得分
        momentum_scores = []
        for name, group in df.groupby(industry_col):
            momentum_scores.append(rank_within_group(group, 'momentum_score', ascending=False))
        df['momentum_score'] = pd.concat(momentum_scores)

        # 行业内估值得分 (PE 越小越好)
        if df['pe_ttm'].notna().any():
            value_scores = []
            for name, group in df.groupby(industry_col):
                value_scores.append(rank_within_group(group, 'pe_ttm', ascending=True))
            df['value_score'] = pd.concat(value_scores)
        else:
            df['value_score'] = 50.0

        # 行业内市值得分 (使用中间值最优)
        if df['market_cap'].notna().any():
            size_scores = []
            for name, group in df.groupby(industry_col):
                size_scores.append(size_score_group(group))
            df['size_score'] = pd.concat(size_scores)
        else:
            df['size_score'] = 50.0

        # 综合得分
        df['stock_score'] = (
            weights['liquidity'] * df['liquidity_score'] +
            weights['momentum'] * df['momentum_score'] +
            weights['value'] * df['value_score'] +
            weights['size'] * df['size_score']
        )

        return df

    def _normalize_score(
        self,
        series: pd.Series,
        ascending: bool = True,
        use_quantile: bool = True
    ) -> pd.Series:
        """
        归一化得分到 0-100

        Args:
            series: 原始数据
            ascending: True=越大越好，False=越小越好
            use_quantile: 是否使用分位数截断 (处理异常值)
        """
        series = series.fillna(series.median())

        if len(series) == 0:
            return pd.Series(dtype=float)

        if use_quantile:
            # 使用 1% 和 99% 分位数截断，避免异常值影响
            min_val = series.quantile(0.01)
            max_val = series.quantile(0.99)
        else:
            min_val = series.min()
            max_val = series.max()

        if max_val == min_val:
            return pd.Series(50.0, index=series.index)

        if ascending:
            score = (series - min_val) / (max_val - min_val) * 100
        else:
            score = (max_val - series) / (max_val - min_val) * 100

        return score.clip(0, 100)

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
