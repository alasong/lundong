"""
股票筛选器
提供股票过滤功能，包括ST/*ST股票处理
"""

import pandas as pd
from typing import List, Optional
from loguru import logger


class StockFilter:
    """股票筛选器"""

    def __init__(self):
        """初始化筛选器"""
        logger.info("股票筛选器初始化完成")

    def filter_st_stocks(
        self,
        stock_list: pd.DataFrame,
        include_st: bool = True,
        stock_code_col: str = "stock_code",
        stock_name_col: str = "stock_name",
    ) -> pd.DataFrame:
        """
        过滤ST/*ST股票

        Args:
            stock_list: 股票列表 DataFrame
            include_st: 是否包含 ST/*ST 股票
            stock_code_col: 股票代码列名
            stock_name_col: 股票名称列名

        Returns:
            过滤后的股票列表
        """
        if stock_list.empty:
            logger.warning("股票列表为空")
            return stock_list

        # 检查必要列是否存在
        code_col = stock_code_col if stock_code_col in stock_list.columns else None
        name_col = stock_name_col if stock_name_col in stock_list.columns else None

        if code_col is None and name_col is None:
            logger.warning("未找到股票代码或名称列，返回原列表")
            return stock_list

        # 识别 ST/*ST 股票
        st_mask = pd.Series([False] * len(stock_list), index=stock_list.index)

        # 使用股票名称匹配
        if name_col and name_col in stock_list.columns:
            st_mask |= stock_list[name_col].astype(str).str.contains("ST", na=False)
            st_mask |= stock_list[name_col].astype(str).str.contains("*ST", na=False)

        # 使用股票代码匹配 (部分 ST 股票代码可能包含标识)
        if code_col and code_col in stock_list.columns:
            st_mask |= stock_list[code_col].astype(str).str.contains("ST", na=False)

        st_stocks = stock_list[st_mask]
        non_st_stocks = stock_list[~st_mask]

        # 日志记录
        if len(st_stocks) > 0:
            logger.info(f"发现 {len(st_stocks)} 只 ST/*ST 股票")
            if not include_st:
                logger.info(
                    f"排除 ST/*ST 股票：{', '.join(st_stocks[name_col].astype(str).tolist()[:10])}"
                )
            else:
                logger.info(
                    f"包含 ST/*ST 股票：{', '.join(st_stocks[name_col].astype(str).tolist()[:10])}"
                )

        if include_st:
            result = stock_list
            logger.info(f"包含 ST/*ST 股票，共 {len(result)} 只")
        else:
            result = non_st_stocks
            logger.info(f"排除 ST/*ST 股票后剩余 {len(result)} 只")

        return result

    def filter_by_rules(
        self,
        stock_list: pd.DataFrame,
        rules: dict,
        stock_code_col: str = "stock_code",
        stock_name_col: str = "stock_name",
    ) -> pd.DataFrame:
        """
        应用多规则过滤

        Args:
            stock_list: 股票列表 DataFrame
            rules: 过滤规则字典
                - min_avg_amount: 最小日均成交额
                - min_avg_turnover: 最小日均换手率
                - min_market_cap: 最小市值
                - max_market_cap: 最大市值
                - max_pe: 最大 PE
                - min_pb: 最小 PB
                - max_pb: 最大 PB
                - max_volatility: 最大波动率
                - include_st: 是否包含 ST/*ST 股票
            stock_code_col: 股票代码列名
            stock_name_col: 股票名称列名

        Returns:
            过滤后的股票列表
        """
        if stock_list.empty:
            logger.warning("股票列表为空")
            return stock_list

        df = stock_list.copy()
        rules = rules.copy()

        # 处理 ST/*ST 过滤
        include_st = rules.pop("include_st", True)
        df = self.filter_st_stocks(
            df,
            include_st=include_st,
            stock_code_col=stock_code_col,
            stock_name_col=stock_name_col,
        )

        # 流动性过滤
        if "min_avg_amount" in rules and "avg_amount_20d" in df.columns:
            original_count = len(df)
            df = df[df["avg_amount_20d"] >= rules["min_avg_amount"]]
            if len(df) < original_count:
                logger.info(f"成交额过滤：{original_count} -> {len(df)} 只")

        # 换手率过滤
        if "min_avg_turnover" in rules and "avg_turnover_20d" in df.columns:
            if df["avg_turnover_20d"].notna().any():
                original_count = len(df)
                df = df[df["avg_turnover_20d"] >= rules["min_avg_turnover"]]
                if len(df) < original_count:
                    logger.info(f"换手率过滤：{original_count} -> {len(df)} 只")

        # 市值过滤
        if "min_market_cap" in rules and "market_cap" in df.columns:
            if df["market_cap"].notna().any():
                original_count = len(df)
                df = df[df["market_cap"] >= rules["min_market_cap"]]
                if len(df) < original_count:
                    logger.info(f"最小市值过滤：{original_count} -> {len(df)} 只")

        if "max_market_cap" in rules and "market_cap" in df.columns:
            if df["market_cap"].notna().any():
                original_count = len(df)
                df = df[df["market_cap"] <= rules["max_market_cap"]]
                if len(df) < original_count:
                    logger.info(f"最大市值过滤：{original_count} -> {len(df)} 只")

        # 估值过滤
        if "max_pe" in rules and "pe_ttm" in df.columns:
            if df["pe_ttm"].notna().any():
                original_count = len(df)
                df = df[df["pe_ttm"] <= rules["max_pe"]]
                if len(df) < original_count:
                    logger.info(f"PE 过滤：{original_count} -> {len(df)} 只")

        if "min_pb" in rules and "pb_ttm" in df.columns:
            if df["pb_ttm"].notna().any():
                original_count = len(df)
                df = df[df["pb_ttm"] >= rules["min_pb"]]
                if len(df) < original_count:
                    logger.info(f"最小 PB 过滤：{original_count} -> {len(df)} 只")

        if "max_pb" in rules and "pb_ttm" in df.columns:
            if df["pb_ttm"].notna().any():
                original_count = len(df)
                df = df[df["pb_ttm"] <= rules["max_pb"]]
                if len(df) < original_count:
                    logger.info(f"最大 PB 过滤：{original_count} -> {len(df)} 只")

        # 波动率过滤
        if "max_volatility" in rules and "volatility_20d" in df.columns:
            original_count = len(df)
            df = df[df["volatility_20d"] <= rules["max_volatility"]]
            if len(df) < original_count:
                logger.info(f"波动率过滤：{original_count} -> {len(df)} 只")

        return df

    def get_st_stock_list(
        self,
        stock_list: pd.DataFrame,
        stock_code_col: str = "stock_code",
        stock_name_col: str = "stock_name",
    ) -> pd.DataFrame:
        """
        获取 ST/*ST 股票列表

        Args:
            stock_list: 股票列表 DataFrame
            stock_code_col: 股票代码列名
            stock_name_col: 股票名称列名

        Returns:
            ST/*ST 股票列表
        """
        if stock_list.empty:
            return stock_list

        # 识别 ST/*ST 股票
        st_mask = pd.Series([False] * len(stock_list), index=stock_list.index)

        if stock_name_col and stock_name_col in stock_list.columns:
            st_mask |= (
                stock_list[stock_name_col].astype(str).str.contains("ST", na=False)
            )
            st_mask |= (
                stock_list[stock_name_col].astype(str).str.contains("*ST", na=False)
            )

        if stock_code_col and stock_code_col in stock_list.columns:
            st_mask |= (
                stock_list[stock_code_col].astype(str).str.contains("ST", na=False)
            )

        st_stocks = stock_list[st_mask].copy()

        if len(st_stocks) > 0:
            logger.info(f"ST/*ST 股票列表：{len(st_stocks)} 只")
            logger.debug(
                f"ST/*ST 股票详情：{st_stocks[[stock_code_col, stock_name_col]].to_dict('records')}"
            )

        return st_stocks
