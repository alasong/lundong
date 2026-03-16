"""
增强型数据采集器
整合基本面、资金流向、分析师预期数据
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from loguru import logger
import time
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from data.database import get_database
from data.fundamental_collector import FundamentalCollector
from data.moneyflow_collector import MoneyflowCollector
from data.analyst_collector import AnalystCollector


class EnhancedDataCollector:
    """增强型数据采集器"""

    def __init__(self):
        """初始化"""
        self.db = get_database()
        self.fundamental = FundamentalCollector()
        self.moneyflow = MoneyflowCollector()
        self.analyst = AnalystCollector()
        logger.info("增强型数据采集器初始化完成")

    def collect_single_stock(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Tuple[
        Optional[pd.DataFrame],
        Optional[pd.DataFrame],
        Optional[pd.DataFrame],
        Optional[pd.DataFrame],
    ]:
        """
        采集单只股票的所有数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            (stock_data, fundamental_data, moneyflow_data, analyst_data)
        """
        logger.info(f"采集 {ts_code} 的增强数据...")

        # 1. 获取日线行情
        stock_data = self.db.get_stock_data(ts_code, start_date, end_date)

        # 2. 获取基本面数据
        fundamental_data = self.fundamental.collect_all_fundamental(
            ts_code, start_date, end_date
        )

        # 3. 获取资金流向数据
        moneyflow_data = self.moneyflow.collect_all_moneyflow(
            ts_code, start_date, end_date
        )

        # 4. 获取分析师预期数据
        analyst_data = self.analyst.collect_all_analyst(ts_code, start_date, end_date)

        return stock_data, fundamental_data, moneyflow_data, analyst_data

    def collect_batch(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        max_workers: int = 10,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量采集多只股票数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            max_workers: 最大并发数

        Returns:
            {
                'stock': stock_data,
                'fundamental': fundamental_data,
                'moneyflow': moneyflow_data,
                'analyst': analyst_data
            }
        """
        logger.info(f"开始批量采集 {len(stock_codes)} 只股票的增强数据...")

        all_stock = []
        all_fundamental = []
        all_moneyflow = []
        all_analyst = []

        success_count = 0
        fail_count = 0

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def collect_single(code):
            try:
                result = self.collect_single_stock(code, start_date, end_date)
                return code, result, None
            except Exception as e:
                return code, None, str(e)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(collect_single, code): code for code in stock_codes
            }

            for i, future in enumerate(as_completed(futures)):
                code, result, error = future.result()

                if error:
                    fail_count += 1
                    logger.warning(f"{code} 采集失败：{error[:80]}")
                else:
                    stock_data, fundamental_data, moneyflow_data, analyst_data = result

                    if stock_data is not None and not stock_data.empty:
                        all_stock.append(stock_data)
                        success_count += 1

                    if fundamental_data is not None and not fundamental_data.empty:
                        all_fundamental.append(fundamental_data)

                    if moneyflow_data is not None and not moneyflow_data.empty:
                        all_moneyflow.append(moneyflow_data)

                    if analyst_data is not None and not analyst_data.empty:
                        all_analyst.append(analyst_data)

                if (i + 1) % 10 == 0:
                    logger.info(
                        f"采集进度：{i + 1}/{len(stock_codes)}，成功={success_count}，失败={fail_count}"
                    )

        # 合并数据
        results = {}

        if all_stock:
            results["stock"] = pd.concat(all_stock, ignore_index=True)
            logger.info(f"股票行情数据：{len(results['stock'])} 条")

        if all_fundamental:
            results["fundamental"] = pd.concat(all_fundamental, ignore_index=True)
            logger.info(f"基本面数据：{len(results['fundamental'])} 条")

        if all_moneyflow:
            results["moneyflow"] = pd.concat(all_moneyflow, ignore_index=True)
            logger.info(f"资金流向数据：{len(results['moneyflow'])} 条")

        if all_analyst:
            results["analyst"] = pd.concat(all_analyst, ignore_index=True)
            logger.info(f"分析师预期数据：{len(results['analyst'])} 条")

        logger.info(f"批量采集完成：成功={success_count}/{len(stock_codes)}")

        return results

    def collect_from_concepts(
        self,
        concept_codes: List[str],
        start_date: str,
        end_date: str,
        top_n_per_concept: int = 5,
    ) -> Dict[str, pd.DataFrame]:
        """
        从板块成分股采集增强数据

        Args:
            concept_codes: 板块代码列表
            start_date: 开始日期
            end_date: 结束日期
            top_n_per_concept: 每个板块采集的股票数量

        Returns:
            采集结果
        """
        logger.info(f"从 {len(concept_codes)} 个板块采集增强数据...")

        # 1. 获取成分股
        constituent_df = self.db.get_constituent_stocks(concept_codes)

        if constituent_df.empty:
            logger.warning("未找到成分股数据")
            return {}

        # 2. 每个板块选 top_n 只股票
        stock_codes = []
        for concept in concept_codes:
            concept_stocks = constituent_df[constituent_df["concept_code"] == concept]
            # 按权重排序
            if "weight" in concept_stocks.columns:
                concept_stocks = concept_stocks.nlargest(top_n_per_concept, "weight")
            else:
                concept_stocks = concept_stocks.head(top_n_per_concept)

            stock_codes.extend(concept_stocks["stock_code"].tolist())

        stock_codes = list(set(stock_codes))  # 去重
        logger.info(f"共采集 {len(stock_codes)} 只股票")

        # 3. 批量采集
        results = self.collect_batch(stock_codes, start_date, end_date)

        return results


def main():
    """测试"""
    collector = EnhancedDataCollector()

    # 测试单只股票
    print("\n=== 测试单只股票 ===")
    stock, fundamental, moneyflow, analyst = collector.collect_single_stock(
        ts_code="000001.SZ", start_date="20250101", end_date="20260316"
    )

    if stock is not None:
        print(f"\n股票行情：{len(stock)} 条")
    if fundamental is not None:
        print(
            f"基本面：{len(fundamental)} 条，字段={fundamental.columns.tolist()[:10]}..."
        )
    if moneyflow is not None:
        print(f"资金流向：{len(moneyflow)} 条")
    if analyst is not None:
        print(f"分析师预期：{len(analyst)} 条")

    # 测试批量采集
    print("\n=== 测试批量采集 ===")
    test_codes = ["000001.SZ", "000002.SZ", "000063.SZ"]
    results = collector.collect_batch(
        stock_codes=test_codes,
        start_date="20250101",
        end_date="20260316",
        max_workers=5,
    )

    for key, df in results.items():
        print(f"{key}: {len(df)} 条")


if __name__ == "__main__":
    main()
