"""
数据采集调度器
负责定时采集各类数据 - 使用 Tushare 数据源
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings, ensure_directories
from data.tushare_client import TushareClient
from data.tushare_ths_client import TushareTHSClient


class DataCollector:
    """数据采集器 - Tushare 数据源"""

    def __init__(self, 
                 em_client: Optional[TushareClient] = None,
                 ths_client: Optional[TushareTHSClient] = None):
        """初始化数据采集器
        
        Args:
            em_client: 东方财富客户端
            ths_client: 同花顺客户端
        """
        if em_client is None:
            from core.settings import settings as core_settings
            self.em_client = TushareClient(
                token=core_settings.tushare_token,
                max_retries=3,
                retry_delay=1.0
            )
        else:
            self.em_client = em_client
            
        if ths_client is None:
            from core.settings import settings as core_settings
            self.ths_client = TushareTHSClient(
                token=core_settings.tushare_token,
                max_retries=3
            )
        else:
            self.ths_client = ths_client
            
        ensure_directories()
        logger.info("数据采集器初始化完成（Tushare 数据源）")

    def collect_basic_data(self):
        """采集基础数据 - 行业概念列表"""
        logger.info("开始采集基础数据...")

        # 东方财富概念列表
        concept_df = self.em_client.get_concept_list()
        if not concept_df.empty:
            save_path = os.path.join(settings.raw_data_dir, "eastmoney_concepts.csv")
            concept_df.to_csv(save_path, index=False)
            logger.info(f"东方财富概念：{len(concept_df)} 条 -> {save_path}")

        # 东方财富行业列表
        industry_df = self.em_client.get_index_list()
        if not industry_df.empty:
            save_path = os.path.join(settings.raw_data_dir, "eastmoney_industries.csv")
            industry_df.to_csv(save_path, index=False)
            logger.info(f"东方财富行业：{len(industry_df)} 条 -> {save_path}")

        # 同花顺指数列表
        ths_indices = self.ths_client.get_ths_indices()
        if not ths_indices.empty:
            save_path = os.path.join(settings.raw_data_dir, "ths_indices.csv")
            ths_indices.to_csv(save_path, index=False)
            logger.info(f"同花顺指数：{len(ths_indices)} 条 -> {save_path}")

        # 同花顺一级行业
        ths_industries = self.ths_client.get_ths_industries(level=1)
        if not ths_industries.empty:
            save_path = os.path.join(settings.raw_data_dir, "ths_industries_l1.csv")
            ths_industries.to_csv(save_path, index=False)
            logger.info(f"同花顺一级行业：{len(ths_industries)} 条 -> {save_path}")

    def collect_daily_data(self, trade_date: Optional[str] = None):
        """
        采集每日数据

        Args:
            trade_date: 交易日期 YYYYMMDD，默认昨天
        """
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        logger.info(f"开始采集每日数据：{trade_date}")
        # 这里可以添加每日数据采集逻辑
        logger.info(f"每日数据采集完成：{trade_date}")

    def collect_history_data(
        self,
        start_date: str,
        end_date: str,
        data_types: List[str] = None
    ):
        """
        批量采集历史数据

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            data_types: 数据类型列表，默认全部
        """
        if data_types is None:
            data_types = ["eastmoney_industry", "eastmoney_concept", "ths_industry"]

        logger.info(f"开始采集历史数据：{start_date} - {end_date}")

        # 加载列表数据
        industry_file = os.path.join(settings.raw_data_dir, "eastmoney_industries.csv")
        concept_file = os.path.join(settings.raw_data_dir, "eastmoney_concepts.csv")
        ths_file = os.path.join(settings.raw_data_dir, "ths_industries_l1.csv")

        if "eastmoney_industry" in data_types and os.path.exists(industry_file):
            logger.info("采集东方财富行业历史...")
            industries = pd.read_csv(industry_file)
            self._collect_em_history(industries, start_date, end_date, "industry")

        if "eastmoney_concept" in data_types and os.path.exists(concept_file):
            logger.info("采集东方财富概念历史...")
            concepts = pd.read_csv(concept_file)
            self._collect_em_history(concepts, start_date, end_date, "concept")

        if "ths_industry" in data_types and os.path.exists(ths_file):
            logger.info("采集同花顺行业历史...")
            ths_industries = pd.read_csv(ths_file)
            self._collect_ths_history(ths_industries, start_date, end_date)

        logger.info("历史数据采集完成")

    def _collect_em_history(self, indexes: pd.DataFrame, 
                           start_date: str, end_date: str, 
                           data_type: str):
        """采集东方财富历史数据"""
        total = len(indexes)
        success = 0
        
        for i, (idx, row) in enumerate(indexes.iterrows(), 1):
            ts_code = row.get('ts_code', row.get('code', ''))
            name = row.get('industry_name', row.get('concept_name', row.get('name', '')))
            
            if not ts_code:
                continue
                
            try:
                hist = self.em_client.get_index_history(ts_code, start_date, end_date)
                if not hist.empty:
                    prefix = "industry" if data_type == "industry" else "concept"
                    save_path = os.path.join(settings.raw_data_dir, f"{prefix}_{ts_code.replace('.', '_')}.csv")
                    hist.to_csv(save_path, index=False)
                    success += 1
                    
                    if i % 10 == 0:
                        logger.info(f"进度：{i}/{total} - {name}")
                        
            except Exception as e:
                logger.error(f"采集失败 {name}: {e}")
                
            # 避免限流
            if i % 10 == 0:
                import time
                time.sleep(0.5)
        
        logger.info(f"东方财富{data_type}历史采集完成：{success}/{total}")

    def _collect_ths_history(self, indexes: pd.DataFrame,
                            start_date: str, end_date: str):
        """采集同花顺历史数据"""
        total = len(indexes)
        success = 0
        
        for i, (idx, row) in enumerate(indexes.iterrows(), 1):
            ts_code = row.get('ts_code', '')
            name = row.get('name', '')
            
            if not ts_code:
                continue
                
            try:
                hist = self.ths_client.get_ths_history(ts_code, start_date, end_date)
                if hist is not None and len(hist) > 0:
                    save_path = os.path.join(settings.raw_data_dir, f"ths_{ts_code.replace('.', '_')}.csv")
                    hist.to_csv(save_path, index=False)
                    success += 1
                    
                    if i % 10 == 0:
                        logger.info(f"进度：{i}/{total} - {name}")
                        
            except Exception as e:
                logger.error(f"采集失败 {name}: {e}")
                
            # 避免限流
            if i % 10 == 0:
                import time
                time.sleep(0.5)
        
        logger.info(f"同花顺历史采集完成：{success}/{total}")


def main():
    """主函数"""
    collector = DataCollector()

    # 采集基础数据
    collector.collect_basic_data()

    # 采集最近 30 天的数据
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

    collector.collect_history_data(start_date, end_date, 
                                   data_types=["eastmoney_industry", "eastmoney_concept"])


if __name__ == "__main__":
    main()
