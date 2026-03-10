"""
数据采集 Agent
负责定时采集各类数据 - 使用 Tushare 数据源
"""
import pandas as pd
from typing import Optional, Dict, Any, List
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.base_agent import BaseAgent, AgentResult
from data.tushare_client import TushareClient
from data.tushare_ths_client import TushareTHSClient
from data.data_collector import DataCollector
from config import settings, ensure_directories


class DataAgent(BaseAgent):
    """数据采集 Agent - Tushare 数据源"""

    def __init__(self, 
                 em_client: Optional[TushareClient] = None,
                 ths_client: Optional[TushareTHSClient] = None):
        super().__init__("DataAgent")
        
        # 初始化客户端
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
            
        self.collector = DataCollector(self.em_client, self.ths_client)
        ensure_directories()
        logger.info("DataAgent 初始化完成（Tushare 数据源）")

    def run(
        self,
        task: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行数据采集任务

        Args:
            task: 任务类型 daily/history/basic/lists
            start_date: 开始日期
            end_date: 结束日期
        """
        if task == "daily":
            return self._collect_daily(start_date)
        elif task == "history":
            return self._collect_history(start_date, end_date, **kwargs)
        elif task == "basic":
            return self._collect_basic()
        elif task == "lists":
            return self._collect_lists()
        else:
            raise ValueError(f"未知任务类型：{task}")

    def _collect_lists(self) -> Dict:
        """采集行业概念列表数据"""
        logger.info("开始采集列表数据")

        results = {}

        # 东方财富概念列表
        concept_df = self.em_client.get_concept_list()
        if not concept_df.empty:
            save_path = os.path.join(settings.raw_data_dir, "eastmoney_concepts.csv")
            concept_df.to_csv(save_path, index=False)
            results["eastmoney_concepts"] = len(concept_df)
            logger.info(f"东方财富概念：{len(concept_df)} 条")

        # 东方财富行业列表
        industry_df = self.em_client.get_index_list()
        if not industry_df.empty:
            save_path = os.path.join(settings.raw_data_dir, "eastmoney_industries.csv")
            industry_df.to_csv(save_path, index=False)
            results["eastmoney_industries"] = len(industry_df)
            logger.info(f"东方财富行业：{len(industry_df)} 条")

        # 同花顺指数列表
        ths_indices = self.ths_client.get_ths_indices()
        if not ths_indices.empty:
            save_path = os.path.join(settings.raw_data_dir, "ths_indices.csv")
            ths_indices.to_csv(save_path, index=False)
            results["ths_indices"] = len(ths_indices)
            logger.info(f"同花顺指数：{len(ths_indices)} 条")

        # 同花顺一级行业
        ths_industries = self.ths_client.get_ths_industries(level=1)
        if not ths_industries.empty:
            save_path = os.path.join(settings.raw_data_dir, "ths_industries_l1.csv")
            ths_industries.to_csv(save_path, index=False)
            results["ths_industries_l1"] = len(ths_industries)
            logger.info(f"同花顺一级行业：{len(ths_industries)} 条")

        logger.info(f"列表数据采集完成：{results}")
        return results

    def _collect_daily(self, trade_date: Optional[str] = None) -> Dict:
        """采集每日数据"""
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        logger.info(f"开始采集每日数据：{trade_date}")
        results = {"trade_date": trade_date}

        # 这里可以添加每日数据采集逻辑
        # 目前主要采集列表数据
        
        logger.info(f"每日数据采集完成：{results}")
        return results

    def _collect_history(
        self,
        start_date: str,
        end_date: str,
        data_types: Optional[List[str]] = None
    ) -> Dict:
        """采集历史数据"""
        if data_types is None:
            data_types = ["eastmoney_industry", "eastmoney_concept", "ths_industry"]

        logger.info(f"开始采集历史数据：{start_date} - {end_date}")

        results = {
            "start_date": start_date,
            "end_date": end_date,
            "data_types": data_types,
            "collected": 0,
            "failed": 0
        }

        # 调用 collector 采集历史数据
        self.collector.collect_history_data(start_date, end_date, data_types)

        logger.info(f"历史数据采集完成：{results}")
        return results

    def _collect_basic(self) -> Dict:
        """采集基础数据"""
        logger.info("开始采集基础数据")
        
        results = self.collector.collect_basic_data()
        
        logger.info(f"基础数据采集完成")
        return results

    def check_data_availability(self, trade_date: str) -> Dict[str, bool]:
        """检查数据是否可用"""
        files = [
            f"eastmoney_concepts.csv",
            f"eastmoney_industries.csv",
            f"ths_indices.csv",
            f"ths_industries_l1.csv"
        ]

        availability = {}
        for f in files:
            filepath = os.path.join(settings.raw_data_dir, f)
            availability[f] = os.path.exists(filepath)

        return availability
    
    def get_data_summary(self) -> Dict[str, Any]:
        """获取数据摘要信息"""
        summary = {
            "lists": {},
            "history_files": 0,
            "total_records": 0
        }
        
        # 统计列表数据
        for name in ["eastmoney_concepts", "eastmoney_industries", 
                     "ths_indices", "ths_industries_l1"]:
            filepath = os.path.join(settings.raw_data_dir, f"{name}.csv")
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                summary["lists"][name] = len(df)
                summary["total_records"] += len(df)
        
        # 统计历史文件
        import glob
        history_files = glob.glob(os.path.join(settings.raw_data_dir, "*_BK*.csv"))
        summary["history_files"] = len(history_files)
        
        return summary


def main():
    """主函数"""
    agent = DataAgent()

    # 采集基础数据（列表）
    result = agent.run(task="lists")
    print(f"\n列表数据采集：{result}")
    
    # 获取数据摘要
    summary = agent.get_data_summary()
    print(f"\n数据摘要：{summary}")


if __name__ == "__main__":
    main()
