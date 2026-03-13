"""
数据采集 Agent
负责定时采集各类数据 - 只使用同花顺数据源
"""
import pandas as pd
from typing import Optional, Dict, Any, List
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.base_agent import BaseAgent, AgentResult
from data.tushare_ths_client import TushareTHSClient
from data.data_collector import DataCollector
from config import settings, ensure_directories


class DataAgent(BaseAgent):
    """数据采集 Agent - 只使用同花顺数据源"""

    def __init__(self,
                 ths_client: Optional[TushareTHSClient] = None):
        super().__init__("DataAgent")

        # 初始化同花顺客户端
        if ths_client is None:
            from core.settings import settings as core_settings
            self.ths_client = TushareTHSClient(
                token=core_settings.tushare_token,
                max_retries=3
            )
        else:
            self.ths_client = ths_client

        self.collector = DataCollector(ths_client=self.ths_client)
        ensure_directories()
        logger.info("DataAgent 初始化完成（同花顺数据源）")

    def run(
        self,
        task: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sector_type: str = "all",
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行数据采集任务

        Args:
            task: 任务类型 daily/history/basic/lists
            start_date: 开始日期
            end_date: 结束日期
            sector_type: 板块类型 (all/concept/industry)
        """
        if task == "daily":
            return self._collect_daily(start_date, sector_type)
        elif task == "history":
            return self._collect_history(start_date, end_date, **kwargs)
        elif task == "basic":
            return self._collect_basic()
        elif task == "lists":
            return self._collect_lists()
        else:
            raise ValueError(f"未知任务类型：{task}")

    def _collect_lists(self) -> Dict:
        """采集行业概念列表数据 - 只使用同花顺"""
        logger.info("开始采集列表数据（同花顺）")

        results = {}

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

        # 同花顺二级行业
        ths_industries_l2 = self.ths_client.get_ths_industries(level=2)
        if not ths_industries_l2.empty:
            save_path = os.path.join(settings.raw_data_dir, "ths_industries_l2.csv")
            ths_industries_l2.to_csv(save_path, index=False)
            results["ths_industries_l2"] = len(ths_industries_l2)
            logger.info(f"同花顺二级行业：{len(ths_industries_l2)} 条")

        logger.info(f"列表数据采集完成：{results}")
        return results

    def _collect_daily(self, trade_date: Optional[str] = None, sector_type: str = "all") -> Dict:
        """采集每日数据

        Args:
            trade_date: 交易日期
            sector_type: 板块类型 (all/concept/industry)
        """
        # 默认获取最新数据（自动判断日期）
        if trade_date is None:
            from data.storage_manager import StorageManager
            manager = StorageManager()
            latest = manager.get_latest_date()

            if latest:
                # 从最新日期的下一个交易日开始
                latest_dt = datetime.strptime(latest, "%Y%m%d")
                start_date = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
                logger.info(f"检测到最新数据为 {latest}，将从 {start_date} 开始更新")
            else:
                # 没有历史数据，从 30 天前开始
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
                logger.info(f"未检测到历史数据，从 {start_date} 开始采集")

            # 更新到昨天
            trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            logger.info(f"目标日期：{trade_date}")
        else:
            start_date = trade_date

        results = {"trade_date": trade_date}

        # 采集列表数据
        list_results = self._collect_lists()
        results.update(list_results)

        # 使用高速采集器更新数据
        logger.info(f"开始更新数据：{start_date} - {trade_date} (板块类型：{sector_type})")
        try:
            from data.fast_collector import HighSpeedDataCollector
            from core.settings import settings as core_settings

            collector = HighSpeedDataCollector(
                token=core_settings.tushare_token,
                max_workers=10
            )

            # 获取板块列表（根据类型筛选）
            indices = collector.client.get_ths_indices(exclude_bse=True)

            # 根据 sector_type 筛选板块
            if sector_type == "concept":
                indices = indices[indices['ts_code'].str.startswith('885', na=False)]
                logger.info(f"筛选概念板块：{len(indices)} 个")
            elif sector_type == "industry":
                indices = indices[indices['ts_code'].str.startswith('881', na=False)]
                logger.info(f"筛选行业板块：{len(indices)} 个")
            else:
                # all - 包括 881 行业、882 地区、885 概念（已排除 87 北交所）
                indices = indices[
                    indices['ts_code'].str.startswith(('881', '882', '885'), na=False)
                ]
                logger.info(f"全部板块（行业 + 地区 + 概念）：{len(indices)} 个")

            codes = indices['ts_code'].tolist()

            # 批量下载（直接写入数据库）
            collector.download_batch(codes, start_date, trade_date)

            results["status"] = "success"
            results["downloaded"] = collector.downloaded_count
            results["skipped"] = collector.skipped_count
            results["failed"] = collector.failed_count

            # 自动触发数据导出
            logger.info("正在导出数据库数据到 CSV...")
            self._organize_data()

        except Exception as e:
            logger.error(f"更新失败：{e}")
            results["status"] = "error"
            results["error"] = str(e)

        logger.info(f"每日数据采集完成：{results}")
        return results

    def _organize_data(self):
        """导出数据（从数据库导出 CSV）"""
        from data.data_organizer import DataOrganizer

        organizer = DataOrganizer()

        # 从数据库导出合并数据
        merged = organizer.merge_all_data()

        if len(merged) > 0:
            logger.info(f"数据导出完成：{len(merged):,} 条记录")
        else:
            logger.warning("数据导出后为空")

    def _collect_history(
        self,
        start_date: str,
        end_date: str,
        data_types: Optional[List[str]] = None
    ) -> Dict:
        """采集历史数据 - 只使用同花顺"""
        if data_types is None:
            data_types = ["ths_industry"]

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
            "ths_indices.csv",
            "ths_industries_l1.csv",
            "ths_industries_l2.csv"
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
        for name in ["ths_indices", "ths_industries_l1", "ths_industries_l2"]:
            filepath = os.path.join(settings.raw_data_dir, f"{name}.csv")
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                summary["lists"][name] = len(df)
                summary["total_records"] += len(df)

        # 统计历史文件 - 同花顺格式 ths_881xxx_TI.csv
        import glob
        history_files = glob.glob(os.path.join(settings.raw_data_dir, "ths_*.csv"))
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
