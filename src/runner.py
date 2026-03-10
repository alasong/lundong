"""
简化运行器
替代原有的 Coordinator 层，直接编排数据流
"""
import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from loguru import logger
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings, ensure_directories
from agents.data_agent import DataAgent
from agents.analysis_agent import AnalysisAgent
from agents.predict_agent import PredictAgent


class SimpleRunner:
    """简化运行器 - 直接编排数据流"""

    def __init__(self):
        ensure_directories()
        self.data_agent = DataAgent()
        self.analysis_agent = AnalysisAgent()
        self.predict_agent = PredictAgent()

    def run_daily(self, date: Optional[str] = None, train: bool = False) -> Dict[str, Any]:
        """
        每日运行流程

        Args:
            date: 日期，默认昨天
            train: 是否训练模型
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        logger.info(f"=== 开始每日运行：{date} ===")
        results = {"date": date, "success": True}

        # Step 1: 数据采集
        logger.info("Step 1: 数据采集")
        data_result = self.data_agent.execute(task="daily", start_date=date)
        results["data"] = data_result

        if not data_result["success"]:
            logger.error("数据采集失败")
            results["success"] = False
            return results

        # Step 2: 热点和轮动分析
        logger.info("Step 2: 热点和轮动分析")
        analysis_result = self.analysis_agent.execute(task="all")
        results["analysis"] = analysis_result

        # Step 3: 模型训练（可选）
        if train:
            logger.info("Step 3: 训练模型")
            train_result = self.predict_agent.execute(task="train", horizon="all")
            results["training"] = train_result

        # Step 4: 预测
        logger.info("Step 4: 预测")
        predict_result = self.predict_agent.execute(task="predict", horizon="all")
        results["prediction"] = predict_result

        # Step 5: 生成简单报告
        logger.info("Step 5: 生成报告")
        report = self._generate_simple_report(results)
        results["report"] = report

        logger.info("=== 每日运行完成 ===")
        return results

    def quick_analysis(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        快速分析（不采集新数据，使用已有数据）

        Args:
            date: 日期
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        logger.info(f"=== 快速分析：{date} ===")
        results = {"date": date}

        # 分析
        analysis_result = self.analysis_agent.execute(task="all")
        results["analysis"] = analysis_result

        # 预测
        predict_result = self.predict_agent.execute(task="predict", horizon="all")
        results["prediction"] = predict_result

        # 报告
        report = self._generate_simple_report(results)
        results["report"] = report

        return results

    def _generate_simple_report(self, results: Dict) -> Dict:
        """生成简单报告（替代 LLM 解释）"""
        report = {
            "date": results.get("date"),
            "generated_at": datetime.now().isoformat(),
            "hotspots": [],
            "predictions": [],
            "summary": ""
        }

        # 提取热点
        analysis = results.get("analysis", {})
        if analysis.get("success"):
            result = analysis.get("result", {})
            hotspots = result.get("hotspot", {}).get("top_hotspots", [])
            report["hotspots"] = hotspots[:10]  # 取前 10

        # 提取预测
        prediction = results.get("prediction", {})
        if prediction.get("success"):
            result = prediction.get("result", {})
            combined = result.get("combined", {})
            report["predictions"] = combined.get("combined_ranking", [])[:20]  # 取前 20

        # 简单总结
        if report["hotspots"]:
            top_hotspot = report["hotspots"][0].get("concept_name", "")
            report["summary"] = f"当日最强热点：{top_hotspot}"
        else:
            report["summary"] = "未识别到明显热点"

        return report

    def save_results(self, results: Dict, filename: str):
        """保存结果"""
        results_dir = os.path.join(settings.data_dir, "results")
        os.makedirs(results_dir, exist_ok=True)

        filepath = os.path.join(results_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"结果已保存：{filepath}")
        return filepath


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="A 股热点轮动预测系统")
    parser.add_argument("--mode", choices=["daily", "quick", "train"], default="daily",
                        help="运行模式：daily(每日), quick(快速), train(训练)")
    parser.add_argument("--date", type=str, help="指定日期 YYYYMMDD")
    parser.add_argument("--train", action="store_true", help="是否训练模型")

    args = parser.parse_args()

    runner = SimpleRunner()

    if args.mode == "daily":
        results = runner.run_daily(date=args.date, train=args.train)
        runner.save_results(results, f"daily_{results['date']}.json")

    elif args.mode == "quick":
        results = runner.quick_analysis(date=args.date)
        print_report(results["report"])

    elif args.mode == "train":
        logger.info("训练模型")
        train_result = runner.predict_agent.execute(task="train", horizon="all")
        print(f"训练结果：{train_result}")


def print_report(report: Dict):
    """打印简单报告"""
    print("\n" + "=" * 60)
    print(f"日期：{report.get('date')}")
    print(f"生成时间：{report.get('generated_at')}")
    print("=" * 60)

    print("\n【热点板块 TOP5】")
    for i, hs in enumerate(report.get("hotspots", [])[:5], 1):
        print(f"{i}. {hs.get('concept_name', 'N/A')} - 评分：{hs.get('total_score', 0):.1f}")

    print("\n【预测 TOP5】")
    for i, pred in enumerate(report.get("predictions", [])[:5], 1):
        print(f"{i}. {pred.get('concept_code', 'N/A')} - 综合得分：{pred.get('combined_score', 0):.2f}")

    print(f"\n【总结】{report.get('summary')}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
