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
    """打印预测报告 - 优化版"""
    print("\n" + "=" * 70)
    print("A 股热点轮动预测系统 - 预测报告")
    print("=" * 70)
    print(f"日期：{report.get('date', 'N/A')}")
    print(f"生成时间：{report.get('generated_at', 'N/A')}")
    print("=" * 70)

    # 打印热点板块
    hotspots = report.get("hotspots", [])
    if hotspots:
        print("\n【热点板块 TOP10】")
        print("-" * 70)
        print(f"{'排名':<6}{'板块名称':<20}{'评分':<10}{'状态'}")
        print("-" * 70)
        for i, hs in enumerate(hotspots[:10], 1):
            name = hs.get('concept_name', hs.get('name', 'N/A'))
            score = hs.get('total_score', hs.get('hotspot_score', 0))
            # 判断状态
            if i <= 3:
                status = "🔥 热门"
            elif i <= 6:
                status = "📈 走强"
            else:
                status = "📊 关注"
            print(f"{i:<6}{name:<20}{score:<10.1f}{status}")
        print("-" * 70)

    # 打印预测结果
    predictions = report.get("predictions", [])
    if predictions:
        print("\n【热点轮动预测 TOP10】")
        print("-" * 70)
        print(f"{'排名':<6}{'板块名称':<20}{'综合得分':<10}{'1 日':<8}{'5 日':<8}{'20 日':<8}")
        print("-" * 70)
        for i, pred in enumerate(predictions[:10], 1):
            name = pred.get('concept_name', pred.get('name', 'N/A'))
            # 如果名称为空，使用 code
            if not name or name == 'N/A':
                name = pred.get('concept_code', 'N/A')
            combined = pred.get('combined_score', 0)
            p1d = pred.get('pred_1d', 0)
            p5d = pred.get('pred_5d', 0)
            p20d = pred.get('pred_20d', 0)

            # 根据综合得分标记
            if i <= 3:
                marker = "⭐"
            elif i <= 6:
                marker = "📈"
            else:
                marker = "📊"

            print(f"{i:<6}{name:<20}{combined:<10.2f}{p1d:<8.2f}{p5d:<8.2f}{p20d:<8.2f} {marker}")
        print("-" * 70)

    # 轮动建议
    print("\n【轮动策略建议】")
    print("-" * 70)
    if predictions:
        top3 = predictions[:3]
        print(f"重点关注：{', '.join([p.get('concept_name', p.get('concept_code', 'N/A')) for p in top3])}")

        # 判断市场趋势
        avg_score = sum(p.get('combined_score', 0) for p in top3) / 3 if top3 else 0
        if avg_score > 5:
            print("市场判断：多头行情，建议积极介入热点板块")
        elif avg_score > 0:
            print("市场判断：震荡行情，建议逢低布局轮动板块")
        else:
            print("市场判断：空头行情，建议控制仓位，等待机会")
    else:
        print("暂无预测数据，请先运行模型训练")

    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
