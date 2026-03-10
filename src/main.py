"""
A 股热点轮动预测系统 - 简化版主入口
"""
import argparse
from datetime import datetime, timedelta
from loguru import logger
import sys
import os

# 添加 src 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings, ensure_directories
from runner import SimpleRunner, print_report


def setup_logging():
    """配置日志"""
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="A 股热点轮动预测系统")
    parser.add_argument(
        "--mode",
        choices=["daily", "quick", "train", "data", "history"],
        default="daily",
        help="运行模式：daily(每日), quick(快速), train(训练), data(采集), history(历史)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="指定日期 YYYYMMDD"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="开始日期 YYYYMMDD (history 模式使用)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="结束日期 YYYYMMDD (history 模式使用)"
    )
    parser.add_argument(
        "--train",
        action="store_true",
        help="是否训练模型"
    )

    args = parser.parse_args()

    # 初始化
    setup_logging()
    ensure_directories()

    logger.info("=" * 50)
    logger.info("A 股热点轮动预测系统")
    logger.info("=" * 50)

    # 检查配置
    if not settings.tushare_token:
        logger.error("请设置 TUSHARE_TOKEN 环境变量或在.env 文件中配置")
        return

    # 创建运行器
    runner = SimpleRunner()

    try:
        if args.mode == "daily":
            # 每日工作流
            date = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            logger.info(f"执行每日工作流：{date}")
            results = runner.run_daily(date=date, train=args.train)
            runner.save_results(results, f"daily_{date}.json")
            print_report(results["report"])

        elif args.mode == "quick":
            # 快速分析
            date = args.date
            logger.info(f"执行快速分析：{date or '最新'}")
            results = runner.quick_analysis(date=date)
            print_report(results["report"])

        elif args.mode == "train":
            # 仅训练模型
            logger.info("训练模型")
            result = runner.predict_agent.execute(task="train", horizon="all")
            print(f"训练结果：{result}")

        elif args.mode == "data":
            # 数据采集
            from agents.data_agent import DataAgent
            data_agent = DataAgent()

            if args.date:
                logger.info(f"采集单日数据：{args.date}")
                result = data_agent.execute(task="daily", start_date=args.date)
            else:
                logger.info("采集基础数据")
                result = data_agent.execute(task="basic")
            print(f"数据采集结果：{result}")

        elif args.mode == "history":
            # 历史数据采集
            if not args.start_date or not args.end_date:
                logger.error("历史模式需要指定开始和结束日期")
                return
            logger.info(f"执行历史数据采集：{args.start_date} - {args.end_date}")
            from agents.data_agent import DataAgent
            data_agent = DataAgent()
            result = data_agent.execute(
                task="history",
                start_date=args.start_date,
                end_date=args.end_date
            )
            print(f"历史数据采集结果：{result}")

    except Exception as e:
        logger.error(f"执行失败：{e}")
        raise

    logger.info("执行完成")


if __name__ == "__main__":
    main()
