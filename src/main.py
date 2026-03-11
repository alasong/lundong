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
from data.name_mapper import load_name_mapping, get_block_name


def setup_logging():
    """配置日志"""
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )


def _get_block_info(name: str, code: str) -> dict:
    """
    获取板块信息（名称和说明）

    Args:
        name: 板块名称
        code: 板块代码

    Returns:
        {'name': 板块名称，'desc': 板块说明/类型}
    """
    # 如果名称已经是中文（不是"板块_xxx"格式），直接使用
    if name and not name.startswith("板块_"):
        # 根据代码判断板块类型
        block_type = _get_block_type(code)
        return {'name': name, 'desc': block_type}

    # 使用名称映射获取真实名称
    mapping = load_name_mapping()
    real_name = get_block_name(code, mapping)

    # 如果还是板块_xxx 格式，说明没有映射
    if real_name.startswith("板块_"):
        return {'name': real_name, 'desc': ''}

    block_type = _get_block_type(code)
    return {'name': real_name, 'desc': block_type}


def _get_block_type(code: str) -> str:
    """
    根据代码判断板块类型

    同花顺代码规则:
    - 881xxx: 行业板块
    - 882xxx: 地区板块
    - 885xxx: 概念板块
    - 700xxx: 风格指数

    Args:
        code: 板块代码 (如 885311.TI)

    Returns:
        板块类型说明
    """
    if not code:
        return ''

    # 提取数字部分
    code_num = code.replace('.TI', '').replace('.', '')
    try:
        prefix = code_num[:3]
        if prefix == '881':
            return '行业'
        elif prefix == '882':
            return '地区'
        elif prefix == '885':
            return '概念'
        elif prefix == '700':
            return '风格'
        else:
            return ''
    except:
        return ''


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="A 股热点轮动预测系统")
    parser.add_argument(
        "--mode",
        choices=["daily", "quick", "train", "predict", "data", "history"],
        default="daily",
        help="运行模式：daily(每日), quick(快速), train(训练), predict(预测), data(采集), history(历史)"
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

        elif args.mode == "predict":
            # 仅预测（使用已有模型）
            logger.info("执行预测（使用已有模型）")
            result = runner.predict_agent.execute(task="predict", horizon="all")

            # 格式化输出预测结果
            # execute 返回：{"success": True, "agent": "...", "result": {"success": True, "result": {...}}}
            if result.get("success") and result.get("result"):
                # 第一层 result 是 execute 包装的，里面的 result 是_predict 返回的
                prediction_result = result["result"].get("result", {})

                if isinstance(prediction_result, dict):
                    top_predictions = prediction_result.get("top_10", [])
                else:
                    top_predictions = []

                print("\n" + "=" * 70)
                print("A 股热点轮动预测结果")
                print("=" * 70)

                if top_predictions:
                    print("\n【预测 TOP10】")
                    print("-" * 100)
                    print(f"{'排名':<6}{'板块名称':<25}{'板块说明':<15}{'综合得分':<12}{'1 日':<8}{'5 日':<8}{'20 日':<8}")
                    print("-" * 100)
                    for i, pred in enumerate(top_predictions, 1):
                        # 获取板块名称
                        name = pred.get('concept_name', pred.get('name', pred.get('concept_code', 'N/A')))
                        code = pred.get('concept_code', '')

                        # 提取板块说明（从名称中分离行业/概念属性）
                        block_info = _get_block_info(name, code)
                        block_name = block_info['name']
                        block_desc = block_info['desc']

                        combined = pred.get('combined_score', 0)
                        p1d = pred.get('pred_1d', 0)
                        p5d = pred.get('pred_5d', 0)
                        p20d = pred.get('pred_20d', 0)

                        # 标记
                        if i <= 3:
                            marker = "⭐"
                        elif i <= 6:
                            marker = "📈"
                        else:
                            marker = "📊"

                        print(f"{i:<6}{block_name:<25}{block_desc:<15}{combined:<12.2f}{p1d:<8.2f}{p5d:<8.2f}{p20d:<8.2f} {marker}")
                    print("-" * 100)

                    # 策略建议
                    print("\n【轮动策略】")
                    top3_names = [p.get('concept_name', p.get('name', p.get('concept_code', 'N/A'))) for p in top_predictions[:3]]
                    print(f"重点关注：{', '.join(top3_names)}")

                    avg_score = sum(p.get('combined_score', 0) for p in top_predictions[:3]) / 3
                    if avg_score > 5:
                        print("市场判断：多头行情，建议积极介入")
                    elif avg_score > 0:
                        print("市场判断：震荡行情，建议逢低布局")
                    else:
                        print("市场判断：空头行情，建议控制仓位")
                else:
                    print("暂无预测数据，可能需要先训练模型")
                    print("运行：python src/main.py --mode train")

                print("=" * 70 + "\n")
            else:
                logger.error(f"预测失败：{result.get('error', '未知错误')}")

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
