"""
A 股热点轮动预测系统 - 简化版主入口
"""
import argparse
from datetime import datetime, timedelta
from loguru import logger
import sys
import os
import pandas as pd

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
        choices=["daily", "quick", "train", "predict", "data", "history", "importance", "backtest", "cv", "list", "dedup"],
        default="daily",
        help="运行模式：daily(每日), quick(快速), train(训练), predict(预测), data(采集), history(历史), importance(特征重要性), backtest(回测), cv(交叉验证), list(查看数据), dedup(数据去重)"
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
                    # 检查是否有置信度数据
                    has_confidence = any('confidence' in p for p in top_predictions)

                    print("\n【预测 TOP10】")
                    print("-" * 100)
                    if has_confidence:
                        print(f"{'排名':<6}{'板块名称':<20}{'板块说明':<10}{'综合得分':<10}{'1 日':<8}{'5 日':<8}{'20 日':<8}{'置信度':<10}")
                        print("-" * 100)
                    else:
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

                        if has_confidence:
                            conf = pred.get('confidence', 0)
                            conf_level = pred.get('confidence_level', '')
                            print(f"{i:<6}{block_name:<20}{block_desc:<10}{combined:<10.2f}"
                                  f"{p1d:<8.2f}{p5d:<8.2f}{p20d:<8.2f}{conf:<10.3f} {conf_level} {marker}")
                        else:
                            print(f"{i:<6}{block_name:<25}{block_desc:<15}{combined:<12.2f}"
                                  f"{p1d:<8.2f}{p5d:<8.2f}{p20d:<8.2f} {marker}")

                    print("-" * 100)

                    # 置信度统计
                    if has_confidence:
                        conf_count = sum(1 for p in top_predictions if p.get('confidence_level') == '高')
                        print(f"\n高置信度预测：{conf_count}/{len(top_predictions)} 个")

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

        elif args.mode == "importance":
            # 显示特征重要性
            logger.info("加载特征重要性分析")
            from models.predictor import UnifiedPredictor
            predictor = UnifiedPredictor()
            predictor.print_feature_importance(top_n=20)

        elif args.mode == "list":
            # 查看已采集的数据
            logger.info("查看已采集的数据")
            from data.name_mapper import load_name_mapping, get_block_name
            raw_dir = settings.raw_data_dir

            if not os.path.exists(raw_dir):
                logger.warning("数据目录不存在")
                return

            # 统计文件
            ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]
            other_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv") and not f.endswith("_TI.csv")]

            print("\n" + "=" * 70)
            print("已采集的数据概览")
            print("=" * 70)

            # 同花顺数据
            if ths_files:
                print(f"\n【同花顺板块数据】{len(ths_files)} 个文件")
                print("-" * 70)
                print(f"{'代码':<15}{'板块名称':<25}{'记录数':<12}{'日期范围':<30}")
                print("-" * 70)

                # 加载名称映射
                name_mapping = load_name_mapping()

                # 统计每个文件
                file_stats = []
                import pandas as pd_local
                for filepath in sorted(ths_files)[:50]:  # 只显示前 50 个
                    try:
                        df = pd_local.read_csv(os.path.join(raw_dir, filepath), nrows=1)
                        if 'ts_code' in df.columns:
                            code = df['ts_code'].iloc[0]
                        else:
                            code = filepath.replace('ths_', '').replace('_TI.csv', '')

                        # 读取完整文件获取记录数和日期范围
                        full_df = pd_local.read_csv(os.path.join(raw_dir, filepath))
                        record_count = len(full_df)

                        if 'trade_date' in full_df.columns:
                            date_min = str(full_df['trade_date'].min())
                            date_max = str(full_df['trade_date'].max())
                            date_range = f"{date_min} - {date_max}"
                        else:
                            date_range = "N/A"

                        # 获取板块名称
                        block_name = get_block_name(code, name_mapping)

                        file_stats.append({
                            'code': code,
                            'name': block_name,
                            'records': record_count,
                            'date_range': date_range
                        })
                    except Exception as e:
                        logger.warning(f"读取文件 {filepath} 失败：{e}")

                # 按代码排序并显示
                file_stats.sort(key=lambda x: x['code'])
                for stat in file_stats:
                    print(f"{stat['code']:<15}{stat['name']:<25}{stat['records']:<12}{stat['date_range']:<30}")

                if len(ths_files) > 50:
                    print(f"... 还有 {len(ths_files) - 50} 个文件未显示")
                print("-" * 70)

                # 统计信息
                total_records = sum(s['records'] for s in file_stats)
                print(f"总计：{len(ths_files)} 个文件，{total_records:,} 条记录")
            else:
                print("\n未找到同花顺板块数据")

            # 其他数据
            if other_files:
                print(f"\n【其他数据】{len(other_files)} 个文件")
                for f in other_files:
                    print(f"  - {f}")
            else:
                print("\n无其他数据文件")

            print("=" * 70 + "\n")

        elif args.mode == "dedup":
            # 数据去重
            logger.info("执行数据去重")
            import pandas as pd_local
            raw_dir = settings.raw_data_dir

            if not os.path.exists(raw_dir):
                logger.warning("数据目录不存在")
                return

            # 处理同花顺数据
            ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

            if not ths_files:
                logger.info("未找到需要去重的文件")
                return

            total_removed = 0
            files_processed = 0

            print("\n" + "=" * 70)
            print("数据去重结果")
            print("=" * 70)

            for filepath in sorted(ths_files):
                full_path = os.path.join(raw_dir, filepath)
                try:
                    df = pd_local.read_csv(full_path)
                    original_count = len(df)

                    # 按 trade_date 去重，保留第一条
                    if 'trade_date' in df.columns:
                        df_dedup = df.drop_duplicates(subset=['trade_date'], keep='first')
                    else:
                        df_dedup = df

                    dedup_count = len(df_dedup)
                    removed = original_count - dedup_count

                    if removed > 0:
                        # 保存去重后的数据
                        df_dedup.to_csv(full_path, index=False)
                        total_removed += removed
                        files_processed += 1
                        print(f"{filepath}: 移除 {removed} 条重复记录 ({original_count} -> {dedup_count})")

                except Exception as e:
                    logger.warning(f"处理文件 {filepath} 失败：{e}")

            print("-" * 70)
            print(f"完成：处理 {files_processed} 个文件，共移除 {total_removed} 条重复记录")
            print("=" * 70 + "\n")

        elif args.mode == "backtest":
            # 回测验证
            logger.info("执行回测验证")
            from evaluation.backtester import Backtester
            import pandas as pd

            # 直接从 CSV 文件加载历史数据
            raw_dir = settings.raw_data_dir

            # 加载所有同花顺行业历史数据（ths_*_TI.csv 格式）
            ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

            if not ths_files:
                logger.error("未找到同花顺历史数据文件，请先运行历史数据采集")
                logger.info("运行：python src/main.py --mode history --start-date 20230101 --end-date 20241231")
                return

            logger.info(f"发现 {len(ths_files)} 个历史数据文件")

            # 并行加载所有文件
            from joblib import Parallel, delayed

            def load_single_file(filepath):
                try:
                    df = pd.read_csv(filepath, dtype={
                        'concept_code': str,
                        'trade_date': str,
                        'pct_chg': float,
                        'vol': float
                    })
                    # 重命名字段
                    if 'pct_change' in df.columns:
                        df = df.rename(columns={'pct_change': 'pct_chg'})
                    if 'ts_code' in df.columns:
                        df = df.rename(columns={'ts_code': 'concept_code'})
                    return df
                except Exception as e:
                    logger.warning(f"加载文件 {filepath} 失败：{e}")
                    return None

            dfs = Parallel(n_jobs=-1, backend="threading")(
                delayed(load_single_file)(os.path.join(raw_dir, f))
                for f in ths_files
            )
            dfs = [df for df in dfs if df is not None]

            if not dfs:
                logger.error("无法加载任何数据文件")
                return

            concept_data = pd.concat(dfs, ignore_index=True)
            logger.info(f"加载了 {len(concept_data)} 条历史记录")

            # 应用日期筛选
            start_date = args.start_date or "20230101"
            end_date = args.end_date or "20241231"
            concept_data = concept_data[
                (concept_data["trade_date"] >= start_date) &
                (concept_data["trade_date"] <= end_date)
            ]
            logger.info(f"筛选后数据：{len(concept_data)} 条记录 ({start_date} - {end_date})")

            if len(concept_data) < 1000:
                logger.error("数据量不足，无法进行回测")
                return

            # 运行回测
            backtester = Backtester()

            # 解析回测参数
            train_windows = int(os.environ.get("BACKTEST_TRAIN", 12))
            test_windows = int(os.environ.get("BACKTEST_TEST", 3))
            step = int(os.environ.get("BACKTEST_STEP", 3))

            logger.info(f"回测参数：训练={train_windows}月，测试={test_windows}月，步长={step}月")

            results = backtester.run_walk_forward(
                concept_data,
                train_windows=train_windows,
                test_windows=test_windows,
                step=step
            )

            # 输出回测结果
            print("\n" + "=" * 70)
            print("回测结果汇总")
            print("=" * 70)

            if 'avg_metrics' in results:
                m = results['avg_metrics']
                print(f"回测折叠数：{m['folds']}")
                print(f"平均 IC: {m['avg_ic']:.4f}")
                print(f"平均 RankIC: {m['avg_rank_ic']:.4f}")
                print(f"平均 Sharpe: {m['avg_sharpe']:.2f}")
                print(f"平均收益率：{m['avg_return']:.2f}%")
                print(f"最大回撤：{m['max_drawdown']:.2%}")
                print(f"胜率：{m['win_rate']:.2%}")

                # 折叠详情
                if 'fold_results' in results and results['fold_results']:
                    print("\n【各折叠详情】")
                    print("-" * 70)
                    print(f"{'折叠':<6}{'IC':<10}{'RankIC':<10}{'Sharpe':<10}{'收益率':<12}{'最大回撤':<12}")
                    print("-" * 70)
                    for fold in results['fold_results']:
                        print(f"{fold.get('fold', 0):<6}{fold.get('ic', 0):<10.4f}"
                              f"{fold.get('rank_ic', 0):<10.4f}{fold.get('sharpe', 0):<10.2f}"
                              f"{fold.get('total_return', 0):<12.2f}%{fold.get('max_drawdown', 0):<12.2%}")
                    print("-" * 70)
            else:
                print(f"回测失败：{results.get('error', '未知错误')}")

            print("=" * 70 + "\n")

        elif args.mode == "cv":
            # 时序交叉验证（Purged K-Fold）
            logger.info("执行时序交叉验证（Purged K-Fold）")
            from evaluation.backtester import Backtester
            import pandas as pd

            # 直接从 CSV 文件加载历史数据
            raw_dir = settings.raw_data_dir
            ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

            if not ths_files:
                logger.error("未找到历史数据文件")
                return

            logger.info(f"发现 {len(ths_files)} 个历史数据文件")

            # 并行加载
            from joblib import Parallel, delayed

            def load_single_file(filepath):
                try:
                    df = pd.read_csv(filepath, dtype={
                        'concept_code': str,
                        'trade_date': str,
                        'pct_chg': float
                    })
                    # 字段重命名映射
                    if 'pct_change' in df.columns:
                        df = df.rename(columns={'pct_change': 'pct_chg'})
                    if 'ts_code' in df.columns:
                        df = df.rename(columns={'ts_code': 'concept_code'})
                    # 确保 concept_code 列存在
                    if 'concept_code' not in df.columns:
                        # 尝试从文件名提取
                        filename = os.path.basename(filepath)
                        if filename.startswith('ths_') and '_TI.csv' in filename:
                            code_part = filename.replace('ths_', '').replace('_TI.csv', '')
                            df['concept_code'] = code_part
                    return df
                except Exception as e:
                    logger.warning(f"加载文件 {filepath} 失败：{e}")
                    return None

            dfs = Parallel(n_jobs=-1, backend="threading")(
                delayed(load_single_file)(os.path.join(raw_dir, f))
                for f in ths_files
            )
            dfs = [df for df in dfs if df is not None]

            if not dfs:
                logger.error("无法加载任何数据文件")
                return

            concept_data = pd.concat(dfs, ignore_index=True)
            logger.info(f"加载了 {len(concept_data)} 条历史记录")

            # 应用日期筛选
            start_date = args.start_date or "20200101"
            end_date = args.end_date or "20251231"
            concept_data = concept_data[
                (concept_data["trade_date"] >= start_date) &
                (concept_data["trade_date"] <= end_date)
            ]
            logger.info(f"筛选后数据：{len(concept_data)} 条记录")

            # 运行交叉验证
            backtester = Backtester()

            # 解析参数
            n_splits = int(os.environ.get("CV_SPLITS", 5))
            train_months = int(os.environ.get("CV_TRAIN_MONTHS", 24))
            purge_days = int(os.environ.get("CV_PURGE", 5))
            embargo_days = int(os.environ.get("CV_EMBARGO", 2))

            logger.info(f"交叉验证参数：n_splits={n_splits}, train_window={train_months}月，"
                       f"purge={purge_days}天，embargo={embargo_days}天")

            results = backtester.run_purged_kfold(
                concept_data,
                n_splits=n_splits,
                train_window_months=train_months,
                purge_days=purge_days,
                embargo_days=embargo_days
            )

            # 输出结果
            print("\n" + "=" * 70)
            print("时序交叉验证结果（Purged K-Fold）")
            print("=" * 70)

            if 'avg_metrics' in results:
                m = results['avg_metrics']
                print(f"\n验证折叠数：{m['folds']}/{n_splits}")
                print(f"平均 IC: {m['avg_ic']:.4f} (±{m['ic_std']:.4f})")
                print(f"平均 RankIC: {m['avg_rank_ic']:.4f}")
                print(f"平均 Sharpe: {m['avg_sharpe']:.2f} (±{m['sharpe_std']:.2f})")
                print(f"平均收益率：{m['avg_return']:.2f}%")
                print(f"最大回撤：{m['max_drawdown']:.2%}")
                print(f"胜率：{m['win_rate']:.2%}")

                # 折叠详情
                if 'fold_results' in results and results['fold_results']:
                    print("\n【各折叠详情】")
                    print("-" * 90)
                    print(f"{'折叠':<6}{'时间范围':<25}{'IC':<10}{'RankIC':<10}{'Sharpe':<10}{'收益率':<12}")
                    print("-" * 90)
                    for fold in results['fold_results']:
                        time_range = f"{fold.get('test_start', 'N/A')} - {fold.get('test_end', 'N/A')}"
                        print(f"{fold.get('fold', 0):<6}{time_range:<25}"
                              f"{fold.get('ic', 0):<10.4f}{fold.get('rank_ic', 0):<10.4f}"
                              f"{fold.get('sharpe', 0):<10.2f}{fold.get('total_return', 0):<12.2f}%")
                    print("-" * 90)
            else:
                print(f"验证失败：{results.get('error', '未知错误')}")

            print("=" * 70 + "\n")

    except Exception as e:
        logger.error(f"执行失败：{e}")
        raise

    logger.info("执行完成")


if __name__ == "__main__":
    main()
