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
        choices=["daily", "quick", "train", "predict", "data", "history", "importance", "backtest", "cv", "list", "dedup", "fast", "organize", "storage", "sync", "portfolio", "full", "stock"],
        default="daily",
        help="运行模式：daily(每日), quick(快速), train(训练), predict(预测), data(采集), history(历史), importance(特征重要性), backtest(回测), cv(交叉验证), list(查看数据), dedup(数据去重), fast(高速采集), organize(数据整理), storage(存储管理), sync(同步数据), portfolio(组合构建), full(一键式：板块 + 个股预测), stock(个股数据采集)"
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
        "--sector-type",
        choices=["all", "concept", "industry", "region"],
        default="all",
        help="板块类型 (all/concept/industry/region) (data/fast 模式使用)"
    )
    parser.add_argument(
        "--train",
        action="store_true",
        help="是否训练模型"
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="组合持仓股票数量 (portfolio 模式使用)"
    )
    parser.add_argument(
        "--storage-action",
        choices=["verify", "cleanup", "stats"],
        default="verify",
        help="存储管理操作类型 (storage 模式使用)"
    )
    parser.add_argument(
        "--stock-type",
        choices=["all", "csi500", "gem", "star"],
        default="all",
        help="个股采集类型 (stock 模式使用): all(全部), csi500(中证500), gem(创业板), star(科创板)"
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="使用优化训练（超参数调优 + Stacking 集成）(train 模式使用)"
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=30,
        help="超参数调优试验次数 (train --optimize 模式使用)"
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
            # 训练模型
            if args.optimize:
                # 使用优化训练（超参数调优 + Stacking）
                logger.info("使用优化训练（超参数调优 + Stacking 集成）")
                from models.predictor import UnifiedPredictor
                from data.database import get_database

                db = get_database()
                data = db.get_all_concept_data()

                if data is not None and not data.empty:
                    predictor = UnifiedPredictor(use_enhanced_features=True)
                    features = predictor.prepare_features(data, n_jobs=8)

                    if not features.empty:
                        result = predictor.train_with_optimization(
                            features,
                            use_tuning=True,
                            use_stacking=True,
                            n_trials=args.n_trials
                        )
                        print(f"\n优化训练完成：")
                        print(f"  特征数: {len(result.get('feature_cols', []))}")
                        for horizon, metrics in result.get('metrics', {}).items():
                            print(f"  {horizon}: MSE={metrics.get('mse', 0):.4f}, R2={metrics.get('r2', 0):.4f}")
                    else:
                        print("特征准备失败")
                else:
                    print("无法获取训练数据")
            else:
                # 标准训练
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
                        p1d = pred.get('pred_1d_pct', pred.get('pred_1d', 0))
                        p5d = pred.get('pred_5d_pct', pred.get('pred_5d', 0))
                        p20d = pred.get('pred_20d_pct', pred.get('pred_20d', 0))

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
            # 数据采集 - 默认更新到最新数据
            from agents.data_agent import DataAgent
            data_agent = DataAgent()

            if args.date:
                logger.info(f"采集单日数据：{args.date} (板块类型：{args.sector_type})")
                result = data_agent.execute(task="daily", start_date=args.date, sector_type=args.sector_type)
            else:
                # 默认更新到最新数据
                logger.info(f"采集最新数据（自动判断日期，板块类型：{args.sector_type}）")
                result = data_agent.execute(task="daily", sector_type=args.sector_type)
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
            from data.storage_manager import StorageManager
            import pandas as pd_local

            # 优先从合并文件读取
            manager = StorageManager()
            df = manager.load_merged_data()

            print("\n" + "=" * 70)
            print("已采集的数据概览")
            print("=" * 70)

            if not df.empty:
                # 从合并文件统计
                print(f"\n【合并数据】{manager.merged_file}")
                print("-" * 70)
                print(f"总记录数：{len(df):,}")
                print(f"板块数量：{df['ts_code'].nunique()}")
                print(f"日期范围：{df['trade_date'].min()} - {df['trade_date'].max()}")
                print("-" * 70)

                # 显示各板块统计
                print("\n【板块详情】（按代码排序）")
                print("-" * 90)
                print(f"{'代码':<15}{'板块名称':<25}{'记录数':<12}{'日期范围':<30}")
                print("-" * 90)

                name_mapping = load_name_mapping()
                grouped = df.groupby('ts_code')

                stats = []
                for code, group in grouped:
                    block_name = get_block_name(code, name_mapping)
                    date_min = str(group['trade_date'].min())
                    date_max = str(group['trade_date'].max())
                    stats.append({
                        'code': code,
                        'name': block_name,
                        'records': len(group),
                        'date_range': f"{date_min} - {date_max}"
                    })

                # 排序并显示前 50 个
                stats.sort(key=lambda x: x['code'])
                for stat in stats[:50]:
                    print(f"{stat['code']:<15}{stat['name']:<25}{stat['records']:<12}{stat['date_range']:<30}")

                if len(stats) > 50:
                    print(f"... 还有 {len(stats) - 50} 个板块未显示")

                print("-" * 90)
                print(f"总计：{len(stats)} 个板块")
            else:
                print("\n未找到合并数据，请运行：python src/main.py --mode organize")

                # 回退到读取原始文件
                raw_dir = settings.raw_data_dir
                if os.path.exists(raw_dir):
                    other_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
                    if other_files:
                        print(f"\n【原始文件】{len(other_files)} 个")
                        for f in other_files:
                            print(f"  - {f}")

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

        elif args.mode == "fast":
            # 高速并发采集（默认 8 线程，避免触发 API 限流）
            logger.info("执行高速并发采集（8 线程，智能限流）")
            from data.fast_collector import HighSpeedDataCollector

            if not settings.tushare_token:
                logger.error("请设置 TUSHARE_TOKEN")
                return

            start_date = args.start_date or "20200101"
            end_date = args.end_date or datetime.now().strftime("%Y%m%d")

            collector = HighSpeedDataCollector(
                token=settings.tushare_token,
                max_workers=8,   # 8 线程并发（避免触发 500 次/分钟限流）
                api_limit=450    # API 每分钟限制 450 次（预留缓冲）
            )

            # 下载所有板块历史数据
            # sector_type: all=全部板块，concept=概念板块，industry=行业板块，region=地区板块
            collector.download_all_history(
                start_date,
                end_date,
                concurrent=True,
                sector_type=args.sector_type or "all"
            )

        elif args.mode == "organize":
            # 数据整理
            logger.info("执行数据整理")
            from data.data_organizer import DataOrganizer

            organizer = DataOrganizer()
            organizer.organize_directory()

        elif args.mode == "storage":
            # 存储管理
            logger.info("执行存储管理")
            from data.storage_manager import StorageManager

            manager = StorageManager()

            if args.storage_action == "cleanup":
                print("\n" + "=" * 70)
                print("存储清理")
                print("=" * 70)
                print("警告：此操作将删除所有单板块文件，只保留合并文件")
                print("这将释放磁盘空间，但单个板块数据将需要从合并文件中读取")
                print("\n合并文件位置：", manager.merged_file)
                print("将删除的文件数：", len([f for f in os.listdir(settings.raw_data_dir) if f.startswith('ths_') and ('.TI.csv' in f or f.endswith('_TI.csv'))]))
                print("\n是否继续？(Y/n): ", end="")
                # 非确认模式（脚本调用时自动确认）
                if os.environ.get("AUTO_CONFIRM") == "1":
                    print("Y (自动确认)")
                    confirm = "y"
                else:
                    confirm = input().strip().lower()

                if confirm in ("y", ""):
                    deleted = manager.cleanup_raw_files(keep_history=False)
                    print(f"\n清理完成：删除 {deleted} 个文件")
                else:
                    print("已取消")
            else:
                result = manager.verify_data_integrity()

                print("\n" + "=" * 70)
                print("存储状态验证")
                print("=" * 70)
                print(f"状态：{result['status']}")
                print(f"总记录数：{result['total_records']:,}")
                print(f"板块数量：{result['unique_codes']}")
                print(f"日期范围：{result['date_range'][0]} - {result['date_range'][1]}")
                if result.get('duplicates', 0) > 0:
                    print(f"重复记录：{result['duplicates']:,}")
                if result.get('null_fields'):
                    print(f"空值字段：{result['null_fields']}")
                print("=" * 70)

                # 统计原始文件
                raw_files = [f for f in os.listdir(settings.raw_data_dir) if f.startswith('ths_') and ('.TI.csv' in f or f.endswith('_TI.csv'))]
                history_files = [f for f in os.listdir(settings.raw_data_dir) if 'all_history' in f]
                print(f"\n原始目录：{len(raw_files)} 个单板块文件，{len(history_files)} 个合集文件")
                print(f"合并文件：{manager.merged_file}")
                print("=" * 70 + "\n")

        elif args.mode == "sync":
            # 数据同步 - 将 raw 目录零散数据同步到合并文件
            logger.info("执行数据同步")
            from data.storage_manager import StorageManager

            manager = StorageManager()

            print("\n" + "=" * 70)
            print("数据同步")
            print("=" * 70)

            # 显示同步前状态
            raw_files = [f for f in os.listdir(settings.raw_data_dir) if f.startswith('ths_') and ('.TI.csv' in f or f.endswith('_TI.csv'))]
            history_files = [f for f in os.listdir(settings.raw_data_dir) if 'all_history' in f]
            print(f"\n同步前：{len(raw_files)} 个单板块文件，{len(history_files)} 个合集文件")

            # 执行同步
            new_total, total = manager.sync_from_raw()

            print(f"\n同步后：合并文件包含 {new_total:,} 条记录")
            print("=" * 70 + "\n")

        elif args.mode == "backtest":
            # 回测验证
            logger.info("执行回测验证")
            from evaluation.backtester import Backtester
            from data.storage_manager import StorageManager

            # 从合并文件加载历史数据
            manager = StorageManager()
            concept_data = manager.load_merged_data()

            if concept_data.empty:
                logger.error("未找到合并数据文件，请先运行数据整理")
                logger.info("运行：python src/main.py --mode organize")
                return

            logger.info(f"加载了 {len(concept_data)} 条历史记录")

            # 字段重命名（合并文件使用 ts_code/pct_change，回测使用 concept_code/pct_chg）
            if 'ts_code' in concept_data.columns and 'concept_code' not in concept_data.columns:
                concept_data = concept_data.rename(columns={'ts_code': 'concept_code'})
            if 'pct_change' in concept_data.columns and 'pct_chg' not in concept_data.columns:
                concept_data = concept_data.rename(columns={'pct_change': 'pct_chg'})

            # 应用日期筛选
            start_date = args.start_date or "20230101"
            end_date = args.end_date or "20241231"
            concept_data = concept_data[
                (concept_data["trade_date"] >= int(start_date)) &
                (concept_data["trade_date"] <= int(end_date))
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
            from data.storage_manager import StorageManager

            # 从合并文件加载历史数据
            manager = StorageManager()
            concept_data = manager.load_merged_data()

            if concept_data.empty:
                logger.error("未找到合并数据文件，请先运行数据整理")
                logger.info("运行：python src/main.py --mode organize")
                return

            logger.info(f"加载了 {len(concept_data)} 条历史记录")

            # 字段重命名（合并文件使用 ts_code/pct_change，回测使用 concept_code/pct_chg）
            if 'ts_code' in concept_data.columns and 'concept_code' not in concept_data.columns:
                concept_data = concept_data.rename(columns={'ts_code': 'concept_code'})
            if 'pct_change' in concept_data.columns and 'pct_chg' not in concept_data.columns:
                concept_data = concept_data.rename(columns={'pct_change': 'pct_chg'})

            # 应用日期筛选
            start_date = args.start_date or "20200101"
            end_date = args.end_date or "20251231"
            concept_data = concept_data[
                (concept_data["trade_date"] >= int(start_date)) &
                (concept_data["trade_date"] <= int(end_date))
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

        elif args.mode == "portfolio":
            # 组合构建
            logger.info("执行组合构建...")
            from agents.portfolio_agent import PortfolioAgent

            agent = PortfolioAgent()

            # 获取板块预测
            logger.info("Step 1: 获取板块预测...")
            predict_result = runner.predict_agent.execute(task="predict", horizon="all")

            concept_predictions = None
            concept_codes = []

            if predict_result.get("success") and predict_result.get("result"):
                prediction_data = predict_result["result"].get("result", {})
                if isinstance(prediction_data, dict):
                    all_predictions = prediction_data.get("predictions", [])
                    if all_predictions:
                        # 提取板块代码和预测
                        concept_predictions = pd.DataFrame(all_predictions)
                        concept_codes = concept_predictions['concept_code'].tolist()
                        logger.info(f"获取到 {len(concept_codes)} 个板块预测")

            if not concept_codes:
                logger.error("无法获取板块预测，请先训练模型")
                return

            # 过滤：只保留有成分股数据的板块
            # 直接查询数据库中有成分股的板块
            from data.database import get_database
            db = get_database()

            # 查询所有有成分股的板块
            import sqlite3
            conn = sqlite3.connect('data/stock.db')
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT concept_code FROM concept_constituent')
            available_concepts = [row[0] for row in cursor.fetchall()]
            conn.close()

            logger.info(f"数据库中有成分股的板块：{available_concepts}")

            # 找出同时在预测结果和成分股中的板块
            concept_codes_set = set(concept_predictions['concept_code'].unique().tolist())
            available_set = set(available_concepts)
            valid_concepts = list(concept_codes_set & available_set)

            logger.info(f"预测结果中有成分股的板块：{valid_concepts}")

            if valid_concepts:
                # 过滤预测 DataFrame
                concept_predictions = concept_predictions[
                    concept_predictions['concept_code'].isin(valid_concepts)
                ]
                concept_codes = valid_concepts
            else:
                logger.warning("预测结果中的板块都没有成分股数据")
                logger.info("请使用以下板块测试：{available_concepts}")
                # 使用有成分股的板块来测试
                concept_predictions = concept_predictions[
                    concept_predictions['concept_code'].isin(available_concepts[:3])
                ]
                concept_codes = available_concepts[:3]

            # 构建组合
            logger.info("Step 2: 构建投资组合...")
            result = agent.run(
                task="build",
                concept_codes=concept_codes,
                concept_predictions=concept_predictions,
                top_n_stocks=args.top_n
            )

            # 输出结果
            print("\n" + "=" * 70)
            print("投资组合构建结果")
            print("=" * 70)

            if result.get("success"):
                portfolio = result.get("portfolio", [])
                metrics = result.get("metrics", {})

                print(f"\n持仓数量：{len(portfolio)} 只股票")

                print("\n【持仓明细】")
                print("-" * 100)
                print(f"{'代码':<12}{'名称':<15}{'权重':<12}{'所属板块':<20}{'1 日预测':<12}{'5 日预测':<12}")
                print("-" * 100)
                for pos in portfolio:
                    print(f"{pos['ts_code']:<12}{pos['stock_name']:<15}{pos['weight']:>10.1%}  {pos['concept_name']:<20}{pos.get('pred_1d', 0):>10.2f}%{pos.get('pred_5d', 0):>10.2f}%")
                print("-" * 100)

                print("\n【预期指标】")
                print(f"  预期年化收益：{metrics.get('expected_return', 0):.1%}")
                print(f"  预期年化波动率：{metrics.get('expected_volatility', 0):.1%}")
                print(f"  夏普比率：{metrics.get('sharpe', 0):.2f}")
                print(f"  最大回撤估计：{metrics.get('max_drawdown', 0):.1%}")

                # 风险分析
                risk = result.get("risk_analysis", {})
                print("\n【风险分析】")
                print(f"  板块集中度：{risk.get('sector_concentration', 0):.1%}")
                print(f"  平均相关性：{risk.get('avg_correlation', 0):.3f}")
            else:
                print(f"组合构建失败：{result.get('error', '未知错误')}")

            print("=" * 70 + "\n")

        elif args.mode == "full":
            # 一键式：热点板块预测 + 个股预测 + 组合构建
            logger.info("执行一键式预测（热点板块 + 个股 + 组合）...")
            from agents.portfolio_agent import PortfolioAgent

            print("\n" + "=" * 70)
            print("A 股热点轮动 - 一键式预测")
            print("=" * 70)

            agent = PortfolioAgent()

            # Step 1: 板块预测
            print("\n【Step 1/3】预测热点板块...")
            predict_result = runner.predict_agent.execute(task="predict", horizon="all")

            concept_predictions = None
            concept_codes = []

            if predict_result.get("success") and predict_result.get("result"):
                prediction_data = predict_result["result"].get("result", {})
                if isinstance(prediction_data, dict):
                    all_predictions = prediction_data.get("predictions", [])
                    if all_predictions:
                        concept_predictions = pd.DataFrame(all_predictions)
                        concept_codes = concept_predictions['concept_code'].unique().tolist()
                        logger.info(f"获取到 {len(concept_codes)} 个板块预测")

            # 输出板块预测结果
            if concept_predictions is not None and not concept_predictions.empty:
                top_concepts = concept_predictions.nlargest(10, 'combined_score')
                print("\n【热点板块 TOP10】")
                print("-" * 80)
                print(f"{'排名':<6}{'板块代码':<15}{'综合得分':<12}{'1 日预测':<10}{'5 日预测':<10}{'20 日预测':<10}")
                print("-" * 80)
                for i, row in top_concepts.iterrows():
                    print(f"{i:<6}{row['concept_code']:<15}{row['combined_score']:<12.2f}"
                          f"{row['pred_1d']:<10.2f}{row['pred_5d']:<10.2f}{row['pred_20d']:<10.2f}")
                print("-" * 80)

            # Step 2: 筛选成分股
            print("\n【Step 2/3】筛选成分股...")
            from data.database import get_database
            db = get_database()

            import sqlite3
            conn = sqlite3.connect('data/stock.db')
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT concept_code FROM concept_constituent')
            available_concepts = [row[0] for row in cursor.fetchall()]
            conn.close()

            # 找出同时在预测结果和成分股中的板块
            if concept_predictions is not None:
                concept_codes_set = set(concept_predictions['concept_code'].unique().tolist())
                available_set = set(available_concepts)
                valid_concepts = list(concept_codes_set & available_set)

                if valid_concepts:
                    concept_predictions = concept_predictions[
                        concept_predictions['concept_code'].isin(valid_concepts)
                    ]
                    concept_codes = valid_concepts
                    logger.info(f"有效板块：{len(concept_codes)} 个（有成分股数据）")
                else:
                    logger.warning("预测板块中没有成分股数据，使用默认板块测试")
                    concept_codes = available_concepts[:3] if available_concepts else []
                    if concept_codes:
                        concept_predictions = concept_predictions[
                            concept_predictions['concept_code'].isin(concept_codes)
                        ]

            if not concept_codes:
                logger.error("无有效板块数据")
                return

            # Step 3: 构建投资组合（包含个股筛选、预测、优化）
            print("\n【Step 3/3】构建投资组合...")
            result = agent.run(
                task="build",
                concept_codes=concept_codes,
                concept_predictions=concept_predictions,
                top_n_stocks=args.top_n
            )

            # 输出结果
            print("\n" + "=" * 70)
            print("投资组合构建结果")
            print("=" * 70)

            if result.get("success"):
                portfolio = result.get("portfolio", [])
                metrics = result.get("metrics", {})
                risk = result.get("risk_analysis", {})

                print(f"\n持仓数量：{len(portfolio)} 只股票")

                if portfolio:
                    print("\n【持仓明细】")
                    print("-" * 100)
                    print(f"{'代码':<12}{'名称':<15}{'权重':>10}{'所属板块':<20}{'1 日预测':>10}{'5 日预测':>10}")
                    print("-" * 100)
                    for pos in portfolio:
                        print(f"{pos['ts_code']:<12}{pos['stock_name']:<15}{pos['weight']:>10.1%}  "
                              f"{pos['concept_code']:<20}{pos.get('pred_1d', 0):>10.2f}%{pos.get('pred_5d', 0):>10.2f}%")
                    print("-" * 100)

                print("\n【预期指标】")
                print(f"  预期年化收益：{metrics.get('expected_return', 0):.1%}")
                print(f"  预期年化波动率：{metrics.get('expected_volatility', 0):.1%}")
                print(f"  夏普比率：{metrics.get('sharpe', 0):.2f}")
                print(f"  最大回撤估计：{metrics.get('max_drawdown', 0):.1%}")

                print("\n【风险分析】")
                print(f"  板块集中度：{risk.get('sector_concentration', 0):.1%}")
                print(f"  平均相关性：{risk.get('avg_correlation', 0):.3f}")
            else:
                print(f"组合构建失败：{result.get('error', '未知错误')}")

            print("=" * 70 + "\n")

        elif args.mode == "stock":
            # 个股数据采集（中证500、创业板、科创板）
            logger.info("采集个股数据...")
            from data.extended_stock_collector import ExtendedStockCollector

            print("\n" + "=" * 70)
            print("个股数据采集")
            print("=" * 70)

            # 确定日期范围
            start_date = args.start_date or "20200101"
            end_date = args.end_date or datetime.now().strftime("%Y%m%d")

            print(f"\n日期范围: {start_date} ~ {end_date}")
            print(f"采集类型: {args.stock_type}")

            collector = ExtendedStockCollector()

            # 根据类型采集
            if args.stock_type == "all":
                result = collector.collect_all(
                    start_date=start_date,
                    end_date=end_date,
                    include_csi500=True,
                    include_gem=True,
                    include_star=True
                )
            elif args.stock_type == "csi500":
                result = collector.collect_all(
                    start_date=start_date,
                    end_date=end_date,
                    include_csi500=True,
                    include_gem=False,
                    include_star=False
                )
            elif args.stock_type == "gem":
                result = collector.collect_all(
                    start_date=start_date,
                    end_date=end_date,
                    include_csi500=False,
                    include_gem=True,
                    include_star=False
                )
            elif args.stock_type == "star":
                result = collector.collect_all(
                    start_date=start_date,
                    end_date=end_date,
                    include_csi500=False,
                    include_gem=False,
                    include_star=True
                )

            print("\n" + "=" * 70)
            print("采集结果")
            print("=" * 70)
            print(f"  来源: {result.get('sources', {})}")
            print(f"  总股票数: {result['stats']['total']}")
            print(f"  成功: {result['stats']['success']}")
            print(f"  失败: {result['stats']['fail']}")
            print(f"  总记录数: {result['stats']['records']:,}")
            print("=" * 70 + "\n")

    except Exception as e:
        logger.error(f"执行失败：{e}")
        raise

    logger.info("执行完成")


if __name__ == "__main__":
    main()
