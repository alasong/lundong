#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
预测模块压力测试脚本
测试系统支持的并发程度和性能瓶颈
"""
import os
import sys
import time
import argparse
from datetime import datetime
from loguru import logger
import pandas as pd

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
from config import settings
from models.predictor import UnifiedPredictor
from agents.predict_agent import PredictAgent


def setup_logging():
    """配置日志"""
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )


def load_test_data(recent_days: int = 60):
    """加载测试数据"""
    from joblib import Parallel, delayed
    import pandas as pd

    raw_dir = settings.raw_data_dir
    if not os.path.exists(raw_dir):
        logger.error(f"数据目录不存在：{raw_dir}")
        return None

    ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]
    logger.info(f"发现 {len(ths_files)} 个数据文件")

    def load_single_file(filepath):
        try:
            df = pd.read_csv(filepath, dtype={
                'concept_code': str,
                'trade_date': str,
                'pct_chg': float,
                'vol': float
            })
            if 'pct_change' in df.columns:
                df = df.rename(columns={'pct_change': 'pct_chg'})
            if 'ts_code' in df.columns:
                df = df.rename(columns={'ts_code': 'concept_code'})

            # 从文件名提取 name
            filename = os.path.basename(filepath)
            if 'name' not in df.columns:
                if filename.startswith('ths_') and '_TI.csv' in filename:
                    code_part = filename.replace('ths_', '').replace('_TI.csv', '')
                    df['name'] = f"板块_{code_part}"
                else:
                    df['name'] = df['concept_code']

            return df
        except Exception as e:
            logger.warning(f"加载文件 {filepath} 失败：{e}")
            return None

    start = time.time()
    dfs = Parallel(n_jobs=-1, backend="threading")(
        delayed(load_single_file)(os.path.join(raw_dir, f))
        for f in ths_files
    )
    dfs = [df for df in dfs if df is not None]

    if not dfs:
        logger.error("未能加载任何数据")
        return None

    concept_data = pd.concat(dfs, ignore_index=True)

    # 过滤日期
    concept_data = concept_data.sort_values("trade_date")
    latest_date = concept_data["trade_date"].max()
    latest_date_int = int(latest_date)
    min_date = latest_date_int - (recent_days * 100)
    concept_data = concept_data[concept_data["trade_date"] >= str(min_date)]

    elapsed = time.time() - start
    logger.info(f"数据加载完成：{len(concept_data)} 条记录，{concept_data['concept_code'].nunique()} 个板块，耗时 {elapsed:.2f}s")

    return concept_data


def test_feature_preparation(concept_data: pd.DataFrame, n_jobs_list: list = [8, 16, 32, 64]):
    """测试特征准备性能"""
    logger.info("=" * 60)
    logger.info("测试特征准备性能")
    logger.info("=" * 60)

    predictor = UnifiedPredictor()
    results = []

    for n_jobs in n_jobs_list:
        logger.info(f"\n测试 n_jobs={n_jobs}...")
        start = time.time()
        features = predictor.prepare_features(concept_data, n_jobs=n_jobs)
        elapsed = time.time() - start

        results.append({
            "n_jobs": n_jobs,
            "time": elapsed,
            "samples": len(features),
            "throughput": len(features) / elapsed if elapsed > 0 else 0
        })

        logger.info(f"n_jobs={n_jobs}: {len(features)} 样本，耗时 {elapsed:.2f}s, 吞吐量 {len(features)/elapsed:.0f} 样本/s")

    # 找出最佳并发数
    best = max(results, key=lambda x: x["throughput"])
    logger.info(f"\n最佳并发数：{best['n_jobs']} (吞吐量：{best['throughput']:.0f} 样本/s)")

    return results


def test_prediction_batch_size(features: pd.DataFrame, batch_sizes: list = [1000, 5000, 10000, 50000]):
    """测试预测批处理性能"""
    logger.info("=" * 60)
    logger.info("测试预测批处理性能")
    logger.info("=" * 60)

    predictor = UnifiedPredictor()
    model_result = predictor.load_model()

    if model_result is None:
        logger.error("未找到模型，跳过批处理测试")
        return []

    results = []
    for batch_size in batch_sizes:
        logger.info(f"\n测试 batch_size={batch_size}...")
        start = time.time()
        predictions = predictor.predict(model_result, features, batch_size=batch_size)
        elapsed = time.time() - start

        results.append({
            "batch_size": batch_size,
            "time": elapsed,
            "samples": len(predictions),
            "throughput": len(predictions) / elapsed if elapsed > 0 else 0
        })

        logger.info(f"batch_size={batch_size}: {len(predictions)} 样本，耗时 {elapsed:.2f}s, 吞吐量 {len(predictions)/elapsed:.0f} 样本/s")

    return results


def test_end_to_end(concept_data: pd.DataFrame, n_jobs: int = 32):
    """测试端到端预测性能"""
    logger.info("=" * 60)
    logger.info("测试端到端预测性能")
    logger.info("=" * 60)

    predictor = UnifiedPredictor()

    start = time.time()
    predictions = predictor.predict_latest(concept_data, n_jobs=n_jobs)
    elapsed = time.time() - start

    if predictions.empty:
        logger.error("端到端预测返回空结果")
        return None

    logger.info(f"端到端预测完成：{len(predictions)} 样本，耗时 {elapsed:.2f}s, 吞吐量 {len(predictions)/elapsed:.0f} 样本/s")

    # 显示 TOP10 预测
    logger.info("\n【TOP10 预测】")
    top10 = predictions.nlargest(10, "combined_score")
    for i, (_, row) in enumerate(top10.iterrows(), 1):
        name = row.get("name", row.get("concept_name", "N/A"))
        score = row.get("combined_score", 0)
        p1d = row.get("pred_1d", 0)
        logger.info(f"{i}. {name}: 综合得分={score:.2f}, 1 日={p1d:.2f}%")

    return {
        "time": elapsed,
        "samples": len(predictions),
        "throughput": len(predictions) / elapsed
    }


def test_concurrent_load(num_concurrent: int = 10):
    """测试并发加载"""
    logger.info("=" * 60)
    logger.info(f"测试 {num_concurrent} 个并发预测 Agent 初始化")
    logger.info("=" * 60)

    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []

    def create_and_load_agent(thread_id: int):
        start = time.time()
        try:
            agent = PredictAgent()
            elapsed = time.time() - start
            results.append({"thread": thread_id, "time": elapsed, "success": True})
            logger.info(f"Thread {thread_id}: Agent 初始化完成，耗时 {elapsed:.2f}s")
            return True
        except Exception as e:
            elapsed = time.time() - start
            results.append({"thread": thread_id, "time": elapsed, "success": False, "error": str(e)})
            logger.error(f"Thread {thread_id}: 失败 - {e}")
            return False

    start = time.time()
    with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
        futures = [executor.submit(create_and_load_agent, i) for i in range(num_concurrent)]
        for future in as_completed(futures):
            future.result()

    total_elapsed = time.time() - start
    success_count = sum(1 for r in results if r["success"])

    logger.info(f"\n并发测试结果：{success_count}/{num_concurrent} 成功，总耗时 {total_elapsed:.2f}s")
    return results


def run_all_tests():
    """运行所有测试"""
    logger.info("=" * 70)
    logger.info("A 股热点轮动预测系统 - 压力测试")
    logger.info("=" * 70)

    # 加载测试数据
    concept_data = load_test_data(recent_days=60)
    if concept_data is None:
        logger.error("数据加载失败，退出测试")
        return

    # 测试特征准备
    feature_results = test_feature_preparation(concept_data, n_jobs_list=[8, 16, 32, 64])

    # 准备特征用于后续测试
    predictor = UnifiedPredictor()
    features = predictor.prepare_features(concept_data, n_jobs=32)

    if features.empty:
        logger.error("特征准备失败，退出测试")
        return

    # 测试批处理
    batch_results = test_prediction_batch_size(features, batch_sizes=[1000, 5000, 10000, 50000])

    # 测试端到端
    e2e_result = test_end_to_end(concept_data, n_jobs=32)

    # 测试并发加载
    concurrent_results = test_concurrent_load(num_concurrent=5)

    # 总结
    logger.info("\n" + "=" * 70)
    logger.info("测试总结")
    logger.info("=" * 70)
    logger.info(f"数据规模：{len(concept_data)} 条记录，{concept_data['concept_code'].nunique()} 个板块")
    logger.info(f"特征规模：{len(features)} 条样本，{len(features.columns)} 个特征")
    if e2e_result:
        logger.info(f"端到端性能：{e2e_result['throughput']:.0f} 样本/s")
    logger.info("=" * 70)


if __name__ == "__main__":
    setup_logging()

    parser = argparse.ArgumentParser(description="预测模块压力测试")
    parser.add_argument("--test", choices=["feature", "batch", "e2e", "concurrent", "all"],
                       default="all", help="测试类型")
    parser.add_argument("--days", type=int, default=60, help="加载天数")
    parser.add_argument("--n-jobs", type=int, default=32, help="默认并发数")

    args = parser.parse_args()

    if args.test == "all":
        run_all_tests()
    else:
        logger.error("仅支持完整测试，请使用 --test all")
        sys.exit(1)
