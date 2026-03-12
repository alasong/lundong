#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
热点预测系统回测
评估历史预测的准确率和成功率
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from loguru import logger
import json
import glob


class PredictionBacktester:
    """预测回测器"""

    def __init__(self, db=None):
        """
        初始化回测器

        Args:
            db: 数据库实例
        """
        self.db = db
        self.results_dir = os.path.join(os.path.dirname(__file__), '../../data/results')

    def load_historical_predictions(self) -> List[Dict]:
        """加载历史预测结果"""
        predictions = []

        # 读取所有预测结果文件
        result_files = glob.glob(os.path.join(self.results_dir, 'daily_*.json'))

        for file_path in sorted(result_files):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 提取预测数据
                if 'prediction' in data and data['prediction'].get('success'):
                    pred_data = data['prediction']['result']['result']

                    # 提取预测记录
                    for pred in pred_data.get('predictions', []):
                        predictions.append({
                            'date': pred.get('trade_date'),
                            'concept_code': pred.get('concept_code'),
                            'pred_1d': pred.get('pred_1d'),
                            'pred_5d': pred.get('pred_5d'),
                            'pred_20d': pred.get('pred_20d'),
                            'combined_score': pred.get('combined_score'),
                            'source_file': os.path.basename(file_path)
                        })
            except Exception as e:
                logger.warning(f"读取文件失败 {file_path}: {e}")

        logger.info(f"加载了 {len(predictions)} 条历史预测记录")
        return predictions

    def get_actual_returns(
        self,
        ts_code: str,
        trade_date: int,
        horizons: List[int] = [1, 5, 20]
    ) -> Dict[str, float]:
        """
        获取实际收益率

        Args:
            ts_code: 板块代码
            trade_date: 交易日期
            horizons: 预测周期

        Returns:
            实际收益率字典
        """
        if self.db is None:
            return {f"actual_{h}d": 0.0 for h in horizons}

        actual_returns = {}

        for horizon in horizons:
            # 计算目标日期
            target_date = trade_date + horizon

            # 获取买入日和目标日的收盘价
            buy_result = self.db.query("""
                SELECT close FROM concept_daily
                WHERE ts_code = ? AND trade_date = ?
            """, (ts_code, trade_date))

            sell_result = self.db.query("""
                SELECT close FROM concept_daily
                WHERE ts_code = ? AND trade_date = ?
            """, (ts_code, target_date))

            if buy_result and sell_result and len(buy_result) > 0 and len(sell_result) > 0:
                actual_return = (sell_result[0][0] / buy_result[0][0] - 1) * 100
                actual_returns[f"actual_{horizon}d"] = actual_return
            else:
                actual_returns[f"actual_{horizon}d"] = None

        return actual_returns

    def calculate_directional_accuracy(
        self,
        predictions: List[Dict]
    ) -> Dict[str, float]:
        """
        计算方向准确率（涨跌预测是否正确）

        Args:
            predictions: 预测记录列表

        Returns:
            各周期方向准确率
        """
        accuracy = {
            '1d': {'correct': 0, 'total': 0},
            '5d': {'correct': 0, 'total': 0},
            '20d': {'correct': 0, 'total': 0}
        }

        for pred in predictions:
            ts_code = pred['concept_code']
            trade_date = pred['date']

            if not ts_code or not trade_date:
                continue

            # 获取实际收益率
            actual = self.get_actual_returns(ts_code, trade_date)

            for horizon in ['1d', '5d', '20d']:
                pred_key = f"pred_{horizon}"
                actual_key = f"actual_{horizon}"

                if pred_key in pred and actual_key in actual and actual[actual_key] is not None:
                    pred_direction = 1 if pred[pred_key] > 0 else -1
                    actual_direction = 1 if actual[actual_key] > 0 else -1

                    accuracy[horizon]['total'] += 1
                    if pred_direction == actual_direction:
                        accuracy[horizon]['correct'] += 1

        # 计算准确率
        result = {}
        for horizon, stats in accuracy.items():
            if stats['total'] > 0:
                result[horizon] = stats['correct'] / stats['total'] * 100
            else:
                result[horizon] = 0.0

        return result

    def calculate_hit_rate(
        self,
        predictions: List[Dict],
        top_n: int = 10
    ) -> Dict[str, float]:
        """
        计算 Top-N 推荐命中率

        Args:
            predictions: 预测记录列表
            top_n: 推荐数量

        Returns:
            命中率统计
        """
        # 按日期分组
        by_date = {}
        for pred in predictions:
            date = pred['date']
            if date not in by_date:
                by_date[date] = []
            by_date[date].append(pred)

        hit_stats = {
            '1d': {'hit': 0, 'total': 0},
            '5d': {'hit': 0, 'total': 0},
            '20d': {'hit': 0, 'total': 0}
        }

        for date, preds in by_date.items():
            # 按综合得分排序选 Top-N
            sorted_preds = sorted(preds, key=lambda x: x.get('combined_score', 0), reverse=True)
            top_preds = sorted_preds[:top_n]

            for pred in top_preds:
                ts_code = pred['concept_code']
                actual = self.get_actual_returns(ts_code, date)

                for horizon in ['1d', '5d', '20d']:
                    actual_key = f"actual_{horizon}"
                    if actual_key in actual and actual[actual_key] is not None:
                        hit_stats[horizon]['total'] += 1
                        # 实际上涨视为命中
                        if actual[actual_key] > 0:
                            hit_stats[horizon]['hit'] += 1

        # 计算命中率
        result = {}
        for horizon, stats in hit_stats.items():
            if stats['total'] > 0:
                result[horizon] = stats['hit'] / stats['total'] * 100
            else:
                result[horizon] = 0.0

        return result

    def calculate_ic(
        self,
        predictions: List[Dict]
    ) -> Dict[str, float]:
        """
        计算 IC (Information Coefficient) - 预测值与实际值的相关性

        Args:
            predictions: 预测记录列表

        Returns:
            各周期 IC 值
        """
        ic_data = {
            '1d': {'pred': [], 'actual': []},
            '5d': {'pred': [], 'actual': []},
            '20d': {'pred': [], 'actual': []}
        }

        for pred in predictions:
            ts_code = pred['concept_code']
            trade_date = pred['date']

            if not ts_code or not trade_date:
                continue

            actual = self.get_actual_returns(ts_code, trade_date)

            for horizon in ['1d', '5d', '20d']:
                pred_key = f"pred_{horizon}"
                actual_key = f"actual_{horizon}"

                if pred_key in pred and actual_key in actual and actual[actual_key] is not None:
                    ic_data[horizon]['pred'].append(pred[pred_key])
                    ic_data[horizon]['actual'].append(actual[actual_key])

        # 计算相关系数
        ic_result = {}
        for horizon, data in ic_data.items():
            if len(data['pred']) > 10:
                corr = np.corrcoef(data['pred'], data['actual'])[0, 1]
                ic_result[horizon] = corr if not np.isnan(corr) else 0.0
            else:
                ic_result[horizon] = 0.0

        return ic_result

    def run_backtest(self) -> Dict:
        """
        运行完整回测

        Returns:
            回测结果
        """
        logger.info("开始预测系统回测...")

        # 加载历史预测
        predictions = self.load_historical_predictions()

        if not predictions:
            logger.warning("未找到历史预测数据")
            return {'error': '无历史预测数据'}

        # 计算方向准确率
        directional_acc = self.calculate_directional_accuracy(predictions)

        # 计算 Top-N 命中率
        hit_rate = self.calculate_hit_rate(predictions, top_n=10)

        # 计算 IC
        ic = self.calculate_ic(predictions)

        # 综合评估
        result = {
            'total_predictions': len(predictions),
            'directional_accuracy': directional_acc,
            'top_10_hit_rate': hit_rate,
            'information_coefficient': ic,
            'evaluation': self._evaluate_performance(directional_acc, hit_rate, ic)
        }

        logger.info("=" * 60)
        logger.info("热点预测系统回测结果")
        logger.info("=" * 60)
        logger.info(f"总预测记录数：{len(predictions)}")
        logger.info(f"\n方向准确率:")
        for horizon, acc in directional_acc.items():
            logger.info(f"  {horizon}日：{acc:.2f}%")
        logger.info(f"\nTop-10 命中率:")
        for horizon, rate in hit_rate.items():
            logger.info(f"  {horizon}日：{rate:.2f}%")
        logger.info(f"\nIC (信息系数):")
        for horizon, ic_val in ic.items():
            logger.info(f"  {horizon}日：{ic_val:.4f}")
        logger.info(f"\n综合评估：{result['evaluation']}")
        logger.info("=" * 60)

        return result

    def _evaluate_performance(
        self,
        directional_acc: Dict,
        hit_rate: Dict,
        ic: Dict
    ) -> str:
        """评估整体表现"""
        avg_direction = np.mean(list(directional_acc.values()))
        avg_hit = np.mean(list(hit_rate.values()))
        avg_ic = np.mean([abs(v) for v in ic.values()])

        # 综合评分
        score = (
            (avg_direction / 50) * 40 +  # 方向准确率权重 40%
            (avg_hit / 50) * 40 +         # 命中率权重 40%
            (avg_ic / 0.1) * 20           # IC 权重 20%
        )

        if score >= 80:
            return f"优秀 (综合评分：{score:.1f}/100) - 可用于实盘"
        elif score >= 60:
            return f"良好 (综合评分：{score:.1f}/100) - 建议改进"
        elif score >= 40:
            return f"一般 (综合评分：{score:.1f}/100) - 需要优化"
        else:
            return f"较差 (综合评分：{score:.1f}/100) - 重新训练模型"


def main():
    """主函数"""
    print("=" * 60)
    print("热点预测系统回测")
    print("=" * 60)

    # 导入数据库
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../data'))
    from database import get_database

    db = get_database()

    if db is None:
        print("错误：无法连接数据库")
        return

    try:
        # 运行回测
        backtester = PredictionBacktester(db)
        result = backtester.run_backtest()

        # 保存结果
        output_path = os.path.join(os.path.dirname(__file__), '../../data/results/prediction_backtest.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            # 序列化结果
            serializable_result = {}
            for key, value in result.items():
                if isinstance(value, dict):
                    serializable_result[key] = {k: float(v) if isinstance(v, (np.floating, float)) else v
                                               for k, v in value.items()}
                else:
                    serializable_result[key] = value
            json.dump(serializable_result, f, indent=2, ensure_ascii=False)

        print(f"\n回测结果已保存至：{output_path}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
