#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测模块
验证预测模型的准确性和交易策略效果
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from loguru import logger
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from models.predictor import UnifiedPredictor


class Backtester:
    """回测引擎"""

    # 每月平均交易日数
    TRADING_DAYS_PER_MONTH = 21

    def __init__(self, initial_capital: float = 1000000.0):
        """
        初始化回测器

        Args:
            initial_capital: 初始资金
        """
        self.initial_capital = initial_capital
        self.predictor = UnifiedPredictor()

    def run_walk_forward(
        self,
        concept_data: pd.DataFrame,
        train_windows: int = 20,
        test_windows: int = 5,
        step: int = 5
    ) -> Dict:
        """
        运行滚动回测（Walk-Forward Validation）

        Args:
            concept_data: 概念板块数据
            train_windows: 训练窗口长度（月）
            test_windows: 测试窗口长度（月）
            step: 滚动步长（月）

        Returns:
            回测结果
        """
        logger.info(f"开始滚动回测：训练={train_windows}月，测试={test_windows}月，步长={step}月")

        # 按日期分组
        dates = sorted(concept_data['trade_date'].unique())

        # 计算切分点（使用实际交易日计算）
        trading_days_per_month = self.TRADING_DAYS_PER_MONTH
        total_months = len(dates) // trading_days_per_month
        if total_months < train_windows + test_windows:
            logger.warning("数据量不足，无法进行回测")
            return {"error": "数据量不足"}

        results = []
        metrics_history = []

        # 滚动回测
        for i in range(0, total_months - train_windows - test_windows, step):
            train_end_idx = (i + train_windows) * trading_days_per_month
            test_end_idx = (i + train_windows + test_windows) * trading_days_per_month

            # 划分训练集和测试集
            train_data = concept_data[concept_data['trade_date'] < dates[train_end_idx]].copy()
            test_data = concept_data[
                (concept_data['trade_date'] >= dates[train_end_idx]) &
                (concept_data['trade_date'] < dates[test_end_idx])
            ].copy()

            if len(train_data) < 1000 or len(test_data) < 100:
                continue

            # 训练模型
            logger.info(f"折叠 {i//step + 1}: 训练模型...")
            features = self.predictor.prepare_features(train_data, n_jobs=16)
            if features.empty:
                continue

            model_result = self.predictor.train(features)

            # 在测试集上预测
            test_features = self.predictor.prepare_features(test_data, n_jobs=16)
            if test_features.empty:
                continue

            predictions = self.predictor.predict(model_result, test_features)

            if predictions.empty:
                continue

            # 计算回测指标
            fold_metrics = self._calculate_metrics(predictions, test_data)
            fold_metrics['fold'] = i // step + 1
            results.append(fold_metrics)
            metrics_history.append(fold_metrics)

            logger.info(f"折叠 {i//step + 1}: IC={fold_metrics['ic']:.4f}, "
                       f"RankIC={fold_metrics['rank_ic']:.4f}, "
                       f"Sharpe={fold_metrics['sharpe']:.2f}")

        # 汇总结果
        if not results:
            return {"error": "回测失败"}

        avg_metrics = {
            'avg_ic': np.mean([r['ic'] for r in results]),
            'avg_rank_ic': np.mean([r['rank_ic'] for r in results]),
            'avg_sharpe': np.mean([r['sharpe'] for r in results]),
            'avg_return': np.mean([r['total_return'] for r in results]),
            'max_drawdown': np.min([r['max_drawdown'] for r in results]),
            'win_rate': np.mean([r['win_rate'] for r in results]),
            'folds': len(results)
        }

        logger.info(f"回测完成：{len(results)} 个折叠")
        logger.info(f"平均 IC={avg_metrics['avg_ic']:.4f}, RankIC={avg_metrics['avg_rank_ic']:.4f}")
        logger.info(f"平均 Sharpe={avg_metrics['avg_sharpe']:.2f}, 最大回撤={avg_metrics['max_drawdown']:.2%}")

        return {
            'fold_results': results,
            'avg_metrics': avg_metrics,
            'metrics_history': metrics_history
        }

    def _calculate_metrics(
        self,
        predictions: pd.DataFrame,
        actual_data: pd.DataFrame
    ) -> Dict:
        """
        计算回测指标

        Args:
            predictions: 预测结果
            actual_data: 实际数据

        Returns:
            指标字典
        """
        # 合并预测和实际值
        merged = predictions.merge(
            actual_data[['concept_code', 'trade_date', 'pct_chg']],
            on=['concept_code', 'trade_date'],
            how='left'
        )

        if merged.empty or 'pct_chg' not in merged.columns:
            return {
                'ic': 0, 'rank_ic': 0, 'sharpe': 0,
                'total_return': 0, 'max_drawdown': 0, 'win_rate': 0
            }

        # 计算 IC（Information Coefficient）
        ic = merged['combined_score'].corr(merged['pct_chg'])
        rank_ic = merged['combined_score'].corr(merged['pct_chg'], method='spearman')

        # 模拟交易：每天买入预测 TOP10
        merged = merged.sort_values(['trade_date', 'combined_score'], ascending=[True, False])
        merged['rank'] = merged.groupby('trade_date')['combined_score'].rank(ascending=False)
        top10 = merged[merged['rank'] <= 10]

        # 计算组合收益
        portfolio_returns = top10.groupby('trade_date')['pct_chg'].mean()

        if len(portfolio_returns) < 2:
            return {
                'ic': ic if not np.isnan(ic) else 0,
                'rank_ic': rank_ic if not np.isnan(rank_ic) else 0,
                'sharpe': 0, 'total_return': 0, 'max_drawdown': 0, 'win_rate': 0
            }

        # 累计收益
        cumulative = (1 + portfolio_returns / 100).cumprod()
        total_return = (cumulative.iloc[-1] - 1) * 100

        # 最大回撤
        rolling_max = cumulative.cummax()
        drawdown = (cumulative - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        # 夏普比率（假设无风险利率为 0）
        daily_returns = portfolio_returns / 100
        sharpe = np.sqrt(252) * daily_returns.mean() / daily_returns.std() if daily_returns.std() > 0 else 0

        # 胜率
        win_rate = (portfolio_returns > 0).mean()

        return {
            'ic': ic if not np.isnan(ic) else 0,
            'rank_ic': rank_ic if not np.isnan(rank_ic) else 0,
            'sharpe': sharpe,
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate
        }

    def optimize_weights(
        self,
        concept_data: pd.DataFrame,
        weight_grid: List[Tuple[float, float, float]] = None
    ) -> Dict:
        """
        优化综合评分权重

        Args:
            concept_data: 概念板块数据
            weight_grid: 权重组合列表，默认测试 9 种组合 (1d, 5d, 20d)

        Returns:
            最优权重和回测结果

        Note:
            当前版本未实际使用传入权重，需要在 predictor.predict() 中添加权重参数支持
        """
        if weight_grid is None:
            # 生成权重组合 (1d, 5d, 20d)
            weight_grid = [
                (0.2, 0.5, 0.3),
                (0.3, 0.5, 0.2),
                (0.2, 0.6, 0.2),
                (0.1, 0.6, 0.3),
                (0.2, 0.4, 0.4),
                (0.3, 0.4, 0.3),
                (0.1, 0.5, 0.4),
                (0.4, 0.4, 0.2),
                (0.1, 0.7, 0.2),
            ]

        logger.info(f"开始权重优化，测试 {len(weight_grid)} 种组合")

        results = []
        for w1, w5, w20 in weight_grid:
            # TODO: 实现动态权重传递到 predict 方法
            # 当前版本使用固定权重 (0.3, 0.5, 0.2)
            result = self.run_walk_forward(concept_data, train_windows=12, test_windows=3, step=3)

            if 'avg_metrics' in result:
                results.append({
                    'weights': (w1, w5, w20),
                    'metrics': result['avg_metrics']
                })
                logger.info(f"权重 ({w1}, {w5}, {w20}): "
                           f"Sharpe={result['avg_metrics']['avg_sharpe']:.2f}, "
                           f"IC={result['avg_metrics']['avg_ic']:.4f}")

        if not results:
            return {"error": "权重优化失败"}

        # 找出最优权重（按 Sharpe 比率）
        best = max(results, key=lambda x: x['metrics']['avg_sharpe'])

        logger.info(f"最优权重：1d={best['weights'][0]}, 5d={best['weights'][1]}, 20d={best['weights'][2]}")
        logger.info(f"对应 Sharpe={best['metrics']['avg_sharpe']:.2f}, IC={best['metrics']['avg_ic']:.4f}")

        return {
            'best_weights': best['weights'],
            'best_metrics': best['metrics'],
            'all_results': results
        }

    def run_purged_kfold(
        self,
        concept_data: pd.DataFrame,
        n_splits: int = 5,
        train_window_months: int = 24,
        purge_days: int = 5,
        embargo_days: int = 2
    ) -> Dict:
        """
        运行 Purged K-Fold 时序交叉验证

        防止数据泄露的改进版 K-Fold：
        1. Purge（清除）：测试集之后的数据不用于训练
        2. Embargo（禁运）：训练集和测试集之间留出缓冲期

        Args:
            concept_data: 概念板块数据
            n_splits: 折叠数量
            train_window_months: 训练窗口长度（月）
            purge_days: 清除天数（测试集之后的数据不用于训练）
            embargo_days: 禁运天数（训练集和测试集之间的缓冲期）

        Returns:
            交叉验证结果
        """
        logger.info(f"开始 Purged K-Fold 时序交叉验证：n_splits={n_splits}, "
                   f"train_window={train_window_months}月，purge={purge_days}天，embargo={embargo_days}天")

        # 按日期排序
        df = concept_data.sort_values('trade_date').copy()
        dates = sorted(df['trade_date'].unique())

        total_days = len(dates)
        fold_size = total_days // n_splits

        if fold_size < 20:
            logger.warning("数据量不足，无法进行 K-Fold 交叉验证")
            return {"error": "数据量不足"}

        results = []
        metrics_history = []

        for fold_idx in range(n_splits):
            # 计算测试集时间范围
            test_start_idx = fold_idx * fold_size
            test_end_idx = min((fold_idx + 1) * fold_size, total_days)

            test_start_date = dates[test_start_idx]
            test_end_date = dates[test_end_idx - 1] if test_end_idx <= total_days else dates[-1]

            # 计算训练集时间范围（带 Purge 和 Embargo）
            # 训练集只能用测试集之前的数据
            train_end_date = dates[max(0, test_start_idx - embargo_days)]

            # 训练窗口起始点
            train_days = train_window_months * trading_days_per_month
            train_start_idx = max(0, test_start_idx - train_days)
            train_start_date = dates[train_start_idx] if train_start_idx < len(dates) else dates[0]

            # 划分训练集和测试集
            train_data = df[
                (df['trade_date'] >= train_start_date) &
                (df['trade_date'] <= train_end_date)
            ].copy()

            test_data = df[
                (df['trade_date'] >= test_start_date) &
                (df['trade_date'] <= test_end_date)
            ].copy()

            if len(train_data) < 1000 or len(test_data) < 100:
                logger.warning(f"折叠 {fold_idx + 1}: 数据量不足，跳过")
                continue

            # 训练模型
            logger.info(f"折叠 {fold_idx + 1}/{n_splits}: 训练模型...")
            logger.info(f"  训练集：{len(train_data)} 条，测试集：{len(test_data)} 条")
            logger.info(f"  训练时间：{train_start_date} - {train_end_date}")
            logger.info(f"  测试时间：{test_start_date} - {test_end_date}")

            start_time = time.time()
            features = self.predictor.prepare_features(train_data, n_jobs=16)
            if features.empty:
                logger.warning(f"折叠 {fold_idx + 1}: 特征为空，跳过")
                continue

            model_result = self.predictor.train(features)

            # 在测试集上预测
            test_features = self.predictor.prepare_features(test_data, n_jobs=16)
            if test_features.empty:
                logger.warning(f"折叠 {fold_idx + 1}: 测试特征为空，跳过")
                continue

            predictions = self.predictor.predict(model_result, test_features)

            if predictions.empty:
                logger.warning(f"折叠 {fold_idx + 1}: 预测结果为空，跳过")
                continue

            # 计算指标
            fold_metrics = self._calculate_metrics(predictions, test_data)
            fold_metrics['fold'] = fold_idx + 1
            fold_metrics['train_start'] = train_start_date
            fold_metrics['train_end'] = train_end_date
            fold_metrics['test_start'] = test_start_date
            fold_metrics['test_end'] = test_end_date

            results.append(fold_metrics)
            metrics_history.append(fold_metrics)

            elapsed = time.time() - start_time
            logger.info(f"折叠 {fold_idx + 1}: IC={fold_metrics['ic']:.4f}, "
                       f"RankIC={fold_metrics['rank_ic']:.4f}, "
                       f"Sharpe={fold_metrics['sharpe']:.2f}, "
                       f"耗时 {elapsed:.1f}s")

        # 汇总结果
        if not results:
            return {"error": "交叉验证失败"}

        avg_metrics = {
            'avg_ic': np.mean([r['ic'] for r in results]),
            'avg_rank_ic': np.mean([r['rank_ic'] for r in results]),
            'avg_sharpe': np.mean([r['sharpe'] for r in results]),
            'avg_return': np.mean([r['total_return'] for r in results]),
            'max_drawdown': np.min([r['max_drawdown'] for r in results]),
            'win_rate': np.mean([r['win_rate'] for r in results]),
            'folds': len(results),
            'ic_std': np.std([r['ic'] for r in results]),
            'sharpe_std': np.std([r['sharpe'] for r in results])
        }

        logger.info(f"Purged K-Fold 交叉验证完成：{len(results)}/{n_splits} 个折叠")
        logger.info(f"平均 IC={avg_metrics['avg_ic']:.4f} (±{avg_metrics['ic_std']:.4f})")
        logger.info(f"平均 Sharpe={avg_metrics['avg_sharpe']:.2f} (±{avg_metrics['sharpe_std']:.2f})")

        return {
            'fold_results': results,
            'avg_metrics': avg_metrics,
            'metrics_history': metrics_history,
            'method': 'purged_kfold'
        }


def main():
    """主函数"""
    from agents.data_agent import DataAgent

    # 加载数据
    logger.info("加载历史数据...")
    data_agent = DataAgent()
    data = data_agent.execute(task="history", start_date="20230101", end_date="20241231")

    concept_data = data.get('result', {}).get('concept')
    if concept_data is None or concept_data.empty:
        logger.error("无法加载数据")
        return

    # 运行回测
    backtester = Backtester()
    results = backtester.run_walk_forward(concept_data, train_windows=12, test_windows=3, step=3)

    # 输出结果
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

    print("=" * 70)


if __name__ == "__main__":
    main()
