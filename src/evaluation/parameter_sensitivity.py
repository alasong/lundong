#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
参数敏感性分析模块
用于分析策略参数变化对回测结果的影响，识别最优参数范围
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Callable
from datetime import datetime
from loguru import logger
import itertools


class ParameterSensitivityAnalyzer:
    """
    参数敏感性分析器

    功能：
    1. 参数扫描 - 遍历参数组合
    2. 敏感性分析 - 计算参数对结果的影响程度
    3. 参数热力图 - 可视化参数效果
    4. 稳健性检验 - 识别稳健参数范围
    """

    def __init__(self, backtest_func: Callable, base_params: Dict):
        """
        初始化分析器

        Args:
            backtest_func: 回测函数，接收参数 dict，返回性能指标 dict
            base_params: 基础参数配置
        """
        self.backtest_func = backtest_func
        self.base_params = base_params
        self.results = []
        logger.info("参数敏感性分析器初始化完成")

    def add_parameter_range(self, param_name: str, values: List):
        """
        添加参数扫描范围

        Args:
            param_name: 参数名称
            values: 参数值列表
        """
        if not hasattr(self, 'param_ranges'):
            self.param_ranges = {}
        self.param_ranges[param_name] = values
        logger.info(f"添加参数范围：{param_name} = {values}")

    def run_parameter_scan(self, max_combinations: int = 100) -> pd.DataFrame:
        """
        运行参数扫描

        Args:
            max_combinations: 最大参数组合数

        Returns:
            参数扫描结果 DataFrame
        """
        if not hasattr(self, 'param_ranges') or not self.param_ranges:
            logger.error("未设置参数范围")
            return pd.DataFrame()

        # 生成所有参数组合
        param_names = list(self.param_ranges.keys())
        param_values = [self.param_ranges[name] for name in param_names]
        all_combinations = list(itertools.product(*param_values))

        # 限制组合数
        if len(all_combinations) > max_combinations:
            logger.warning(f"参数组合数 {len(all_combinations)} 超过限制，采样 {max_combinations} 个")
            indices = np.random.choice(len(all_combinations), max_combinations, replace=False)
            all_combinations = [all_combinations[i] for i in sorted(indices)]

        logger.info(f"开始参数扫描：{len(all_combinations)} 个参数组合")

        results = []
        for i, combination in enumerate(all_combinations):
            params = self.base_params.copy()
            for name, value in zip(param_names, combination):
                params[name] = value

            try:
                result = self.backtest_func(params)
                result['params'] = params.copy()
                result['param_index'] = i
                results.append(result)
                logger.debug(f"组合 {i+1}/{len(all_combinations)}: {params} -> {result.get('sharpe', 'N/A')}")
            except Exception as e:
                logger.error(f"组合 {i+1} 失败：{e}")
                continue

        self.results = results
        df = pd.DataFrame(results)
        logger.info(f"参数扫描完成：{len(df)} 个有效结果")

        return df

    def analyze_sensitivity(self, results_df: pd.DataFrame, metric: str = 'sharpe') -> Dict:
        """
        分析参数敏感性

        Args:
            results_df: 参数扫描结果
            metric: 评估指标

        Returns:
            敏感性分析结果
        """
        if results_df.empty:
            return {}

        # 提取参数列
        param_cols = [col for col in results_df.columns if col in self.param_ranges]

        sensitivity = {}

        for param in param_cols:
            # 按参数分组计算指标统计
            grouped = results_df.groupby(param)[metric].agg(['mean', 'std', 'min', 'max', 'count'])

            # 计算敏感性分数（标准差/均值）
            mean_std = grouped['std'].mean()
            overall_mean = results_df[metric].mean()
            sensitivity_score = mean_std / overall_mean if overall_mean > 0 else 0

            sensitivity[param] = {
                'sensitivity_score': round(sensitivity_score, 4),
                'best_value': grouped['mean'].idxmax(),
                'worst_value': grouped['mean'].idxmin(),
                'best_avg': round(grouped['mean'].max(), 4),
                'worst_avg': round(grouped['mean'].min(), 4),
                'range': f"{grouped['mean'].min():.4f} - {grouped['mean'].max():.4f}",
                'statistics': grouped.to_dict()
            }

        # 按敏感性排序
        sorted_params = sorted(sensitivity.items(), key=lambda x: x[1]['sensitivity_score'], reverse=True)

        logger.info("参数敏感性分析完成:")
        for param, data in sorted_params:
            logger.info(f"  {param}: 敏感性={data['sensitivity_score']:.4f}, "
                       f"最优={data['best_value']}, 最劣={data['worst_value']}")

        return {
            'sensitivity': sensitivity,
            'sorted_params': sorted_params,
            'most_sensitive': sorted_params[0][0] if sorted_params else None,
            'least_sensitive': sorted_params[-1][0] if sorted_params else None
        }

    def find_robust_range(self, results_df: pd.DataFrame, metric: str = 'sharpe',
                         threshold_pct: float = 0.8) -> Dict:
        """
        寻找稳健参数范围

        Args:
            results_df: 参数扫描结果
            metric: 评估指标
            threshold_pct: 阈值比例（相对于最优值的比例）

        Returns:
            稳健参数范围
        """
        if results_df.empty:
            return {}

        param_cols = [col for col in results_df.columns if col in self.param_ranges]
        robust_ranges = {}

        best_metric = results_df[metric].max()
        threshold = best_metric * threshold_pct

        for param in param_cols:
            # 找出表现优于阈值的参数值
            good_results = results_df[results_df[metric] >= threshold]

            if len(good_results) == 0:
                continue

            good_values = good_results[param].unique()

            if len(good_values) > 0:
                # 计算稳健范围
                if isinstance(good_values[0], (int, float)):
                    robust_ranges[param] = {
                        'min': float(min(good_values)),
                        'max': float(max(good_values)),
                        'count': len(good_values),
                        'good_ratio': round(len(good_results) / len(results_df) * 100, 2)
                    }
                else:
                    robust_ranges[param] = {
                        'values': list(good_values),
                        'good_ratio': round(len(good_results) / len(results_df) * 100, 2)
                    }

        logger.info(f"稳健参数范围分析完成（阈值：{threshold_pct*100}% 最优值）:")
        for param, data in robust_ranges.items():
            if 'min' in data:
                logger.info(f"  {param}: {data['min']} - {data['max']} "
                           f"({data['good_ratio']}% 组合达标)")
            else:
                logger.info(f"  {param}: {data['values']} ({data['good_ratio']}% 组合达标)")

        return robust_ranges

    def plot_heatmap(self, results_df: pd.DataFrame, x_param: str, y_param: str,
                    metric: str = 'sharpe') -> Optional[pd.DataFrame]:
        """
        生成参数热力图数据

        Args:
            results_df: 参数扫描结果
            x_param: X 轴参数
            y_param: Y 轴参数
            metric: 评估指标

        Returns:
            热力图数据 DataFrame
        """
        if results_df.empty or x_param not in results_df.columns or y_param not in results_df.columns:
            return None

        # 生成透视表
        heatmap_data = results_df.pivot_table(
            index=y_param,
            columns=x_param,
            values=metric,
            aggfunc='mean'
        )

        logger.info(f"热力图数据生成：{x_param} vs {y_param}")
        return heatmap_data

    def print_optimal_params(self, results_df: pd.DataFrame, metric: str = 'sharpe'):
        """打印最优参数组合"""
        if results_df.empty:
            return

        best_idx = results_df[metric].idxmax()
        best_row = results_df.loc[best_idx]

        print("\n" + "=" * 70)
        print(f"最优参数组合（按 {metric}）")
        print("=" * 70)

        # 打印参数
        if 'params' in best_row:
            params = best_row['params']
            print("\n参数配置:")
            for key, value in params.items():
                if key not in ['params']:
                    print(f"  {key}: {value}")

        # 打印性能指标
        print("\n性能指标:")
        for col in results_df.columns:
            if col not in ['params', 'param_index'] + list(self.param_ranges.keys()):
                val = best_row[col]
                if isinstance(val, float):
                    print(f"  {col}: {val:.4f}")
                else:
                    print(f"  {col}: {val}")

        print("=" * 70)


# ============================================
# 示例回测函数（用于测试）
# ============================================

def example_backtest(params: Dict) -> Dict:
    """
    示例回测函数

    实际使用时应替换为真实的策略回测函数
    """
    # 模拟回测结果
    stop_loss = params.get('stop_loss_pct', 0.08)
    take_profit = params.get('take_profit_pct', 0.15)
    position_size = params.get('position_size', 0.1)

    # 模拟：较小的止损和适中的止盈通常表现更好
    base_sharpe = 1.0
    sl_effect = 1.0 - abs(stop_loss - 0.08) * 2  # 8% 止损最优
    tp_effect = 1.0 - abs(take_profit - 0.15) * 1.5  # 15% 止盈最优
    pos_effect = 1.0 - abs(position_size - 0.1) * 0.5  # 10% 仓位最优

    sharpe = base_sharpe * sl_effect * tp_effect * pos_effect
    sharpe += np.random.normal(0, 0.1)  # 添加一些随机性

    return {
        'sharpe': max(0, sharpe),
        'return': sharpe * 10 + np.random.normal(0, 5),
        'max_drawdown': abs(np.random.normal(0.15, 0.05)),
        'win_rate': 0.5 + np.random.normal(0, 0.1)
    }


def main():
    """测试函数"""
    print("=" * 70)
    print("参数敏感性分析工具")
    print("=" * 70)

    # 基础参数
    base_params = {
        'stop_loss_pct': 0.08,
        'take_profit_pct': 0.15,
        'position_size': 0.1
    }

    # 创建分析器
    analyzer = ParameterSensitivityAnalyzer(example_backtest, base_params)

    # 设置参数范围
    analyzer.add_parameter_range('stop_loss_pct', [0.05, 0.08, 0.10, 0.12, 0.15])
    analyzer.add_parameter_range('take_profit_pct', [0.10, 0.15, 0.20, 0.25])
    analyzer.add_parameter_range('position_size', [0.05, 0.10, 0.15, 0.20])

    # 运行参数扫描
    print("\n[1/4] 运行参数扫描...")
    results = analyzer.run_parameter_scan()

    if results.empty:
        print("参数扫描无结果")
        return

    print(f"\n完成：{len(results)} 个参数组合")

    # 敏感性分析
    print("\n[2/4] 参数敏感性分析...")
    sensitivity = analyzer.analyze_sensitivity(results, metric='sharpe')

    # 稳健范围分析
    print("\n[3/4] 稳健参数范围分析...")
    robust_ranges = analyzer.find_robust_range(results, metric='sharpe', threshold_pct=0.85)

    # 打印最优参数
    print("\n[4/4] 最优参数组合...")
    analyzer.print_optimal_params(results, metric='sharpe')

    # 热力图数据
    print("\n【热力图数据】")
    heatmap = analyzer.plot_heatmap(results, 'stop_loss_pct', 'take_profit_pct', 'sharpe')
    if heatmap is not None:
        print("\nSharpe Ratio 热力图 (止损 vs 止盈):")
        print(heatmap.to_string())

    # 总结
    print("\n" + "=" * 70)
    print("参数敏感性分析总结")
    print("=" * 70)

    if sensitivity:
        print("\n参数敏感性排序（从高到低）:")
        for i, (param, data) in enumerate(sensitivity['sorted_params'], 1):
            print(f"  {i}. {param}: {data['sensitivity_score']:.4f}")

        print(f"\n最敏感参数：{sensitivity['most_sensitive']}")
        print(f"最不敏感参数：{sensitivity['least_sensitive']}")

    if robust_ranges:
        print("\n稳健参数范围:")
        for param, data in robust_ranges.items():
            if 'min' in data:
                print(f"  {param}: {data['min']} - {data['max']} "
                     f"(达标率 {data['good_ratio']}%)")
            else:
                print(f"  {param}: {data['values']} (达标率 {data['good_ratio']}%)")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
