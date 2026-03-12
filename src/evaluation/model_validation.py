#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
模型验证模块
用于验证策略的稳健性和统计显著性

功能：
1. 过拟合检测 - Deflated Sharpe Ratio
2. 蒙特卡洛模拟 - 策略稳健性验证
3. 市场状态分场景验证 - 牛/熊/震荡市场
4. 交易成本敏感性分析
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Callable
from datetime import datetime
from loguru import logger
from scipy import stats
import warnings
warnings.filterwarnings('ignore')


class ModelValidator:
    """
    模型验证器

    功能：
    1. 过拟合检测 - Deflated Sharpe Ratio
    2. 蒙特卡洛模拟 - 策略稳健性验证
    3. 市场状态分场景验证
    4. 交易成本敏感性分析
    """

    def __init__(self, returns: pd.Series, benchmark_returns: pd.Series = None):
        """
        初始化验证器

        Args:
            returns: 策略收益率序列（日度）
            benchmark_returns: 基准收益率序列（用于计算 Alpha）
        """
        self.returns = returns.dropna()
        self.benchmark_returns = benchmark_returns
        logger.info("模型验证器初始化完成")

    # ==================== 1. 过拟合检测 ====================

    def deflated_sharpe_ratio(
        self,
        sharpe_ratio: float,
        n_trials: int = 100,
        annual_frequency: int = 252
    ) -> Dict:
        """
        计算 Deflated Sharpe Ratio (DSR) - 过拟合检测

        DSR 考虑了多重测试偏差，更准确地评估策略的统计显著性

        Args:
            sharpe_ratio: 观察到的夏普比率
            n_trials: 回测次数（尝试过的参数组合数）
            annual_frequency: 年交易次数

        Returns:
            DSR 分析结果
        """
        n = len(self.returns)
        skew = self.returns.skew()
        kurt = self.returns.kurtosis()

        # 计算标准误差
        se_sharpe = np.sqrt((1 + 0.5 * sharpe_ratio ** 2) / n)

        # 计算多重测试调整因子
        # 假设每次测试独立，使用 Bonferroni 校正
        adjustment_factor = np.sqrt(2 * np.log(n_trials))

        # 计算 Deflated Sharpe Ratio
        dsr = sharpe_ratio - adjustment_factor * se_sharpe

        # 计算考虑偏度和峰度的调整
        # 使用 Cornish-Fisher 展开
        cf_adjustment = (skew / 6) * (sharpe_ratio ** 2 - 1) + (kurt / 24) * sharpe_ratio
        dsr_cf = dsr - cf_adjustment

        # 计算 p 值（原假设：真实 Sharpe = 0）
        t_stat = sharpe_ratio / se_sharpe
        p_value = 1 - stats.norm.cdf(t_stat)

        # 考虑多重测试后的 p 值
        p_value_adjusted = min(1, p_value * n_trials)

        result = {
            'original_sharpe': sharpe_ratio,
            'deflated_sharpe': round(dsr, 4),
            'deflated_sharpe_cf': round(dsr_cf, 4),
            'standard_error': round(se_sharpe, 4),
            't_statistic': round(t_stat, 4),
            'p_value': round(p_value, 6),
            'p_value_adjusted': round(p_value_adjusted, 6),
            'skewness': round(skew, 4),
            'kurtosis': round(kurt, 4),
            'n_trials': n_trials,
            'is_significant': p_value_adjusted < 0.05,
            'is_robust': dsr > 0.5
        }

        logger.info(f"DSR 分析：原始 Sharpe={sharpe_ratio:.3f}, DSR={dsr:.3f}, "
                   f"显著性={'通过' if result['is_significant'] else '未通过'}")

        return result

    def probability_of_backtest_overfitting(
        self,
        sharpe_ratios: List[float],
        min_sr: float = 0.5
    ) -> Dict:
        """
        计算回测过拟合概率 (PBO)

        Args:
            sharpe_ratios: 所有测试过的策略的夏普比率列表
            min_sr: 最小可接受夏普比率

        Returns:
            PBO 分析结果
        """
        if len(sharpe_ratios) < 2:
            return {'error': '需要至少 2 个策略进行 PBO 分析'}

        sr_array = np.array(sharpe_ratios)
        best_sr = np.max(sr_array)
        second_best_sr = np.sort(sr_array)[-2]

        # 计算过拟合概率
        # PBO = P(第二好的策略真实表现 > 最好的策略)
        n = len(sharpe_ratios)

        # 使用Bootstrap估计
        n_bootstrap = 1000
        bootstrap_overfit_count = 0

        for _ in range(n_bootstrap):
            # 有放回抽样
            sample = np.random.choice(sr_array, size=n, replace=True)
            best_idx = np.argmax(sample)
            # 如果最佳样本是随机选择的，可能是过拟合
            if best_idx != np.argmax(sharpe_ratios):
                bootstrap_overfit_count += 1

        pbo = bootstrap_overfit_count / n_bootstrap

        # 计算最佳策略的置信区间
        best_sr_se = np.std(sharpe_ratios) / np.sqrt(n)
        best_sr_ci = (best_sr - 1.96 * best_sr_se, best_sr + 1.96 * best_sr_se)

        result = {
            'best_sharpe': round(best_sr, 4),
            'second_best_sharpe': round(second_best_sr, 4),
            'pbo_probability': round(pbo, 4),
            'pbo_risk': '高' if pbo > 0.5 else ('中' if pbo > 0.3 else '低'),
            'confidence_interval_95': [round(best_sr_ci[0], 4), round(best_sr_ci[1], 4)],
            'n_strategies_tested': n,
            'strategies_above_threshold': sum(sr > min_sr for sr in sharpe_ratios)
        }

        logger.info(f"PBO 分析：最佳 Sharpe={best_sr:.3f}, PBO={pbo:.3f}, "
                   f"风险等级={result['pbo_risk']}")

        return result

    # ==================== 2. 蒙特卡洛模拟 ====================

    def monte_carlo_simulation(
        self,
        strategy_func: Callable,
        base_params: Dict,
        n_simulations: int = 1000,
        perturbations: Dict[str, float] = None
    ) -> Dict:
        """
        蒙特卡洛模拟 - 验证策略稳健性

        Args:
            strategy_func: 策略函数，返回收益率
            base_params: 基础参数
            n_simulations: 模拟次数
            perturbations: 参数扰动范围，如 {'lookback': 0.2} 表示±20%

        Returns:
            蒙特卡洛模拟结果
        """
        logger.info(f"开始蒙特卡洛模拟：{n_simulations} 次")

        results = []

        for i in range(n_simulations):
            # 扰动参数
            perturbed_params = base_params.copy()

            if perturbations:
                for param, range_pct in perturbations.items():
                    if param in base_params:
                        base_value = base_params[param]
                        # 在 ±range_pct 范围内随机扰动
                        perturbation = np.random.uniform(-range_pct, range_pct)
                        perturbed_params[param] = base_value * (1 + perturbation)

            try:
                # 运行策略
                result = strategy_func(perturbed_params)
                results.append(result)
            except Exception as e:
                logger.debug(f"模拟 {i+1} 失败：{e}")
                continue

        if len(results) == 0:
            return {'error': '所有模拟都失败'}

        # 统计分析
        result_df = pd.DataFrame(results)

        # 关键指标的分布
        sharpe_values = result_df.get('sharpe', pd.Series()).values
        returns_values = result_df.get('total_return', pd.Series()).values
        maxdd_values = result_df.get('max_drawdown', pd.Series()).values

        analysis = {
            'n_successful_simulations': len(results),
            'success_rate': round(len(results) / n_simulations * 100, 2),

            # 夏普比率统计
            'sharpe_mean': round(np.mean(sharpe_values), 4) if len(sharpe_values) > 0 else None,
            'sharpe_std': round(np.std(sharpe_values), 4) if len(sharpe_values) > 0 else None,
            'sharpe_median': round(np.median(sharpe_values), 4) if len(sharpe_values) > 0 else None,
            'sharpe_min': round(np.min(sharpe_values), 4) if len(sharpe_values) > 0 else None,
            'sharpe_max': round(np.max(sharpe_values), 4) if len(sharpe_values) > 0 else None,
            'sharpe_5pct': round(np.percentile(sharpe_values, 5), 4) if len(sharpe_values) > 0 else None,
            'sharpe_95pct': round(np.percentile(sharpe_values, 95), 4) if len(sharpe_values) > 0 else None,

            # 收益率统计
            'return_mean': round(np.mean(returns_values), 4) if len(returns_values) > 0 else None,
            'return_std': round(np.std(returns_values), 4) if len(returns_values) > 0 else None,
            'return_positive_ratio': round(sum(returns_values > 0) / len(returns_values) * 100, 2) if len(returns_values) > 0 else None,

            # 回撤统计
            'maxdd_mean': round(np.mean(maxdd_values), 4) if len(maxdd_values) > 0 else None,
            'maxdd_worst': round(np.min(maxdd_values), 4) if len(maxdd_values) > 0 else None,

            # 稳健性评分
            'robustness_score': self._calculate_robustness_score(sharpe_values, returns_values)
        }

        # 稳健性判断
        if analysis['sharpe_5pct'] and analysis['sharpe_5pct'] > 0:
            analysis['is_robust'] = True
            analysis['robustness_level'] = '高'
        elif analysis['sharpe_median'] and analysis['sharpe_median'] > 0:
            analysis['is_robust'] = True
            analysis['robustness_level'] = '中'
        else:
            analysis['is_robust'] = False
            analysis['robustness_level'] = '低'

        logger.info(f"蒙特卡洛完成：成功={len(results)}/{n_simulations}, "
                   f"稳健性={analysis['robustness_level']}")

        return analysis

    def _calculate_robustness_score(self, sharpe_values: np.ndarray,
                                    return_values: np.ndarray) -> float:
        """计算稳健性评分（0-100）"""
        if len(sharpe_values) == 0 or len(return_values) == 0:
            return 0

        score = 0

        # 夏普比率中位数为正（30 分）
        if np.median(sharpe_values) > 0:
            score += 30

        # 5% 分位数为正（30 分）
        if np.percentile(sharpe_values, 5) > 0:
            score += 30

        # 正收益比例 > 70%（20 分）
        positive_ratio = sum(return_values > 0) / len(return_values)
        score += min(20, positive_ratio * 20)

        # 标准差小（20 分）
        if np.std(sharpe_values) < 0.5:
            score += 20
        elif np.std(sharpe_values) < 1.0:
            score += 10

        return round(score, 1)

    # ==================== 3. 市场状态分场景验证 ====================

    def market_regime_analysis(
        self,
        prices: pd.Series,
        benchmark_prices: pd.Series = None,
        regime_threshold: float = 0.2
    ) -> Dict:
        """
        市场状态分场景验证

        Args:
            prices: 价格序列（策略净值或指数）
            benchmark_prices: 基准价格序列（用于定义市场状态）
            regime_threshold: 牛熊阈值（默认 20%）

        Returns:
            分场景分析结果
        """
        # 计算收益率
        strategy_returns = prices.pct_change().dropna()

        # 定义市场状态
        if benchmark_prices is not None:
            benchmark_returns = benchmark_prices.pct_change().dropna()
            # 使用基准定义牛熊
            rolling_benchmark = benchmark_returns.rolling(252).sum()

            bull_market = rolling_benchmark > regime_threshold
            bear_market = rolling_benchmark < -regime_threshold
            # 中间状态为震荡
            sideways_market = ~(bull_market | bear_market)
        else:
            # 使用策略自身定义
            rolling_returns = strategy_returns.rolling(252).sum()
            bull_market = rolling_returns > regime_threshold
            bear_market = rolling_returns < -regime_threshold
            sideways_market = ~(bull_market | bear_market)

        # 分场景统计
        regimes = {
            'bull': bull_market,
            'bear': bear_market,
            'sideways': sideways_market
        }

        regime_stats = {}

        for regime_name, regime_mask in regimes.items():
            regime_returns = strategy_returns[regime_mask]

            if len(regime_returns) == 0:
                regime_stats[regime_name] = {
                    'n_days': 0,
                    'n_days_pct': 0,
                    'total_return': None,
                    'annual_return': None,
                    'volatility': None,
                    'sharpe': None,
                    'max_drawdown': None
                }
                continue

            # 计算该场景下的指标
            cum_return = (1 + regime_returns).prod() - 1
            annual_return = regime_returns.mean() * 252
            volatility = regime_returns.std() * np.sqrt(252)
            sharpe = annual_return / volatility if volatility > 0 else 0

            # 计算最大回撤
            cum_returns = (1 + regime_returns).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns - running_max) / running_max
            max_dd = drawdown.min()

            regime_stats[regime_name] = {
                'n_days': int(regime_mask.sum()),
                'n_days_pct': round(regime_mask.sum() / len(regime_mask) * 100, 2),
                'total_return': round(cum_return, 4),
                'annual_return': round(annual_return, 4),
                'volatility': round(volatility, 4),
                'sharpe': round(sharpe, 4),
                'max_drawdown': round(max_dd, 4)
            }

        # 计算 Alpha（如果有基准）
        alpha = None
        beta = None
        if benchmark_prices is not None:
            try:
                # 使用整个样本期计算 Alpha/Beta
                aligned_strategy = strategy_returns.align(benchmark_returns, join='inner')[0]
                aligned_benchmark = strategy_returns.align(benchmark_returns, join='inner')[1]

                # CAPM 回归
                slope, intercept, r_value, p_value, std_err = stats.linregress(
                    aligned_benchmark, aligned_strategy
                )
                beta = slope
                # 年化 Alpha
                alpha = (intercept * 252) - beta * (benchmark_returns.mean() * 252)
            except Exception as e:
                logger.warning(f"Alpha/Beta 计算失败：{e}")

        result = {
            'regime_stats': regime_stats,
            'alpha': round(alpha, 4) if alpha else None,
            'beta': round(beta, 4) if beta else None,
            'best_regime': max(regime_stats.keys(),
                              key=lambda k: regime_stats[k]['sharpe'] if regime_stats[k]['sharpe'] else -999),
            'worst_regime': min(regime_stats.keys(),
                               key=lambda k: regime_stats[k]['sharpe'] if regime_stats[k]['sharpe'] else 999)
        }

        logger.info(f"市场状态分析：牛市={regime_stats['bull']['n_days_pct']}%, "
                   f"熊市={regime_stats['bear']['n_days_pct']}%, "
                   f"震荡={regime_stats['sideways']['n_days_pct']}%")

        return result


    def regime_robustness_score(self, regime_stats: Dict) -> float:
        """
        计算策略在不同市场状态下的稳健性评分

        Args:
            regime_stats: 市场状态统计结果

        Returns:
            稳健性评分（0-100）
        """
        score = 0

        # 牛市表现（25 分）
        bull_sharpe = regime_stats.get('bull', {}).get('sharpe') or 0
        if bull_sharpe > 0.5:
            score += 25
        elif bull_sharpe > 0:
            score += 12.5

        # 熊市表现（35 分）- 更重要
        bear_sharpe = regime_stats.get('bear', {}).get('sharpe') or 0
        if bear_sharpe > 0:
            score += 35  # 熊市能盈利
        elif bear_sharpe > -0.5:
            score += 20  # 熊市少亏
        elif bear_sharpe > -1:
            score += 10

        # 震荡市表现（20 分）
        sideways_sharpe = regime_stats.get('sideways', {}).get('sharpe') or 0
        if sideways_sharpe > 0.3:
            score += 20
        elif sideways_sharpe > 0:
            score += 10

        # 全市场覆盖（20 分）
        positive_regimes = sum(1 for r in regime_stats.values()
                              if (r.get('sharpe') or 0) > 0)
        score += positive_regimes * 7

        return min(100, round(score, 1))

    # ==================== 4. 交易成本敏感性分析 ====================

    def transaction_cost_sensitivity(
        self,
        strategy_returns: pd.Series,
        turnover_series: pd.Series = None,
        cost_scenarios: Dict[str, float] = None
    ) -> Dict:
        """
        交易成本敏感性分析

        Args:
            strategy_returns: 策略收益率（未扣除成本）
            turnover_series: 换手率序列
            cost_scenarios: 成本场景，如 {'low': 0.001, 'base': 0.003, 'high': 0.005}

        Returns:
            成本敏感性分析结果
        """
        if cost_scenarios is None:
            cost_scenarios = {
                'zero_cost': 0.0000,  # 零成本
                'low_cost': 0.0005,   # 低成本
                'base_cost': 0.0015,  # 基准成本（万 15）
                'high_cost': 0.0030,  # 高成本（万 30）
                'extreme_cost': 0.0050  # 极端成本（万 50）
            }

        if turnover_series is None:
            # 假设固定换手率
            turnover_series = pd.Series(0.1, index=strategy_returns.index)

        results = {}
        base_sr = None

        for scenario_name, cost_rate in cost_scenarios.items():
            # 计算扣除成本后的收益率
            cost_deduction = turnover_series.abs() * cost_rate
            net_returns = strategy_returns - cost_deduction

            # 计算指标
            annual_return = net_returns.mean() * 252
            volatility = net_returns.std() * np.sqrt(252)
            sharpe = annual_return / volatility if volatility > 0 else 0

            # 累计收益
            cum_return = (1 + net_returns).prod() - 1

            # 最大回撤
            cum_returns = (1 + net_returns).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns - running_max) / running_max
            max_dd = drawdown.min()

            results[scenario_name] = {
                'cost_rate': cost_rate,
                'total_cost': round((cost_deduction.sum()) * 100, 2),  # 百分比
                'annual_return': round(annual_return, 4),
                'total_return': round(cum_return, 4),
                'volatility': round(volatility, 4),
                'sharpe': round(sharpe, 4),
                'max_drawdown': round(max_dd, 4)
            }

            if base_sr is None:
                base_sr = sharpe

        # 计算成本弹性
        # 夏普比率对成本的敏感度
        cost_change = cost_scenarios['extreme_cost'] - cost_scenarios['zero_cost']
        sharpe_change = results['extreme_cost']['sharpe'] - results['zero_cost']['sharpe']
        elasticity = abs(sharpe_change / cost_change) if cost_change > 0 else 0

        # 盈亏平衡成本
        # 找到夏普比率=0 时的成本
        sharpe_values = [r['sharpe'] for r in results.values()]
        cost_values = [r['cost_rate'] for r in results.values()]

        if sharpe_values[0] > 0 and sharpe_values[-1] < 0:
            # 线性插值
            breakeven_cost = np.interp(0, sharpe_values[::-1], cost_values[::-1])
        else:
            breakeven_cost = None

        analysis = {
            'scenarios': results,
            'base_sharpe': base_sr,
            'elasticity': round(elasticity, 2),
            'breakeven_cost': round(breakeven_cost, 6) if breakeven_cost else None,
            'cost_sensitivity': '低' if elasticity < 50 else ('中' if elasticity < 100 else '高'),
            'is_robust': results.get('high_cost', {}).get('sharpe', 0) > 0.5
        }

        logger.info(f"交易成本分析：基准 Sharpe={base_sr:.3f}, "
                   f"高成本 Sharpe={results.get('high_cost', {}).get('sharpe', 0):.3f}, "
                   f"敏感性={analysis['cost_sensitivity']}")

        return analysis

    # ==================== 5. 综合验证报告 ====================

    def generate_validation_report(
        self,
        strategy_func: Callable = None,
        base_params: Dict = None,
        sharpe_ratio: float = None,
        n_trials: int = 100,
        prices: pd.Series = None,
        benchmark_prices: pd.Series = None,
        turnover: pd.Series = None
    ) -> Dict:
        """
        生成综合验证报告

        Returns:
            包含所有验证结果的综合报告
        """
        logger.info("生成综合验证报告...")

        report = {
            'summary': {},
            'overfitting_analysis': None,
            'robustness_analysis': None,
            'regime_analysis': None,
            'cost_analysis': None,
            'overall_score': 0,
            'recommendation': ''
        }

        total_score = 0
        n_tests = 0

        # 1. 过拟合检测
        if sharpe_ratio is not None:
            dsr_result = self.deflated_sharpe_ratio(sharpe_ratio, n_trials)
            report['overfitting_analysis'] = dsr_result
            if dsr_result['is_significant']:
                total_score += 25
            if dsr_result['is_robust']:
                total_score += 15
            n_tests += 2

        # 2. 蒙特卡洛稳健性
        if strategy_func is not None and base_params is not None:
            mc_result = self.monte_carlo_simulation(strategy_func, base_params)
            report['robustness_analysis'] = mc_result
            robustness_score = mc_result.get('robustness_score', 0)
            total_score += robustness_score * 0.3  # 30 分权重
            n_tests += 1

        # 3. 市场状态分析
        if prices is not None:
            regime_result = self.market_regime_analysis(prices, benchmark_prices)
            report['regime_analysis'] = regime_result
            regime_robustness = self.regime_robustness_score(regime_result['regime_stats'])
            total_score += regime_robustness * 0.3  # 30 分权重
            n_tests += 1

        # 4. 交易成本分析
        if strategy_func is not None and base_params is not None:
            # 模拟生成收益率
            try:
                returns = strategy_func(base_params)
                if isinstance(returns, dict):
                    returns = returns.get('returns', pd.Series())
                cost_result = self.transaction_cost_sensitivity(returns, turnover)
                report['cost_analysis'] = cost_result
                if cost_result['is_robust']:
                    total_score += 15
                n_tests += 1
            except Exception as e:
                logger.warning(f"成本分析失败：{e}")

        # 计算总分
        report['overall_score'] = round(total_score, 1)
        report['max_score'] = 100

        # 给出建议
        if total_score >= 80:
            report['recommendation'] = '策略验证通过，可以实盘'
        elif total_score >= 60:
            report['recommendation'] = '策略基本稳健，建议小仓位测试'
        elif total_score >= 40:
            report['recommendation'] = '策略稳健性一般，需要进一步优化'
        else:
            report['recommendation'] = '策略验证未通过，不建议实盘'

        logger.info(f"验证报告完成：总分={total_score}/100, 建议={report['recommendation']}")

        return report


def main():
    """测试函数"""
    print("=" * 60)
    print("模型验证模块测试")
    print("=" * 60)

    # 生成模拟数据
    np.random.seed(42)
    n_days = 1000

    # 模拟策略收益（有 Alpha）
    strategy_returns = pd.Series(
        np.random.normal(0.0005, 0.02, n_days),
        index=pd.date_range('2020-01-01', periods=n_days, freq='D')
    )

    # 模拟基准收益
    benchmark_returns = pd.Series(
        np.random.normal(0.0003, 0.015, n_days),
        index=strategy_returns.index
    )

    # 模拟价格序列
    prices = (1 + strategy_returns).cumprod()
    benchmark_prices = (1 + benchmark_returns).cumprod()

    # 初始化验证器
    validator = ModelValidator(strategy_returns, benchmark_returns)

    # 计算基础指标
    annual_return = strategy_returns.mean() * 252
    volatility = strategy_returns.std() * np.sqrt(252)
    sharpe = annual_return / volatility

    print(f"\n基础指标:")
    print(f"  年化收益：{annual_return:.2%}")
    print(f"  波动率：{volatility:.2%}")
    print(f"  夏普比率：{sharpe:.3f}")

    # 1. 过拟合检测
    print("\n" + "=" * 60)
    print("1. 过拟合检测 (Deflated Sharpe Ratio)")
    print("=" * 60)
    dsr_result = validator.deflated_sharpe_ratio(sharpe, n_trials=50)
    print(f"  原始夏普：{dsr_result['original_sharpe']:.4f}")
    print(f"  DSR: {dsr_result['deflated_sharpe']:.4f}")
    print(f"  DSR(校正): {dsr_result['deflated_sharpe_cf']:.4f}")
    print(f"  p 值：{dsr_result['p_value']:.6f}")
    print(f"  调整后 p 值：{dsr_result['p_value_adjusted']:.6f}")
    print(f"  统计显著：{'是' if dsr_result['is_significant'] else '否'}")
    print(f"  稳健：{'是' if dsr_result['is_robust'] else '否'}")

    # 2. 蒙特卡洛模拟
    print("\n" + "=" * 60)
    print("2. 蒙特卡洛模拟")
    print("=" * 60)

    def dummy_strategy(params):
        """模拟策略函数"""
        lookback = int(params.get('lookback', 20))
        # 模拟收益
        returns = pd.Series(np.random.normal(0.0005, 0.02, 500))
        return {
            'sharpe': np.random.normal(1.0, 0.3),
            'total_return': np.random.normal(0.1, 0.05),
            'max_drawdown': np.random.normal(-0.15, 0.05)
        }

    mc_result = validator.monte_carlo_simulation(
        dummy_strategy,
        {'lookback': 20},
        n_simulations=100,
        perturbations={'lookback': 0.3}
    )

    print(f"  成功模拟：{mc_result['n_successful_simulations']}次")
    print(f"  夏普均值：{mc_result['sharpe_mean']:.4f}")
    print(f"  夏普标准差：{mc_result['sharpe_std']:.4f}")
    print(f"  夏普 5% 分位：{mc_result['sharpe_5pct']:.4f}")
    print(f"  稳健性评分：{mc_result['robustness_score']:.1f}/100")
    print(f"  稳健性等级：{mc_result['robustness_level']}")

    # 3. 市场状态分析
    print("\n" + "=" * 60)
    print("3. 市场状态分场景验证")
    print("=" * 60)
    regime_result = validator.market_regime_analysis(prices, benchmark_prices)

    for regime_name, stats in regime_result['regime_stats'].items():
        print(f"\n  {regime_name.upper()}市场:")
        print(f"    天数占比：{stats['n_days_pct']}%")
        print(f"    年化收益：{stats['annual_return']:.2%}" if stats['annual_return'] else "    年化收益：N/A")
        print(f"    夏普比率：{stats['sharpe']:.4f}" if stats['sharpe'] else "    夏普比率：N/A")
        print(f"    最大回撤：{stats['max_drawdown']:.2%}" if stats['max_drawdown'] else "    最大回撤：N/A")

    print(f"\n  Alpha: {regime_result['alpha']}" if regime_result['alpha'] else "\n  Alpha: N/A")
    print(f"  Beta: {regime_result['beta']:.4f}" if regime_result['beta'] else "  Beta: N/A")
    print(f"  最佳市场：{regime_result['best_regime']}")
    print(f"  最差市场：{regime_result['worst_regime']}")

    # 4. 交易成本分析
    print("\n" + "=" * 60)
    print("4. 交易成本敏感性分析")
    print("=" * 60)
    cost_result = validator.transaction_cost_sensitivity(strategy_returns)

    print(f"\n  成本弹性：{cost_result['elasticity']}")
    print(f"  盈亏平衡成本：{cost_result['breakeven_cost']:.6f}" if cost_result['breakeven_cost'] else "  盈亏平衡成本：N/A")
    print(f"  成本敏感性：{cost_result['cost_sensitivity']}")

    print("\n  各场景对比:")
    for name, stats in cost_result['scenarios'].items():
        print(f"    {name}: Sharpe={stats['sharpe']:.4f}, "
              f"收益={stats['total_return']:.2%}, "
              f"成本={stats['total_cost']:.2f}%")

    # 5. 综合验证报告
    print("\n" + "=" * 60)
    print("5. 综合验证报告")
    print("=" * 60)

    report = validator.generate_validation_report(
        strategy_func=dummy_strategy,
        base_params={'lookback': 20},
        sharpe_ratio=sharpe,
        n_trials=50,
        prices=prices,
        benchmark_prices=benchmark_prices
    )

    print(f"\n  总分：{report['overall_score']}/100")
    print(f"  建议：{report['recommendation']}")

    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
