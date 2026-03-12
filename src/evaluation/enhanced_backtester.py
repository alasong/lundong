"""
增强的回测模块
在原有基础上添加：
- Benchmark 对比（沪深 300）
- 分场景分析（牛/熊/震荡）
- 收益归因分析
- 更完善的报告输出
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from models.predictor import UnifiedPredictor
from evaluation.backtester import Backtester
from risk.transaction_cost import TransactionCostModel, estimate_impact_on_returns


class EnhancedBacktester(Backtester):
    """增强版回测器"""

    def __init__(
        self,
        initial_capital: float = 1000000.0,
        commission_rate: float = 0.00025,
        stamp_tax_rate: float = 0.0005
    ):
        """
        初始化增强回测器

        Args:
            initial_capital: 初始资金
            commission_rate: 佣金率
            stamp_tax_rate: 印花税率
        """
        super().__init__(initial_capital)
        self.cost_model = TransactionCostModel(
            commission_rate=commission_rate,
            stamp_tax_rate=stamp_tax_rate
        )
        logger.info("增强回测器初始化完成")

    def run_walk_forward(
        self,
        concept_data: pd.DataFrame,
        train_windows: int = 20,
        test_windows: int = 5,
        step: int = 5,
        benchmark_data: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        运行滚动回测（带 Benchmark 对比）

        Args:
            concept_data: 概念板块数据
            train_windows: 训练窗口长度（月）
            test_windows: 测试窗口长度（月）
            step: 滚动步长（月）
            benchmark_data: Benchmark 数据（如沪深 300）

        Returns:
            回测结果（包含 Benchmark 对比）
        """
        logger.info(f"开始增强滚动回测：训练={train_windows}月，测试={test_windows}月")

        # 按日期分组
        dates = sorted(concept_data['trade_date'].unique())

        # 计算切分点
        trading_days_per_month = self.TRADING_DAYS_PER_MONTH
        total_months = len(dates) // trading_days_per_month

        if total_months < train_windows + test_windows:
            logger.warning("数据量不足，无法进行回测")
            return {"error": "数据量不足"}

        results = []
        metrics_history = []
        benchmark_returns = []

        # 如果没有传入 Benchmark 数据，尝试从数据库加载沪深 300
        if benchmark_data is None:
            benchmark_data = self._load_benchmark_data()

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

            # 计算回测指标（增强版）
            fold_metrics = self._calculate_enhanced_metrics(
                predictions, test_data, benchmark_data
            )
            fold_metrics['fold'] = i // step + 1
            results.append(fold_metrics)
            metrics_history.append(fold_metrics)

            # 计算 Benchmark 收益
            if benchmark_data is not None and not benchmark_data.empty:
                bench_period = benchmark_data[
                    (benchmark_data['trade_date'] >= dates[train_end_idx]) &
                    (benchmark_data['trade_date'] < dates[test_end_idx])
                ]
                if not bench_period.empty:
                    bench_return = bench_period['pct_chg'].sum()
                    benchmark_returns.append(bench_return)

            logger.info(f"折叠 {i//step + 1}: IC={fold_metrics['ic']:.4f}, "
                       f"Sharpe={fold_metrics['sharpe']:.2f}, "
                       f"超额={fold_metrics.get('excess_return', 0):.2f}%")

        # 汇总结果
        if not results:
            return {"error": "回测失败"}

        avg_metrics = self._aggregate_metrics(results, benchmark_returns)

        logger.info(f"回测完成：{len(results)} 个折叠")
        logger.info(f"平均 IC={avg_metrics['avg_ic']:.4f}, RankIC={avg_metrics['avg_rank_ic']:.4f}")
        logger.info(f"平均 Sharpe={avg_metrics['avg_sharpe']:.2f}")

        return {
            'fold_results': results,
            'avg_metrics': avg_metrics,
            'metrics_history': metrics_history,
            'benchmark_comparison': self._format_benchmark_comparison(avg_metrics, benchmark_returns)
        }

    def _load_benchmark_data(self) -> Optional[pd.DataFrame]:
        """加载 Benchmark 数据（沪深 300）"""
        try:
            from data.database import get_database
            db = get_database()

            # 尝试加载沪深 300 数据
            benchmark_codes = ['000300.SH', '399300.SZ']  # 沪深 300 的两种代码

            for code in benchmark_codes:
                df = db.get_stock_data(code, days=3650)  # 10 年数据
                if not df.empty:
                    logger.info(f"加载 Benchmark 数据：{code}, {len(df)} 条记录")
                    return df

            logger.warning("未找到 Benchmark 数据，将跳过 Benchmark 对比")
            return None

        except Exception as e:
            logger.error(f"加载 Benchmark 数据失败：{e}")
            return None

    def _calculate_enhanced_metrics(
        self,
        predictions: pd.DataFrame,
        actual_data: pd.DataFrame,
        benchmark_data: Optional[pd.DataFrame] = None
    ) -> Dict:
        """计算增强版回测指标"""
        # 基础指标
        base_metrics = self._calculate_metrics(predictions, actual_data)

        # 合并预测和实际值
        merged = predictions.merge(
            actual_data[['concept_code', 'trade_date', 'pct_chg']],
            on=['concept_code', 'trade_date'],
            how='left'
        )

        if merged.empty or 'pct_chg' not in merged.columns:
            return base_metrics

        # 模拟交易：每天买入预测 TOP10
        merged = merged.sort_values(['trade_date', 'combined_score'], ascending=[True, False])
        merged['rank'] = merged.groupby('trade_date')['combined_score'].rank(ascending=False)
        top10 = merged[merged['rank'] <= 10]

        # 计算组合收益
        portfolio_returns = top10.groupby('trade_date')['pct_chg'].mean()

        if len(portfolio_returns) < 2:
            return base_metrics

        # 累计收益
        cumulative = (1 + portfolio_returns / 100).cumprod()
        total_return = (cumulative.iloc[-1] - 1) * 100

        # 估算交易成本影响（假设年换手率 10 倍）
        annual_turnover = 10
        cost_impact = estimate_impact_on_returns(
            annual_turnover,
            self.cost_model.commission_rate,
            self.cost_model.stamp_tax_rate
        )
        # 成本拖累（年化）
        cost_drag = cost_impact / 12  # 月度成本

        # 净收益（扣除成本）
        net_return = total_return - cost_drag

        # 计算 Benchmark 收益
        benchmark_return = 0
        if benchmark_data is not None and not benchmark_data.empty:
            bench_period = benchmark_data[
                benchmark_data['trade_date'].isin(portfolio_returns.index)
            ]
            if not bench_period.empty:
                benchmark_return = bench_period['pct_chg'].sum()

        # 超额收益
        excess_return = net_return - benchmark_return

        # 信息比率（超额收益 / 跟踪误差）
        if benchmark_data is not None and not benchmark_data.empty:
            # 合并计算每日超额收益
            merged_bench = portfolio_returns.to_frame('portfolio').merge(
                benchmark_data[['trade_date', 'pct_chg']].set_index('trade_date'),
                left_index=True, right_index=True, how='inner'
            )
            if len(merged_bench) > 1:
                merged_bench['excess'] = merged_bench['portfolio'] - merged_bench['pct_chg']
                tracking_error = merged_bench['excess'].std() * np.sqrt(252)
                information_ratio = (merged_bench['excess'].mean() * 252) / tracking_error if tracking_error > 0 else 0
            else:
                information_ratio = 0
        else:
            information_ratio = 0

        # 更新指标
        base_metrics.update({
            'total_return': net_return,  # 扣除成本后的收益
            'gross_return': total_return,  # 扣除成本前的收益
            'benchmark_return': benchmark_return,
            'excess_return': excess_return,
            'information_ratio': information_ratio,
            'cost_drag': cost_drag,
        })

        return base_metrics

    def _aggregate_metrics(
        self,
        results: List[Dict],
        benchmark_returns: List[float]
    ) -> Dict:
        """汇总回测指标"""
        avg_metrics = {
            'avg_ic': np.mean([r['ic'] for r in results]),
            'avg_rank_ic': np.mean([r['rank_ic'] for r in results]),
            'avg_sharpe': np.mean([r['sharpe'] for r in results]),
            'avg_return': np.mean([r.get('total_return', r['total_return']) for r in results]),
            'max_drawdown': np.min([r['max_drawdown'] for r in results]),
            'win_rate': np.mean([r['win_rate'] for r in results]),
            'folds': len(results)
        }

        # Benchmark 相关指标
        if benchmark_returns:
            avg_metrics['avg_benchmark_return'] = np.mean(benchmark_returns)
            avg_metrics['avg_excess_return'] = avg_metrics['avg_return'] - avg_metrics['avg_benchmark_return']
            # 胜率（相对于 Benchmark）
            beat_benchmark = sum(1 for r, br in zip(results, benchmark_returns)
                               if r.get('total_return', r['total_return']) > br)
            avg_metrics['beat_benchmark_rate'] = beat_benchmark / len(results)
        else:
            avg_metrics['avg_benchmark_return'] = 0
            avg_metrics['avg_excess_return'] = 0
            avg_metrics['beat_benchmark_rate'] = 0

        return avg_metrics

    def _format_benchmark_comparison(
        self,
        avg_metrics: Dict,
        benchmark_returns: List[float]
    ) -> Dict:
        """格式化 Benchmark 对比结果"""
        return {
            'strategy_return': avg_metrics['avg_return'],
            'benchmark_return': avg_metrics.get('avg_benchmark_return', 0),
            'excess_return': avg_metrics.get('avg_excess_return', 0),
            'beat_rate': avg_metrics.get('beat_benchmark_rate', 0),
            'information_ratio': avg_metrics.get('information_ratio', 0)
        }

    def analyze_by_market_regime(
        self,
        concept_data: pd.DataFrame,
        benchmark_data: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        分市场状态分析（牛/熊/震荡）

        Args:
            concept_data: 概念板块数据
            benchmark_data: Benchmark 数据

        Returns:
            分场景分析结果
        """
        logger.info("执行分市场状态分析...")

        # 如果没有传入 Benchmark，尝试加载
        if benchmark_data is None:
            benchmark_data = self._load_benchmark_data()

        if benchmark_data is None or benchmark_data.empty:
            logger.warning("缺少 Benchmark 数据，无法进行市场状态分析")
            return {}

        # 识别市场状态
        benchmark_data = benchmark_data.sort_values('trade_date')
        market_states = self._identify_market_states(benchmark_data)

        # 合并数据
        merged = concept_data.merge(
            market_states[['trade_date', 'market_state']],
            on='trade_date',
            how='left'
        )

        # 分场景统计
        results = {}
        for state in ['bull', 'bear', 'sideways']:
            state_data = merged[merged['market_state'] == state]
            if state_data.empty:
                continue

            # 计算该市场状态下的表现
            state_metrics = self._calculate_state_metrics(state_data)
            results[state] = state_metrics

        logger.info(f"分场景分析完成：牛={results.get('bull', {}).get('count', 0)}天，"
                   f"熊={results.get('bear', {}).get('count', 0)}天，"
                   f"震荡={results.get('sideways', {}).get('count', 0)}天")

        return results

    def _identify_market_states(
        self,
        benchmark_data: pd.DataFrame,
        window: int = 60
    ) -> pd.DataFrame:
        """
        识别市场状态

        Args:
            benchmark_data: Benchmark 数据
            window: 滚动窗口天数

        Returns:
            包含 market_state 列的 DataFrame
        """
        df = benchmark_data.sort_values('trade_date').copy()

        # 计算滚动收益
        df['rolling_return'] = df['pct_chg'].rolling(window=window).sum()

        # 计算波动率
        df['rolling_vol'] = df['pct_chg'].rolling(window=window).std() * np.sqrt(252)

        # 市场状态判断
        def classify_state(row):
            ret = row['rolling_return']
            vol = row['rolling_vol']

            if pd.isna(ret) or pd.isna(vol):
                return 'unknown'

            # 收益率 > 波动率：牛市
            if ret > vol * 0.5:
                return 'bull'
            # 收益率 < -波动率：熊市
            elif ret < -vol * 0.5:
                return 'bear'
            # 否则：震荡市
            else:
                return 'sideways'

        df['market_state'] = df.apply(classify_state, axis=1)

        return df[['trade_date', 'market_state']]

    def _calculate_state_metrics(
        self,
        state_data: pd.DataFrame
    ) -> Dict:
        """计算特定市场状态下的指标"""
        if state_data.empty:
            return {}

        # 按日期分组计算收益
        daily_returns = state_data.groupby('trade_date')['pct_chg'].mean()

        if len(daily_returns) < 2:
            return {'count': len(daily_returns)}

        # 累计收益
        cumulative = (1 + daily_returns / 100).cumprod()
        total_return = (cumulative.iloc[-1] - 1) * 100

        # 夏普比率
        sharpe = np.sqrt(252) * daily_returns.mean() / daily_returns.std() if daily_returns.std() > 0 else 0

        # 胜率
        win_rate = (daily_returns > 0).mean()

        # 最大回撤
        rolling_max = cumulative.cummax()
        drawdown = (cumulative - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        return {
            'count': len(daily_returns),
            'total_return': total_return,
            'sharpe': sharpe,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown,
            'avg_daily_return': daily_returns.mean()
        }


def print_enhanced_report(backtest_result: Dict) -> str:
    """
    打印增强版回测报告

    Args:
        backtest_result: 回测结果

    Returns:
        报告字符串
    """
    lines = []
    lines.append("=" * 70)
    lines.append("增强版回测报告")
    lines.append("=" * 70)

    if 'avg_metrics' not in backtest_result:
        lines.append("回测失败或结果异常")
        return "\n".join(lines)

    m = backtest_result['avg_metrics']
    bc = backtest_result.get('benchmark_comparison', {})

    lines.append("")
    lines.append("【基础指标】")
    lines.append(f"  回测折叠数：{m['folds']}")
    lines.append(f"  平均 IC: {m['avg_ic']:.4f}")
    lines.append(f"  平均 RankIC: {m['avg_rank_ic']:.4f}")
    lines.append(f"  平均 Sharpe: {m['avg_sharpe']:.2f}")
    lines.append(f"  胜率：{m['win_rate']:.1%}")
    lines.append(f"  最大回撤：{m['max_drawdown']:.1%}")

    lines.append("")
    lines.append("【收益对比】")
    lines.append(f"  策略年化收益：{m['avg_return']:.1f}%")
    lines.append(f"  Benchmark 收益：{bc.get('benchmark_return', 0):.1f}%")
    lines.append(f"  超额收益：{bc.get('excess_return', 0):.1f}%")
    lines.append(f"  信息比率：{bc.get('information_ratio', 0):.2f}")
    lines.append(f"  跑赢 Benchmark 概率：{bc.get('beat_rate', 0):.1%}")

    lines.append("")
    lines.append("【成本估算】")
    if 'cost_drag' in backtest_result.get('fold_results', [{}])[0]:
        avg_cost = np.mean([r.get('cost_drag', 0) for r in backtest_result['fold_results']])
        lines.append(f"  交易成本拖累（月均）：{avg_cost:.2f}%")

    lines.append("")
    lines.append("=" * 70)

    report = "\n".join(lines)
    logger.info("回测报告生成完成")
    return report


def main():
    """主函数"""
    from agents.data_agent import DataAgent

    print("=" * 70)
    print("增强版回测模块测试")
    print("=" * 70)

    # 加载数据
    logger.info("加载历史数据...")
    data_agent = DataAgent()
    data = data_agent.execute(task="history", start_date="20230101", end_date="20241231")

    concept_data = data.get('result', {}).get('concept')
    if concept_data is None or concept_data.empty:
        logger.error("无法加载数据")
        return

    # 运行增强回测
    backtester = EnhancedBacktester()
    results = backtester.run_walk_forward(
        concept_data,
        train_windows=12,
        test_windows=3,
        step=3
    )

    # 输出报告
    report = print_enhanced_report(results)
    print("\n" + report)

    # 分场景分析
    regime_results = backtester.analyze_by_market_regime(concept_data)
    if regime_results:
        print("\n【分市场状态表现】")
        state_names = {'bull': '牛市', 'bear': '熊市', 'sideways': '震荡市'}
        for state, metrics in regime_results.items():
            name = state_names.get(state, state)
            if metrics:
                print(f"  {name} ({metrics.get('count', 0)}天):")
                print(f"    收益：{metrics.get('total_return', 0):.1f}%, "
                      f"Sharpe: {metrics.get('sharpe', 0):.2f}, "
                      f"胜率：{metrics.get('win_rate', 0):.1%}")


if __name__ == "__main__":
    main()
