"""
策略绩效评估模块
评估策略表现，计算夏普比率、最大回撤等指标
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from loguru import logger
from datetime import datetime, timedelta


class StrategyEvaluator:
    """策略绩效评估器"""

    def __init__(self, risk_free_rate: float = 0.03):
        """
        初始化评估器

        Args:
            risk_free_rate: 无风险利率 (年化)
        """
        self.risk_free_rate = risk_free_rate
        logger.info(f"策略评估器初始化完成，无风险利率：{risk_free_rate:.1%}")

    def evaluate(
        self,
        portfolio_values: pd.Series,
        benchmark_values: pd.Series = None,
        strategy_name: str = "strategy",
    ) -> Dict[str, Any]:
        """
        评估策略绩效

        Args:
            portfolio_values: 组合净值序列
            benchmark_values: 基准净值序列 (可选)
            strategy_name: 策略名称

        Returns:
            绩效指标字典
        """
        if portfolio_values.empty:
            return {"error": "无数据"}

        # 计算日收益率
        returns = portfolio_values.pct_change().dropna()

        metrics = {
            "strategy_name": strategy_name,
            "start_date": str(portfolio_values.index[0])
            if len(portfolio_values) > 0
            else "",
            "end_date": str(portfolio_values.index[-1])
            if len(portfolio_values) > 0
            else "",
            "total_days": len(portfolio_values),
        }

        # 1. 总收益
        total_return = (portfolio_values.iloc[-1] / portfolio_values.iloc[0]) - 1
        metrics["total_return"] = round(total_return, 4)

        # 2. 年化收益
        days = len(portfolio_values)
        if days > 0:
            annual_return = (1 + total_return) ** (252 / days) - 1
        else:
            annual_return = 0
        metrics["annual_return"] = round(annual_return, 4)

        # 3. 波动率
        if len(returns) > 0:
            daily_vol = returns.std()
            annual_vol = daily_vol * np.sqrt(252)
        else:
            annual_vol = 0
        metrics["volatility"] = round(annual_vol, 4)

        # 4. 夏普比率
        if annual_vol > 0:
            sharpe = (annual_return - self.risk_free_rate) / annual_vol
        else:
            sharpe = 0
        metrics["sharpe"] = round(sharpe, 2)

        # 5. 最大回撤
        cummax = portfolio_values.cummax()
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min()
        metrics["max_drawdown"] = round(max_drawdown, 4)

        # 6. 卡玛比率 (收益/回撤)
        if max_drawdown != 0:
            calmar = annual_return / abs(max_drawdown)
        else:
            calmar = 0
        metrics["calmar"] = round(calmar, 2)

        # 7. 胜率
        if len(returns) > 0:
            win_rate = (returns > 0).sum() / len(returns)
        else:
            win_rate = 0
        metrics["win_rate"] = round(win_rate, 4)

        # 8. 盈亏比
        if len(returns) > 0:
            avg_win = returns[returns > 0].mean() if (returns > 0).any() else 0
            avg_loss = abs(returns[returns < 0].mean()) if (returns < 0).any() else 0
            if avg_loss > 0:
                profit_loss_ratio = avg_win / avg_loss
            else:
                profit_loss_ratio = 0
        else:
            profit_loss_ratio = 0
        metrics["profit_loss_ratio"] = round(profit_loss_ratio, 2)

        # 9. 相对基准超额收益 (如果有基准)
        if benchmark_values is not None and not benchmark_values.empty:
            benchmark_return = (
                benchmark_values.iloc[-1] / benchmark_values.iloc[0]
            ) - 1
            metrics["benchmark_return"] = round(benchmark_return, 4)
            metrics["alpha"] = round(total_return - benchmark_return, 4)

            # 计算 Beta
            if len(returns) > 1:
                benchmark_returns = benchmark_values.pct_change().dropna()
                if len(benchmark_returns) == len(returns):
                    covariance = returns.cov(benchmark_returns)
                    benchmark_var = benchmark_returns.var()
                    if benchmark_var > 0:
                        beta = covariance / benchmark_var
                    else:
                        beta = 0
                else:
                    beta = 0
            else:
                beta = 0
            metrics["beta"] = round(beta, 2)

        # 10. 综合评分
        score = self._calculate_composite_score(metrics)
        metrics["composite_score"] = round(score, 1)

        logger.info(
            f"{strategy_name} 评估完成：夏普={metrics['sharpe']:.2f}, 回撤={metrics['max_drawdown']:.1%}, 评分={score:.1f}"
        )

        return metrics

    def _calculate_composite_score(self, metrics: Dict) -> float:
        """
        计算综合评分 (0-100)

        评分权重:
        - 夏普比率：30%
        - 年化收益：25%
        - 最大回撤：25%
        - 胜率：10%
        - 盈亏比：10%
        """
        # 夏普评分 (0-100)
        sharpe = metrics.get("sharpe", 0)
        sharpe_score = min(100, max(0, (sharpe + 1) * 50))

        # 收益评分 (0-100)
        annual = metrics.get("annual_return", 0)
        return_score = min(100, max(0, (annual + 0.2) * 250))

        # 回撤评分 (0-100)
        maxdd = metrics.get("max_drawdown", 0)
        drawdown_score = min(100, max(0, (1 + maxdd) * 100))

        # 胜率评分 (0-100)
        win = metrics.get("win_rate", 0)
        win_score = win * 100

        # 盈亏比评分 (0-100)
        pl = metrics.get("profit_loss_ratio", 0)
        pl_score = min(100, pl * 50)

        # 加权综合
        total = (
            sharpe_score * 0.30
            + return_score * 0.25
            + drawdown_score * 0.25
            + win_score * 0.10
            + pl_score * 0.10
        )

        return min(100, max(0, total))

    def compare_strategies(
        self, strategy_metrics: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        对比多个策略的绩效

        Args:
            strategy_metrics: 策略绩效指标列表

        Returns:
            对比 DataFrame
        """
        df = pd.DataFrame(strategy_metrics)

        # 按综合评分排序
        if "composite_score" in df.columns:
            df = df.sort_values("composite_score", ascending=False)

        # 选择关键指标
        key_cols = [
            "strategy_name",
            "total_return",
            "annual_return",
            "sharpe",
            "max_drawdown",
            "calmar",
            "win_rate",
            "composite_score",
        ]

        available_cols = [c for c in key_cols if c in df.columns]
        return df[available_cols].reset_index(drop=True)
