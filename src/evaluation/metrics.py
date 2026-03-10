"""
模型评估模块
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class ModelEvaluator:
    """模型评估器"""

    def __init__(self):
        pass

    def evaluate_prediction(
        self,
        predictions: pd.DataFrame,
        actuals: pd.DataFrame,
        horizon: str = "short"
    ) -> Dict:
        """
        评估预测结果

        Args:
            predictions: 预测结果
            actuals: 实际结果
            horizon: 预测周期 short/mid/long
        """
        # 合并预测和实际值
        pred_col = {
            "short": "predicted_pct_chg",
            "mid": "predicted_pct_5d",
            "long": "predicted_pct_20d"
        }.get(horizon, "predicted_pct_chg")

        actual_col = {
            "short": "pct_chg",
            "mid": "pct_5d_sum",
            "long": "pct_20d_sum"
        }.get(horizon, "pct_chg")

        merged = predictions.merge(
            actuals[["concept_code", "trade_date", actual_col]],
            on=["concept_code", "trade_date"],
            how="inner"
        )

        if merged.empty:
            return {}

        y_pred = merged[pred_col].values
        y_true = merged[actual_col].values

        # 回归指标
        mse = np.mean((y_pred - y_true) ** 2)
        mae = np.mean(np.abs(y_pred - y_true))
        rmse = np.sqrt(mse)

        # 方向准确率
        direction_acc = np.mean((y_pred > 0) == (y_true > 0))

        # 相关系数
        corr = np.corrcoef(y_pred, y_true)[0, 1]

        # R²
        ss_res = np.sum((y_true - y_pred) ** 2)
        ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        metrics = {
            "mse": mse,
            "mae": mae,
            "rmse": rmse,
            "direction_accuracy": direction_acc,
            "correlation": corr,
            "r2": r2,
            "sample_count": len(merged)
        }

        return metrics

    def evaluate_hotspot_prediction(
        self,
        predicted_hotspots: pd.DataFrame,
        actual_hotspots: pd.DataFrame,
        top_n: int = 10
    ) -> Dict:
        """
        评估热点预测准确率

        Args:
            predicted_hotspots: 预测的热点
            actual_hotspots: 实际热点
            top_n: 评估前N个热点
        """
        results = []

        for trade_date in predicted_hotspots["trade_date"].unique():
            pred = predicted_hotspots[predicted_hotspots["trade_date"] == trade_date]
            actual = actual_hotspots[actual_hotspots["trade_date"] == trade_date]

            if actual.empty:
                continue

            # 获取预测和实际的前N个热点
            pred_top = set(pred.nlargest(top_n, "predicted_score")["concept_code"])
            actual_top = set(actual.nlargest(top_n, "hotspot_score")["concept_code"])

            # 计算命中率
            hit_count = len(pred_top & actual_top)
            hit_rate = hit_count / top_n

            results.append({
                "trade_date": trade_date,
                "hit_count": hit_count,
                "hit_rate": hit_rate
            })

        if not results:
            return {}

        results_df = pd.DataFrame(results)

        return {
            "avg_hit_rate": results_df["hit_rate"].mean(),
            "median_hit_rate": results_df["hit_rate"].median(),
            "min_hit_rate": results_df["hit_rate"].min(),
            "max_hit_rate": results_df["hit_rate"].max(),
            "sample_count": len(results)
        }

    def evaluate_rotation_prediction(
        self,
        predicted_rotation: pd.DataFrame,
        actual_rotation: pd.DataFrame
    ) -> Dict:
        """
        评估轮动预测准确率

        Args:
            predicted_rotation: 预测的轮动
            actual_rotation: 实际轮动
        """
        # 合并数据
        merged = predicted_rotation.merge(
            actual_rotation,
            on=["trade_date", "from_concept"],
            how="inner"
        )

        if merged.empty:
            return {}

        # 计算预测的轮动目标是否正确
        correct = merged["predicted_to_concept"] == merged["actual_to_concept"]

        return {
            "rotation_accuracy": correct.mean(),
            "sample_count": len(merged)
        }

    def compute_backtest_returns(
        self,
        predictions: pd.DataFrame,
        actuals: pd.DataFrame,
        top_n: int = 5,
        horizon: str = "mid"
    ) -> Dict:
        """
        回测预测策略收益

        Args:
            predictions: 预测结果
            actuals: 实际结果
            top_n: 持仓数量
            horizon: 预测周期
        """
        pred_col = {
            "short": "predicted_pct_chg",
            "mid": "predicted_pct_5d",
            "long": "predicted_pct_20d"
        }.get(horizon, "predicted_pct_5d")

        actual_col = {
            "short": "pct_chg",
            "mid": "pct_5d_sum",
            "long": "pct_20d_sum"
        }.get(horizon, "pct_5d_sum")

        returns = []

        for trade_date in predictions["trade_date"].unique():
            pred = predictions[predictions["trade_date"] == trade_date]
            actual = actuals[actuals["trade_date"] == trade_date]

            # 选择预测涨幅最大的N个
            selected = pred.nlargest(top_n, pred_col)

            # 计算实际收益
            merged = selected.merge(
                actual[["concept_code", actual_col]],
                on="concept_code",
                how="left"
            )

            daily_return = merged[actual_col].mean()
            returns.append({
                "trade_date": trade_date,
                "return": daily_return
            })

        if not returns:
            return {}

        returns_df = pd.DataFrame(returns)

        # 计算累计收益
        returns_df["cumulative_return"] = (1 + returns_df["return"] / 100).cumprod() - 1

        # 计算夏普比率
        avg_return = returns_df["return"].mean()
        std_return = returns_df["return"].std()
        sharpe = avg_return / std_return * np.sqrt(252 / 5) if std_return > 0 else 0  # 假设周频

        # 最大回撤
        cumulative = returns_df["cumulative_return"]
        running_max = cumulative.cummax()
        drawdown = cumulative - running_max
        max_drawdown = drawdown.min()

        return {
            "total_return": returns_df["cumulative_return"].iloc[-1],
            "avg_return": avg_return,
            "std_return": std_return,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_drawdown,
            "win_rate": (returns_df["return"] > 0).mean(),
            "sample_count": len(returns)
        }

    def generate_evaluation_report(
        self,
        all_metrics: Dict,
        save_path: Optional[str] = None
    ) -> str:
        """
        生成评估报告

        Args:
            all_metrics: 所有评估指标
            save_path: 保存路径
        """
        report_lines = [
            "# 模型评估报告",
            f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n## 短期预测评估\n"
        ]

        if "short_term" in all_metrics:
            m = all_metrics["short_term"]
            report_lines.extend([
                f"- 方向准确率: {m.get('direction_accuracy', 0):.2%}",
                f"- 相关系数: {m.get('correlation', 0):.4f}",
                f"- RMSE: {m.get('rmse', 0):.4f}",
                f"- 样本数: {m.get('sample_count', 0)}"
            ])

        report_lines.append("\n## 中期预测评估\n")

        if "mid_term" in all_metrics:
            m = all_metrics["mid_term"]
            report_lines.extend([
                f"- 方向准确率: {m.get('direction_accuracy', 0):.2%}",
                f"- 相关系数: {m.get('correlation', 0):.4f}",
                f"- RMSE: {m.get('rmse', 0):.4f}",
                f"- 样本数: {m.get('sample_count', 0)}"
            ])

        report_lines.append("\n## 长期预测评估\n")

        if "long_term" in all_metrics:
            m = all_metrics["long_term"]
            report_lines.extend([
                f"- 方向准确率: {m.get('direction_accuracy', 0):.2%}",
                f"- 相关系数: {m.get('correlation', 0):.4f}",
                f"- RMSE: {m.get('rmse', 0):.4f}",
                f"- 样本数: {m.get('sample_count', 0)}"
            ])

        report_lines.append("\n## 热点预测评估\n")

        if "hotspot" in all_metrics:
            m = all_metrics["hotspot"]
            report_lines.extend([
                f"- 平均命中率: {m.get('avg_hit_rate', 0):.2%}",
                f"- 中位数命中率: {m.get('median_hit_rate', 0):.2%}"
            ])

        report_lines.append("\n## 回测结果\n")

        if "backtest" in all_metrics:
            m = all_metrics["backtest"]
            report_lines.extend([
                f"- 累计收益: {m.get('total_return', 0):.2%}",
                f"- 夏普比率: {m.get('sharpe_ratio', 0):.4f}",
                f"- 最大回撤: {m.get('max_drawdown', 0):.2%}",
                f"- 胜率: {m.get('win_rate', 0):.2%}"
            ])

        report = "\n".join(report_lines)

        if save_path:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(report)
            logger.info(f"评估报告已保存: {save_path}")

        return report


def main():
    """主函数"""
    evaluator = ModelEvaluator()
    logger.info("模型评估模块已就绪")


if __name__ == "__main__":
    main()
