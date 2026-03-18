"""
策略性能追踪器
- 运行日志记录
- 绩效指标计算
- 低收益策略识别
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import math

try:
    from data.database import get_database, SQLiteDatabase
except ImportError:
    from src.data.database import get_database, SQLiteDatabase


class StrategyPerformanceTracker:
    def __init__(self, db: SQLiteDatabase = None):
        self.db = db or get_database()

    def log_run(
        self,
        strategy_name: str,
        version: str,
        signals: List,
        portfolio_return: float = None,
    ) -> int:
        """记录策略运行，返回 run_id"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                # 统计信号数量
                signals_count = len(signals) if signals else 0
                buy_signals = (
                    sum(1 for signal in signals if signal.get("action") == "buy")
                    if signals
                    else 0
                )

                # 获取当前日期
                run_date = datetime.now().strftime("%Y-%m-%d")

                # 插入运行记录
                cursor.execute(
                    """
                    INSERT INTO strategy_runs 
                    (strategy_name, version, run_date, signals_count, buy_signals, portfolio_return)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_name,
                        version,
                        run_date,
                        signals_count,
                        buy_signals,
                        portfolio_return,
                    ),
                )

                run_id = cursor.lastrowid
                conn.commit()

                # 更新策略绩效汇总
                self._update_performance_summary(strategy_name)

                return run_id
        except Exception as e:
            raise e

    def update_performance(self, strategy_name: str, run_id: int, metrics: dict):
        """更新运行绩效指标"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                # 更新特定运行的绩效指标
                update_fields = []
                params = []

                for key, value in metrics.items():
                    if key in [
                        "sharpe",
                        "max_drawdown",
                        "win_rate",
                        "total_return",
                        "volatility",
                        "benchmark_return",
                    ]:
                        update_fields.append(f"{key} = ?")
                        params.extend([value])

                if update_fields:
                    sql = f"""
                    UPDATE strategy_runs 
                    SET {", ".join(update_fields)}
                    WHERE id = ?
                    """
                    params.append(run_id)
                    cursor.execute(sql, params)

                    conn.commit()

                    # 更新策略绩效汇总
                    self._update_performance_summary(strategy_name)
        except Exception as e:
            raise e

    def get_performance(self, strategy_name: str, days: int = 30) -> dict:
        """获取策略绩效汇总"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                # 获取指定天数内的运行记录
                start_date = (datetime.now() - timedelta(days=days)).strftime(
                    "%Y-%m-%d"
                )

                cursor.execute(
                    """
                    SELECT run_date, portfolio_return, sharpe, max_drawdown, win_rate, 
                           total_return, volatility, benchmark_return
                    FROM strategy_runs 
                    WHERE strategy_name = ? AND run_date >= ?
                    ORDER BY run_date DESC
                    """,
                    (strategy_name, start_date),
                )

                runs = cursor.fetchall()

                if not runs:
                    return {
                        "strategy_name": strategy_name,
                        "days": days,
                        "total_runs": 0,
                        "avg_return": 0.0,
                        "avg_sharpe": 0.0,
                        "avg_max_drawdown": 0.0,
                        "avg_win_rate": 0.0,
                        "total_return": 0.0,
                        "annualized_return": 0.0,
                        "best_day": 0.0,
                        "worst_day": 0.0,
                        "recent_runs": [],
                    }

                # 计算汇总指标
                returns = [r[1] for r in runs if r[1] is not None]
                sharpe_values = [r[2] for r in runs if r[2] is not None]
                drawdowns = [r[3] for r in runs if r[3] is not None]
                win_rates = [r[4] for r in runs if r[4] is not None]
                total_returns = [r[5] for r in runs if r[5] is not None]
                volatilities = [r[6] for r in runs if r[6] is not None]
                benchmark_returns = [r[7] for r in runs if r[7] is not None]

                avg_return = sum(returns) / len(returns) if returns else 0.0
                avg_sharpe = (
                    sum(sharpe_values) / len(sharpe_values) if sharpe_values else 0.0
                )
                avg_max_drawdown = sum(drawdowns) / len(drawdowns) if drawdowns else 0.0
                avg_win_rate = sum(win_rates) / len(win_rates) if win_rates else 0.0
                total_return = sum(total_returns) if total_returns else 0.0
                annualized_return = avg_return * 252  # 假设年交易日为252天
                best_day = max(returns) if returns else 0.0
                worst_day = min(returns) if returns else 0.0

                # 获取最近的运行记录
                recent_runs = []
                for run in runs[:10]:  # 最近10次运行
                    recent_runs.append(
                        {
                            "date": run[0],
                            "portfolio_return": run[1],
                            "sharpe": run[2],
                            "max_drawdown": run[3],
                            "win_rate": run[4],
                            "total_return": run[5],
                            "volatility": run[6],
                            "benchmark_return": run[7],
                        }
                    )

                return {
                    "strategy_name": strategy_name,
                    "days": days,
                    "total_runs": len(runs),
                    "avg_return": avg_return,
                    "avg_sharpe": avg_sharpe,
                    "avg_max_drawdown": avg_max_drawdown,
                    "avg_win_rate": avg_win_rate,
                    "total_return": total_return,
                    "annualized_return": annualized_return,
                    "best_day": best_day,
                    "worst_day": worst_day,
                    "recent_runs": recent_runs,
                }
        except Exception as e:
            raise e

    def get_low_performers(
        self, threshold_return: float = 0.0, min_runs: int = 10
    ) -> List[dict]:
        """识别低收益策略"""
        try:
            cursor = self.db.conn.cursor()

            # 获取所有策略的平均收益
            cursor.execute(
                """
                SELECT strategy_name, COUNT(*) as run_count, AVG(portfolio_return) as avg_return
                FROM strategy_runs
                GROUP BY strategy_name
                HAVING run_count >= ?
                """,
                (min_runs,),
            )

            strategies = cursor.fetchall()

            low_performers = []
            for strategy_name, run_count, avg_return in strategies:
                if avg_return is not None and avg_return < threshold_return:
                    # 获取更多详细信息
                    perf_info = self.get_performance(strategy_name, days=90)  # 最近90天
                    low_performers.append(
                        {
                            "strategy_name": strategy_name,
                            "avg_return": avg_return,
                            "run_count": run_count,
                            "sharpe": perf_info.get("avg_sharpe", 0.0),
                            "max_drawdown": perf_info.get("avg_max_drawdown", 0.0),
                            "win_rate": perf_info.get("avg_win_rate", 0.0),
                            "total_return": perf_info.get("total_return", 0.0),
                        }
                    )

            # 按平均收益排序（升序）
            low_performers.sort(key=lambda x: x["avg_return"])
            return low_performers
        except Exception as e:
            raise e

    def calculate_sharpe(
        self, returns: List[float], risk_free_rate: float = 0.02
    ) -> float:
        """计算夏普比率"""
        if not returns or len(returns) < 2:
            return 0.0

        # 年化收益率
        avg_return = sum(returns) / len(returns)
        annualized_return = avg_return * 252  # 假设年交易日为252天

        # 计算波动率
        squared_diffs = [(r - avg_return) ** 2 for r in returns]
        variance = sum(squared_diffs) / (len(returns) - 1)
        std_dev = math.sqrt(variance)
        annualized_volatility = std_dev * math.sqrt(252)

        if annualized_volatility == 0:
            return 0.0

        # 夏普比率 = (年化收益率 - 无风险利率) / 年化波动率
        sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
        return sharpe_ratio

    def calculate_max_drawdown(self, returns: List[float]) -> float:
        """计算最大回撤"""
        if not returns:
            return 0.0

        # 计算累计收益曲线
        cumulative_returns = [0.0]
        for ret in returns:
            cumulative_returns.append(cumulative_returns[-1] + ret)

        # 计算回撤曲线
        peak = cumulative_returns[0]
        drawdowns = []

        for value in cumulative_returns:
            if value > peak:
                peak = value
            drawdown = (peak - value) / (peak + 1e-10)  # 避免除零
            drawdowns.append(drawdown)

        # 最大回撤
        max_drawdown = max(drawdowns) if drawdowns else 0.0
        return max_drawdown

    def _update_performance_summary(self, strategy_name: str):
        """更新策略绩效汇总"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                # 获取策略最新的运行数据
                cursor.execute(
                    """
                    SELECT version, COUNT(*) as total_runs, 
                           SUM(signals_count) as total_trades,
                           AVG(portfolio_return) as avg_return,
                           AVG(sharpe) as avg_sharpe,
                           AVG(max_drawdown) as avg_max_drawdown,
                           AVG(win_rate) as avg_win_rate,
                           SUM(total_return) as total_return,
                           MAX(run_date) as last_run_date,
                           MAX(run_time) as last_run_time
                    FROM strategy_runs 
                    WHERE strategy_name = ?
                    """,
                    (strategy_name,),
                )

                result = cursor.fetchone()
                if not result:
                    return

                (
                    version,
                    total_runs,
                    total_trades,
                    avg_return,
                    avg_sharpe,
                    avg_max_drawdown,
                    avg_win_rate,
                    total_return,
                    last_run_date,
                    last_run_time,
                ) = result

                # 计算年化收益率
                annualized_return = (avg_return or 0) * 252

                # 检查是否已存在汇总记录
                cursor.execute(
                    "SELECT 1 FROM strategy_performance WHERE strategy_name = ?",
                    (strategy_name,),
                )
                exists = cursor.fetchone()

                if exists:
                    # 更新现有记录
                    cursor.execute(
                        """
                        UPDATE strategy_performance 
                        SET current_version = ?, total_runs = ?, total_trades = ?, 
                            avg_return = ?, avg_sharpe = ?, max_drawdown = ?, 
                            total_return = ?, annualized_return = ?, win_rate = ?, 
                            last_run_date = ?, last_run_time = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE strategy_name = ?
                        """,
                        (
                            version,
                            total_runs or 0,
                            total_trades or 0,
                            avg_return or 0.0,
                            avg_sharpe or 0.0,
                            avg_max_drawdown or 0.0,
                            total_return or 0.0,
                            annualized_return,
                            avg_win_rate or 0.0,
                            last_run_date,
                            last_run_time,
                            strategy_name,
                        ),
                    )
                else:
                    # 插入新记录
                    cursor.execute(
                        """
                        INSERT INTO strategy_performance
                        (strategy_name, current_version, total_runs, total_trades,
                         avg_return, avg_sharpe, max_drawdown, total_return,
                         annualized_return, win_rate, last_run_date, last_run_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            strategy_name,
                            version,
                            total_runs or 0,
                            total_trades or 0,
                            avg_return or 0.0,
                            avg_sharpe or 0.0,
                            avg_max_drawdown or 0.0,
                            total_return or 0.0,
                            annualized_return,
                            avg_win_rate or 0.0,
                            last_run_date,
                            last_run_time,
                        ),
                    )

                conn.commit()
        except Exception as e:
            raise e
