#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志审计系统
提供完整的操作审计追踪、日志分析和归档功能

功能：
1. 结构化日志记录 - JSON 格式日志
2. 操作审计追踪 - 记录所有关键操作
3. 日志分析工具 - 日志聚合统计
4. 日志轮转归档 - 自动清理和备份
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd
from loguru import logger


class AuditLogger:
    """
    审计日志器

    功能：
    1. 结构化日志记录（JSON 格式）
    2. 操作审计追踪
    3. 日志轮转和归档
    4. 日志查询和分析
    """

    def __init__(
        self,
        name: str = "audit",
        log_dir: str = "logs",
        log_level: str = "INFO",
        retention_days: int = 30,
        max_size_mb: int = 100,
        backup_count: int = 10
    ):
        """
        初始化审计日志器

        Args:
            name: 日志器名称
            log_dir: 日志目录
            log_level: 日志级别
            retention_days: 日志保留天数
            max_size_mb: 单个日志文件最大大小（MB）
            backup_count: 备份文件数量
        """
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_level = log_level
        self.retention_days = retention_days
        self.max_size_mb = max_size_mb
        self.backup_count = backup_count

        # 确保日志目录存在
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 创建日志器
        self._setup_logger()

        logger.info(f"审计日志器初始化完成：{log_dir}")

    def _setup_logger(self):
        """配置日志处理器"""
        # 创建专用的 logger
        self.audit_logger = logger.bind(audit=True)

        # 移除默认处理器
        self.audit_logger.remove()

        # 添加控制台处理器
        self.audit_logger.add(
            sys.stderr,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            level=self.log_level,
            colorize=True
        )

        # 添加普通日志文件处理器
        self.audit_logger.add(
            self.log_dir / "{time:YYYYMMDD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            level=self.log_level,
            rotation=f"{self.max_size_mb} MB",
            retention=f"{self.retention_days} days",
            enqueue=True,
            backtrace=True,
            diagnose=True
        )

        # 添加结构化日志（JSON 格式）
        structured_dir = self.log_dir / "structured"
        structured_dir.mkdir(parents=True, exist_ok=True)
        self.audit_logger.add(
            structured_dir / "structured_{time:YYYYMMDD}.json",
            format="{message}",
            level=self.log_level,
            rotation="00:00",
            retention=f"{self.retention_days} days",
            serialize=True,
            enqueue=True
        )

        # 添加审计日志文件（关键操作）
        audit_log_path = self.log_dir / "audit" / "audit_{time:YYYYMMDD}.log"
        audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_logger.add(
            audit_log_path,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
            level="INFO",
            rotation="00:00",
            retention=f"{self.retention_days * 2} days",
            enqueue=True
        )

    def log_operation(
        self,
        operation: str,
        user: str = "system",
        module: str = "",
        action: str = "",
        target: str = "",
        result: str = "success",
        details: Dict = None,
        level: str = "INFO"
    ):
        """
        记录操作审计日志

        Args:
            operation: 操作名称
            user: 操作用户
            module: 模块名称
            action: 动作类型
            target: 操作目标
            result: 操作结果（success/failure）
            details: 详细信息
            level: 日志级别
        """
        audit_info = {
            "operation": operation,
            "user": user,
            "module": module,
            "action": action,
            "target": target,
            "result": result,
            "timestamp": datetime.now().isoformat(),
            "hostname": os.uname().nodename if hasattr(os, 'uname') else "localhost"
        }

        log_entry = {
            "type": "audit",
            "audit_info": audit_info,
            "details": details or {}
        }

        # 记录到审计日志
        self.audit_logger.patch(lambda r: r.update(extra={"audit_info": json.dumps(audit_info)})).log(
            level,
            f"{operation} | user={user} | module={module} | action={action} | target={target} | result={result}"
        )

        # 记录结构化日志
        self._write_structured_log(log_entry)

    def _write_structured_log(self, log_entry: Dict):
        """写入结构化日志"""
        structured_dir = self.log_dir / "structured"
        structured_dir.mkdir(parents=True, exist_ok=True)

        log_file = structured_dir / f"structured_{datetime.now().strftime('%Y%m%d')}.jsonl"

        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            self.audit_logger.error(f"写入结构化日志失败：{e}")

    def log_trade(
        self,
        ts_code: str,
        action: str,
        shares: int,
        price: float,
        amount: float,
        commission: float = 0,
        strategy: str = "",
        signal_source: str = ""
    ):
        """
        记录交易审计日志

        Args:
            ts_code: 股票代码
            action: 交易动作（buy/sell）
            shares: 股数
            price: 价格
            amount: 金额
            commission: 手续费
            strategy: 策略名称
            signal_source: 信号来源
        """
        details = {
            "ts_code": ts_code,
            "action": action,
            "shares": shares,
            "price": price,
            "amount": amount,
            "commission": commission,
            "strategy": strategy,
            "signal_source": signal_source,
            "trade_type": "stock"
        }

        self.log_operation(
            operation="trade",
            user="trading_system",
            module="trading",
            action=action,
            target=ts_code,
            result="success",
            details=details
        )

    def log_signal(
        self,
        strategy: str,
        ts_code: str,
        signal: int,
        strength: float,
        params: Dict = None
    ):
        """
        记录策略信号日志

        Args:
            strategy: 策略名称
            ts_code: 股票代码
            signal: 信号（1=买入，-1=卖出，0=持有）
            strength: 信号强度
            params: 策略参数
        """
        details = {
            "strategy": strategy,
            "ts_code": ts_code,
            "signal": signal,
            "strength": strength,
            "params": params or {}
        }

        self.log_operation(
            operation="signal",
            user="strategy_system",
            module="strategy",
            action="generate",
            target=ts_code,
            result="success",
            details=details
        )

    def log_risk_event(
        self,
        risk_type: str,
        level: str,
        message: str,
        metrics: Dict = None
    ):
        """
        记录风险事件日志

        Args:
            risk_type: 风险类型
            level: 风险级别（low/medium/high/critical）
            message: 风险描述
            metrics: 风险指标
        """
        details = {
            "risk_type": risk_type,
            "risk_level": level,
            "metrics": metrics or {}
        }

        self.log_operation(
            operation="risk_event",
            user="risk_system",
            module="risk",
            action=level,
            target=risk_type,
            result="triggered",
            details=details,
            level="WARNING" if level in ["high", "critical"] else "INFO"
        )

    def log_system_event(
        self,
        event_type: str,
        message: str,
        details: Dict = None
    ):
        """
        记录系统事件日志

        Args:
            event_type: 事件类型
            message: 事件描述
            details: 详细信息
        """
        self.log_operation(
            operation=event_type,
            user="system",
            module="system",
            action=event_type,
            target="",
            result="success",
            details=details or {}
        )

    def query_logs(
        self,
        start_date: str = None,
        end_date: str = None,
        operation: str = None,
        user: str = None,
        module: str = None,
        result: str = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        查询日志

        Args:
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            operation: 操作类型
            user: 用户
            module: 模块
            result: 结果
            limit: 返回数量限制

        Returns:
            DataFrame with log entries
        """
        logs = []
        structured_dir = self.log_dir / "structured"

        if not structured_dir.exists():
            return pd.DataFrame()

        # 解析日期范围
        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start = datetime.now() - timedelta(days=self.retention_days)

        if end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end = datetime.now()

        # 读取日志文件
        for log_file in structured_dir.glob("*.jsonl"):
            # 检查文件日期是否在范围内
            try:
                file_date_str = log_file.stem.split('_')[-1]
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
                if file_date < start or file_date > end:
                    continue
            except:
                pass

            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            audit_info = entry.get('audit_info', {})

                            # 过滤条件
                            if operation and audit_info.get('operation') != operation:
                                continue
                            if user and audit_info.get('user') != user:
                                continue
                            if module and audit_info.get('module') != module:
                                continue
                            if result and audit_info.get('result') != result:
                                continue

                            # 合并字段
                            flat_entry = {**audit_info, **entry.get('details', {})}
                            logs.append(flat_entry)

                            if len(logs) >= limit:
                                break
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.error(f"读取日志文件失败 {log_file}: {e}")

            if len(logs) >= limit:
                break

        if not logs:
            return pd.DataFrame()

        df = pd.DataFrame(logs)
        return df

    def analyze_logs(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> Dict:
        """
        分析日志

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            分析结果
        """
        df = self.query_logs(start_date, end_date, limit=100000)

        if df.empty:
            return {"error": "无日志数据"}

        analysis = {
            "total_entries": len(df),
            "by_operation": df['operation'].value_counts().to_dict() if 'operation' in df.columns else {},
            "by_module": df['module'].value_counts().to_dict() if 'module' in df.columns else {},
            "by_user": df['user'].value_counts().to_dict() if 'user' in df.columns else {},
            "by_result": df['result'].value_counts().to_dict() if 'result' in df.columns else {},
            "trades": {},
            "signals": {},
            "risk_events": {}
        }

        # 交易统计
        trade_df = df[df['operation'] == 'trade'] if 'operation' in df.columns else pd.DataFrame()
        if not trade_df.empty:
            analysis['trades'] = {
                "total_count": len(trade_df),
                "buy_count": len(trade_df[trade_df['action'] == 'buy']) if 'action' in trade_df.columns else 0,
                "sell_count": len(trade_df[trade_df['action'] == 'sell']) if 'action' in trade_df.columns else 0,
                "total_amount": trade_df['amount'].sum() if 'amount' in trade_df.columns else 0
            }

        # 信号统计
        signal_df = df[df['operation'] == 'signal'] if 'operation' in df.columns else pd.DataFrame()
        if not signal_df.empty:
            analysis['signals'] = {
                "total_count": len(signal_df),
                "by_strategy": signal_df['strategy'].value_counts().to_dict() if 'strategy' in signal_df.columns else {}
            }

        # 风险事件统计
        risk_df = df[df['operation'] == 'risk_event'] if 'operation' in df.columns else pd.DataFrame()
        if not risk_df.empty:
            analysis['risk_events'] = {
                "total_count": len(risk_df),
                "by_level": risk_df['risk_level'].value_counts().to_dict() if 'risk_level' in risk_df.columns else {}
            }

        return analysis

    def cleanup_old_logs(self):
        """清理过期日志"""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)

        for log_file in self.log_dir.rglob("*.log*"):
            try:
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if file_mtime < cutoff_date:
                    log_file.unlink()
                    logger.info(f"清理过期日志：{log_file}")
            except Exception as e:
                logger.error(f"清理日志失败 {log_file}: {e}")

        for log_file in self.log_dir.rglob("*.jsonl"):
            try:
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if file_mtime < cutoff_date:
                    log_file.unlink()
                    logger.info(f"清理过期日志：{log_file}")
            except Exception as e:
                logger.error(f"清理日志失败 {log_file}: {e}")

    def export_report(
        self,
        output_path: str,
        start_date: str = None,
        end_date: str = None,
        format: str = "csv"
    ) -> str:
        """
        导出日志报告

        Args:
            output_path: 输出路径
            start_date: 开始日期
            end_date: 结束日期
            format: 输出格式（csv/json/excel）

        Returns:
            输出文件路径
        """
        df = self.query_logs(start_date, end_date, limit=100000)

        if df.empty:
            logger.warning("无日志数据可导出")
            return ""

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "csv":
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        elif format == "json":
            df.to_json(output_path, orient='records', force_ascii=False, indent=2)
        elif format == "excel":
            df.to_excel(output_path, index=False, engine='openpyxl')

        logger.info(f"导出日志报告：{output_path}")
        return str(output_path)


# 全局审计日志器实例
_audit_logger = None


def get_audit_logger() -> AuditLogger:
    """获取全局审计日志器实例"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger


def init_audit_logger(
    log_dir: str = "logs",
    log_level: str = "INFO",
    retention_days: int = 30
) -> AuditLogger:
    """初始化全局审计日志器"""
    global _audit_logger
    _audit_logger = AuditLogger(
        log_dir=log_dir,
        log_level=log_level,
        retention_days=retention_days
    )
    return _audit_logger


def main():
    """测试函数"""
    print("=" * 90)
    print("日志审计系统测试")
    print("=" * 90)

    # 初始化审计日志器
    audit = AuditLogger(log_dir="test_logs")

    # 记录各种操作
    print("\n[1] 记录操作日志...")
    audit.log_operation(
        operation="data_collection",
        user="data_agent",
        module="data",
        action="collect",
        target="stock_daily",
        result="success",
        details={"stocks": 100, "records": 50000}
    )

    audit.log_operation(
        operation="strategy_signal",
        user="strategy_system",
        module="strategy",
        action="generate",
        target="000001.SZ",
        result="success",
        details={"strategy": "mean_reversion", "signal": 1, "strength": 0.8}
    )

    print("[2] 记录交易日志...")
    audit.log_trade(
        ts_code="000001.SZ",
        action="buy",
        shares=1000,
        price=12.5,
        amount=12500,
        commission=3.75,
        strategy="mean_reversion"
    )

    audit.log_trade(
        ts_code="000001.SZ",
        action="sell",
        shares=1000,
        price=13.2,
        amount=13200,
        commission=3.96,
        strategy="mean_reversion"
    )

    print("[3] 记录风险事件...")
    audit.log_risk_event(
        risk_type="max_drawdown",
        level="warning",
        message="组合回撤超过阈值",
        metrics={"current_drawdown": -0.08, "threshold": -0.05}
    )

    audit.log_risk_event(
        risk_type="var_breach",
        level="high",
        message="VaR 超过限额",
        metrics={"var_95": 500000, "limit": 400000}
    )

    print("[4] 记录系统事件...")
    audit.log_system_event(
        event_type="system_startup",
        message="系统启动",
        details={"version": "1.0.0", "environment": "production"}
    )

    # 等待日志写入
    import time
    time.sleep(1)

    # 查询日志
    print("\n[5] 查询日志...")
    df = audit.query_logs(limit=100)
    print(f"查询到 {len(df)} 条日志")
    if not df.empty:
        print(df[['timestamp', 'operation', 'user', 'result']].head())

    # 分析日志
    print("\n[6] 分析日志...")
    analysis = audit.analyze_logs()
    print(json.dumps(analysis, indent=2, ensure_ascii=False))

    # 导出报告
    print("\n[7] 导出报告...")
    report_path = audit.export_report("test_logs/report.csv")
    print(f"报告已导出：{report_path}")

    # 清理过期日志
    print("\n[8] 清理过期日志...")
    audit.cleanup_old_logs()

    print("\n" + "=" * 90)
    print("日志审计系统测试完成!")
    print("=" * 90)


if __name__ == "__main__":
    main()
