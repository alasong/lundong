# 日志审计系统文档

## 概述

日志审计系统提供完整的操作审计追踪、日志分析和归档功能，是量化系统运维监控的重要组成部分。

## 核心功能

### 1. 结构化日志记录
- JSON 格式日志，便于机器读取和分析
- 统一的日志 schema，包含操作信息、用户、模块等字段
- 支持多种日志级别（DEBUG/INFO/WARNING/ERROR/CRITICAL）

### 2. 操作审计追踪
- 记录所有关键操作（数据收集、策略信号、交易执行、风险事件）
- 完整的操作上下文（操作人、时间、结果、详细信息）
- 不可篡改的审计日志

### 3. 日志分析工具
- 日志查询（按日期、操作、用户、模块、结果过滤）
- 统计分析（操作分布、模块分布、用户分布）
- 交易统计、信号统计、风险事件统计

### 4. 日志轮转归档
- 自动日志轮转（按大小、按时间）
- 过期日志清理（可配置保留天数）
- 日志报告导出（CSV/JSON/Excel）

## 使用示例

### 初始化审计日志器

```python
from src.utils.audit_logger import AuditLogger, init_audit_logger, get_audit_logger

# 方式 1：直接创建实例
audit = AuditLogger(
    log_dir="logs",
    log_level="INFO",
    retention_days=30
)

# 方式 2：初始化全局实例
audit = init_audit_logger(log_dir="logs", retention_days=30)

# 方式 3：获取全局实例
audit = get_audit_logger()
```

### 记录操作日志

```python
# 记录一般操作
audit.log_operation(
    operation="data_collection",
    user="data_agent",
    module="data",
    action="collect",
    target="stock_daily",
    result="success",
    details={"stocks": 100, "records": 50000}
)

# 记录失败操作
audit.log_operation(
    operation="api_call",
    user="system",
    module="tushare_client",
    action="fetch",
    target="dc_daily",
    result="failure",
    details={"error": "API timeout", "retry_count": 3},
    level="ERROR"
)
```

### 记录交易日志

```python
# 记录买入交易
audit.log_trade(
    ts_code="000001.SZ",
    action="buy",
    shares=1000,
    price=12.5,
    amount=12500,
    commission=3.75,
    strategy="mean_reversion",
    signal_source="bb_oversold"
)

# 记录卖出交易
audit.log_trade(
    ts_code="000001.SZ",
    action="sell",
    shares=1000,
    price=13.2,
    amount=13200,
    commission=3.96,
    strategy="mean_reversion",
    signal_source="bb_overbought"
)
```

### 记录策略信号

```python
audit.log_signal(
    strategy="mean_reversion",
    ts_code="000001.SZ",
    signal=1,  # 1=买入，-1=卖出，0=持有
    strength=0.85,
    params={
        "bb_period": 20,
        "bb_std": 2.0,
        "rsi": 25.5
    }
)
```

### 记录风险事件

```python
# 记录回撤超限
audit.log_risk_event(
    risk_type="max_drawdown",
    level="warning",  # low/medium/high/critical
    message="组合回撤超过阈值",
    metrics={
        "current_drawdown": -0.08,
        "threshold": -0.05
    }
)

# 记录 VaR 超限
audit.log_risk_event(
    risk_type="var_breach",
    level="high",
    message="VaR 超过限额",
    metrics={
        "var_95": 500000,
        "limit": 400000
    }
)
```

### 记录系统事件

```python
# 系统启动
audit.log_system_event(
    event_type="system_startup",
    message="系统启动完成",
    details={
        "version": "1.0.0",
        "environment": "production",
        "modules_loaded": ["data", "strategy", "risk", "trading"]
    }
)

# 系统关闭
audit.log_system_event(
    event_type="system_shutdown",
    message="系统正常关闭",
    details={"uptime_hours": 24.5}
)
```

### 查询日志

```python
# 查询所有日志
df = audit.query_logs(limit=1000)

# 按日期范围查询
df = audit.query_logs(
    start_date="2026-03-01",
    end_date="2026-03-12"
)

# 按操作类型查询
df = audit.query_logs(operation="trade")

# 按模块查询
df = audit.query_logs(module="trading")

# 按结果查询（只查询失败的）
df = audit.query_logs(result="failure")

# 组合查询
df = audit.query_logs(
    start_date="2026-03-01",
    operation="trade",
    user="trading_system",
    limit=500
)
```

### 分析日志

```python
# 获取日志分析报告
analysis = audit.analyze_logs(
    start_date="2026-03-01",
    end_date="2026-03-12"
)

# 分析结果包含：
# - total_entries: 总日志数
# - by_operation: 按操作类型统计
# - by_module: 按模块统计
# - by_user: 按用户统计
# - by_result: 按结果统计
# - trades: 交易统计
# - signals: 信号统计
# - risk_events: 风险事件统计

print(json.dumps(analysis, indent=2))
```

### 导出报告

```python
# 导出 CSV 报告
audit.export_report(
    output_path="reports/audit_report.csv",
    start_date="2026-03-01",
    end_date="2026-03-12",
    format="csv"
)

# 导出 JSON 报告
audit.export_report(
    output_path="reports/audit_report.json",
    format="json"
)

# 导出 Excel 报告
audit.export_report(
    output_path="reports/audit_report.xlsx",
    format="excel"
)
```

### 清理过期日志

```python
# 清理超过保留天数的日志
audit.cleanup_old_logs()

# 定期执行（建议每日执行）
import schedule
schedule.every().day.at("02:00").do(audit.cleanup_old_logs)
```

## 日志文件结构

```
logs/
├── 20260312.log                    # 普通日志
├── audit/
│   └── audit_20260312.log          # 审计日志
├── structured/
│   └── structured_20260312.jsonl   # 结构化日志（JSON Lines 格式）
└── reports/
    └── audit_report.csv            # 导出的报告
```

## 结构化日志格式

```json
{
  "type": "audit",
  "audit_info": {
    "operation": "trade",
    "user": "trading_system",
    "module": "trading",
    "action": "buy",
    "target": "000001.SZ",
    "result": "success",
    "timestamp": "2026-03-12T12:30:00",
    "hostname": "server01"
  },
  "details": {
    "ts_code": "000001.SZ",
    "shares": 1000,
    "price": 12.5,
    "amount": 12500,
    "strategy": "mean_reversion"
  }
}
```

## 集成到现有系统

### 在数据收集模块中

```python
from src.utils.audit_logger import get_audit_logger

audit = get_audit_logger()

def collect_daily_data():
    try:
        # 收集数据
        records = tushare_client.fetch_daily()

        # 记录成功日志
        audit.log_operation(
            operation="data_collection",
            user="data_agent",
            module="tushare",
            action="fetch",
            target="stock_daily",
            result="success",
            details={"records": len(records)}
        )

        return records
    except Exception as e:
        # 记录失败日志
        audit.log_operation(
            operation="data_collection",
            user="data_agent",
            module="tushare",
            action="fetch",
            target="stock_daily",
            result="failure",
            details={"error": str(e)},
            level="ERROR"
        )
        raise
```

### 在策略模块中

```python
def generate_signals():
    audit = get_audit_logger()

    for ts_code in stock_list:
        signal = calculate_signal(ts_code)

        if signal != 0:
            # 记录信号日志
            audit.log_signal(
                strategy="mean_reversion",
                ts_code=ts_code,
                signal=signal,
                strength=0.8
            )
```

### 在交易模块中

```python
def execute_trade(signal):
    audit = get_audit_logger()

    # 执行交易
    result = broker.place_order(signal)

    # 记录交易日志
    audit.log_trade(
        ts_code=signal.ts_code,
        action="buy" if signal > 0 else "sell",
        shares=result.shares,
        price=result.price,
        amount=result.amount,
        commission=result.commission,
        strategy=signal.strategy
    )
```

### 在风控模块中

```python
def check_risk_limits():
    audit = get_audit_logger()

    # 检查回撤
    if current_drawdown < -threshold:
        audit.log_risk_event(
            risk_type="max_drawdown",
            level="high",
            message="组合回撤超过阈值",
            metrics={
                "current_drawdown": current_drawdown,
                "threshold": threshold
            }
        )
```

## 最佳实践

1. **及时记录**: 所有关键操作都应及时记录日志
2. **信息完整**: 日志应包含足够的上下文信息以便追踪问题
3. **级别适当**: 根据事件严重程度选择合适的日志级别
4. **敏感信息脱敏**: 不要在日志中记录密码、密钥等敏感信息
5. **定期审计**: 定期检查审计日志，发现异常操作
6. **备份归档**: 重要日志应定期备份和归档

## 注意事项

1. 日志会占用磁盘空间，建议设置合理的保留天数
2. 高频操作应使用异步日志，避免影响性能
3. 生产环境应使用 INFO 级别，开发环境可使用 DEBUG 级别
4. 审计日志应写入独立的文件，便于查询和分析

## 故障排查

### 日志不写入文件

检查日志目录权限：
```bash
ls -la logs/
chmod 755 logs/
```

### 日志文件过大

检查日志轮转配置，确保设置了合理的 `max_size_mb` 和 `retention_days`。

### 查询日志慢

1. 减少查询的日期范围
2. 添加更多过滤条件
3. 使用增量查询

## 相关文件

- `src/utils/audit_logger.py` - 审计日志器实现
- `src/utils/logger.py` - 基础日志工具
- `docs/LOGGING.md` - 本文档
