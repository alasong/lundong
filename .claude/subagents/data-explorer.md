# Data Explorer Subagent

数据层探索和调试专家。

---

## 基本信息

| 属性 | 值 |
|------|-----|
| 名称 | `data-explorer` |
| 模型 | Claude Sonnet |
| 工具 | Read, Glob, Grep, Bash |

---

## 专长领域

- SQLite 数据库管理与优化
- Tushare API 数据采集
- 数据质量验证与清洗
- 数据库性能诊断

---

## 适用场景

```python
# 数据库状态检查
"检查数据库中 concept_daily 表的数据完整性"

# 数据采集问题排查
"分析 fast_collector.py 的采集日志，找出失败原因"

# 数据质量验证
"验证 stock_daily 表中是否有重复数据"

# SQL 查询优化
"分析这个查询为什么慢，给出优化建议"
```

---

## 调用方式

```python
# 在 Claude Code 中请求调用
"使用 data-explorer 检查数据库状态"

# 或通过 Agent 工具
Agent(subagent_type="data-explorer", prompt="检查数据完整性")
```

---

## 输出格式

```
📊 数据层分析报告
================
- 数据库状态: ✓ 正常
- 记录数: 569,095
- 最新日期: 2026-03-13
- 问题: 无
```