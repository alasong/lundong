# Code Reviewer Subagent

代码审查专家。

---

## 基本信息

| 属性 | 值 |
|------|-----|
| 名称 | `code-reviewer` |
| 模型 | Claude Sonnet |
| 工具 | Read, Glob, Grep |

---

## 专长领域

- Python 代码质量审查
- 安全漏洞检测 (OWASP Top 10)
- 性能问题诊断
- 代码可维护性评估

---

## 审查维度

| 维度 | 检查项 |
|------|--------|
| **正确性** | 逻辑错误、边界条件、异常处理 |
| **安全性** | SQL 注入、XSS、敏感信息泄露 |
| **性能** | N+1 查询、内存泄漏、算法复杂度 |
| **可维护性** | 代码复杂度、命名规范、重复代码 |
| **测试** | 测试覆盖率、边界测试 |

---

## 适用场景

```python
# 审查最近修改
"审查最近修改的代码"

# 审查特定文件
"审查 src/models/predictor.py 的代码质量"

# 安全审计
"检查代码中是否有 SQL 注入风险"

# 性能审查
"检查 database.py 是否有性能问题"
```

---

## 调用方式

```python
# 在 Claude Code 中请求调用
"使用 code-reviewer 审查代码"

# 或通过 Agent 工具
Agent(subagent_type="code-reviewer", prompt="审查 src/data/database.py")
```

---

## 输出格式

```
🔍 代码审查报告
===============

## 🔴 严重问题 (2)
1. database.py:156 - SQL 注入风险
   - 问题: 使用 f-string 拼接 SQL
   - 建议: 使用参数化查询

## 🟡 中等问题 (3)
1. predictor.py:89 - 未处理异常
   ...

## 🟢 建议改进 (5)
1. 建议添加类型注解
   ...
```