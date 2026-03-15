# Test Runner Subagent

测试执行专家。

---

## 基本信息

| 属性 | 值 |
|------|-----|
| 名称 | `test-runner` |
| 模型 | Claude Haiku (快速) |
| 工具 | Read, Glob, Grep, Bash |

---

## 专长领域

- pytest 测试执行
- 测试失败分析
- 测试覆盖率报告
- CI/CD 集成

---

## 适用场景

```python
# 运行全部测试
"运行所有测试并生成报告"

# 运行特定测试
"运行 test_database.py 的测试"

# 分析失败
"分析最近测试失败的原因"

# 覆盖率报告
"生成测试覆盖率报告"
```

---

## 测试命令

```bash
# 全部测试
python -m pytest tests/ -v --tb=short

# 特定模块
python -m pytest tests/test_database.py -v

# 覆盖率
python -m pytest tests/ --cov=src --cov-report=term-missing
```

---

## 调用方式

```python
# 在 Claude Code 中请求调用
"使用 test-runner 运行测试"

# 或通过 Agent 工具
Agent(subagent_type="test-runner", prompt="运行测试并报告结果")
```

---

## 输出格式

```
🧪 测试报告
===========
总计: 45 测试
通过: 43 ✓
失败: 2 ✗
跳过: 0

失败测试:
- test_concept_daily_insert: 数据库锁定
- test_prediction_output: 模型文件不存在
```