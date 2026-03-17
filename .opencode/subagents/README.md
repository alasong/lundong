# Subagents 配置指南

Claude Code 子代理配置，用于并行处理复杂任务。

---

## 可用 Subagents

| 名称 | 用途 | 模型 | 工具 |
|------|------|------|------|
| `data-explorer` | 数据层调试 | Sonnet | Read, Glob, Grep, Bash |
| `model-trainer` | 模型训练 | Sonnet | Read, Glob, Grep, Bash, Write, Edit |
| `test-runner` | 测试执行 | Haiku | Read, Glob, Grep, Bash |
| `code-reviewer` | 代码审查 | Sonnet | Read, Glob, Grep |
| `doc-writer` | 文档撰写 | Haiku | Read, Write, Edit |

---

## 使用方式

### 1. 直接调用

在对话中请求使用特定 subagent：

```
"使用 data-explorer 检查数据库状态"
"使用 test-runner 运行测试"
"使用 code-reviewer 审查代码"
```

### 2. 并行调用

多个 subagent 可以并行执行独立任务：

```python
# 同时检查数据和模型状态
"并行运行：
1. data-explorer: 检查数据库完整性
2. model-trainer: 检查模型状态"
```

### 3. 串行工作流

按顺序使用多个 subagent 完成复杂任务：

```
"先运行 test-runner 找出失败的测试，
然后用 model-trainer 修复问题，
最后用 test-runner 验证修复"
```

---

## 配置文件

```
.claude/
├── settings.json        # subagent 定义
└── subagents/
    ├── data-explorer.md
    ├── model-trainer.md
    ├── test-runner.md
    ├── code-reviewer.md
    └── doc-writer.md
```

---

## 最佳实践

### 选择合适的 subagent

| 任务类型 | 推荐 subagent |
|----------|---------------|
| 数据库问题 | `data-explorer` |
| 模型训练 | `model-trainer` |
| 运行测试 | `test-runner` |
| 代码质量 | `code-reviewer` |
| 更新文档 | `doc-writer` |

### 并行执行

当任务相互独立时，使用并行执行提高效率：

```
# 并行执行 3 个独立任务
"并行执行：
1. data-explorer: 数据质量报告
2. code-reviewer: 代码审查报告
3. test-runner: 测试执行报告"
```

### 模型选择

- **Sonnet**: 复杂分析、代码修改
- **Haiku**: 快速查询、简单任务

---

## 自定义 Subagent

在 `settings.json` 的 `subagents.definitions` 中添加：

```json
{
  "name": "my-agent",
  "description": "自定义 agent 描述",
  "model": "sonnet",
  "tools": ["Read", "Glob", "Grep", "Bash"],
  "systemPrompt": "系统提示词..."
}
```