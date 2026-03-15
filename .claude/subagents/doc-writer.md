# Doc Writer Subagent

文档撰写专家。

---

## 基本信息

| 属性 | 值 |
|------|-----|
| 名称 | `doc-writer` |
| 模型 | Claude Haiku (快速) |
| 工具 | Read, Write, Edit |

---

## 专长领域

- CLAUDE.md 多层文档维护
- README 更新
- 代码注释编写
- API 文档生成

---

## 文档结构

```
CLAUDE.md (根目录索引)
├── src/agents/CLAUDE.md (Agent 层)
├── src/data/CLAUDE.md (数据层)
├── src/models/CLAUDE.md (模型层)
├── src/analysis/CLAUDE.md (分析层)
├── src/portfolio/CLAUDE.md (组合优化)
└── src/evaluation/CLAUDE.md (评估模块)
```

---

## 适用场景

```python
# 更新模块文档
"更新 src/data/CLAUDE.md，反映最新的数据库优化"

# 更新 README
"更新 README.md，添加新的命令说明"

# 添加代码注释
"为 predictor.py 的核心方法添加文档注释"

# 生成 API 文档
"为 database.py 的公开方法生成文档"
```

---

## 调用方式

```python
# 在 Claude Code 中请求调用
"使用 doc-writer 更新文档"

# 或通过 Agent 工具
Agent(subagent_type="doc-writer", prompt="更新 src/data/CLAUDE.md")
```

---

## 输出格式

```
📝 文档更新报告
===============
- 文件: src/data/CLAUDE.md
- 更新内容:
  - 添加交易日历表说明
  - 添加归档表说明
  - 更新 PRAGMA 配置参数
- 状态: ✓ 已完成
```