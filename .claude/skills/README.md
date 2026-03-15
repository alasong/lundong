# Lundong 项目自定义 Skills

此目录包含项目特定的自动化技能定义。

---

## 可用 Skills

### /train-model
训练预测模型，显示特征重要性。

### /collect-data
从 Tushare 采集最新板块和个股数据。

### /run-prediction
执行完整预测流程，生成投资组合建议。

### /run-tests
运行完整测试套件并生成覆盖率报告。

### /db-stats
查看数据库统计信息和性能指标。

---

## 使用方式

```bash
# 在 Claude Code 中直接调用
/train-model
/collect-data
/run-prediction
```

---

## 自定义 Skill 模板

创建新 skill 文件 `my-skill.md`:

```markdown
---
name: my-skill
description: 简短描述
---

## 触发条件
- 用户说 "xxx"
- 或手动调用 /my-skill

## 执行步骤
1. 第一步
2. 第二步
3. 完成验证

## 预期输出
- 输出说明
```