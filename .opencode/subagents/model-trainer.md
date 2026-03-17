# Model Trainer Subagent

模型训练专家。

---

## 基本信息

| 属性 | 值 |
|------|-----|
| 名称 | `model-trainer` |
| 模型 | Claude Sonnet |
| 工具 | Read, Glob, Grep, Bash, Write, Edit |

---

## 专长领域

- XGBoost / LightGBM 模型训练
- 特征工程（65 个特征）
- 超参数调优
- 模型评估与诊断

---

## 适用场景

```python
# 训练模型
"训练新的预测模型并显示特征重要性"

# 特征工程
"分析当前特征工程，提出优化建议"

# 模型诊断
"模型预测效果不好，诊断原因"

# 超参数调优
"使用交叉验证调优 XGBoost 超参数"
```

---

## 关键文件

| 文件 | 说明 |
|------|------|
| `src/models/predictor.py` | 主预测器（728 行） |
| `src/models/stock_predictor.py` | 个股预测器 |
| `data/models/` | 模型存储目录 |

---

## 调用方式

```python
# 在 Claude Code 中请求调用
"使用 model-trainer 重新训练模型"

# 或通过 Agent 工具
Agent(subagent_type="model-trainer", prompt="训练模型并评估效果")
```

---

## 输出格式

```
🎯 模型训练报告
================
- 训练数据: 2020-01-02 ~ 2026-03-13
- 特征数: 65
- 模型: XGBoost

评估指标:
- IC: 0.12
- RankIC: 0.15
- Sharpe: 1.8

Top 5 特征重要性:
1. momentum_5: 0.15
2. volatility_10: 0.12
...
```