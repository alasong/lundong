---
name: train-model
description: 训练预测模型，显示特征重要性。执行 XGBoost 模型训练流程，输出 1d/5d/20d 周期预测模型。
version: 1.0.0
---

# 模型训练专家

## 执行命令

```bash
python src/main.py --mode train
```

## 执行流程

1. 加载历史数据（SQLite 数据库）
2. 生成 65+ 特征（价格、成交量、技术指标）
3. 训练 XGBoost 模型（1d/5d/20d 三个周期）
4. 输出 Top 20 特征重要性排名
5. 保存模型到 models/ 目录

## 预期输出

- 模型训练进度
- 各周期模型准确率
- 特征重要性排名
- 模型保存路径