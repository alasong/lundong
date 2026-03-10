# 架构文档 - A 股热点轮动预测系统

## 架构概述

本系统使用 **Tushare Pro** 作为唯一数据源，采集东方财富和同花顺的行业概念板块数据，使用机器学习模型预测热点轮动。

---

## 核心架构

```
src/
├── main.py                 # 主入口
├── config.py               # 配置管理
├── runner.py               # 流程编排器
├── agents/                 # Agent 层
│   ├── base_agent.py       # 基类
│   ├── data_agent.py       # 数据采集
│   ├── analysis_agent.py   # 热点分析
│   └── predict_agent.py    # 预测
├── analysis/               # 分析核心
│   ├── hotspot_detector.py
│   ├── rotation_analyzer.py
│   └── pattern_learner.py
├── data/                   # 数据层
│   ├── tushare_client.py       # 东方财富客户端
│   ├── tushare_ths_client.py   # 同花顺客户端
│   ├── data_collector.py       # 采集调度器
│   └── feature_engineer.py     # 特征工程
├── models/                 # 预测模型
│   └── predictor.py        # XGBoost 预测
├── evaluation/             # 评估
│   └── metrics.py
├── learning/               # 学习模块
│   └── rotation_learner.py
└── core/                   # 核心工具
    └── settings.py
```

---

## 数据源

| 数据源 | 客户端类 | 用途 |
|--------|---------|------|
| Tushare Pro (东方财富) | `TushareClient` | 东方财富行业/概念数据 |
| Tushare Pro (同花顺) | `TushareTHSClient` | 同花顺行业/指数数据 |

---

## 运行模式

| 模式 | 说明 | 命令 |
|------|------|------|
| daily | 每日工作流 | `python src/main.py --mode daily --train` |
| quick | 快速分析 | `python src/main.py --mode quick` |
| train | 训练模型 | `python src/main.py --mode train` |
| history | 历史数据采集 | `python src/main.py --mode history --start-date 20230101 --end-date 20231231` |
| data | 数据采集 | `python src/main.py --mode data` |

---

## 数据存储

```
data/
├── raw/          # 原始 CSV 数据
├── processed/    # 处理后数据
├── features/     # 特征工程结果
├── models/       # 训练好的模型
├── results/      # 分析结果
└── finetune_*/   # LLM 微调数据集
```

---

## 配置

### 环境变量 (.env)

```bash
TUSHARE_TOKEN=your_token_here    # Tushare API Token (必需)
DASHSCOPE_API_KEY=your_key       # 可选，用于 LLM 功能
DATABASE_URL=sqlite:///data/stock.db
LOG_LEVEL=INFO
```

---

## 依赖

### 核心依赖
- pandas, numpy
- scikit-learn
- xgboost, lightgbm
- tushare
- sqlalchemy
- pydantic-settings
- loguru

### 已移除的依赖
- langchain, dashscope (LLM 模块已删除)
- torch (LSTM 模型已简化为 XGBoost)
- redis, TA-Lib, streamlit (非必要依赖)

---

## 简化说明

相比原始架构，做了以下简化：
1. **删除 LLM 模块** - 移除 LangChain、通义千问依赖
2. **删除 ExplainAgent** - 用简单报告模板替代
3. **删除 Coordinator 层** - 直接用 SimpleRunner 编排
4. **合并预测模型** - 单一 XGBoost 模型替代多周期模型
5. **简化依赖** - 移除非必要依赖

---

## 故障排查

### TUSHARE_TOKEN 未配置
```bash
# 检查 .env 文件
cat .env | grep TUSHARE_TOKEN
export TUSHARE_TOKEN=your_token
```

### dc_daily 返回空数据
系统自动切换到 `index_daily()` 备用接口

### 权限不足
访问 https://tushare.pro/user/token 确认积分 >= 5000

---

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
