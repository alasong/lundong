# A 股热点轮动预测系统

一个简化的 A 股市场热点轮动预测系统，使用机器学习技术分析市场热点板块并预测未来走势。

## 简化后架构

```
src/
├── main.py              # 主入口
├── config.py            # 配置管理
├── runner.py            # 流程编排器
├── agents/              # Agent 模块
│   ├── base_agent.py    # Agent 基类
│   ├── data_agent.py    # 数据采集
│   ├── analysis_agent.py # 热点分析
│   └── predict_agent.py # 预测
├── analysis/            # 分析核心
│   ├── hotspot_detector.py
│   ├── rotation_analyzer.py
│   └── pattern_learner.py
├── data/                # 数据采集
│   ├── tushare_client.py
│   ├── data_collector.py
│   └── feature_engineer.py
└── models/              # 预测模型
    └── predictor.py     # 统一预测模型
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件：

```bash
TUSHARE_TOKEN=your_tushare_token_here
DATABASE_URL=sqlite:///data/stock.db
LOG_LEVEL=INFO
```

### 3. 运行

```bash
# 每日工作流（数据采集 + 分析 + 预测）
python src/main.py --mode daily --train

# 快速分析（使用已有数据）
python src/main.py --mode quick

# 仅训练模型
python src/main.py --mode train

# 采集历史数据
python src/main.py --mode history --start-date 20230101 --end-date 20231231
```

## 功能模块

- **数据采集**：Tushare 数据源（日线行情、概念板块、资金流向、龙虎榜等）
- **热点识别**：多维度热点强度计算（涨幅、资金、情绪）
- **轮动分析**：板块相关性、领涨 - 滞后关系、轮动路径
- **预测模型**：统一 XGBoost 模型，支持 1 日/5 日/20 日预测

## 运行模式

| 模式 | 说明 | 命令 |
|------|------|------|
| daily | 每日工作流 | `--mode daily --train` |
| quick | 快速分析 | `--mode quick` |
| train | 训练模型 | `--mode train` |
| history | 历史数据采集 | `--mode history --start-date YYYYMMDD --end-date YYYYMMDD` |

## 数据目录结构

```
data/
├── raw/          # 原始 CSV 数据
├── processed/    # 处理后数据
├── features/     # 特征工程结果
├── models/       # 训练好的模型
└── results/      # 分析结果
```

## 简化说明

相比原始架构，做了以下简化：

1. **删除 LLM 模块** - 移除 LangChain、通义千问依赖，用简单报告模板替代
2. **删除 ExplainAgent** - 不需要 AI 生成解释
3. **删除 Coordinator 层** - 直接用 SimpleRunner 编排流程
4. **合并预测模型** - 单一 XGBoost 模型替代 LSTM+ 多周期模型
5. **简化依赖** - 移除 Redis、TA-Lib、Streamlit 等非必要依赖

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
