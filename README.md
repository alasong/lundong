# A 股热点轮动预测系统

基于机器学习的 A 股市场热点轮动预测系统，通过 Tushare 数据采集、热点识别、轮动规律学习，预测板块走势。

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
# 运行测试
python test_simple.py

# 运行 pytest 测试
python -m pytest tests/ -v

# 每日工作流（采集 + 分析 + 预测）
python src/main.py --mode daily --train

# 快速分析（使用已有数据）
python src/main.py --mode quick

# 仅训练模型
python src/main.py --mode train

# 采集历史数据
python src/main.py --mode history --start-date 20230101 --end-date 20231231
```

## 运行模式

| 模式 | 说明 | 命令 |
|------|------|------|
| `daily` | 每日工作流（采集 + 分析 + 预测） | `--mode daily --train` |
| `quick` | 快速分析（使用已有数据预测） | `--mode quick` |
| `train` | 训练模型 | `--mode train` |
| `predict` | 预测（使用已有模型） | `--mode predict` |
| `history` | 历史数据采集 | `--mode history --start-date YYYYMMDD --end-date YYYYMMDD` |
| `data` | 数据采集 | `--mode data` |

## 架构

```
src/
├── main.py              # 主入口
├── config.py            # 配置管理
├── runner.py            # 流程编排器
├── agents/              # Agent 层
│   ├── base_agent.py    # Agent 基类
│   ├── data_agent.py    # 数据采集（同花顺）
│   ├── analysis_agent.py # 热点分析
│   └── predict_agent.py # 预测
├── analysis/            # 分析核心
│   ├── hotspot_detector.py    # 热点识别
│   ├── rotation_analyzer.py   # 轮动分析
│   └── pattern_learner.py     # 模式学习
├── data/                # 数据层
│   ├── tushare_ths_client.py  # 同花顺客户端
│   └── data_collector.py      # 采集调度器
├── models/              # 预测模型
│   └── predictor.py     # XGBoost 预测
├── evaluation/          # 评估
│   └── metrics.py       # 评估指标
├── learning/            # 学习模块
│   └── rotation_learner.py # 轮动规律学习
├── core/                # 核心工具
│   └── settings.py      # 系统设置
└── utils/               # 工具类
    └── logger.py        # 日志工具
```

## 功能模块

| 模块 | 功能 |
|------|------|
| **数据采集** | Tushare 同花顺数据源（行业/指数板块数据） |
| **热点识别** | 多维度热点强度计算（涨幅、资金、情绪、持续性） |
| **轮动分析** | 板块相关性、领涨 - 滞后关系、轮动路径 |
| **预测模型** | XGBoost 统一模型，支持 1 日/5 日/20 日预测 |
| **规律学习** | 动量效应、反转效应、轮动周期学习 |

## 数据目录

```
data/
├── raw/          # 原始 CSV 数据（ths_*.csv 格式）
├── processed/    # 处理后数据
├── features/     # 特征工程结果
├── models/       # 训练好的模型
├── patterns/     # 学习的规律
└── results/      # 分析结果
```

## 数据源

| 数据源 | 客户端 | 说明 |
|--------|--------|------|
| Tushare Pro (同花顺) | `TushareTHSClient` | 行业/指数板块数据 |

**数据文件格式**: `ths_{ts_code}.csv`（例如：`ths_881101_TI.csv`）

**必需字段**: `concept_code`, `trade_date`, `pct_chg`, `name`, `vol`, `close`

**接口权限**: 需 Tushare 积分 >= 5000

## 测试

```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行特定测试模块
python -m pytest tests/test_e2e_pipeline.py -v
python -m pytest tests/test_analysis.py -v
python -m pytest tests/test_prediction.py -v

# 运行覆盖率测试
python -m pytest tests/ --cov=src -v
```

**测试覆盖**:
- `test_data_collection.py` - 数据采集测试
- `test_analysis.py` - 热点和轮动分析测试
- `test_prediction.py` - 预测模型测试
- `test_e2e_pipeline.py` - 端到端流程测试

## 简化说明

相比原始架构，做了以下简化：

1. **只保留同花顺数据源** - 删除东方财富（dc_系列）接口
2. **删除 LLM 模块** - 移除 LangChain、通义千问依赖
3. **删除 ExplainAgent** - 用简单报告模板替代
4. **删除 Coordinator 层** - 直接用 SimpleRunner 编排
5. **合并预测模型** - 单一 XGBoost 模型替代多周期模型
6. **简化依赖** - 移除 Redis、TA-Lib、Streamlit 等

## 依赖

**核心依赖**: pandas, numpy, scikit-learn, xgboost, lightgbm, tushare, sqlalchemy, pydantic-settings, loguru

详见 `requirements.txt`

## 故障排查

### TUSHARE_TOKEN 未配置
```bash
# 检查 .env 文件
cat .env | grep TUSHARE_TOKEN
# 设置环境变量
export TUSHARE_TOKEN=your_token
```

### ths_daily 返回空数据
系统自动切换到 `index_daily()` 备用接口

### 权限不足
访问 https://tushare.pro/user/token 确认积分 >= 5000

## 相关文档

- `CLAUDE.md` - 项目配置说明
- `TEST_SUMMARY.md` - 测试总结

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
