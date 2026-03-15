# CLAUDE.md - 系统上下文索引

> 本文档帮助 Claude 快速理解项目架构和定位关键代码。

---

## 系统架构

```
┌────────────────────────────────────────────────────────────────┐
│                        CLI (main.py)                           │
│                    17 种运行模式入口                             │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                   Runner (runner.py)                           │
│                     工作流编排器                                 │
└────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│  agents/      │    │  analysis/    │    │  models/      │
│  Agent 层     │───▶│  分析层       │───▶│  模型层       │
│  业务编排     │    │  热点+轮动     │    │  XGBoost      │
└───────────────┘    └───────────────┘    └───────────────┘
        │                                        │
        ▼                                        ▼
┌───────────────┐                       ┌───────────────┐
│  data/        │                       │  portfolio/   │
│  数据层       │                       │  组合优化     │
│  DB+API       │                       │  风险控制     │
└───────────────┘                       └───────────────┘
```

---

## 模块文档索引

| 模块 | 文档 | 核心职责 |
|------|------|----------|
| **Agent 层** | [src/agents/CLAUDE.md](src/agents/CLAUDE.md) | 业务编排、工作流控制 |
| **数据层** | [src/data/CLAUDE.md](src/data/CLAUDE.md) | 数据采集、存储、验证 |
| **模型层** | [src/models/CLAUDE.md](src/models/CLAUDE.md) | 特征工程、预测模型 |
| **分析层** | [src/analysis/CLAUDE.md](src/analysis/CLAUDE.md) | 热点识别、轮动分析 |
| **组合优化** | [src/portfolio/CLAUDE.md](src/portfolio/CLAUDE.md) | 股票筛选、权重优化 |
| **评估模块** | [src/evaluation/CLAUDE.md](src/evaluation/CLAUDE.md) | 回测、交叉验证 |

---

## 关键文件速查

### 入口文件

| 文件 | 行数 | 职责 |
|------|------|------|
| `src/main.py` | 946 | CLI 入口，17 种模式 |
| `src/runner.py` | 259 | 工作流编排 |
| `src/config.py` | 68 | 配置管理 |

### 核心模块

| 文件 | 行数 | 职责 | 修改注意 |
|------|------|------|----------|
| `src/data/database.py` | 1069 | SQLite 管理、WAL 模式 | 连接池、并发安全 |
| `src/models/predictor.py` | 728 | XGBoost 预测器 | 特征工程是核心 |
| `src/data/stock_screener.py` | 742 | 个股筛选 | 筛选规则影响组合 |
| `src/data/fast_collector.py` | 675 | 高速采集 | API 限流处理 |

---

## 数据流向

```
Tushare API
     │
     ▼ (采集)
┌─────────────┐
│   SQLite    │  ◀── 主存储 (WAL 模式)
│  stock.db   │
└─────────────┘
     │
     ▼ (导出)
┌─────────────┐
│    CSV      │  ◀── 备份/分析用
│  merged_*   │
└─────────────┘
     │
     ▼ (加载)
┌─────────────┐
│  DataFrame  │  ◀── 内存处理
└─────────────┘
     │
     ▼ (特征)
┌─────────────┐
│  Features   │  ◀── 65+ 特征
└─────────────┘
     │
     ▼ (预测)
┌─────────────┐
│ Predictions │  ◀── 1d/5d/20d
└─────────────┘
```

---

## 常见修改场景

### 1. 添加新的数据源

1. 在 `src/data/` 创建新的 client 文件
2. 在 `DataAgent` 中添加采集方法
3. 更新 `database.py` 表结构（如需要）

### 2. 添加新的预测特征

1. 修改 `src/models/predictor.py` 的 `_process_single_concept_vectorized()`
2. 确保特征长度一致
3. 重新训练模型

### 3. 修改热点评分权重

1. 修改 `src/config.py` 中的 `hotspot_weights`
2. 或修改 `src/analysis/hotspot_detector.py`

### 4. 添加新的运行模式

1. 在 `src/main.py` 添加 argparse 参数
2. 在 `main()` 函数添加处理分支
3. 更新 README.md

### 5. 修改组合约束

1. 修改 `src/portfolio/optimizer.py`
2. 调整 `max_position`、`max_sector` 参数

---

## 配置要点

### 环境变量 (.env)

```bash
TUSHARE_TOKEN=xxx      # 必需，Tushare API Token
DATABASE_URL=sqlite:///data/stock.db
LOG_LEVEL=INFO
```

### 关键配置 (src/config.py)

```python
hotspot_weights = {
    "price_strength": 0.30,
    "money_strength": 0.25,
    "sentiment_strength": 0.20,
    "persistence": 0.15,
    "market_position": 0.10,
}

prediction_horizons = {
    "short_term": 1,   # 1 日
    "mid_term": 5,     # 5 日
    "long_term": 20,   # 20 日
}
```

---

## 调试命令

```python
# 查看数据库统计
from data.database import get_database
db = get_database()
db.get_statistics()

# 测试 Agent
from agents.predict_agent import PredictAgent
agent = PredictAgent()
result = agent.execute(task="predict")

# 查看特征重要性
from models.predictor import UnifiedPredictor
p = UnifiedPredictor()
p.print_feature_importance(top_n=20)
```

---

## 测试文件

```bash
# 运行测试
python -m pytest tests/ -v

# 单独测试
python test_simple.py
python -m pytest tests/test_prediction.py -v
```

---

## 相关文档

- `README.md` - 用户使用指南
- `ARCHITECTURE.md` - 架构详解