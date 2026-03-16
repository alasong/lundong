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
| **Agent 层** | [src/agents/AGENTS.md](src/agents/AGENTS.md) | 业务编排、工作流控制 |
| **数据层** | [src/data/AGENTS.md](src/data/AGENTS.md) | 数据采集、存储、验证 |
| **模型层** | [src/models/AGENTS.md](src/models/AGENTS.md) | 特征工程、预测模型 |
| **分析层** | [src/analysis/AGENTS.md](src/analysis/AGENTS.md) | 热点识别、轮动分析 |
| **组合优化** | [src/portfolio/AGENTS.md](src/portfolio/AGENTS.md) | 股票筛选、权重优化 |
| **评估模块** | [src/evaluation/AGENTS.md](src/evaluation/AGENTS.md) | 回测、交叉验证 |

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

## Subagents 子代理配置

### 可用 Subagents

| 名称 | 用途 | 模型 | 调用示例 |
|------|------|------|----------|
| `data-explorer` | 数据层调试 | Sonnet | "使用 data-explorer 检查数据库" |
| `model-trainer` | 模型训练 | Sonnet | "使用 model-trainer 训练模型" |
| `test-runner` | 测试执行 | Haiku | "使用 test-runner 运行测试" |
| `code-reviewer` | 代码审查 | Sonnet | "使用 code-reviewer 审查代码" |
| `doc-writer` | 文档撰写 | Haiku | "使用 doc-writer 更新文档" |

### 高效使用方式

**1. 并行执行**（独立任务同时运行）
```
"并行执行：
1. data-explorer: 数据质量报告
2. test-runner: 运行测试
3. code-reviewer: 代码审查"
```

**2. 串行工作流**（有依赖的任务按顺序执行）
```
"先运行 test-runner 找失败测试，
再用 model-trainer 修复，
最后用 test-runner 验证"
```

**3. 模型选择策略**
- **Sonnet**: 复杂分析、代码修改、深度诊断
- **Haiku**: 快速查询、简单任务、批量执行

### 配置文件

```
.claude/
├── settings.json          # subagent 定义
└── subagents/
    ├── README.md          # 使用指南
    ├── data-explorer.md   # 数据层专家
    ├── model-trainer.md   # 模型训练专家
    ├── test-runner.md     # 测试执行专家
    ├── code-reviewer.md   # 代码审查专家
    └── doc-writer.md      # 文档撰写专家
```

---

## Auto Coding 高级配置

### 项目配置文件

| 文件 | 作用 |
|------|------|
| `.claude/settings.json` | 主配置：hooks、权限、subagents、自动模式 |
| `.claude/subagents/` | 子代理定义文件 |
| `.claude/skills/` | 自定义技能定义 |
| `.claude/memory/` | 项目记忆持久化 |
| `scripts/workflow.py` | 自动化工作流脚本 |

### 自动化 Hooks

```
PreToolUse  → 执行前日志
PostToolUse → 文件修改后语法检查、模块提示
PreCommit   → 提交前测试+代码检查
Stop        → 任务完成提示+git status
```

### 自定义 Skills

```bash
/train-model     # 训练预测模型
/collect-data    # 采集最新数据
/run-prediction  # 执行预测生成报告
/run-tests       # 运行测试套件
/db-stats        # 查看数据库状态
```

### 文件变更自动测试

| 文件路径 | 自动测试 |
|----------|----------|
| `src/data/*.py` | `tests/test_database.py` |
| `src/models/*.py` | `tests/test_prediction.py` |
| `src/agents/*.py` | `tests/test_agents.py` |

### 自动化规则

1. **代码修改后自动验证**
   - Python 文件保存后自动运行 `py_compile` 语法检查
   - 测试文件修改后自动运行相关测试

2. **Git 操作规范**
   - 提交前自动检查测试是否通过
   - 提交信息格式：`<type>: <description>`
   - 类型：feat/fix/refactor/test/docs/style

3. **代码风格**
   - 使用 Python 3.12+ 特性
   - 类型注解推荐但非强制
   - 遵循现有代码风格

### 工作流脚本

```bash
# 语法检查
python scripts/workflow.py syntax src/main.py

# 运行测试
python scripts/workflow.py test

# 训练模型
python scripts/workflow.py train

# 采集数据
python scripts/workflow.py collect

# 执行预测
python scripts/workflow.py predict
```

### 自动模式配置

当前配置：
- `maxSteps: 100` - 最大自动步骤
- `autoCommit: true` - 自动提交
- `autoPush: false` - 不自动推送
- `autoTest: true` - 自动运行测试

---

## 相关文档

- `README.md` - 用户使用指南
- `ARCHITECTURE.md` - 架构详解