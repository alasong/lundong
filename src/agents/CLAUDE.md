# Agent 层上下文

> Agent 层负责业务编排，协调其他模块完成复杂任务。

---

## 文件结构

```
src/agents/
├── __init__.py
├── base_agent.py        # 基类 (101 行)
├── data_agent.py        # 数据采集 (294 行)
├── analysis_agent.py    # 热点分析 (321 行)
├── predict_agent.py     # 预测 (506 行)
└── portfolio_agent.py   # 组合构建 (360 行)
```

---

## 基类：BaseAgent

**文件**: `base_agent.py`

**核心方法**:
```python
class BaseAgent(ABC):
    def __init__(self, name: str)      # 初始化
    def run(self, *args, **kwargs)     # 抽象方法，子类实现
    def execute(self, *args, **kwargs) # 包装器，自动错误处理
    def get_status(self) -> Dict       # 获取状态
    def reset(self)                    # 重置状态
```

**状态管理**:
- `status`: idle → running → success/error
- `error_count`: 错误计数，超过 `max_errors` 停止
- `last_run_time`: 最后运行时间

**返回格式**:
```python
{"success": bool, "agent": str, "timestamp": str, "result": dict, "error": str}
```

---

## DataAgent - 数据采集

**文件**: `data_agent.py`

**依赖**:
- `TushareTHSClient` - Tushare 同花顺接口
- `HighSpeedDataCollector` - 高速并发采集
- `StorageManager` - 存储管理

**任务类型**:

| 任务 | 方法 | 说明 |
|------|------|------|
| `daily` | `_collect_daily()` | 每日增量采集 |
| `history` | `_collect_history()` | 历史数据采集 |
| `lists` | `_collect_lists()` | 板块列表采集 |
| `basic` | `_collect_basic()` | 基础数据采集 |

**关键逻辑**:
```python
# _collect_daily() 流程
1. 检测最新日期 → 判断起始日期
2. 调用 _collect_lists() 获取板块列表
3. 使用 HighSpeedDataCollector 批量下载
4. 自动导出到 CSV
```

**sector_type 参数**:
- `all`: 881(行业) + 882(地区) + 885(概念)
- `concept`: 仅 885 概念板块
- `industry`: 仅 881 行业板块

**修改注意**:
- API 限流 500 次/分钟，`fast_collector.py` 自动处理
- 新增数据源需修改此 Agent

---

## AnalysisAgent - 热点分析

**文件**: `analysis_agent.py`

**依赖**:
- `HotspotDetector` - 热点识别
- `RotationAnalyzer` - 轮动分析
- `PatternLearner` - 规律学习

**任务类型**:

| 任务 | 方法 | 输出 |
|------|------|------|
| `hotspot` | `_analyze_hotspot()` | 热点评分、TOP10 |
| `rotation` | `_analyze_rotation()` | 相关性矩阵、轮动信号 |
| `pattern` | `_learn_patterns()` | 轮动规则 |
| `all` | `_full_analysis()` | 全部分析 |

**数据加载**:
```python
# _load_latest_data() 从数据库加载最近 60 天数据
# 支持 ths_*_TI.csv 格式（同花顺）
# 使用 joblib 并行加载
```

**修改注意**:
- 修改权重在 `config.py` 的 `hotspot_weights`
- 添加新分析方法需修改 `run()` 分支

---

## PredictAgent - 预测

**文件**: `predict_agent.py`

**依赖**:
- `UnifiedPredictor` - XGBoost 预测器
- `SQLiteDatabase` - 数据库

**任务类型**:

| 任务 | 方法 | 说明 |
|------|------|------|
| `train` | `_train_models()` | 训练 3 个周期模型 |
| `predict` | `_predict()` | 预测并格式化输出 |

**训练流程**:
```python
# _train_models()
1. _stream_prepare_features() 流式加载特征
2. predictor.train() 训练模型
3. 保存到 data/models/unified_model.pkl
```

**预测流程**:
```python
# _predict()
1. _load_latest_data() 加载最近数据
2. prepare_features() 准备特征 (32 线程)
3. predict() 预测 (32 线程，带置信度)
4. _format_predictions() 格式化输出
```

**流式特征准备**:
```python
# _stream_prepare_features() - 避免 OOM
# 分批从数据库读取，边加载边处理
# batch_size=50 个板块/批次
```

**模型位置**: `data/models/unified_model.pkl`

**修改注意**:
- 特征工程在 `models/predictor.py`
- 预测并发数 `n_jobs=32` 可调整

---

## PortfolioAgent - 组合构建

**文件**: `portfolio_agent.py`

**依赖**:
- `StockScreener` - 个股筛选
- `StockPredictor` - 个股预测
- `PortfolioOptimizer` - 组合优化
- `SQLiteDatabase` - 数据库

**任务类型**:

| 任务 | 方法 | 流程 |
|------|------|------|
| `build` | `_build_portfolio()` | 筛选→预测→优化 |
| `screen` | `_screen_stocks()` | 仅筛选 |
| `predict` | `_predict_stocks()` | 筛选→预测 |
| `optimize` | `_optimize()` | 仅优化 |

**完整流程**:
```python
# _build_portfolio()
1. 过滤有成分股的板块
2. StockScreener.screen_stocks() 筛选
3. _get_stock_data() 获取个股历史
4. StockPredictor.prepare_features() 准备特征
5. StockPredictor.predict() 预测
6. PortfolioOptimizer.optimize() 优化权重
```

**前置条件**:
- `concept_constituent` 表有成分股数据
- 板块预测结果 (`concept_predictions`)

**修改注意**:
- 筛选规则在 `data/stock_screener.py`
- 优化约束在 `portfolio/optimizer.py`

---

## Runner - 工作流编排

**文件**: `runner.py` (非 Agent)

**职责**: 串联各 Agent 完成工作流

**核心方法**:
```python
class SimpleRunner:
    def __init__(self):
        self.data_agent = DataAgent()
        self.analysis_agent = AnalysisAgent()
        self.predict_agent = PredictAgent()

    def run_daily(self, date, train=False):
        # 采集 → 分析 → [训练] → 预测 → 报告

    def quick_analysis(self, date=None):
        # 分析 → 预测 → 报告（不采集新数据）
```

---

## 调试技巧

```python
# 单独测试 Agent
from agents.data_agent import DataAgent
agent = DataAgent()
result = agent.execute(task="daily")
print(result)

# 查看状态
print(agent.status, agent.error_count)

# 直接调用 run()（调试用，会抛真实异常）
try:
    result = agent.run(task="xxx")
except Exception as e:
    print(f"真实错误: {e}")
```