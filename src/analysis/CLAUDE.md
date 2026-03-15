# Analysis 层上下文

> 分析层负责热点识别、轮动分析和规律学习。

---

## 文件结构

```
src/analysis/
├── __init__.py
├── hotspot_detector.py   # 热点识别
├── rotation_analyzer.py  # 轮动分析
└── pattern_learner.py    # 规律学习
```

---

## HotspotDetector - 热点识别

**文件**: `hotspot_detector.py`

**职责**: 计算板块热点强度，识别市场热点

### 热点评分模型

```python
hotspot_score = (
    0.30 * price_strength +      # 涨幅强度
    0.25 * money_strength +      # 资金强度
    0.20 * sentiment_strength +  # 情绪强度
    0.15 * persistence +         # 持续性
    0.10 * market_position       # 市场地位
)
```

### 权重配置

**文件**: `src/config.py`

```python
hotspot_weights = {
    "price_strength": 0.30,
    "money_strength": 0.25,
    "sentiment_strength": 0.20,
    "persistence": 0.15,
    "market_position": 0.10,
}
```

### 核心方法

```python
class HotspotDetector:
    def compute_hotspot_score(concept_data, moneyflow_data, limit_data)
    def _compute_price_strength(row, day_data)      # 百分位排名
    def _compute_money_strength(row, moneyflow)     # 资金净流入
    def _compute_sentiment_strength(row, limit)     # 涨跌停/连板
    def _compute_persistence(row, historical)       # 连续上涨天数
    def _compute_market_position(row, day_data)     # 成交额占比
```

### 评分维度

| 维度 | 计算方法 | 数据依赖 |
|------|----------|----------|
| 涨幅强度 | 涨幅百分位排名 | pct_chg |
| 资金强度 | 主力净流入占比 | moneyflow |
| 情绪强度 | 涨停股数量/连板 | limit_data |
| 持续性 | 连续上涨天数 | historical |
| 市场地位 | 成交额占比 | amount |

### 输出格式

```python
DataFrame columns:
- trade_date, concept_code, concept_name
- price_strength, money_strength, sentiment_strength
- persistence, market_position
- hotspot_score  # 综合评分 0-100
```

---

## RotationAnalyzer - 轮动分析

**文件**: `rotation_analyzer.py`

**职责**: 分析板块轮动规律，识别轮动信号

### 核心方法

```python
class RotationAnalyzer:
    def compute_correlation_matrix(price_data, window=20)  # 相关性矩阵
    def compute_lead_lag_matrix(price_data, max_lag=5)     # 领涨滞后
    def detect_rotation_signal(price_data, corr_matrix)    # 轮动信号
    def find_rotation_path(price_data, top_n=5)            # 轮动路径
```

### 相关性矩阵

```python
def compute_correlation_matrix(price_data, window=20):
    """
    1. 转换为宽格式 (trade_date x concept_code)
    2. 计算滚动相关性
    3. 返回最近一天的相关性矩阵
    """
```

**输出**: N x N 相关性矩阵

### 领涨-滞后关系

```python
def compute_lead_lag_matrix(price_data, max_lag=5):
    """
    对于每对板块 (A, B):
    1. 计算 A 领先 1-5 天的相关性
    2. 找出最佳滞后天数
    3. 记录相关系数和滞后天数
    """
```

**输出**:
```python
# lead_lag.loc[leader, lagger] = {
#     'corr': 0.85,    # 相关性
#     'lag': 2         # 领先天数
# }
```

### 轮动信号检测

```python
def detect_rotation_signal(price_data, corr_matrix):
    """
    识别轮动信号:
    1. 高相关性板块对 (>0.7)
    2. 近期走势背离 (一个涨一个跌)
    3. 预测可能轮动
    """
```

---

## PatternLearner - 规律学习

**文件**: `pattern_learner.py`

**职责**: 从历史数据中学习热点轮动规律

### 核心方法

```python
class PatternLearner:
    def learn_rotation_rules(hotspot_scores, rotation_paths)  # 学习轮动规则
    def _learn_concept_sequences(hotspot_scores)              # 概念序列
    def _learn_duration_patterns(rotation_paths)              # 持续时间
    def _learn_intensity_patterns(hotspot_scores)             # 强度变化
```

### 轮动规则结构

```python
rules = {
    "concept_sequences": {    # 概念序列规律
        "885001.TI": {        # 从板块 A
            "885002.TI": 0.15,  # 到板块 B 的概率
            "885003.TI": 0.12,  # 到板块 C 的概率
        }
    },
    "duration_patterns": {},   # 热点持续时间规律
    "intensity_patterns": {},  # 热点强度变化规律
}
```

### 概念序列学习

```python
def _learn_concept_sequences(hotspot_scores, top_n=5):
    """
    1. 获取每日 Top N 热点
    2. 分析连续两天热点转换
    3. 计算转换概率矩阵
    """
```

### 输出存储

**位置**: `data/patterns/rotation_rules.json`

---

## 数据流

```
concept_daily 数据
        │
        ▼
HotspotDetector.compute_hotspot_score()
        │
        ├─→ hotspot_score (每日热点评分)
        │
        ▼
RotationAnalyzer.compute_correlation_matrix()
        │
        ├─→ 相关性矩阵
        ├─→ 领涨滞后关系
        └─→ 轮动信号
        │
        ▼
PatternLearner.learn_rotation_rules()
        │
        └─→ rotation_rules.json
```

---

## 调试命令

```python
# 测试热点识别
from analysis.hotspot_detector import HotspotDetector
from data.database import get_database

db = get_database()
data = db.get_concept_data(days=60)

detector = HotspotDetector()
scores = detector.compute_hotspot_score(data)
print(scores.nlargest(10, 'hotspot_score'))

# 测试轮动分析
from analysis.rotation_analyzer import RotationAnalyzer

analyzer = RotationAnalyzer()
corr = analyzer.compute_correlation_matrix(data)
print(corr.head())

# 测试规律学习
from analysis.pattern_learner import PatternLearner

learner = PatternLearner()
rules = learner.learn_rotation_rules(scores, None)
print(rules['concept_sequences'])
```

---

## 修改注意

- 热点权重在 `config.py` 配置
- 新增评分维度需更新 `compute_hotspot_score()`
- 规律学习结果用于增强预测
- 相关性计算需要足够数据量 (>20天)

---

## 相关文档

- [../models/CLAUDE.md](../models/CLAUDE.md) - 模型层
- [../agents/CLAUDE.md](../agents/CLAUDE.md) - Agent 层