# Models 层上下文

> 模型层负责特征工程和预测模型。

---

## 文件结构

```
src/models/
├── __init__.py
├── predictor.py        # 板块预测 (728 行) ⭐ 核心
└── stock_predictor.py  # 个股预测
```

---

## 核心类：UnifiedPredictor

**文件**: `predictor.py`

**职责**: 板块走势预测，XGBoost 模型

**模型配置**:
```python
# XGBoost 主模型
xgb.XGBRegressor(
    n_estimators=200,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    n_jobs=-1
)

# LightGBM 备用
lgb.LGBMRegressor(
    n_estimators=200,
    max_depth=5,
    learning_rate=0.05
)
```

**预测周期**:
```python
HORIZON_WEIGHTS = {
    "1d": 0.3,   # 次日预测权重
    "5d": 0.5,   # 5日预测权重
    "20d": 0.2   # 20日预测权重
}

# 综合评分
combined_score = pred_1d * 0.3 + pred_5d * 0.5 + pred_20d * 0.2
```

**核心方法**:
```python
class UnifiedPredictor:
    def prepare_features(concept_data, lookback=10, n_jobs=32)
    def train(features_df)
    def predict(model, features_df, n_jobs=32)
    def save_model(path)
    def load_model(path)
    def print_feature_importance(top_n=20)
```

---

## 特征工程

**文件**: `predictor.py` → `_process_single_concept_vectorized()`

**特征数量**: 65+ 个

### 滚动统计特征
```python
for period in [3, 5, 10]:
    pct_mean_{period}    # 均值
    pct_std_{period}     # 标准差
    pct_max_{period}     # 最大值
    pct_min_{period}     # 最小值
```

### 动量特征
```python
momentum_3      # 3日动量
momentum_5      # 5日动量
momentum_10     # 10日动量
momentum_accel  # 动量加速度
```

### 波动率特征
```python
for period in [3, 5, 10, 20]:
    volatility_{period}   # 波动率
    skewness_{period}     # 偏度
    kurtosis_{period}     # 峰度
```

### 价格位置特征
```python
for period in [5, 10, 20]:
    pct_rank_{period}    # 百分位排名
    breakout_{period}    # 突破信号
```

### MACD 特征
```python
MACD          # MACD 线
MACD_signal   # 信号线
MACD_hist     # 柱状图
```

### RSI 特征
```python
RSI_6         # 6日 RSI
RSI_12        # 12日 RSI
```

### 量价关系
```python
vol_ratio         # 量比
vol_trend         # 成交量趋势
vol_price_corr_5  # 5日量价相关
vol_price_corr_10 # 10日量价相关
```

### 均值回归
```python
for period in [5, 10]:
    zscore_{period}      # Z分数
    mean_revert_{period} # 均值回归信号
```

### 形态特征
```python
gap_up        # 向上跳空
gap_down      # 向下跳空
extreme_up    # 极端上涨
extreme_down  # 极端下跌
```

### 趋势特征
```python
trend            # 上涨天数占比
连续上涨天数      # 连续上涨天数
```

---

## 特征准备流程

**文件**: `predictor.py` → `prepare_features()`

```python
def prepare_features(concept_data, lookback=10, n_jobs=32):
    """
    1. 按 concept_code 分组
    2. 并行处理每个板块 (joblib + multiprocessing)
    3. 向量化计算特征 (numpy stride_tricks)
    4. 合并所有板块特征
    """
```

**并行处理**:
```python
from joblib import Parallel, delayed

results = Parallel(
    n_jobs=actual_jobs,
    backend="multiprocessing",  # CPU 密集型用多进程
    verbose=0
)(
    delayed(self._process_single_concept_vectorized)(...)
    for concept_code in concept_codes
)
```

**向量化优化**:
```python
# 使用 stride_tricks 创建滚动窗口
window_data = np.lib.stride_tricks.sliding_window_view(
    pct_chg, window_shape=period
)[:valid_samples]
```

---

## 训练流程

**文件**: `predictor.py` → `train()`

```python
def train(features_df):
    """
    1. 分离特征和目标
    2. 划分训练/验证集 (80/20)
    3. 训练 3 个周期模型 (1d/5d/20d)
    4. 保存模型到 data/models/unified_model.pkl
    """
```

**目标变量**:
```python
target_1d  = pct_chg.shift(-1)   # 次日涨跌幅
target_5d  = pct_chg.rolling(5).sum().shift(-5)  # 5日累计
target_20d = pct_chg.rolling(20).sum().shift(-20)  # 20日累计
```

---

## 预测流程

**文件**: `predictor.py` → `predict()`

```python
def predict(model, features_df, n_jobs=32):
    """
    1. 加载模型
    2. 准备特征 (并行)
    3. 预测 3 个周期 (并行)
    4. 计算综合评分
    5. 返回预测结果
    """
```

**输出格式**:
```python
DataFrame columns:
- concept_code, trade_date, name
- pred_1d, pred_5d, pred_20d
- confidence_1d, confidence_5d, confidence_20d
- combined_score
```

---

## 模型存储

**位置**: `data/models/unified_model.pkl`

**保存内容**:
```python
{
    'model_1d': xgb_model,
    'model_5d': xgb_model,
    'model_20d': xgb_model,
    'feature_names': [...],
    'train_date': '20260314',
}
```

---

## StockPredictor

**文件**: `stock_predictor.py`

**职责**: 个股预测

**与板块预测的区别**:
- 特征包含市值、PE、PB 等基本面
- 输出股票代码而非板块代码
- 综合评分权重不同 (1d: 0.4, 5d: 0.4, 20d: 0.2)

**核心方法**:
```python
class StockPredictor:
    def prepare_features(stock_data, concept_data)
    def train(features_df)
    def predict(model, features_df)
```

---

## 流式特征准备

**文件**: `predict_agent.py` → `_stream_prepare_features()`

**用途**: 避免大特征矩阵 OOM

```python
def _stream_prepare_features(db, batch_size=50):
    """
    1. 分批从数据库读取板块数据
    2. 逐批处理特征
    3. 边加载边处理，不一次性加载全部
    """
```

---

## 调试命令

```python
# 查看特征重要性
from models.predictor import UnifiedPredictor
p = UnifiedPredictor()
p.load_model("data/models/unified_model.pkl")
p.print_feature_importance(top_n=20)

# 单独测试特征准备
from data.database import get_database
db = get_database()
data = db.get_concept_data(codes=['885001.TI'])
features = p.prepare_features(data, n_jobs=8)
print(features.columns.tolist())
```

---

## 修改注意

- 特征工程是核心，修改需谨慎
- 新增特征需确保长度与现有特征一致
- 并行数 `n_jobs` 可根据 CPU 调整
- 模型文件较大 (~50MB)，注意存储

---

## 相关文档

- [../data/CLAUDE.md](../data/CLAUDE.md) - 数据层
- [../agents/CLAUDE.md](../agents/CLAUDE.md) - Agent 层