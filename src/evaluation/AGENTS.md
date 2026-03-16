# Evaluation 层上下文

> 评估模块负责模型回测、交叉验证和性能指标计算。

---

## 文件结构

```
src/evaluation/
├── __init__.py
├── backtester.py            # 回测引擎
├── enhanced_backtester.py   # 增强回测
├── metrics.py               # 评估指标
├── model_validation.py      # 模型验证
├── parameter_sensitivity.py # 参数敏感性
└── backtest_prediction.py   # 预测回测
```

---

## Backtester - 回测引擎

**文件**: `backtester.py`

**职责**: 滚动回测验证模型效果

### 核心方法

```python
class Backtester:
    def __init__(initial_capital=1000000.0)
    def run_walk_forward(concept_data, train_windows, test_windows, step)
    def _run_single_fold(train_data, test_data)
    def _evaluate_predictions(predictions, actuals)
```

### 滚动回测 (Walk-Forward)

```python
def run_walk_forward(concept_data, train_windows=20, test_windows=5, step=5):
    """
    参数:
        train_windows: 训练窗口 (月)
        test_windows: 测试窗口 (月)
        step: 滚动步长 (月)

    流程:
    for each_fold:
        1. 划分训练集/测试集
        2. 训练模型
        3. 预测测试集
        4. 计算评估指标
        5. 滚动到下一窗口
    """
```

### 日期划分

```python
# 每月约 21 个交易日
TRADING_DAYS_PER_MONTH = 21

train_end_idx = (i + train_windows) * 21
test_end_idx = (i + train_windows + test_windows) * 21
```

### 输出格式

```python
{
    'fold_results': [
        {
            'fold': 1,
            'train_start': '20230101',
            'train_end': '20231231',
            'test_start': '20240101',
            'test_end': '20240301',
            'metrics': {...}
        },
        ...
    ],
    'summary': {
        'avg_ic': 0.05,
        'avg_rank_ic': 0.06,
        'avg_sharpe': 1.2,
        'avg_direction_acc': 0.55
    }
}
```

---

## ModelEvaluator - 评估指标

**文件**: `metrics.py`

**职责**: 计算预测性能指标

### 核心方法

```python
class ModelEvaluator:
    def evaluate_prediction(predictions, actuals, horizon)
    def evaluate_hotspot_prediction(predicted, actual, top_n)
    def compute_ic(predicted_rank, actual_rank)
```

### 评估指标

| 指标 | 说明 | 计算方法 |
|------|------|----------|
| **IC** | 预测相关性 | Pearson 相关系数 |
| **RankIC** | 排名相关性 | Spearman 相关系数 |
| **MSE** | 均方误差 | mean((pred - actual)²) |
| **MAE** | 平均绝对误差 | mean(\|pred - actual\|) |
| **R²** | 决定系数 | 1 - SS_res/SS_tot |
| **Direction Acc** | 方向准确率 | 预测方向正确比例 |
| **Sharpe** | 夏普比率 | 年化收益 / 波动率 |
| **Max Drawdown** | 最大回撤 | 峰值到谷底最大跌幅 |

### IC 计算

```python
def compute_ic(predictions, actuals):
    """
    IC = Pearson(pred, actual)
    RankIC = Spearman(pred, actual)

    通常 IC > 0.05 即有预测能力
    """
    ic = np.corrcoef(predictions, actuals)[0, 1]
    rank_ic = scipy.stats.spearmanr(predictions, actuals).correlation
    return ic, rank_ic
```

### 热点预测评估

```python
def evaluate_hotspot_prediction(predicted_hotspots, actual_hotspots, top_n=10):
    """
    评估 TOP N 热点预测准确率

    指标:
    - hit_rate: 命中率 (预测热点实际也是热点)
    - precision: 精确率
    - recall: 召回率
    """
```

---

## 模型验证

**文件**: `model_validation.py`

**职责**: 交叉验证和模型稳定性检验

### Purged K-Fold 交叉验证

```python
# 防止数据泄露的时序交叉验证
# 在训练集和验证集之间设置 purge 和 embargo

CV_SPLITS = 5
CV_TRAIN_MONTHS = 24
CV_PURGE = 5       # 清除天数
CV_EMBARGO = 2     # 禁运天数
```

### 验证流程

```python
def purged_kfold_cv(data, n_splits=5, purge=5, embargo=2):
    """
    1. 按时间顺序划分 K 个折叠
    2. 每个折叠:
       - 训练集 (移除末尾 purge 天)
       - 验证集 (移除开头 embargo 天)
    3. 计算 K 次验证的平均指标
    """
```

---

## 参数敏感性分析

**文件**: `parameter_sensitivity.py`

**职责**: 分析模型参数对结果的影响

### 敏感性测试

```python
def analyze_parameter_sensitivity(
    data,
    param_name,
    param_values,
    base_params
):
    """
    测试单个参数变化对性能的影响

    例如:
    - n_estimators: [100, 200, 300]
    - max_depth: [3, 5, 7]
    - learning_rate: [0.01, 0.05, 0.1]
    """
```

---

## 回测命令

```bash
# 滚动回测
python src/main.py --mode backtest --start-date 20230101 --end-date 20241231

# 自定义参数
BACKTEST_TRAIN=12 BACKTEST_TEST=3 BACKTEST_STEP=3 \
  python src/main.py --mode backtest --start-date 20230101 --end-date 20241231

# 交叉验证
python src/main.py --mode cv --start-date 20230101 --end-date 20241231

# 自定义 CV 参数
CV_SPLITS=5 CV_TRAIN_MONTHS=24 CV_PURGE=5 CV_EMBARGO=2 \
  python src/main.py --mode cv --start-date 20230101 --end-date 20241231
```

---

## 调试命令

```python
# 测试回测
from evaluation.backtester import Backtester
from data.database import get_database

db = get_database()
data = db.get_concept_data(days=500)

backtester = Backtester()
results = backtester.run_walk_forward(
    data,
    train_windows=12,
    test_windows=3,
    step=3
)

print(f"平均 IC: {results['summary']['avg_ic']:.4f}")
print(f"方向准确率: {results['summary']['avg_direction_acc']:.2%}")

# 测试指标计算
from evaluation.metrics import ModelEvaluator

evaluator = ModelEvaluator()
metrics = evaluator.evaluate_prediction(predictions, actuals, horizon='short')
print(metrics)
```

---

## 修改注意

- 回测窗口需根据数据量调整
- Purge/Embargo 防止数据泄露
- IC 指标是预测能力的核心度量
- 回测结果可能过拟合，需谨慎解读

---

## 相关文档

- [../models/CLAUDE.md](../models/CLAUDE.md) - 模型层
- [../agents/CLAUDE.md](../agents/CLAUDE.md) - Agent 层