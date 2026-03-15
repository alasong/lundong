# Portfolio 层上下文

> 组合优化层负责股票筛选、权重优化和风险控制。

---

## 文件结构

```
src/portfolio/
├── __init__.py
└── optimizer.py   # 组合优化器
```

---

## PortfolioOptimizer - 组合优化器

**文件**: `optimizer.py`

**职责**: 构建投资组合，决定最终持仓权重

### 核心方法

```python
class PortfolioOptimizer:
    def optimize(stock_predictions, concept_predictions, **constraints)
    def _calculate_correlation(stock_codes)           # 计算相关性
    def _risk_parity_weights(candidates, corr)        # 风险平价
    def _apply_black_litterman(weights, views)        # Black-Litterman
    def _apply_constraints(weights, max_pos, max_sector)  # 应用约束
```

### 优化流程

```python
def optimize(stock_predictions, concept_predictions, ...):
    """
    1. 选择候选股票 (TOP N * 2)
    2. 计算个股相关性矩阵
    3. 风险平价初始权重
    4. Black-Litterman 融入板块观点
    5. 应用权重约束
    6. 计算预期指标
    """
```

### 约束条件

| 约束 | 默认值 | 说明 |
|------|--------|------|
| `max_position` | 10% | 单股最大权重 |
| `min_position` | 2% | 单股最小权重 |
| `max_sector` | 25% | 单板块最大权重 |
| `target_risk` | 15% | 目标年化波动率 |
| `top_n_stocks` | 10 | 最终持仓数量 |

### 风险平价模型

```python
def _risk_parity_weights(candidates, corr_matrix):
    """
    让每只股票对组合风险贡献相等

    权重计算:
    w_i ∝ 1 / σ_i

    其中 σ_i 是个股波动率
    """
```

### Black-Litterman 模型

```python
def _apply_black_litterman(weights, views):
    """
    将板块预测观点融入权重

    views: 板块预测涨跌幅
    - 看好板块 → 提高成分股权重
    - 看空板块 → 降低成分股权重
    """
```

### 输出格式

```python
{
    'portfolio': [
        {
            'ts_code': '000001.SZ',
            'stock_name': '平安银行',
            'weight': 0.08,
            'concept_code': '885001.TI',
            'concept_name': '银行',
            'combined_score': 75.2
        },
        ...
    ],
    'metrics': {
        'expected_return': 0.25,      # 预期年化收益
        'expected_volatility': 0.15,  # 预期波动率
        'sharpe': 1.67,               # 夏普比率
        'max_drawdown': 0.12,         # 最大回撤
        'sector_concentration': 0.35, # 板块集中度
    },
    'risk_analysis': {
        'avg_correlation': 0.45,      # 平均相关性
        'sector_distribution': {...}, # 板块分布
    }
}
```

---

## 组合构建完整流程

```
板块预测 (concept_predictions)
        │
        ▼
StockScreener.screen_stocks()
        │
        ├─→ 筛选流动性/估值/市值合规个股
        ├─→ 计算个股评分
        │
        ▼
StockPredictor.predict()
        │
        ├─→ 预测个股 1d/5d/20d 涨幅
        ├─→ 计算综合得分
        │
        ▼
PortfolioOptimizer.optimize()
        │
        ├─→ 风险平价初始权重
        ├─→ Black-Litterman 调整
        ├─→ 应用约束
        │
        ▼
最终投资组合
```

---

## 板块集中度控制

```python
def _apply_constraints(weights, max_position, max_sector):
    """
    1. 单股权重不超过 max_position
    2. 单板块权重不超过 max_sector
    3. 权重归一化
    """
    # 按板块分组
    sector_weights = weights.groupby('concept_code')['weight'].sum()

    # 超限板块等比例缩减
    for sector, total in sector_weights.items():
        if total > max_sector:
            scale = max_sector / total
            weights[weights['concept_code'] == sector] *= scale
```

---

## 调试命令

```python
# 测试组合优化
from portfolio.optimizer import PortfolioOptimizer
from data.database import get_database
import pandas as pd

optimizer = PortfolioOptimizer()

# 模拟输入
stock_preds = pd.DataFrame({
    'ts_code': ['000001.SZ', '000002.SZ'],
    'stock_name': ['平安银行', '万科A'],
    'concept_code': ['885001.TI', '885002.TI'],
    'combined_score': [75, 70]
})

result = optimizer.optimize(stock_preds, top_n_stocks=2)
print(result['portfolio'])
print(result['metrics'])
```

---

## 修改注意

- 约束参数影响最终持仓分布
- 风险平价依赖历史波动率
- Black-Litterman 依赖板块预测准确性
- 相关性矩阵计算需足够数据

---

## 相关文档

- [../data/CLAUDE.md](../data/CLAUDE.md) - 数据层 (StockScreener)
- [../models/CLAUDE.md](../models/CLAUDE.md) - 模型层 (StockPredictor)
- [../agents/CLAUDE.md](../agents/CLAUDE.md) - Agent 层 (PortfolioAgent)