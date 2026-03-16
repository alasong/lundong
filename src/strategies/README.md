# 多策略框架文档

## 架构设计

```
strategies/
├── base_strategy.py       # 策略基类 (统一接口)
├── strategy_factory.py    # 策略工厂 (创建/注册策略)
├── multi_strategy.py      # 多策略组合器 (信号合并)
├── hot_rotation.py        # 热点轮动策略 (现有策略重构)
├── momentum.py            # 动量策略 (示例策略)
├── config.py              # 策略配置
├── register.py            # 策略注册
└── example.py             # 使用示例
```

## 快速开始

### 1. 使用单个策略

```python
from strategies.register import *  # 注册所有策略
from strategies.strategy_factory import StrategyFactory

# 创建策略
strategy = StrategyFactory.create_strategy("hot_rotation", params={...})

# 生成信号
signals = strategy.generate_signals()

# 组合优化
portfolio = strategy.optimize_portfolio(signals)
```

### 2. 使用多策略组合

```python
from strategies.multi_strategy import MultiStrategyPortfolio

# 创建多个策略
strategies = StrategyFactory.create_multiple_strategies({
    "hot_rotation": {"enabled": True, "weight": 0.6},
    "momentum": {"enabled": True, "weight": 0.4},
})

# 创建多策略组合器
multi = MultiStrategyPortfolio(
    strategies=strategies,
    strategy_weights={"hot_rotation": 0.6, "momentum": 0.4},
    combination_method="weighted_score"  # 或 "voting"
)

# 生成合并信号
merged_signals = multi.generate_signals()

# 组合优化
portfolio = multi.optimize_portfolio(merged_signals)
```

## 策略接口

### BaseStrategy 基类

所有策略必须实现以下方法：

```python
class BaseStrategy(ABC):
    def generate_signals(**kwargs) -> List[StrategySignal]:
        """生成交易信号"""
        pass
    
    def get_required_data() -> Dict[str, Any]:
        """获取策略所需数据"""
        pass
    
    def optimize_portfolio(signals, **kwargs) -> Dict[str, Any]:
        """组合优化（可选）"""
        pass
```

### StrategySignal 信号

统一信号格式：

```python
@dataclass
class StrategySignal:
    ts_code: str           # 股票代码
    stock_name: str        # 股票名称
    strategy_type: str     # 策略类型
    signal_type: str       # buy/sell/hold
    weight: float          # 建议权重 0-1
    score: float           # 策略评分 0-100
    reason: str            # 信号原因
    metadata: Dict         # 附加信息
```

## 可用策略

### 1. 热点轮动策略 (hot_rotation)

**逻辑**: 板块热点分析 + XGBoost 预测 + 个股筛选

**参数**:
```python
{
    "top_n_concepts": 10,       # 选择 TOP N 板块
    "min_hotspot_score": 60,    # 最小热点评分
    "stocks_per_concept": 5,    # 每个板块选股数量
    "use_prediction": True,     # 是否使用模型预测
}
```

**适用场景**: 震荡市、结构性行情

### 2. 动量策略 (momentum)

**逻辑**: 20 日涨幅 + 成交量突破

**参数**:
```python
{
    "momentum_window": 20,      # 动量计算周期
    "volume_window": 20,        # 成交量计算周期
    "min_momentum": 0.05,       # 最小动量阈值 5%
    "min_volume_ratio": 1.5,    # 最小成交量比率
    "top_n_stocks": 20,         # 选股数量
}
```

**适用场景**: 趋势市

## 信号合并方法

### 1. weighted_score (加权评分)

对同一只股票，多个策略的信号按权重加权平均：

```python
final_score = sum(score_i * weight_i) / sum(weight_i)
```

### 2. voting (投票)

多数策略看好才买入：

```python
if buy_signals > total_signals / 2:
    买入
```

## 添加新策略

### Step 1: 创建策略类

```python
# src/strategies/value.py
from strategies.base_strategy import BaseStrategy, StrategySignal

class ValueStrategy(BaseStrategy):
    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        # 实现价值策略逻辑
        ...
    
    def get_required_data(self) -> Dict:
        return {
            "concept_data": False,
            "stock_data": True,
            "history_days": 60,
            "features": ["pe", "pb", "roe"]
        }
```

### Step 2: 注册策略

```python
# src/strategies/register.py
from strategies.value import ValueStrategy

StrategyFactory.register_strategy("value", ValueStrategy)
```

### Step 3: 使用策略

```python
strategy = StrategyFactory.create_strategy("value")
```

## 配置方式

在 `src/strategies/config.py` 中配置：

```python
strategies_config = {
    "hot_rotation": {
        "enabled": True,
        "weight": 0.6,
        "params": {...}
    },
    "momentum": {
        "enabled": True,
        "weight": 0.4,
        "params": {...}
    },
}
```

## 使用示例

```bash
# 运行示例
python src/strategies/example.py
```

## 调试技巧

```python
# 查看可用策略
available = StrategyFactory.get_available_strategies()
print(available)

# 查看策略信息
strategy = StrategyFactory.create_strategy("hot_rotation")
print(strategy.get_info())

# 测试信号生成
signals = strategy.generate_signals()
for sig in signals[:5]:
    print(f"{sig.ts_code}: {sig.score:.1f}")
```

## 注意事项

1. **策略独立性**: 每个策略应独立生成信号，不依赖其他策略
2. **信号标准化**: 所有策略输出统一的 StrategySignal 格式
3. **权重分配**: 策略权重应总和为 1，或自动归一化
4. **数据依赖**: 策略应明确声明所需数据 (get_required_data)
5. **错误处理**: 单个策略失败不应影响其他策略

## 扩展方向

- 添加更多策略 (价值/均值回归/事件驱动等)
- 实现策略间对冲逻辑
- 添加策略绩效评估
- 实现动态权重调整
- 添加机器学习策略融合
