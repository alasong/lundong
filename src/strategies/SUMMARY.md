# 多策略框架实现总结

## 完成的工作

### 1. 核心框架文件

| 文件 | 说明 | 行数 |
|------|------|------|
| `base_strategy.py` | 策略基类 (统一接口) | ~150 |
| `strategy_factory.py` | 策略工厂 (创建/注册) | ~80 |
| `multi_strategy.py` | 多策略组合器 | ~200 |
| `hot_rotation.py` | 热点轮动策略 | ~200 |
| `momentum.py` | 动量策略 | ~180 |
| `config.py` | 策略配置 | ~40 |
| `register.py` | 策略注册 | ~15 |
| `example.py` | 使用示例 | ~80 |
| `README.md` | 完整文档 | ~250 |

### 2. 策略架构

```
strategies/
├── base_strategy.py       # 抽象基类
│   ├── BaseStrategy       # 策略基类
│   └── StrategySignal     # 信号数据类
│
├── strategy_factory.py    # 工厂模式
│   └── StrategyFactory    # 策略创建器
│
├── multi_strategy.py      # 组合器
│   └── MultiStrategyPortfolio  # 多策略组合
│
├── hot_rotation.py        # 热点轮动策略
│   └── HotRotationStrategy
│
├── momentum.py            # 动量策略
│   └── MomentumStrategy
│
└── config.py              # 配置
```

### 3. 策略接口

**BaseStrategy 基类**:
```python
class BaseStrategy(ABC):
    def generate_signals(**kwargs) -> List[StrategySignal]
    def get_required_data() -> Dict[str, Any]
    def optimize_portfolio(signals) -> Dict[str, Any]
```

**StrategySignal 信号**:
```python
@dataclass
class StrategySignal:
    ts_code: str
    stock_name: str
    strategy_type: str
    signal_type: str  # buy/sell/hold
    weight: float
    score: float
    reason: str
    metadata: Dict
```

### 4. 可用策略

#### 热点轮动策略 (hot_rotation)
- **逻辑**: 板块热点分析 + XGBoost 预测 + 个股筛选
- **参数**:
  - `top_n_concepts`: 10 (选择 TOP N 板块)
  - `min_hotspot_score`: 60 (最小热点评分)
  - `stocks_per_concept`: 5 (每个板块选股数)
  - `use_prediction`: True (是否使用模型预测)
- **适用**: 震荡市、结构性行情

#### 动量策略 (momentum)
- **逻辑**: 20 日涨幅 + 成交量突破
- **参数**:
  - `momentum_window`: 20 (动量周期)
  - `volume_window`: 20 (成交量周期)
  - `min_momentum`: 0.05 (最小动量 5%)
  - `min_volume_ratio`: 1.5 (最小成交量比)
  - `top_n_stocks`: 20 (选股数量)
- **适用**: 趋势市

### 5. 使用方式

#### 单策略模式
```python
from strategies.register import *
from strategies.strategy_factory import StrategyFactory

# 创建策略
strategy = StrategyFactory.create_strategy("hot_rotation")

# 生成信号
signals = strategy.generate_signals()

# 组合优化
portfolio = strategy.optimize_portfolio(signals)
```

#### 多策略组合模式
```python
from strategies.multi_strategy import MultiStrategyPortfolio

# 创建多个策略
strategies = StrategyFactory.create_multiple_strategies({
    "hot_rotation": {"enabled": True, "weight": 0.6},
    "momentum": {"enabled": True, "weight": 0.4},
})

# 创建组合器
multi = MultiStrategyPortfolio(
    strategies=strategies,
    strategy_weights={"hot_rotation": 0.6, "momentum": 0.4},
    combination_method="weighted_score"
)

# 生成合并信号
merged = multi.generate_signals()

# 组合优化
portfolio = multi.optimize_portfolio(merged)
```

### 6. 信号合并方法

1. **weighted_score** (加权评分)
   - 对同一股票，多个策略信号按权重加权
   - `final_score = sum(score * weight) / sum(weight)`

2. **voting** (投票)
   - 多数策略看好才买入
   - `if buy_signals > total / 2: 买入`

### 7. 添加新策略

**Step 1**: 创建策略类
```python
# src/strategies/value.py
from strategies.base_strategy import BaseStrategy, StrategySignal

class ValueStrategy(BaseStrategy):
    def generate_signals(self, **kwargs):
        # 实现逻辑
        ...
```

**Step 2**: 注册策略
```python
# src/strategies/register.py
from strategies.value import ValueStrategy
StrategyFactory.register_strategy("value", ValueStrategy)
```

**Step 3**: 使用
```python
strategy = StrategyFactory.create_strategy("value")
```

### 8. 测试验证

```bash
# 测试策略注册
python -c "
import sys; sys.path.insert(0, 'src')
from strategies.register import *
from strategies.strategy_factory import StrategyFactory
print('可用策略:', StrategyFactory.get_available_strategies())
"

# 输出:
# 可用策略：['hot_rotation', 'momentum']
```

### 9. 与现有系统集成

**原系统**: 单一热点轮动策略
```
main.py → Runner → PortfolioAgent → 组合优化
```

**新系统**: 多策略框架
```
main.py → Runner → MultiStrategyPortfolio → [策略 1, 策略 2, ...] → 组合优化
```

**向后兼容**: 现有代码不受影响，新增策略框架作为可选模块。

### 10. 后续扩展方向

1. **添加更多策略**
   - 价值策略 (低 PE/PB)
   - 均值回归策略 (RSI 超卖)
   - 事件驱动策略 (财报/重组)
   - 资金流策略 (主力流入)

2. **高级功能**
   - 策略绩效评估
   - 动态权重调整
   - 策略间对冲
   - 机器学习融合

3. **集成到主流程**
   - 在 `main.py` 添加 `--strategy` 参数
   - 支持 `--strategy multi` 运行多策略
   - 添加策略回测报告

## 文件清单

```
src/strategies/
├── __init__.py           # 模块导出
├── base_strategy.py      # [新增] 策略基类
├── strategy_factory.py   # [新增] 策略工厂
├── multi_strategy.py     # [新增] 多策略组合器
├── hot_rotation.py       # [新增] 热点轮动策略
├── momentum.py           # [修改] 动量策略 (兼容框架版)
├── config.py             # [新增] 策略配置
├── register.py           # [新增] 策略注册
├── example.py            # [新增] 使用示例
├── README.md             # [新增] 完整文档
└── SUMMARY.md            # [本文件] 实现总结
```

## 关键设计决策

1. **策略独立性**: 每个策略独立生成信号，不依赖其他策略
2. **统一接口**: 所有策略继承 `BaseStrategy`，输出统一信号格式
3. **懒加载**: 数据库连接等重资源按需加载
4. **配置驱动**: 策略参数可通过配置文件管理
5. **扩展友好**: 添加新策略只需 2 步 (实现 + 注册)

## 注意事项

1. 策略权重总和不必为 1，会自动归一化
2. 单个策略失败不影响其他策略
3. 所有策略信号会被合并，可能产生重复股票
4. 多策略组合需要数据库中有足够的数据

## 下一步

1. 在 `main.py` 中添加多策略支持
2. 添加更多策略 (价值/均值回归等)
3. 实现策略绩效评估模块
4. 添加策略权重优化功能
