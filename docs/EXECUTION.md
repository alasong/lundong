# 执行算法文档

## 概述

执行算法模块提供智能订单执行功能，通过将大单拆分为小单执行，减少市场冲击成本，获得更好的成交价格。

## 核心算法

### 1. VWAP (成交量加权平均价格)

**原理：** 按照历史成交量分布执行订单，使执行价格接近市场 VWAP。

**适用场景：**
- 大单执行（占日均成交量 5-20%）
- 对执行价格敏感的交易
- 流动性较好的股票

**参数：**
- `lookback_days`: 回溯天数（用于计算成交量分布）

**使用示例：**
```python
from src.execution import VWAPExecutor, Order, OrderSide, OrderType

executor = VWAPExecutor(lookback_days=20)

order = Order(
    ts_code="000001.SZ",
    side=OrderSide.BUY,
    total_shares=10000,
    order_type=OrderType.VWAP
)

report = executor.execute(order, market_data, current_price=12.50)
print(f"执行均价：{report.avg_execution_price:.4f}")
print(f"滑点：{report.slippage_bps:.2f}bps")
```

### 2. TWAP (时间加权平均价格)

**原理：** 在指定时间段内均匀执行订单。

**适用场景：**
- 中等规模订单（占日均成交量 1-5%）
- 需要在规定时间内完成执行
- 成交量分布不均匀的股票

**参数：**
- `num_slices`: 切片数量
- `slice_interval_minutes`: 切片间隔（分钟）

**使用示例：**
```python
from src.execution import TWAPExecutor

executor = TWAPExecutor(num_slices=12, slice_interval_minutes=5)

order = Order(
    ts_code="000001.SZ",
    side=OrderSide.SELL,
    total_shares=5000,
    order_type=OrderType.TWAP
)

report = executor.execute(order, market_data, current_price=12.50)
```

### 3. Iceberg (冰山订单)

**原理：** 将大单拆分为小单执行，隐藏真实订单规模，减少市场冲击。

**适用场景：**
- 超大单执行（占日均成交量 20-50%）
- 需要隐藏交易意图
- 流动性较差的股票

**参数：**
- `display_ratio`: 显示比例（每次显示多少）
- `refresh_threshold`: 补单阈值

**使用示例：**
```python
from src.execution import IcebergExecutor

executor = IcebergExecutor(display_ratio=0.1)  # 每次显示 10%

order = Order(
    ts_code="000001.SZ",
    side=OrderSide.BUY,
    total_shares=100000,
    order_type=OrderType.ICEBERG
)

report = executor.execute(order, market_data, current_price=12.50)
# 实际执行会分成约 10 次隐藏订单
```

### 4. POV (成交量参与率)

**原理：** 按照市场成交量的一定比例参与交易。

**适用场景：**
- 巨大单执行（占日均成交量 50% 以上）
- 需要跟随市场节奏
- 流动性很差的股票

**参数：**
- `participation_rate`: 目标参与率
- `max_active_rate`: 最大活跃率

**使用示例：**
```python
from src.execution import POVExecutor

executor = POVExecutor(participation_rate=0.15)  # 15% 参与率

order = Order(
    ts_code="000001.SZ",
    side=OrderSide.SELL,
    total_shares=200000,
    order_type=OrderType.POV,
    participation_rate=0.15
)

report = executor.execute(order, market_data, current_price=12.50, market_volumes=[...])
```

### 5. SmartOrderExecutor (智能执行器)

**原理：** 根据订单特征自动选择最优执行算法。

**选择逻辑：**
| 订单规模（占日均量） | 选择算法 |
|---------------------|---------|
| < 1% | 市价单 |
| 1-5% | TWAP |
| 5-20% | VWAP |
| 20-50% | 冰山订单 |
| > 50% | POV |

**使用示例：**
```python
from src.execution import SmartOrderExecutor, Order, OrderSide

executor = SmartOrderExecutor()

order = Order(
    ts_code="000001.SZ",
    side=OrderSide.BUY,
    total_shares=10000,
    order_type="market"  # 自动选择算法
)

report = executor.execute(order, market_data, current_price=12.50, avg_daily_volume=500000)
print(f"选择算法：{report.order.order_type}")
```

## 执行报告

所有执行算法返回 `ExecutionReport` 对象，包含以下信息：

```python
@dataclass
class ExecutionReport:
    order: Order                      # 原始订单
    total_executed_shares: int        # 总执行股数
    total_executed_amount: float      # 总执行金额
    avg_execution_price: float        # 执行均价
    benchmark_vwap: float             # 基准 VWAP
    slippage_bps: float               # 滑点（基点）
    execution_rate: float             # 执行率
    market_impact: float              # 市场冲击成本
    timing_cost: float                # 时机成本
```

## 与交易系统集成

### 与模拟交易 API 集成

```python
from src.execution import SmartOrderExecutor
from src.trading.trading_api import PaperTradingAPI, Side

# 初始化
api = PaperTradingAPI(initial_capital=1000000)
executor = SmartOrderExecutor()

# 设置市场价格
api.set_market_price("000001.SZ", 12.50)

# 创建订单
order = Order(
    ts_code="000001.SZ",
    side=OrderSide.BUY,
    total_shares=10000,
    order_type="market"
)

# 智能执行
market_data = pd.DataFrame({...})  # 市场数据
report = executor.execute(order, market_data, current_price=12.50)

# 提交到交易 API
if report.executed_shares > 0:
    trade_order = api.submit_order(
        ts_code=order.ts_code,
        side=Side.BUY,
        shares=report.executed_shares,
        order_type="market"
    )
```

### 与策略模块集成

```python
from src.strategies import MeanReversionStrategy
from src.execution import SmartOrderExecutor

# 生成信号
strategy = MeanReversionStrategy()
signal = strategy.generate_signal(stock_data)

if signal == 1:  # 买入信号
    # 使用 VWAP 执行
    executor = VWAPExecutor()
    order = Order(ts_code="000001.SZ", side=OrderSide.BUY, total_shares=10000)
    report = executor.execute(order, market_data, current_price)
```

## 性能指标

### 滑点（Slippage）

滑点是执行价格与基准价格的差异，以基点（bps）表示：

```
滑点 (bps) = (执行均价 - 基准 VWAP) / 基准 VWAP * 10000
```

- 买入：滑点为正表示执行价格高于基准
- 卖出：滑点为负表示执行价格低于基准（好）

### 市场冲击成本

市场冲击成本是大单执行对市场价格的影响：

```
市场冲击 = 滑点 * 冲击系数
```

不同算法的冲击系数：
- 市价单：1.0
- TWAP: 0.8
- VWAP: 0.7
- 冰山：0.5
- POV: 0.7

### 执行率

```
执行率 = 实际执行股数 / 计划执行股数
```

## 最佳实践

1. **选择合适的算法**：根据订单规模和流动性选择算法
2. **设置合理参数**：根据市场情况调整算法参数
3. **监控执行进度**：实时跟踪执行情况和滑点
4. **设置止损条件**：当市场价格大幅不利时暂停执行
5. **避免集中执行**：不要在短时间内执行大量订单

## 注意事项

1. 执行算法不能消除市场风险，只能减少冲击成本
2. 算法执行需要市场数据支持
3. 实盘执行可能受到涨跌停限制
4. 算法参数需要根据实际回测结果调整

## 相关文件

- `src/execution/algorithms.py` - 执行算法实现
- `src/execution/__init__.py` - 模块导出
- `src/trading/trading_api.py` - 交易接口
