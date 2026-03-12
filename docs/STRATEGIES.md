# 量化策略库文档

## 概述

本量化策略库包含多种交易策略，涵盖趋势跟踪、均值回归、事件驱动和统计套利等多种类型。

## 策略列表

### 1. 均值回归策略 (MeanReversionStrategy)

**策略类型：** 均值回归

**核心逻辑：**
- **布林带策略**：当价格触及下轨时买入，触及上轨时卖出
- **RSI 策略**：当 RSI 进入超卖区（<30）时买入，进入超买区（>70）时卖出
- **双策略结合**：加权综合信号生成

**参数：**
| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| bb_period | 20 | 布林带周期 |
| bb_std | 2.0 | 布林带标准差倍数 |
| rsi_period | 14 | RSI 周期 |
| rsi_oversold | 30 | RSI 超卖阈值 |
| rsi_overbought | 70 | RSI 超买阈值 |
| stop_loss | 0.08 | 止损比例 |
| take_profit | 0.15 | 止盈比例 |

**使用示例：**
```python
from strategies.mean_reversion import MeanReversionStrategy

strategy = MeanReversionStrategy(
    bb_period=20,
    bb_std=2.0,
    rsi_period=14
)

# 生成信号
df = strategy.generate_combined_signals(price_data)

# 回测
results = strategy.backtest(df, initial_capital=1000000)
```

**适用场景：**
- 震荡市场
- 波动率稳定的股票
- 无明显趋势的行情

---

### 2. 动量策略 (MomentumStrategy)

**策略类型：** 趋势跟踪

**核心逻辑：**
- **价格动量**：基于历史收益率排序
- **均线交叉**：金叉买入，死叉卖出
- **趋势跟随**：动量为正且金叉时买入

**参数：**
| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| momentum_period | 20 | 动量周期 |
| reversal_period | 5 | 反转信号周期 |
| ma_short | 5 | 短期均线 |
| ma_long | 20 | 长期均线 |
| stop_loss | 0.08 | 止损比例 |
| take_profit | 0.15 | 止盈比例 |

**使用示例：**
```python
from strategies.momentum import MomentumStrategy

strategy = MomentumStrategy(
    momentum_period=20,
    ma_short=5,
    ma_long=20
)

# 生成信号
df = strategy.generate_signals(price_data)

# 回测
results = strategy.backtest(df, initial_capital=1000000)
```

**适用场景：**
- 趋势市场
- 强势股
- 有明显上涨/下跌趋势的行情

---

### 3. 事件驱动策略 (EventDrivenStrategy)

**策略类型：** 事件驱动

**核心逻辑：**
- **财报事件**：超预期财报买入，低于预期卖出
- **公告事件**：重大利好公告买入，利空公告卖出
- **调研事件**：机构调研活跃度买入信号
- **高管增减持**：高管增持买入，减持卖出

**参数：**
| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| hold_period | 5 | 持有期（交易日） |
| stop_loss | 0.08 | 止损比例 |
| take_profit | 0.15 | 止盈比例 |
| earnings_weight | 0.4 | 财报事件权重 |
| announcement_weight | 0.3 | 公告事件权重 |
| survey_weight | 0.2 | 调研事件权重 |
| insider_weight | 0.1 | 高管增减持权重 |

**使用示例：**
```python
from strategies.event_driven import EventDrivenStrategy

strategy = EventDrivenStrategy()

# 准备事件数据
earnings_data = pd.DataFrame([...])  # 财报数据
announcement_data = pd.DataFrame([...])  # 公告数据
survey_data = pd.DataFrame([...])  # 调研数据
insider_data = pd.DataFrame([...])  # 增减持数据

# 生成信号
df = strategy.generate_combined_signals(
    price_data,
    earnings_data=earnings_data,
    announcement_data=announcement_data,
    survey_data=survey_data,
    insider_data=insider_data
)

# 回测
results = strategy.backtest(df, initial_capital=1000000)
```

**适用场景：**
- 财报季
- 重大公告发布
- 机构调研活跃的股票
- 有高管增减持的股票

---

### 4. 统计套利策略 (PairsTradingStrategy)

**策略类型：** 统计套利

**核心逻辑：**
- **寻找相关股票对**：相关系数 > 0.7
- **计算价差序列**：spread = price1 - hedge_ratio * price2
- **开仓信号**：价差偏离均值超过 2 倍标准差
- **平仓信号**：价差回归均值

**参数：**
| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| lookback_period | 60 | 回溯期 |
| entry_threshold | 2.0 | 开仓阈值（标准差倍数） |
| exit_threshold | 0.5 | 平仓阈值（标准差倍数） |
| stop_loss | 3.0 | 止损阈值（标准差倍数） |
| hold_period | 10 | 最短持有期 |
| min_correlation | 0.7 | 最小相关系数 |

**使用示例：**
```python
from strategies.statistical_arbitrage import PairsTradingStrategy

strategy = PairsTradingStrategy()

# 准备价格数据
price_data = {
    '000001.SZ': df1,
    '000002.SZ': df2
}

# 寻找股票对
pairs = strategy.find_pairs(price_data)

# 回测
results = strategy.backtest(
    price_data,
    initial_capital=1000000
)
```

**适用场景：**
- 高度相关的股票对
- 同行业股票
- 期现套利
- 跨市场套利

---

## 策略回测框架

### StrategyBacktester

统一的策略回测框架，支持多策略对比。

**使用示例：**
```python
from strategies.backtester import StrategyBacktester
from strategies.mean_reversion import MeanReversionStrategy
from strategies.momentum import MomentumStrategy

# 创建回测框架
backtester = StrategyBacktester()

# 注册策略
backtester.register_strategy('均值回归', MeanReversionStrategy())
backtester.register_strategy('动量策略', MomentumStrategy())

# 回测所有策略
results = backtester.backtest_all_strategies(
    ts_code='000001.SZ',
    initial_capital=1000000
)

# 对比策略表现
df = backtester.compare_strategies(results)
print(df)

# 打印报告
backtester.print_report(results)
```

---

## 策略选择指南

| 市场环境 | 推荐策略 | 理由 |
|---------|---------|------|
| 震荡市 | 均值回归 | 利用价格波动获利 |
| 趋势市 | 动量策略 | 跟随趋势获利 |
| 财报季 | 事件驱动 | 捕捉财报超预期机会 |
| 高相关股票 | 统计套利 | 利用价差回归获利 |
| 波动率低 | 均值回归 | 布林带收口后突破 |
| 波动率高 | 动量策略 | 趋势延续性强 |

---

## 策略组合建议

### 保守型组合
- 40% 均值回归
- 40% 动量策略
- 20% 统计套利

### 平衡型组合
- 30% 均值回归
- 30% 动量策略
- 25% 事件驱动
- 15% 统计套利

### 激进型组合
- 25% 均值回归
- 25% 动量策略
- 35% 事件驱动
- 15% 统计套利

---

## 风险提示

1. **历史回测不代表未来表现**：所有策略的历史回测结果仅供参考
2. **参数过拟合风险**：避免过度优化参数
3. **交易成本影响**：高频策略受手续费影响较大
4. **流动性风险**：小盘股可能存在流动性问题
5. **黑天鹅事件**：极端市场条件下策略可能失效

---

## 免责声明

本策略库仅供学习和研究使用，不构成投资建议。股市有风险，投资需谨慎。
