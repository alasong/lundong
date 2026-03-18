# 打板策略使用手册

> A股涨停板交易策略完整指南 v1.0.0

---

## 目录

1. [策略概述](#一策略概述)
2. [策略详解](#二策略详解)
3. [快速开始](#三快速开始)
4. [参数配置](#四参数配置)
5. [风险管理](#五风险管理)
6. [实时操作流程](#六实时操作流程)
7. [代码示例](#七代码示例)
8. [常见问题](#八常见问题)

---

## 一、策略概述

### 1.1 什么是打板策略？

打板策略是A股市场特有的短线交易策略，专注于捕捉股票涨停板的交易机会。主要类型：

| 策略类型 | 说明 | 风险等级 | 预期收益 |
|---------|------|---------|---------|
| **首板策略** | 识别首次涨停股票 | 中 | 2-5%/日 |
| **一进二策略** | 捕捉连板延续机会 | 中高 | 3-8%/日 |
| **龙头股策略** | 识别板块龙头股票 | 高 | 5-10%/日 |

### 1.2 策略文件结构

```
src/strategies/
├── first_limit.py           # 首板策略
├── one_to_two.py            # 一进二策略
├── enhanced_dragon_head.py  # 综合策略（含龙头）
├── daban_version.py         # 版本控制
├── daban_backtester.py      # 回测框架
└── config.py                # 策略配置
```

### 1.3 数据要求

| 数据类型 | 说明 | 最小天数 |
|---------|------|---------|
| 个股日线 | 开高低收量、市值、PE/PB | 200天 |
| 板块日线 | 板块涨跌幅、成交额 | 30天 |
| 板块成分 | 股票-板块对应关系 | - |

---

## 二、策略详解

### 2.1 首板策略 (First Limit)

**核心逻辑**：识别首次涨停或接近涨停的股票，捕捉题材溢价机会。

**适用场景**：
- 市场情绪好转，出现板块热点
- 个股首次涨停，具有题材溢价
- 成交量放大3-15倍，资金介入明显

**筛选条件**：

| 条件 | 主板 | 创业板/科创板 |
|------|------|--------------|
| 涨幅阈值 | ≥9.5% | ≥19.5% |
| 成交量比 | 3-15倍 | 3-15倍 |
| 市值范围 | 70-520亿 | 70-520亿 |
| 价格范围 | 2-50元 | 2-50元 |
| 首次涨停 | 180天内≤2次 | 180天内≤2次 |

**评分体系**：

```
综合评分 = 成交量得分×40% + 动量得分×30% + 接近涨停程度×30%
```

**退出规则**：
- 止损：-3%
- 止盈：+1.5%
- 时段退出：11:28止盈50%，14:50强制平仓

---

### 2.2 一进二策略 (One-to-Two)

**核心逻辑**：捕捉昨日首板今日跳空高开的连板延续机会。

**适用场景**：
- 昨日首板涨停，今日跳空高开
- 跳空幅度1-6%，符合黄金区间
- 成交量继续放大，市场认可度高

**筛选条件**：

| 条件 | 要求 |
|------|------|
| 昨日涨幅 | ≥涨停线90% |
| 今日跳空 | 1-6% |
| 今日涨幅 | ≥2% |
| 成交量比 | >2倍昨日量 |
| 前日涨幅 | <涨停线90%（确保首板） |

**评分体系**：

```
综合评分 = 跳空得分×30% + 成交量比×25% + 今日动量×25% + 昨日强度×20%
```

**退出规则**：
- 止损：-3%
- 止盈：+2.5%
- 时段退出：同首板策略

---

### 2.3 龙头股策略 (Leader Stock)

**核心逻辑**：识别板块中具有持续性和号召力的龙头股票。

**适用场景**：
- 板块整体走强，出现领涨股
- 龙头股具有持续性和号召力
- 板块资金集中度高

**筛选条件**：

| 条件 | 要求 |
|------|------|
| 10日动量 | >5% |
| 20日动量 | >10% |
| 板块强度 | >0.6 |
| 市值/价格 | 同首板策略 |

**评分体系**：

```
综合评分 = 动量得分×40% + 板块强度×30% + 成交量×20% + 基础分×10%
```

---

## 三、快速开始

### 3.1 环境准备

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量
echo "TUSHARE_TOKEN=your_token" >> .env

# 3. 采集数据
python src/main.py --mode fast --date 20260301
```

### 3.2 运行策略

```bash
# 方式一：命令行运行
python src/main.py --mode strategy --strategy first_limit

# 方式二：Python脚本
python -c "
from strategies.register import *
from strategies.strategy_factory import StrategyFactory

strategy = StrategyFactory.create_strategy('first_limit')
signals = strategy.generate_signals()
print(f'发现 {len(signals)} 个信号')
"

# 方式三：交互式指南
python DABAN_STRATEGY_GUIDE.py
```

### 3.3 查看结果

```python
# 查看信号详情
for sig in signals:
    print(f"""
    股票: {sig.ts_code} {sig.stock_name}
    评分: {sig.score:.1f}/100
    信号: {sig.signal_type}
    原因: {sig.reason}
    止损: {sig.metadata['stop_loss_pct']:.1%}
    止盈: {sig.metadata['take_profit_pct']:.1%}
    """)
```

---

## 四、参数配置

### 4.1 首板策略参数

```python
params = {
    # 涨停阈值
    "limit_up_threshold": 0.095,       # 主板9.5%
    "limit_up_threshold_20": 0.195,    # 创业板/科创板19.5%
    
    # 成交量条件
    "min_volume_ratio": 3.0,           # 最小3倍量
    "max_volume_ratio": 15.0,          # 最大15倍量
    
    # 流动性条件
    "min_turnover_amount": 5e8,        # 最小成交额5亿
    
    # 市值/价格条件
    "min_market_cap": 7e9,             # 最小市值70亿
    "max_market_cap": 5.2e10,          # 最大市值520亿
    "min_price": 2.0,                  # 最小价格2元
    "max_price": 50.0,                 # 最大价格50元
    
    # 首次涨停判断
    "first_limit_days": 180,           # 回溯180天
    
    # 输出控制
    "top_n_stocks": 10,                # 最多输出10只
    
    # 风险控制
    "stop_loss_pct": -0.03,            # 止损-3%
    "take_profit_pct": 0.015,          # 止盈+1.5%
}
```

### 4.2 一进二策略参数

```python
params = {
    # 跳空条件
    "gap_open_min": 0.01,              # 最小跳空1%
    "gap_open_max": 0.06,              # 最大跳空6%
    
    # 成交量条件
    "min_volume_ratio": 2.0,           # 最小2倍昨日量
    
    # 其他条件同首板策略
    "stop_loss_pct": -0.03,
    "take_profit_pct": 0.025,          # 止盈+2.5%
}
```

### 4.3 龙头股策略参数

```python
params = {
    # 动量条件
    "min_momentum_10d": 0.05,          # 10日动量>5%
    "min_momentum_20d": 0.10,          # 20日动量>10%
    
    # 板块条件
    "min_sector_strength": 0.6,        # 板块强度>0.6
    
    # 风险控制
    "stop_loss_pct": -0.03,
    "take_profit_pct": 0.02,
}
```

### 4.4 配置文件方式

在 `src/strategies/config.py` 中配置：

```python
daban_config = {
    "first_limit": {
        "enabled": True,
        "weight": 0.4,
        "params": {
            "min_volume_ratio": 3.0,
            "top_n_stocks": 5,
        }
    },
    "one_to_two": {
        "enabled": True,
        "weight": 0.4,
        "params": {
            "gap_open_min": 0.01,
            "gap_open_max": 0.06,
        }
    },
    "leader": {
        "enabled": False,  # 默认禁用，风险较高
        "weight": 0.2,
        "params": {}
    }
}
```

---

## 五、风险管理

### 5.1 硬性规则（不可违反）

| 规则 | 说明 |
|------|------|
| **止损** | 任何持仓亏损达-3%立即平仓 |
| **时段** | 11:28止盈50%，14:50强制平仓 |
| **仓位** | 单股≤10%，单日≤30%总资金 |
| **熔断** | 大盘跌停或个股跌停立即停止 |

### 5.2 动态风控

| 条件 | 操作 |
|------|------|
| 日亏损达5% | 暂停当日交易 |
| 连续3次亏损 | 暂停策略，分析原因 |
| 市场波动>8% | 降低仓位至50% |
| 板块集中度>40% | 分散持仓 |

### 5.3 应急处理

```python
# 监控脚本
python monitoring_dashboard.py

# 输出示例：
# 🚨 风险监控面板
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 持仓数量: 3
# 今日盈亏: +1.2%
# 单股最大亏损: -0.5%
# 板块集中度: 35%
# 
# ✅ 风险状态: 正常
```

---

## 六、实时操作流程

### 6.1 早盘准备 (9:15-9:25)

```bash
# 1. 启动监控
python monitoring_dashboard.py

# 2. 检查昨日涨停股票
python -c "
from data.database import get_database
db = get_database()
latest = db.get_latest_date()
data = db.get_all_stock_data(latest)
limit_ups = data[data['pct_chg'] >= 9.5]
print(f'昨日涨停: {len(limit_ups)} 只')
print(limit_ups[['ts_code', 'name', 'pct_chg']].to_string())
"

# 3. 生成今日信号
python src/main.py --mode strategy --strategy one_to_two
```

### 6.2 集合竞价 (9:25-9:30)

**观察要点**：
1. 一进二候选股开盘价
   - 跳空1-5%: 符合条件，准备买入
   - 跳空>5%: 风险过高，放弃
   - 跳空<1%: 动力不足，放弃

2. 计算集合竞价成交量比

### 6.3 开盘执行 (9:30-10:00)

**首板策略执行**：
```
1. 观察接近涨停股票 (涨幅>8%)
2. 确认成交量放大 (>3倍)
3. 检查封单情况
4. 打板买入 (涨停价挂单)
```

**一进二策略执行**：
```
1. 确认跳空幅度符合条件
2. 观察开盘后走势
3. 快速买入 (开盘5分钟内)
```

### 6.4 盘中监控

```python
# 实时监控脚本
import time
from datetime import datetime

while True:
    now = datetime.now()
    
    # 11:28 止盈50%
    if now.hour == 11 and now.minute == 28:
        print("⏰ 执行11:28止盈")
        # 执行止盈逻辑
        
    # 14:50 强制平仓
    if now.hour == 14 and now.minute == 50:
        print("⏰ 执行14:50平仓")
        # 执行平仓逻辑
        
    time.sleep(60)
```

### 6.5 盘后复盘

```bash
# 1. 记录交易明细
# 2. 分析成功/失败原因
# 3. 更新策略参数

# 查看今日表现
python -c "
from strategies.daban_backtester import DabanBacktester
bt = DabanBacktester()
result = bt.run_backtest('first_limit', days=1)
print(result)
"
```

---

## 七、代码示例

### 7.1 单策略运行

```python
from strategies.register import *
from strategies.strategy_factory import StrategyFactory

# 创建首板策略
strategy = StrategyFactory.create_strategy(
    "first_limit",
    params={
        "min_volume_ratio": 3.0,
        "top_n_stocks": 5,
    }
)

# 生成信号
signals = strategy.generate_signals()

# 显示结果
for sig in signals:
    print(f"{sig.ts_code}: 评分{sig.score:.1f}, {sig.reason}")
```

### 7.2 多策略组合

```python
from strategies.multi_strategy import MultiStrategyPortfolio

# 创建多个策略
strategies = [
    StrategyFactory.create_strategy("first_limit", {"weight": 0.5}),
    StrategyFactory.create_strategy("one_to_two", {"weight": 0.5})
]

# 创建组合
portfolio = MultiStrategyPortfolio(
    strategies=strategies,
    strategy_weights={"first_limit": 0.5, "one_to_two": 0.5}
)

# 生成合并信号
merged_signals = portfolio.generate_signals()
```

### 7.3 回测验证

```python
from strategies.daban_backtester import DabanBacktester

# 创建回测器
backtester = DabanBacktester()

# 运行回测
result = backtester.run_backtest(
    strategy_name="first_limit",
    start_date="20250101",
    end_date="20260301",
    initial_capital=100000
)

# 查看结果
print(f"""
回测结果:
━━━━━━━━━━━━━━━━━━━━━━━━━━━
总收益: {result['total_return']:.2%}
夏普比率: {result['sharpe_ratio']:.2f}
最大回撤: {result['max_drawdown']:.2%}
胜率: {result['win_rate']:.2%}
交易次数: {result['num_trades']}
""")
```

### 7.4 版本控制

```python
from strategies.daban_version import DabanStrategyVersion

# 查看当前版本
version = DabanStrategyVersion.get_current_version()
print(f"当前版本: {version}")

# 查看参数历史
params = DabanStrategyVersion.get_current_parameters()
print("首板参数:", params["first_limit"])

# 查看预期性能
performance = DabanStrategyVersion.get_expected_performance()
print("预期收益:", performance["expected_return"])
```

---

## 八、常见问题

### Q1: 为什么没有生成信号？

**可能原因**：
1. 数据过期（检查数据日期）
2. 筛选条件过严（降低阈值）
3. 市场无符合条件的股票

**解决方案**：
```python
# 检查数据日期
from data.database import get_database
db = get_database()
print(f"最新数据日期: {db.get_latest_date()}")

# 放宽筛选条件
strategy = StrategyFactory.create_strategy(
    "first_limit",
    params={
        "min_volume_ratio": 2.0,  # 降低成交量要求
        "min_market_cap": 5e9,    # 降低市值要求
    }
)
```

### Q2: 如何更新数据？

```bash
# 快速采集最新数据
python src/main.py --mode fast

# 或指定日期
python src/main.py --mode fast --date 20260318
```

### Q3: 如何自定义策略参数？

```python
# 方式一：创建时指定
strategy = StrategyFactory.create_strategy(
    "first_limit",
    params={"min_volume_ratio": 5.0}  # 自定义参数
)

# 方式二：修改配置文件
# 编辑 src/strategies/config.py
```

### Q4: 如何获取实时数据？

实时数据需要 Tushare Pro 会员（约500元/年）：

```python
# 实时行情接口（需Pro会员）
from tushare.pro import pro_api
pro = pro_api('your_token')

# 获取实时行情
df = pro.realtime_quote(ts_code='000001.SZ')
```

### Q5: 策略信号类型说明？

| 信号类型 | 说明 | 操作建议 |
|---------|------|---------|
| `buy` | 已涨停，符合买入条件 | 可打板买入 |
| `watch` | 接近涨停，需密切关注 | 准备操作 |
| `monitor` | 潜力股，持续观察 | 纳入监控 |

### Q6: 如何调整风险参数？

```python
# 更严格的风险控制
params = {
    "stop_loss_pct": -0.02,     # 止损收紧到-2%
    "take_profit_pct": 0.01,    # 止盈降到+1%
    "max_position_per_stock": 0.05,  # 单股仓位降到5%
}
```

---

## 附录

### A. 策略版本历史

| 版本 | 日期 | 更新内容 |
|------|------|---------|
| v1.0.0 | 2026-03-18 | 初始版本，包含首板、一进二、龙头股策略 |

### B. 相关文档

- `README.md` - 系统概览
- `ARCHITECTURE.md` - 架构详解
- `DATA_UPDATE_GUIDE.py` - 数据更新指南
- `monitoring_dashboard.py` - 实时监控工具
- `small_capital_test.py` - 小资金测试配置

### C. 免责声明

本策略仅供学习和研究使用，不构成投资建议。
股市有风险，投资需谨慎。
历史表现不代表未来收益。

---

**最后更新**: 2026-03-18
**维护者**: AI Trading System