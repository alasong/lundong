# 热点轮动 + 个股筛选 + 组合优化 使用指南

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                      组合构建 Agent                          │
│  (PortfolioAgent)                                           │
└─────────────┬───────────────────────────────────────────────┘
              │
      ┌───────┼───────┬───────────┐
      │       │       │           │
┌─────┴─────┐ │ ┌─────┴─────┐   │
│ 板块预测   │ │ │ 个股筛选   │   │
│ (Predict) │ │ │ (Screener)│   │
└───────────┘ │ └─────┬─────┘   │
              │       │         │
              │ ┌─────┴──────┐  │
              │ │ 个股预测    │  │
              │ │(StockPred) │  │
              │ └─────┬──────┘  │
              │       │         │
              │ ┌─────┴──────┐  │
              │ │ 组合优化    │  │
              │ │(Optimizer) │  │
              │ └────────────┘  │
              └─────────────────┘
```

## 快速开始

### 1. 数据采集

首先需要采集板块和个股数据：

```bash
# 采集板块数据（高速模式）
python src/main.py --mode fast --sector-type all

# 采集成分股数据（需要先有板块数据）
python -c "
from data.stock_collector import StockCollector
collector = StockCollector()
collector.collect_constituent_stocks('881101.TI', '20240101', '20241231')
"

# 采集个股历史数据
python -c "
from data.stock_collector import StockCollector
collector = StockCollector()
codes = ['000001.SZ', '000002.SZ', '600000.SH']  # 股票代码列表
collector.collect_stocks_batch(codes, '20240101', '20241231')
"
```

### 2. 训练模型

```bash
# 训练板块预测模型
python src/main.py --mode train

# 训练个股预测模型
python -c "
from models.stock_predictor import StockPredictor
from data.database import get_database

predictor = StockPredictor()
db = get_database()

# 获取个股数据
stock_data = db.get_all_stock_data()

# 准备特征
features = predictor.prepare_features(stock_data, n_jobs=16)

# 训练模型
result = predictor.train(features, model_type='xgboost')
print(f'训练完成：{result[\"metrics\"]}')
"
```

### 3. 构建组合

```bash
# 一键构建投资组合
python src/main.py --mode portfolio

# 指定持仓数量
python src/main.py --mode portfolio --top-n 15
```

## Python API 调用

### 完整流程

```python
from agents.portfolio_agent import PortfolioAgent

agent = PortfolioAgent()

# 构建组合
result = agent.run(
    task="build",
    concept_codes=['881101.TI', '881102.TI'],  # 目标板块
    top_n_stocks=10
)

# 输出结果
if result.get('success'):
    print("持仓明细:")
    for pos in result['portfolio']:
        print(f"  {pos['stock_name']}: {pos['weight']:.1%}")

    print("\n预期指标:")
    metrics = result['metrics']
    print(f"  预期收益：{metrics['expected_return']:.1%}")
    print(f"  夏普比率：{metrics['sharpe']:.2f}")
```

### 分步执行

```python
from data.stock_screener import StockScreener
from models.stock_predictor import StockPredictor
from portfolio.optimizer import PortfolioOptimizer

# Step 1: 筛选股票
screener = StockScreener()
stocks = screener.screen_stocks(
    concept_codes=['881101.TI', '881102.TI'],
    top_n_per_concept=5
)

# Step 2: 个股预测
predictor = StockPredictor()
model = predictor.load_model()  # 加载预训练模型

# 获取数据并准备特征
from data.database import get_database
db = get_database()
stock_codes = stocks['stock_code'].unique().tolist()
stock_data = db.get_constituent_stocks_data(stock_codes)
features = predictor.prepare_features(stock_data)

# 预测
predictions = predictor.predict(model, features)

# Step 3: 组合优化
optimizer = PortfolioOptimizer()
result = optimizer.optimize(
    stock_predictions=predictions,
    top_n_stocks=10,
    max_position=0.10,  # 单股≤10%
    max_sector=0.25     # 单板块≤25%
)

print(f"组合构建完成：{len(result['portfolio'])} 只股票")
```

### 仅筛选股票

```python
from data.stock_screener import StockScreener

screener = StockScreener()

# 筛选特定板块的股票
stocks = screener.screen_stocks(
    concept_codes=['881101.TI'],  # 半导体行业
    top_n_per_concept=10
)

print(f"筛选出 {len(stocks)} 只股票")
print(stocks[['stock_code', 'stock_name', 'stock_score']].head())
```

### 仅优化组合

```python
from portfolio.optimizer import PortfolioOptimizer
import pandas as pd

# 构造预测数据
predictions = pd.DataFrame({
    'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
    'stock_name': ['平安银行', '万科 A', '浦发银行'],
    'concept_code': ['881101.TI', '881102.TI', '881101.TI'],
    'pred_1d': [1.5, 1.2, 1.3],
    'pred_5d': [5.0, 4.5, 4.8],
    'combined_score': [85, 80, 82]
})

optimizer = PortfolioOptimizer()
result = optimizer.optimize(
    stock_predictions=predictions,
    top_n_stocks=5
)

print(f"预期波动率：{result['metrics']['expected_volatility']:.1%}")
```

## 输出示例

```
======================================================================
投资组合构建结果
======================================================================

持仓数量：10 只股票

【持仓明细】
------------------------------------------------------------------------------------------
代码          名称             权重      所属板块              1 日预测      5 日预测
------------------------------------------------------------------------------------------
000001.SZ     平安银行          10.0%    银行                 1.50%      5.00%
600036.SH     招商银行           9.5%    银行                 1.80%      6.00%
000858.SZ     五粮液             9.2%    食品饮料             2.00%      7.00%
...

【预期指标】
  预期年化收益：22.5%
  预期年化波动率：15.0%
  夏普比率：1.50
  最大回撤估计：18.8%

【风险分析】
  板块集中度：25.0%
  平均相关性：0.35
======================================================================
```

## 筛选条件配置

可以在 `src/data/stock_screener.py` 中修改筛选条件：

```python
SCREENING_RULES = {
    # 流动性要求
    'min_avg_amount': 5000,        # 日均成交额≥5000 万
    'min_avg_turnover': 1.0,       # 日均换手率≥1%

    # 市值要求
    'min_market_cap': 50,          # 市值≥50 亿
    'max_market_cap': 2000,        # 市值≤2000 亿

    # 估值要求
    'max_pe': 100,                 # PE<100
    'min_pb': 0.3,                 # PB>0.3
    'max_pb': 30,                  # PB<30

    # 技术面要求
    'max_volatility': 0.25,        # 20 日波动率<25%
}
```

## 组合约束配置

可以在调用时修改组合约束：

```python
result = optimizer.optimize(
    stock_predictions=predictions,
    max_position=0.08,      # 单股≤8%
    max_sector=0.20,        # 单板块≤20%
    target_risk=0.12,       # 目标波动率 12%
    top_n_stocks=8          # 持仓 8 只股票
)
```

## 测试

```bash
# 运行完整测试
python test_portfolio.py

# 测试单个模块
python -c "
from data.stock_screener import StockScreener
screener = StockScreener()
stocks = screener.get_top_stocks(['881101.TI'], top_n=5)
print(stocks)
"
```

## 常见问题

### Q: 筛选结果为空？

A: 可能原因：
1. 个股数据未采集
2. 筛选条件过严
3. 成分股关系未建立

解决方法：
```bash
# 先采集数据
python src/main.py --mode fast

# 查看股票数据量
python -c "
from data.database import get_database
db = get_database()
print(db.get_stock_statistics())
"
```

### Q: 如何更新成分股列表？

A:
```python
from data.stock_collector import StockCollector
collector = StockCollector()

# refresh=True 强制刷新成分股
collector.collect_constituent_stocks(
    '881101.TI',
    '20240101',
    '20241231',
    refresh=True
)
```

### Q: 如何验证组合效果？

A: 使用回测模块：
```python
from evaluation.backtester import Backtester
backtester = Backtester()

# 加载历史数据进行回测
# ... (参考 backtest 模式使用)
```

## 风险提示

1. **预测准确率有限** - 模型 R² 约 0.3-0.4，不可完全依赖
2. **历史数据不代表未来** - 回测结果不代表实盘表现
3. **流动性风险** - 小市值股票可能存在流动性问题
4. **模型风险** - 市场风格变化可能导致模型失效

**建议**：
- 先模拟盘运行 3 个月
- 小额实盘测试（<10 万）
- 持续监控和调整
- 设置止损机制
