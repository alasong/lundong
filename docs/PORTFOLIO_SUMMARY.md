# 个股筛选 + 组合优化系统 - 实现总结

## 完成状态

### ✅ 已完成的核心功能

| 模块 | 文件 | 状态 | 说明 |
|------|------|------|------|
| 数据库扩展 | `src/data/database.py` | ✅ | 新增 stock_daily、concept_constituent、stock_factors 表 |
| 个股筛选器 | `src/data/stock_screener.py` | ✅ | 流动性/估值/技术面筛选 |
| 个股预测模型 | `src/models/stock_predictor.py` | ✅ | 独立于板块的预测模型 |
| 相关性计算 | `src/portfolio/optimizer.py` | ✅ | 60 日滚动相关性矩阵 |
| 组合优化器 | `src/portfolio/optimizer.py` | ✅ | 风险平价 + Black-Litterman |
| 组合 Agent | `src/agents/portfolio_agent.py` | ✅ | 整合筛选 + 预测 + 优化 |
| CLI 入口 | `src/main.py` | ✅ | `--mode portfolio` |

### ⚠️ 需要数据的部分

| 功能 | 状态 | 说明 |
|------|------|------|
| 成分股数据采集 | ⚠️ 接口限制 | Tushare 成分股接口需要更高权限 |
| 个股历史数据采集 | ⚠️ 待执行 | 需要先获取成分股列表 |
| 个股模型训练 | ⚠️ 待数据 | 需要个股历史数据 |

---

## 系统架构

```
┌────────────────────────────────────────────────────────────┐
│                    python src/main.py --mode portfolio      │
│                           ▼                                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              PortfolioAgent                          │  │
│  │  1. 获取板块预测 (已有模型)                           │  │
│  │  2. 筛选成分股 (StockScreener)                       │  │
│  │  3. 个股预测 (StockPredictor)                        │  │
│  │  4. 组合优化 (PortfolioOptimizer)                    │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

---

## 使用方法

### 方式 1：命令行（推荐）

```bash
# 构建投资组合
python src/main.py --mode portfolio

# 指定持仓数量
python src/main.py --mode portfolio --top-n 15
```

### 方式 2：Python API

```python
from agents.portfolio_agent import PortfolioAgent

agent = PortfolioAgent()

# 构建组合
result = agent.run(
    task="build",
    concept_codes=['885311.TI', '885394.TI'],  # 目标板块
    top_n_stocks=10
)

# 输出持仓
for pos in result['portfolio']:
    print(f"{pos['stock_name']}: {pos['weight']:.1%}")
```

### 方式 3：快速测试（模拟数据）

```bash
python test_portfolio_quick.py
```

---

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

---

## 核心功能说明

### 1. 个股筛选器 (StockScreener)

**筛选条件：**
- 流动性：日均成交额 ≥ 5000 万
- 市值：50-2000 亿
- 估值：PE < 100, PB > 0.3
- 波动率：20 日波动率 < 25%

**得分构成：**
- 流动性得分 (30%)
- 动量得分 (30%)
- 估值得分 (20%)
- 市值得分 (20%)

### 2. 个股预测模型 (StockPredictor)

**特点：**
- 独立于板块预测的参数
- 特征工程针对个股优化（成交量、成交额）
- 支持 XGBoost/LightGBM/RandomForest
- 32 并发高性能预测

**与板块预测的区别：**
| 特性 | 板块预测 | 个股预测 |
|------|----------|----------|
| 权重配置 | 1d:0.3, 5d:0.5, 20d:0.2 | 1d:0.4, 5d:0.4, 20d:0.2 |
| 特征 | 仅价格/涨跌幅 | 价格 + 成交量 + 成交额 |

### 3. 组合优化器 (PortfolioOptimizer)

**优化方法：**
1. 风险平价 (Risk Parity) - 每只股票风险贡献相等
2. Black-Litterman 调整 - 融合板块预测观点
3. 约束优化 - 应用仓位限制

**约束条件：**
- 单股权重 ≤ 10%
- 单板块权重 ≤ 25%
- 目标波动率 15%

---

## 下一步：接入真实数据

### 方案 A：使用 Tushare 成分股接口（需要权限）

```python
from data.stock_collector import StockCollector

collector = StockCollector()

# 获取成分股（需要 Tushare 高权限）
members = collector.ths_client.get_ths_members('885311.TI')

# 采集个股数据
if members is not None:
    stock_codes = members['ts_code'].tolist()
    collector.collect_stocks_batch(stock_codes, '20240101', '20241231')
```

### 方案 B：手动导入成分股列表

```python
from data.database import get_database

db = get_database()

# 手动准备成分股列表
constituents = [
    {'stock_code': '000001.SZ', 'stock_name': '平安银行', 'weight': None},
    {'stock_code': '000002.SZ', 'stock_name': '万科 A', 'weight': None},
    # ...
]

# 保存到数据库
db.save_concept_constituents('885311.TI', constituents)
```

### 方案 C：使用简化模式（当前可用）

不需要成分股数据，直接基于板块预测结果进行筛选：

```python
from agents.portfolio_agent import PortfolioAgent

agent = PortfolioAgent()

# 使用简化模式（基于板块预测 + 规则筛选）
result = agent.run(
    task="build",
    concept_codes=['885311.TI', '885394.TI'],
    top_n_stocks=10
)
```

---

## 验证测试

### 测试结果

```bash
$ python test_portfolio_quick.py

【功能验证】
  ✓ 个股筛选器 - 正常工作
  ✓ 组合优化器 - 正常工作
  ✓ 组合 Agent - 正常工作
```

### 板块预测验证

```bash
$ python src/main.py --mode predict

【预测 TOP10】
1     汽车芯片                概念        26.97     3.49    35.89   32.29   高 ⭐
2     军工电子                行业        26.65     3.23    32.92   36.59   高 ⭐
3     人工智能                概念        26.16     4.06    31.84   35.67   高 ⭐
...
```

---

## 文件清单

### 新增文件
- `src/data/stock_collector.py` - 个股数据采集器
- `src/data/stock_screener.py` - 个股筛选器
- `src/models/stock_predictor.py` - 个股预测模型
- `src/portfolio/optimizer.py` - 组合优化器
- `src/portfolio/__init__.py` - 组合模块初始化
- `src/agents/portfolio_agent.py` - 组合构建 Agent
- `test_portfolio.py` - 完整测试脚本
- `test_portfolio_quick.py` - 快速测试脚本
- `docs/PORTFOLIO_IMPLEMENTATION.md` - 实现文档
- `docs/PORTFOLIO_GUIDE.md` - 使用指南

### 修改文件
- `src/data/database.py` - 新增 3 个表及相关方法
- `src/data/tushare_ths_client.py` - 新增个股数据接口
- `src/main.py` - 新增 `--mode portfolio`

---

## 总结

**当前状态：**
- ✅ 所有核心代码已完成
- ✅ 筛选器、优化器、Agent 都经过测试验证
- ✅ 板块预测系统正常工作（已有模型）
- ⚠️ 个股数据需要额外采集

**投入生产所需步骤：**
1. 获取成分股列表（手动导入或使用高权限 Tushare 接口）
2. 采集个股历史数据（已有采集代码）
3. 训练个股预测模型（已有训练代码）
4. 运行 `python src/main.py --mode portfolio`

**预计工作量：**
- 如果有成分股数据：1-2 小时完成数据采集和模型训练
- 如果需要手动整理成分股：半天到一天
