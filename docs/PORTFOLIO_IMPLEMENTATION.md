# 个股筛选 + 组合优化功能实现总结

## 完成的功能模块

### P0-1: 个股数据采集 ✅

**新增文件:**
- `src/data/stock_collector.py` - 个股数据采集器

**修改文件:**
- `src/data/database.py` - 扩展数据库表和方法
- `src/data/tushare_ths_client.py` - 添加个股数据接口

**数据库新增表:**
```sql
-- 个股行情表
stock_daily (ts_code, trade_date, open, high, low, close, pct_chg, vol, amount, ...)

-- 板块成分股关系表
concept_constituent (concept_code, stock_code, stock_name, weight, is_core, ...)

-- 个股因子表
stock_factors (ts_code, trade_date, market_cap, pe_ttm, pb_ttm, momentum_20d, volatility_20d, ...)
```

**核心功能:**
- 采集 A 股上市公司列表
- 采集个股历史行情数据
- 批量采集（支持 16 并发）
- 增量采集（自动判断缺失日期）
- 采集板块成分股关系

---

### P0-2: 成分股关系表 ✅

**实现方式:**
- 数据库表 `concept_constituent` 存储板块 - 个股关联
- 通过 Tushare `index_member` 接口获取成分股
- 支持查询单个板块或批量查询多个板块的成分股

**核心方法:**
```python
db.save_concept_constituents(concept_code, constituents)  # 保存成分股
db.get_concept_constituents(concept_code)                 # 获取单板块成分股
db.get_constituent_stocks(concept_codes, limit_per_concept)  # 批量获取
```

---

### P0-3: 个股筛选器 ✅

**新增文件:**
- `src/data/stock_screener.py` - 个股筛选器

**筛选条件:**
| 条件 | 阈值 | 说明 |
|------|------|------|
| 日均成交额 | ≥5000 万 | 流动性要求 |
| 日均换手率 | ≥1% | 活跃度要求 |
| 市值 | 50-2000 亿 | 避免太小/太大盘 |
| PE | <100 | 排除极端高估 |
| PB | 0.3-30 | 排除问题股/过度炒作 |
| 波动率 | <25% | 排除过度波动 |

**得分构成:**
- 流动性得分 (30%)
- 动量得分 (30%)
- 估值得分 (20%)
- 市值得分 (20%)

**核心方法:**
```python
screener.screen_stocks(concept_codes, concept_ranking, top_n_per_concept)
screener.get_top_stocks(concept_codes, top_n)
```

---

### P1-1: 个股预测模型 ✅

**新增文件:**
- `src/models/stock_predictor.py` - 个股预测器

**特点:**
- 独立于板块预测的模型参数
- 特征工程针对个股优化（添加成交量、成交额特征）
- 支持 XGBoost/LightGBM/RandomForest
- 32 并发高性能预测
- 置信度评估

**与板块预测的区别:**
| 特性 | 板块预测 | 个股预测 |
|------|----------|----------|
| 权重配置 | 1d:0.3, 5d:0.5, 20d:0.2 | 1d:0.4, 5d:0.4, 20d:0.2 |
| 特征 | 仅价格/涨跌幅 | 价格 + 成交量 + 成交额 |
| 模型文件 | unified_model.pkl | stock_model.pkl |

**核心方法:**
```python
predictor.prepare_features(stock_data, lookback=10, n_jobs=32)
predictor.train(features, model_type="xgboost")
predictor.predict(model_result, features, with_confidence=True)
```

---

### P1-2: 相关性计算 ✅

**实现在:**
- `src/portfolio/optimizer.py` - 组合优化器

**功能:**
- 计算 60 日滚动收益率相关性
- 构建协方差矩阵
- 计算个股波动率

**核心方法:**
```python
optimizer._calculate_correlation(stock_codes)  # 相关性矩阵
optimizer._calculate_volatilities(stock_codes)  # 波动率
```

---

### P1-3: 组合优化器 ✅

**新增文件:**
- `src/portfolio/optimizer.py` - 组合优化器
- `src/portfolio/__init__.py`

**优化方法:**
1. **风险平价 (Risk Parity)** - 每只股票风险贡献相等
2. **Black-Litterman 调整** - 融合板块预测观点
3. **约束优化** - 应用仓位限制

**约束条件:**
| 约束 | 默认值 |
|------|--------|
| 单股上限 | 10% |
| 单板块上限 | 25% |
| 单股下限 | 2% |
| 目标波动率 | 15% |

**输出:**
```python
{
    'portfolio': [
        {'ts_code': 'xxx', 'weight': 0.08, 'stock_name': 'xxx', ...},
        ...
    ],
    'metrics': {
        'expected_return': 0.25,
        'expected_volatility': 0.15,
        'sharpe': 1.67,
        'max_drawdown': 0.12,
    },
    'risk_analysis': {
        'sector_concentration': 0.35,
        'avg_correlation': 0.25
    }
}
```

---

### 组合构建 Agent ✅

**新增文件:**
- `src/agents/portfolio_agent.py`

**功能:**
整合完整流程：筛选 → 预测 → 优化

**任务类型:**
```python
agent.run(task="build", concept_codes, top_n_stocks=10)        # 完整流程
agent.run(task="screen", concept_codes)                        # 仅筛选
agent.run(task="predict", concept_codes)                       # 仅预测
agent.run(task="optimize", concept_codes, concept_predictions) # 仅优化
```

---

## 使用方式

### 命令行模式

```bash
# 构建投资组合（默认 10 只股票）
python src/main.py --mode portfolio

# 指定持仓数量
python src/main.py --mode portfolio --top-n 15
```

### Python 调用

```python
from agents.portfolio_agent import PortfolioAgent

agent = PortfolioAgent()

# 完整流程
result = agent.run(
    task="build",
    concept_codes=['881101.TI', '881102.TI'],  # 目标板块
    top_n_stocks=10
)

# 输出持仓
for pos in result['portfolio']:
    print(f"{pos['stock_name']}: {pos['weight']:.1%}")
```

### 测试

```bash
python test_portfolio.py
```

---

## 数据流

```
[1] 板块预测
    └─→ 输出：TOP 板块列表及预测涨幅

[2] 成分股获取
    └─→ 查询 concept_constituent 表

[3] 个股筛选
    ├─→ 流动性过滤 (≥5000 万)
    ├─→ 估值过滤 (PE<100, PB>0.3)
    ├─→ 技术面排序
    └─→ 输出：优选个股

[4] 个股预测
    ├─→ 特征工程 (65+ 特征)
    ├─→ 模型预测 (1d/5d/20d)
    └─→ 输出：带预测的个股

[5] 组合优化
    ├─→ 风险平价优化
    ├─→ Black-Litterman 调整
    ├─→ 应用约束 (单股≤10%, 单板块≤25%)
    └─→ 输出：最终组合 (8-10 只股票)
```

---

## 后续扩展建议

1. **实时数据对接** - 对接实时行情源
2. **因子增强** - 添加基本面因子、情绪因子
3. **深度学习模型** - LSTM/Transformer 捕捉时序特征
4. **业绩归因** - 分析收益来源
5. **模拟盘验证** - 先模拟跑 3 个月再实盘

---

## 风险提示

- 当前系统仅供研究使用
- 预测准确率有限（R² ~0.3-0.4）
- 实盘前必须经过模拟盘验证
- 建议小额测试，逐步扩大规模
