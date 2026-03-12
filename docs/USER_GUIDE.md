# A 股量化系统使用指南

## 系统概述

本系统是一个完整的 A 股量化交易解决方案，包含：
- ✅ 热点板块预测
- ✅ 个股筛选与评分
- ✅ 投资组合优化
- ✅ 风险管理（止损、VaR、黑名单）
- ✅ 交易成本模型
- ✅ 市场状态识别
- ✅ 多因子选股
- ✅ 自动调仓调度

---

## 快速开始

### 1. 环境配置

```bash
cd /home/song/lundong
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置文件

创建 `.env` 文件：
```bash
TUSHARE_TOKEN=your_token_here
DATABASE_URL=sqlite:///data/stock.db
LOG_LEVEL=INFO
```

### 3. 一键式预测（推荐）

```bash
# 执行完整预测流程
python src/main.py --mode full --top-n 10
```

输出包含：
- 热点板块 TOP10 排行榜
- 投资组合持仓明细
- 预期收益和风险分析

---

## 功能模块使用

### 数据采集

```bash
# 查看已采集的数据
python src/main.py --mode list

# 采集近期数据
python src/main.py --mode data

# 采集历史数据
python src/main.py --mode history --start-date 20230101 --end-date 20241231

# 高速并发采集
python src/main.py --mode fast --start-date 20230101 --end-date 20241231
```

### 模型训练与预测

```bash
# 训练模型
python src/main.py --mode train

# 执行预测
python src/main.py --mode predict

# 查看特征重要性
python src/main.py --mode importance
```

### 回测验证

```bash
# 滚动回测
python src/main.py --mode backtest --start-date 20230101 --end-date 20241231

# 交叉验证
python src/main.py --mode cv --start-date 20230101 --end-date 20241231
```

### 风险管理

```bash
# 测试风险管理器
python src/risk/risk_manager.py

# 测试交易成本模型
python src/risk/transaction_cost.py

# 测试交易信号生成
python src/risk/signal_generator.py
```

### 策略分析

```bash
# 测试多因子选股
python src/strategy/multi_factor.py

# 测试市场状态识别
python src/strategy/market_regime.py
```

### 交易执行

```bash
# 测试订单管理
python src/trading/order_manager.py

# 测试自动调仓
python src/trading/rebalance_scheduler.py
```

### 集成测试

```bash
# 运行完整系统测试
python test_integration_full.py
```

---

## 命令行参数说明

### 主程序参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `--mode` | 运行模式 | `full`, `train`, `predict` |
| `--top-n` | 返回数量 | `10` |
| `--date` | 基准日期 | `20260311` |
| `--start-date` | 开始日期 | `20230101` |
| `--end-date` | 结束日期 | `20241231` |

### 运行模式总表

| 模式 | 说明 | 依赖 |
|------|------|------|
| `full` | 一键式预测 | 历史数据 + 模型 |
| `portfolio` | 组合构建 | 板块预测 |
| `list` | 查看数据 | 无 |
| `data` | 采集数据 | TUSHARE_TOKEN |
| `history` | 历史数据 | TUSHARE_TOKEN |
| `fast` | 高速采集 | TUSHARE_TOKEN |
| `train` | 训练模型 | 历史数据 |
| `predict` | 执行预测 | 模型 |
| `backtest` | 滚动回测 | 历史数据 + 模型 |
| `cv` | 交叉验证 | 历史数据 + 模型 |

---

## 数据结构

### 数据库表

| 表名 | 说明 | 字段示例 |
|------|------|----------|
| `concept_daily` | 板块日线 | ts_code, trade_date, close, pct_change |
| `stock_daily` | 个股日线 | ts_code, trade_date, close, pct_chg |
| `concept_constituent` | 成分股 | concept_code, stock_code, stock_name |
| `stock_daily_basic` | 个股基本面 | ts_code, pe_ttm, pb, total_mv |

### 当前数据状态

| 数据类型 | 记录数 | 说明 |
|---------|--------|------|
| 板块数据 | 567,476 | 426 个板块 |
| 个股数据 | 18,089 | 25 只股票 |
| 成分股 | 36 | 6 个板块 |
| 基本面 | 18,420 | PE/PB/市值 |

---

## 策略配置

### 筛选规则

| 条件 | 阈值 |
|------|------|
| 流动性 | ≥5000 万（20 日日均成交） |
| 市值 | 50-2000 亿 |
| 估值 | PE<100, PB 0.3-30 |
| 波动率 | <25%（年化） |

### 综合评分权重

| 因子 | 权重 |
|------|------|
| 流动性 | 30% |
| 动量 | 30% |
| 估值 | 20% |
| 市值 | 20% |

### 止损规则

| 类型 | 阈值 |
|------|------|
| 固定止损 | 8% |
| 移动止损 | 从最高点回撤 10% |

---

## 常见问题

### TUSHARE_TOKEN 未配置

```bash
# 检查配置
cat .env | grep TUSHARE_TOKEN

# 设置环境变量
export TUSHARE_TOKEN=your_token
```

### 数据为空

系统会自动切换到备用接口，或手动采集：
```bash
python src/main.py --mode data
```

### 成分股数量少

使用批量导入工具：
```bash
python bulk_import_constituents.py
```

---

## 输出示例

### 一键式预测输出

```
======================================================================
A 股热点轮动 - 一键式预测
======================================================================

【热点板块 TOP10】
--------------------------------------------------------------------------------
排名    板块代码        综合得分      1 日预测     5 日预测     20 日预测
--------------------------------------------------------------------------------
0     885945.TI    26.97       3.49      35.89     32.29
1     881276.TI    26.65       3.23      32.92     36.59

【投资组合构建结果】
======================================================================

持仓数量：6 只股票

【持仓明细】
----------------------------------------------------------------------------------------------------
代码          名称         权重    所属板块        1 日预测     5 日预测
----------------------------------------------------------------------------------------------------
603019.SH   中科曙光    27.2%   885311.TI      0.70%      3.28%
300782.SZ   卓胜微      27.2%   885311.TI      0.31%      0.81%

【预期指标】
  预期年化收益：49.8%
  预期年化波动率：33.2%
  夏普比率：1.50
```

---

## 风险提示

⚠️ **重要声明**

1. 本系统仅供学习和研究使用
2. 不构成任何投资建议
3. 股市有风险，投资需谨慎
4. 历史业绩不代表未来收益
5. 实盘交易前请充分测试和评估风险

---

## 技术支持

- 系统文档：`README.md`, `docs/QUANT_SYSTEM_SUMMARY.md`
- 测试文件：`test_integration_full.py`
- 架构说明：`ARCHITECTURE.md`
