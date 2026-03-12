# A 股热点轮动预测系统

基于机器学习的 A 股市场热点轮动预测系统，通过 Tushare 数据采集、特征工程、模型训练，预测板块走势。

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件：

```bash
TUSHARE_TOKEN=your_tushare_token_here    # Tushare API Token（必需）
DATABASE_URL=sqlite:///data/stock.db
LOG_LEVEL=INFO
```

### 3. 快速测试

```bash
# 运行测试
python test_simple.py

# 运行 pytest 测试
python -m pytest tests/ -v
```

---

## 一键式预测（推荐）

```bash
# 一键式预测：热点板块 + 个股预测 + 投资组合
python src/main.py --mode full --top-n 10
```

**输出包含**：
- 热点板块 TOP10 排行榜（综合得分、1 日/5 日/20 日预测）
- 投资组合持仓明细（股票代码、名称、权重、所属板块）
- 预期指标（年化收益、波动率、夏普比率）
- 风险分析（板块集中度、平均相关性）

---

## 当前数据库状态
\n
### 数据概览（截至 2026-03-12）

| 数据表 | 记录数 | 说明 |
|--------|--------|------|
| `concept_daily` | 567,476 | 板块日线行情（426 个板块） |
| `stock_daily` | 18,089 | 成分股日线行情（25 只股票） |
| `concept_constituent` | 30 | 板块成分股（3 个板块） |
| `stock_daily_basic` | 18,420 | 个股每日基本面（PE/PB/市值） |
| `stock_factors` | 0 | 个股因子（待计算） |
\n
### 成分股详情\n\n**覆盖板块**：
| 板块代码 | 板块名称 | 成分股数量 |
|----------|----------|------------|
| 885311.TI | 半导体 | 10 只 |
| 885368.TI | 汽车芯片 | 10 只 |
| 885394.TI | 人工智能 | 10 只 |

**成分股列表**：
```
885311.TI (半导体):    紫光国微、长电科技、中科曙光、通富微电、兆易创新、
                       卓胜微、三安光电、北方华创、圣邦股份、中芯国际

885368.TI (汽车芯片):  联创电子、紫光国微、银轮股份、通富微电、北方华创、
                       四维图新、德赛西威、三安光电、华域汽车、兆易创新

885394.TI (人工智能):  长春高新、新和成、华兰生物、科大讯飞、海康威视、
                       东方财富、智飞生物、沃森生物、康泰生物、迈瑞医疗
```\n\n**行情数据日期范围**：
- 板块数据：2020-01-02 ~ 2026-03-11
- 个股数据：2023-03-13 ~ 2026-03-11
- 基本面数据：2023-01-03 ~ 2026-03-11（18,420 条，覆盖 24 只股票）

---

## 数据说明

### 当前数据类型

| 数据源 | 说明 | 文件格式 | 示例 |
|--------|------|----------|------|
| **同花顺行业板块** | 申万一级行业 | `ths_{code}_TI.csv` | `ths_881101_TI.csv` |
| **同花顺概念板块** | 热门概念板块 | `ths_{code}_TI.csv` | `ths_885xxx_TI.csv` |

### 数据字段

```csv
ts_code,trade_date,open,high,low,close,pre_close,avg_price,change,pct_change,vol,turnover_rate
881101.TI,20231229,3286.22,3314.70,3284.12,3313.87,3290.20,6.59,23.67,0.72,1879658.3,0.91
```

| 字段 | 说明 |
|------|------|
| `ts_code` | 板块代码（如 881101.TI） |
| `trade_date` | 交易日期（YYYYMMDD） |
| `open/high/low/close` | 开盘/最高/最低/收盘 |
| `pct_change` | 涨跌幅（%） |
| `vol` | 成交量 |
| `turnover_rate` | 换手率 |

### 数据存储目录

**V2.0 新版存储架构（SQLite 数据库）**：

```
data/
├── raw/
│   └── stock.db               # SQLite 数据库（统一存储所有板块数据）
│   ├── stock.db-wal           # WAL 日志（自动管理）
│   ├── stock.db-shm           # 共享内存（自动管理）
│   ├── ths_indices.csv        # 板块列表（元数据）
│   └── ths_industries_*.csv   # 行业分类（元数据）
│
├── processed/
│   └── merged_concept_data.csv  # 从数据库导出的合并文件（向后兼容）
│
├── models/                   # 训练好的模型
├── patterns/                 # 学习的规律（JSON）
└── results/                  # 分析结果
```

**旧版存储架构（CSV 文件）**：
data/
├── raw/                      # 原始 CSV 数据（采集的原始文件）
│   ├── ths_all_history_*.csv # 历史合集文件（建议保留）
│   ├── ths_indices.csv       # 板块列表
│   └── ths_name_mapping.csv  # 名称映射
│
├── processed/                # 处理后数据
│   └── merged_concept_data.csv  # 合并去重后的完整数据（主要数据源）
│
├── models/                   # 训练好的模型
├── patterns/                 # 学习的规律（JSON）
└── results/                  # 分析结果
```

---

### 存储架构升级（V2.0）

**新版 SQLite 存储方案特性**：

| 特性 | 旧版 CSV | 新版 SQLite |
|------|----------|-------------|
| **并发写入** | 文件锁冲突 | WAL 模式支持并发 |
| **去重** | 事后批处理 | 实时（唯一约束） |
| **增量更新** | 手动判断 | 自动（主键判断） |
| **存储空间** | 大量小文件 + 冗余 | 单文件，紧凑 |
| **查询性能** | 慢（读多文件） | 快（索引加速） |
| **数据完整性** | 弱 | 强（事务保证） |

**核心优势**：
- **高并发写入**：使用 WAL 模式，支持多进程安全写入
- **实时去重**：通过 `(ts_code, trade_date)` 复合主键保证唯一性
- **增量更新**：自动判断缺失数据，支持断点续传
- **统一存储**：单个数据库文件，无碎片

**迁移工具**：
```bash
# 从旧版 CSV 迁移到新版数据库
python src/data/csv_migrator.py --action all

# 仅迁移（不清理 CSV）
python src/data/csv_migrator.py --action migrate --keep-csv

# 验证迁移结果
python src/data/csv_migrator.py --action verify
```

---

### 旧版 CSV 存储说明（已废弃）

---

## 命令速查

### 组合构建

```bash
# 一键式预测（推荐）
python src/main.py --mode full --top-n 10

# 仅组合构建（需要已有板块预测）
python src/main.py --mode portfolio --top-n 10
```

### 数据采集

```bash
# 查看已采集的数据
python src/main.py --mode list

# 数据去重（数据库自动去重，无需手动执行）
python src/main.py --mode dedup

# 采集基础数据（板块列表 + 近期行情）
python src/main.py --mode data

# 采集历史数据（指定日期范围）
python src/main.py --mode history --start-date 20230101 --end-date 20241231

# 高速并发采集（推荐，直接写入数据库）
python src/main.py --mode fast --start-date 20200101 --end-date 20251231

# 数据整理（从数据库导出 CSV）
python src/main.py --mode organize
```

### 数据库管理

```bash
# 从 CSV 迁移到数据库
python src/data/csv_migrator.py --action all

# 仅迁移
python src/data/csv_migrator.py --action migrate

# 验证迁移结果
python src/data/csv_migrator.py --action verify

# 清理已迁移的 CSV 文件
python src/data/csv_migrator.py --action cleanup

# 查看数据库统计
python src/data/storage_manager.py --action stats

# 验证数据完整性
python src/data/storage_manager.py --action verify
```

### 模型训练

```bash
# 仅训练模型
python src/main.py --mode train

# 查看特征重要性
python src/main.py --mode importance
```

### 预测

```bash
# 执行预测（使用已有模型）
python src/main.py --mode predict

# 每日工作流（采集 + 训练 + 预测）
python src/main.py --mode daily --train

# 快速分析（使用已有数据）
python src/main.py --mode quick
```

### 回测验证

```bash
# 滚动回测（Walk-Forward）
python src/main.py --mode backtest --start-date 20230101 --end-date 20241231

# 自定义回测参数
BACKTEST_TRAIN=12 BACKTEST_TEST=3 BACKTEST_STEP=3 \
  python src/main.py --mode backtest --start-date 20230101 --end-date 20241231

# 时序交叉验证（Purged K-Fold）
python src/main.py --mode cv --start-date 20230101 --end-date 20241231

# 自定义 CV 参数
CV_SPLITS=5 CV_TRAIN_MONTHS=24 CV_PURGE=5 CV_EMBARGO=2 \
  python src/main.py --mode cv --start-date 20230101 --end-date 20241231

# 存储管理（验证/清理）
python src/main.py --mode storage                    # 验证存储状态
python src/main.py --mode storage --storage-action=cleanup   # 清理冗余文件
```

---

## 运行模式总表

| 模式 | 说明 | 命令 | 依赖 |
|------|------|------|------|
| `full` | **一键式预测** | `--mode full --top-n 10` | 历史数据 + 模型 |
| `portfolio` | **组合构建** | `--mode portfolio --top-n 10` | 历史数据 + 模型 |
| `list` | **查看已采集的数据** | `--mode list` | 无 |
| `dedup` | **数据去重** | `--mode dedup` | 无 |
| `fast` | **高速并发采集** | `--mode fast` | TUSHARE_TOKEN |
| `organize` | **数据整理** | `--mode organize` | 无 |
| `storage` | **存储管理** | `--mode storage` | 无 |
| `data` | 采集基础数据 | `--mode data` | TUSHARE_TOKEN |
| `history` | 采集历史数据 | `--mode history --start-date X --end-date Y` | TUSHARE_TOKEN |
| `train` | 训练模型 | `--mode train` | 历史数据 |
| `predict` | 执行预测 | `--mode predict` | 训练好的模型 |
| `daily` | 每日工作流 | `--mode daily --train` | TUSHARE_TOKEN + 历史数据 |
| `quick` | 快速分析 | `--mode quick` | 历史数据 + 模型 |
| `backtest` | 滚动回测 | `--mode backtest` | 历史数据 + 模型 |
| `cv` | 交叉验证 | `--mode cv` | 历史数据 + 模型 |
| `importance` | 特征重要性 | `--mode importance` | 训练好的模型 |

---

## 架构

```
src/
├── main.py                    # 主入口
├── config.py                  # 配置管理
├── runner.py                  # 流程编排器
│
├── agents/                    # Agent 层
│   ├── base_agent.py          # Agent 基类
│   ├── data_agent.py          # 数据采集（同花顺）
│   ├── analysis_agent.py      # 热点分析
│   ├── predict_agent.py       # 预测
│   └── portfolio_agent.py     # 组合构建 Agent
│
├── analysis/                  # 分析核心
│   ├── hotspot_detector.py    # 热点识别
│   ├── rotation_analyzer.py   # 轮动分析
│   └── pattern_learner.py     # 模式学习
│
├── data/                      # 数据层
│   ├── tushare_ths_client.py  # 同花顺客户端
│   ├── database.py            # 数据库管理
│   ├── stock_screener.py      # 个股筛选器
│   └── data_collector.py      # 采集调度器
│
├── models/                    # 预测模型
│   ├── predictor.py           # XGBoost 预测（65 个特征）- 板块预测
│   └── stock_predictor.py     # XGBoost 预测 - 个股预测
│
├── portfolio/                 # 组合优化
│   └── optimizer.py           # 组合优化器（风险平价 + Black-Litterman）
│
├── evaluation/                # 评估
│   ├── backtester.py          # 回测引擎
│   └── metrics.py             # 评估指标
│
├── learning/                  # 学习模块
│   └── rotation_learner.py    # 轮动规律学习
│
├── core/                      # 核心工具
│   └── settings.py            # 系统设置
│
└── utils/                     # 工具类
    └── logger.py              # 日志工具
```

---

## 预测模型

### 模型配置

| 模型 | 配置 |
|------|------|
| **板块预测主模型** | XGBoost (n_estimators=200, max_depth=5, lr=0.05) |
| **板块预测备用 1** | LightGBM (n_estimators=200, max_depth=5, lr=0.05) |
| **板块预测备用 2** | RandomForest (n_estimators=100, max_depth=10) |
| **个股预测模型** | XGBoost (n_estimators=200, max_depth=6, lr=0.05) |

### 特征工程

**板块预测特征（65 个特征）**：

| 类别 | 特征 |
|------|------|
| **滚动统计** | pct_mean_{3,5,10}, pct_std_{3,5,10}, pct_max_{3,5,10}, pct_min_{3,5,10} |
| **动量特征** | momentum_{3,5,10}, momentum_accel |
| **波动率** | volatility_{3,5,10,20}, skewness_{3,5,10,20}, kurtosis_{3,5,10,20} |
| **价格位置** | pct_rank_{5,10,20}, breakout_{5,10,20} |
| **技术指标** | MACD, MACD_signal, MACD_hist, RSI_{6,12} |
| **量价关系** | vol_ratio, vol_trend, vol_price_corr_{5,10} |
| **形态特征** | gap_up, gap_down, extreme_up, extreme_down |
| **均值回归** | zscore_{5,10}, mean_revert_{5,10} |
| **趋势** | trend, 连续上涨天数 |

### 预测周期

| 周期 | 目标 | 说明 |
|------|------|------|
| **1 日** | target_1d | 次日涨跌幅预测 |
| **5 日** | target_5d | 5 日累计涨跌幅预测 |
| **20 日** | target_20d | 20 日累计涨跌幅预测 |

### 综合评分

**板块综合评分**：
```
combined_score = pred_1d × 0.3 + pred_5d × 0.5 + pred_20d × 0.2
```

**个股综合评分**：
```
combined_score = pred_1d × 0.4 + pred_5d × 0.4 + pred_20d × 0.2
```

---

## 投资组合优化

### 组合构建流程

1. **热点板块预测** - 预测各板块 1 日/5 日/20 日收益
2. **成分股筛选** - 从板块成分股中筛选优质个股
3. **个股预测** - 预测个股 1 日/5 日/20 日收益
4. **组合优化** - 风险平价 + Black-Litterman 模型构建最优权重

### 筛选规则

| 条件 | 阈值 | 说明 |
|------|------|------|
| 流动性 | ≥5000 万 | 20 日日均成交额 |
| 市值 | 50-2000 亿 | 总市值 |
| 估值 | PE<100, PB 0.3-30 | 排除极端估值 |
| 波动率 | <25% | 20 日年化波动率 |

### 组合约束

| 约束 | 默认值 | 说明 |
|------|--------|------|
| 单股权重 | ≤10% | 避免过度集中 |
| 单板块权重 | ≤25% | 控制板块风险 |
| 目标波动率 | 15% | 风险平价优化目标 |

### 优化算法

1. **风险平价** - 每只股票对组合风险贡献相等
2. **Black-Litterman** - 将板块预测观点融入权重
3. **约束优化** - 应用权重上下限和板块集中度约束

---

## 回测指标

| 指标 | 说明 |
|------|------|
| **IC** | 预测值与实际值的相关系数（Pearson） |
| **RankIC** | 预测排名与实际排名的相关系数（Spearman） |
| **Sharpe** | 夏普比率（年化收益/波动率） |
| **最大回撤** | 策略最大亏损幅度 |
| **胜率** | 盈利交易占比 |

---

## 测试

```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行特定测试
python -m pytest tests/test_e2e_pipeline.py -v   # 端到端测试
python -m pytest tests/test_analysis.py -v       # 分析模块测试
python -m pytest tests/test_prediction.py -v     # 预测模块测试
python -m pytest test_portfolio.py -v            # 组合构建测试
python -m pytest test_portfolio_quick.py -v      # 组合快速测试

# 覆盖率测试
python -m pytest tests/ --cov=src -v
```

---

## 依赖

**核心依赖**: pandas, numpy, scikit-learn, xgboost, lightgbm, tushare, sqlalchemy, pydantic-settings, loguru

详见 `requirements.txt`

---

## 故障排查

### TUSHARE_TOKEN 未配置
```bash
cat .env | grep TUSHARE_TOKEN
export TUSHARE_TOKEN=your_token
```

### 数据为空
系统自动切换到 `index_daily()` 备用接口

### 权限不足
访问 https://tushare.pro/user/token 确认积分 >= 5000

---

## 相关文档

- `CLAUDE.md` - 项目配置说明
- `ARCHITECTURE.md` - 架构详情
- `TEST_SUMMARY.md` - 测试总结
- `docs/PORTFOLIO_SUMMARY.md` - 组合构建总结
- `docs/PORTFOLIO_IMPLEMENTATION.md` - 组合实现文档
- `docs/PORTFOLIO_GUIDE.md` - 组合使用指南

---

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
