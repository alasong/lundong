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

```
data/
├── raw/          # 原始 CSV 数据
├── models/       # 训练好的模型
├── patterns/     # 学习的规律（JSON）
└── results/      # 分析结果
```

---

## 命令速查

### 数据采集

```bash
# 查看已采集的数据
python src/main.py --mode list

# 数据去重
python src/main.py --mode dedup

# 采集基础数据（板块列表 + 近期行情）
python src/main.py --mode data

# 采集历史数据（指定日期范围）
python src/main.py --mode history --start-date 20230101 --end-date 20241231

# 采集单日数据
python src/main.py --mode data --date 20241231
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
```

---

## 运行模式总表

| 模式 | 说明 | 命令 | 依赖 |
|------|------|------|------|
| `list` | **查看已采集的数据** | `--mode list` | 无 |
| `dedup` | **数据去重** | `--mode dedup` | 无 |
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
│   └── predict_agent.py       # 预测
│
├── analysis/                  # 分析核心
│   ├── hotspot_detector.py    # 热点识别
│   ├── rotation_analyzer.py   # 轮动分析
│   └── pattern_learner.py     # 模式学习
│
├── data/                      # 数据层
│   ├── tushare_ths_client.py  # 同花顺客户端
│   └── data_collector.py      # 采集调度器
│
├── models/                    # 预测模型
│   └── predictor.py           # XGBoost 预测（65 个特征）
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
| **主模型** | XGBoost (n_estimators=200, max_depth=5, lr=0.05) |
| **备用 1** | LightGBM (n_estimators=200, max_depth=5, lr=0.05) |
| **备用 2** | RandomForest (n_estimators=100, max_depth=10) |

### 特征工程（65 个特征）

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

```
combined_score = pred_1d × 0.3 + pred_5d × 0.5 + pred_20d × 0.2
```

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

---

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
