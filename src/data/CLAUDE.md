# Data 层上下文

> 数据层负责数据采集、存储、验证和管理。

---

## 文件结构

```
src/data/
├── __init__.py
├── database.py           # SQLite 数据库 (1534 行) ⭐ 核心
├── fast_collector.py     # 高速采集 (675 行) ⭐ 核心
├── tushare_ths_client.py # 同花顺 API 客户端
├── stock_screener.py     # 个股筛选 (742 行) ⭐ 核心
├── data_collector.py     # 采集调度器
├── data_organizer.py     # 数据导出
├── storage_manager.py    # 存储管理
├── data_validator.py     # 数据验证
├── csv_migrator.py       # CSV 迁移
├── stock_collector.py    # 个股数据采集
├── feature_engineer.py   # 特征工程
├── name_mapper.py        # 名称映射
└── backup.py             # 数据备份
```

---

## 核心类：SQLiteDatabase

**文件**: `database.py`

**职责**: SQLite 数据库管理，支持高并发写入

### PRAGMA 优化配置
```python
# WAL 模式 - 读写不阻塞
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA cache_size=-128000")       # 128MB 缓存
conn.execute("PRAGMA temp_store=MEMORY")
conn.execute("PRAGMA busy_timeout=10000")       # 10 秒超时
conn.execute("PRAGMA mmap_size=268435456")      # 256MB mmap
conn.execute("PRAGMA wal_autocheckpoint=500")   # 每500页checkpoint
conn.execute("PRAGMA journal_size_limit=104857600")  # 100MB WAL上限
```

**核心方法**:
```python
class SQLiteDatabase:
    def __init__(db_path, pool_size=5)      # 初始化连接池
    def batch_insert(table, data, batch_size=10000)  # 事务批量插入
    def get_concept_data(codes, dates)      # 查询板块数据
    def get_latest_date()                   # 获取最新日期
    def get_statistics()                    # 获取统计信息
```

**数据表结构**:

| 表名 | 主键 | 说明 |
|------|------|------|
| `concept_daily` | (ts_code, trade_date) | 板块日线行情 |
| `concept_info` | ts_code | 板块元信息 |
| `stock_daily` | (ts_code, trade_date) | 个股日线行情 |
| `concept_constituent` | (concept_code, stock_code) | 板块成分股 |
| `stock_daily_basic` | (ts_code, trade_date) | 个股基本面 |
| `trade_calendar` | trade_date | 交易日历（新增）|
| `concept_daily_archive` | (ts_code, trade_date) | 板块历史归档（新增）|
| `stock_daily_archive` | (ts_code, trade_date) | 个股历史归档（新增）|
| `collect_task` | id | 采集任务队列 |

### 覆盖索引（优化查询）

```sql
-- 板块行情覆盖索引
CREATE INDEX idx_concept_daily_cover
ON concept_daily(ts_code, trade_date, pct_change, vol, amount, close);

-- 个股行情覆盖索引
CREATE INDEX idx_stock_daily_cover
ON stock_daily(ts_code, trade_date, pct_chg, total_mv, pe, pb, amount);

-- 成分股权重排序索引
CREATE INDEX idx_constituent_weight
ON concept_constituent(concept_code, weight DESC);
```

**连接池**:
- 默认 5 个连接
- 支持多线程并发
- 自动归还连接
- 带健康检查的连接创建

**修改注意**:
- 修改表结构需更新 `_create_tables()`
- 新增表需同步更新 `get_statistics()`
- 保持 WAL 模式以支持高并发

---

## 交易日历操作

**核心方法**:
```python
def init_trade_calendar(start_date, end_date)  # 初始化交易日历
def get_trade_dates(start_date, end_date)      # 获取交易日列表
def is_trade_date(date)                        # 检查是否交易日
def get_prev_trade_date(date)                  # 获取前一交易日
def get_next_trade_date(date)                  # 获取后一交易日
```

**初始化交易日历**:
```python
from data.database import get_database
db = get_database()
db.init_trade_calendar(start_date="20100101")
```

---

## 数据归档操作

**核心方法**:
```python
def archive_old_data(before_date, tables)      # 归档历史数据
def restore_archived_data(start_date, end_date) # 恢复归档数据
def get_archive_statistics()                   # 获取归档统计
```

**归档示例**:
```python
# 归档 2023 年之前的数据
db.archive_old_data(before_date="20230101")

# 恢复特定时间段数据
db.restore_archived_data(start_date="20220101", end_date="20221231")
```

---

## 性能监控

**核心方法**:
```python
def get_performance_stats()  # 获取性能统计
def optimize_database()      # 优化数据库（重建索引、清理空间）
```

**性能统计输出**:
```python
{
    'page_count': 12345,
    'page_size': 4096,
    'db_size_mb': 48.5,
    'wal_log': 100,
    'concept_daily_count': 569095,
    'concept_date_range': ('20200102', '20260313')
}
```

---

## 核心类：HighSpeedDataCollector

**文件**: `fast_collector.py`

**职责**: 高速并发数据采集，直接写入数据库

**API 限流**:
```python
# 500 次/分钟限制
api_limit: int = 450  # 预留缓冲
# 接近 80% 时自动等待
if len(request_times) >= api_limit * 0.8:
    wait_and_reset()
```

**核心方法**:
```python
class HighSpeedDataCollector:
    def __init__(token, db, max_workers=20)
    def download_batch(codes, start_date, end_date)  # 批量下载
    def filter_valid_codes(codes)                     # 过滤无效板块
    def _download_single(code, start, end)            # 单个下载
    def _check_missing_dates(code, start, end)        # 检查缺失日期
```

**采集流程**:
```python
1. 获取板块列表 (get_ths_indices)
2. 过滤无效板块 (filter_valid_codes)
3. 批量并发下载 (download_batch)
4. 自动补全缺失数据
5. 直接写入数据库
```

**断点续传**:
- 检测数据库已有数据范围
- 仅下载缺失日期数据
- 记录失败任务支持重试

**修改注意**:
- API 限制 500 次/分钟，`api_limit` 预留缓冲
- `max_workers` 默认 20，过多可能触发限流
- 无效板块记录在 `_invalid_codes` 集合

---

## 核心类：TushareTHSClient

**文件**: `tushare_ths_client.py`

**职责**: 同花顺 API 封装

**主要接口**:

| 方法 | Tushare API | 说明 |
|------|-------------|------|
| `get_ths_indices()` | `ths_index()` | 板块列表 |
| `get_ths_industries()` | 筛选 881/882 | 行业分类 |
| `get_ths_history()` | `ths_daily()` | 历史行情 |
| `get_ths_constituent()` | `ths_member()` | 成分股 |

**板块代码规则**:
```
881xxx - 一级行业
882xxx - 二级行业/地区
885xxx - 概念板块
87xxxx - 北交所（已排除）
```

**重试机制**:
```python
max_retries: int = 3
# 指数退避
time.sleep(1.0 * (i + 1))
```

---

## 核心类：StockScreener

**文件**: `stock_screener.py`

**职责**: 从板块成分股中优选个股

**筛选规则**:
```python
SCREENING_RULES = {
    # 流动性
    'min_avg_amount': 3000,     # 日均成交额 ≥3000万
    'min_avg_turnover': 1.0,    # 日均换手率 ≥1%

    # 市值
    'min_market_cap': 50,       # ≥50亿
    'max_market_cap': 5000,     # ≤5000亿

    # 估值
    'max_pe': 100,              # PE <100
    'min_pb': 0.3,              # PB >0.3
    'max_pb': 30,               # PB <30

    # 波动率
    'max_volatility': 0.35,     # 20日波动率 <35%
}
```

**评分权重**:
```python
'score_weights': {
    'liquidity': 0.30,   # 流动性
    'momentum': 0.30,    # 动量
    'value': 0.20,       # 估值
    'size': 0.20,        # 市值
}
```

**核心方法**:
```python
class StockScreener:
    def screen_stocks(concept_codes, date, top_n)  # 筛选个股
    def _compute_liquidity_score(stock_data)       # 流动性评分
    def _compute_momentum_score(stock_data)        # 动量评分
    def _compute_value_score(stock_data)           # 估值评分
    def _compute_size_score(stock_data)            # 市值评分
```

**输出格式**:
```python
DataFrame columns:
- stock_code, stock_name
- concept_code (所属板块)
- concept_pred (板块预测)
- stock_score (综合得分)
- liquidity_score, momentum_score, value_score, size_score
```

---

## 数据流向

```
Tushare API (ths_daily)
        │
        ▼
HighSpeedDataCollector
   (并发下载 + 限流)
        │
        ▼
SQLiteDatabase (WAL 模式)
        │
        ├─→ concept_daily 表
        ├─→ stock_daily 表
        └─→ concept_constituent 表
        │
        ▼
DataOrganizer.export_to_csv()
        │
        ▼
data/raw/*.csv
```

---

## 调试命令

```python
# 查看数据库统计
from data.database import get_database
db = get_database()
print(db.get_statistics())

# 测试采集
from data.fast_collector import HighSpeedDataCollector
collector = HighSpeedDataCollector(token="xxx")
indices = collector.client.get_ths_indices()
print(f"板块数量: {len(indices)}")

# 筛选个股
from data.stock_screener import StockScreener
screener = StockScreener()
stocks = screener.screen_stocks(['885001.TI'])
print(stocks.head())
```

---

## 相关文档

- [../agents/CLAUDE.md](../agents/CLAUDE.md) - Agent 层
- [../models/CLAUDE.md](../models/CLAUDE.md) - 模型层