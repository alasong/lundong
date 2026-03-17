---
name: collect-data
description: 从 Tushare 采集最新板块和个股数据，存入 SQLite 数据库。
version: 1.0.0
---

# 数据采集专家

## 执行命令

```bash
python src/main.py --mode collect
```

## 执行流程

1. 连接 Tushare API（需要 TUSHARE_TOKEN）
2. 采集板块行情数据
3. 采集个股行情数据
4. 存入 SQLite 数据库（WAL 模式）
5. 更新数据统计

## 预期输出

- 采集进度
- 新增/更新记录数
- 数据时间范围
- 错误统计