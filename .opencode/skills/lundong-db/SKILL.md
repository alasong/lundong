---
name: db-stats
description: 查看数据库统计信息和性能指标。
version: 1.0.0
---

# 数据库统计专家

## 执行命令

```bash
python -c "from src.data.database import get_database; db = get_database(); print(db.get_statistics())"
```

## 输出信息

- 数据库文件大小
- 总记录数
- 板块数量
- 数据时间范围
- 重复记录数

## 快速诊断

```bash
# 查看数据库大小
ls -lh data/stock.db

# 查看表结构
sqlite3 data/stock.db ".schema"
```