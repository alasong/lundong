# 数据备份系统文档

## 概述

数据备份系统提供完整的数据库备份、恢复、归档功能，确保数据安全性和可恢复性。

## 核心功能

### 1. 完整备份
- 备份整个数据库文件
- 支持 gzip 压缩（节省约 78% 存储空间）
- 自动生成 MD5 校验和
- 保存备份元数据（时间、大小、描述）

### 2. 增量备份
- 只备份指定表的变更数据
- 支持按时间过滤
- 保存为 CSV 格式
- 可选压缩归档

### 3. 备份恢复
- 从完整备份恢复
- 自动备份当前数据库（防止意外）
- 支持自定义恢复路径

### 4. 备份验证
- MD5 校验和验证
- 文件完整性检查
- 可读性测试

### 5. 备份管理
- 列出所有备份
- 清理过期备份
- 备份统计信息

## 使用示例

### 初始化备份系统

```python
from src.data.backup import DatabaseBackup

# 初始化
backup = DatabaseBackup(
    db_path="data/stock.db",
    backup_dir="data/backups",
    compression=True,        # 启用压缩
    retention_days=30,       # 保留 30 天
    max_backups=20           # 最多 20 个备份
)
```

### 完整备份

```python
# 执行完整备份
result = backup.backup_full(description="每日备份")

if result['success']:
    print(f"备份成功：{result['backup_file']}")
    print(f"原始大小：{result['metadata']['original_size'] / (1024*1024):.2f} MB")
    print(f"备份大小：{result['metadata']['backup_size'] / (1024*1024):.2f} MB")
    print(f"压缩率：{result['metadata']['backup_size'] / result['metadata']['original_size']:.2%}")
else:
    print(f"备份失败：{result['error']}")
```

### 增量备份

```python
# 备份最近更新的股票数据
result = backup.backup_incremental(
    tables=["stock_daily", "stock_daily_basic"],
    since=datetime.now() - timedelta(days=1)
)

if result['success']:
    print(f"增量备份完成：{result['metadata']['total_records']} 条记录")
```

### 恢复数据库

```python
# 从备份恢复
result = backup.restore("data/backups/backup_full_20260312_020000.db.gz")

if result['success']:
    print(f"恢复成功：{result['target']}")
else:
    print(f"恢复失败：{result['error']}")
```

### 验证备份

```python
# 验证备份文件完整性
result = backup.verify_backup("data/backups/backup_full_20260312_020000.db.gz")

if result['valid']:
    print("备份文件有效")
    print(f"校验和：{result['checksum']}")
    print(f"备份时间：{result['backup_time']}")
else:
    print(f"备份文件无效：{result['error']}")
```

### 列出备份

```python
# 列出所有备份
backups = backup.list_backups()

for b in backups:
    print(f"{b['backup_time']} - {b['backup_type']} ({b['backup_size']/(1024*1024):.2f} MB)")
    print(f"  描述：{b.get('description', 'N/A')}")
```

### 清理过期备份

```python
# 清理超过 30 天的备份
result = backup.cleanup_old_backups()

print(f"删除 {result['removed_count']} 个备份")
print(f"保留 {result['kept_count']} 个备份")
```

### 备份统计

```python
# 获取备份统计信息
stats = backup.get_backup_stats()

print(f"总备份数：{stats['total_backups']}")
print(f"完整备份：{stats['full_backups']}")
print(f"增量备份：{stats['incremental_backups']}")
print(f"总大小：{stats['total_size_mb']:.2f} MB")
print(f"平均备份大小：{stats['avg_backup_size_mb']:.2f} MB")
print(f"最早备份：{stats['first_backup']}")
print(f"最晚备份：{stats['last_backup']}")
```

## 定时备份

### 使用 cron 定时备份

```bash
# 每天凌晨 2 点备份
0 2 * * * cd /home/song/lundong && source .venv/bin/activate && python -c "from src.data.backup import create_backup_schedule; create_backup_schedule()"
```

### 使用 Python schedule 库

```python
import schedule
import time
from src.data.backup import create_backup_schedule

# 每天凌晨 2 点备份
schedule.every().day.at("02:00").do(create_backup_schedule)

# 每小时备份（测试用）
# schedule.every().hour.do(create_backup_schedule)

while True:
    schedule.run_pending()
    time.sleep(60)
```

## 备份策略建议

### 每日备份
- 完整备份：每天凌晨 2 点
- 保留最近 7 天的备份

### 每周备份
- 完整备份：每周日凌晨 3 点
- 保留最近 4 周的备份

### 每月备份
- 完整备份：每月 1 日凌晨 4 点
- 保留最近 12 个月的备份

### 推荐配置

```python
backup = DatabaseBackup(
    db_path="data/stock.db",
    backup_dir="data/backups",
    compression=True,
    retention_days=30,       # 保留 30 天
    max_backups=20           # 最多 20 个备份
)
```

## 备份文件结构

```
data/backups/
├── backup_full_20260312_020000.db.gz    # 完整备份（压缩）
├── backup_full_20260312_020000.json     # 备份元数据
├── backup_incremental_20260313_020000/  # 增量备份目录
│   ├── stock_daily.csv
│   ├── stock_daily_basic.csv
│   └── metadata.json
└── backup_incremental_20260313_020000.tar.gz  # 压缩后的增量备份
```

## 备份元数据格式

```json
{
  "backup_type": "full",
  "backup_time": "2026-03-12T02:00:00.000000",
  "source_file": "data/stock.db",
  "backup_file": "data/backups/backup_full_20260312_020000.db.gz",
  "description": "每日备份",
  "original_size": 202399744,
  "backup_size": 45019340,
  "checksum": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
}
```

## 恢复流程

### 正常恢复流程

1. 停止正在运行的程序
2. 备份当前数据库（以防万一）
3. 执行恢复操作
4. 验证恢复结果
5. 重启程序

### 灾难恢复流程

1. 找到最近的完整备份
2. 找到所有后续增量备份
3. 恢复完整备份
4. 依次应用增量备份
5. 验证数据完整性

## 最佳实践

1. **定期备份**：至少每天备份一次
2. **异地备份**：定期将备份复制到其他位置
3. **验证备份**：定期验证备份文件完整性
4. **测试恢复**：定期测试恢复流程
5. **监控备份**：设置备份失败告警
6. **文档记录**：记录备份策略和恢复流程

## 注意事项

1. 备份会占用磁盘空间，确保有足够的可用空间
2. 压缩备份会消耗 CPU 资源，但节省存储空间
3. 大数据库备份可能需要较长时间
4. 备份时避免数据库写入操作
5. 定期清理过期备份，避免占用过多空间

## 故障排查

### 备份失败

检查磁盘空间：
```bash
df -h data/
```

检查文件权限：
```bash
ls -la data/stock.db
ls -la data/backups/
```

### 恢复失败

检查备份文件是否存在：
```bash
ls -la data/backups/
```

验证备份文件完整性：
```python
backup.verify_backup("data/backups/backup_full_20260312.db.gz")
```

### 备份文件过大

1. 启用压缩备份
2. 减少备份频率
3. 清理过期备份

## 相关文件

- `src/data/backup.py` - 备份系统实现
- `docs/BACKUP.md` - 本文档

## 与其他模块集成

### 与日志审计系统集成

```python
from src.data.backup import DatabaseBackup
from src.utils.audit_logger import get_audit_logger

backup = DatabaseBackup()
audit = get_audit_logger()

result = backup.backup_full(description="每日备份")

if result['success']:
    audit.log_system_event(
        event_type="backup_success",
        message="数据库备份成功",
        details=result['metadata']
    )
else:
    audit.log_system_event(
        event_type="backup_failure",
        message="数据库备份失败",
        details={"error": result['error']}
    )
```

### 与监控系统集成

```python
# 监控备份状态
stats = backup.get_backup_stats()

if stats['total_backups'] == 0:
    # 发送告警
    alert_system.send_alert("备份告警", "没有可用的备份文件")

if stats['total_size_mb'] > 10000:  # 超过 10GB
    # 发送告警
    alert_system.send_alert("存储空间告警", "备份文件超过 10GB")
```
