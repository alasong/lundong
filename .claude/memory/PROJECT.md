# 项目记忆

此文件存储跨会话的项目知识，帮助 Claude 更好地理解项目。

---

## 项目核心信息

- **项目名称**: lundong (A股热点轮动预测系统)
- **主要语言**: Python 3.12+
- **数据库**: SQLite (WAL 模式)
- **ML 框架**: XGBoost, LightGBM
- **数据源**: Tushare Pro

---

## 关键路径

| 路径 | 说明 |
|------|------|
| `src/main.py` | CLI 入口，17 种模式 |
| `src/data/database.py` | SQLite 数据库管理 |
| `src/models/predictor.py` | XGBoost 预测器 |
| `data/stock.db` | 主数据库 |

---

## 常用命令

```bash
# 完整预测
python src/main.py --mode full --top-n 10

# 数据采集
python src/main.py --mode fast

# 模型训练
python src/main.py --mode train

# 运行测试
python -m pytest tests/ -v
```

---

## 编码规范

1. 使用 Python 3.12+ 特性
2. 类型注解推荐但非强制
3. 日志使用 loguru
4. 数据库操作使用连接池
5. 新功能需添加测试

---

## 最近修改

- 2026-03-15: 数据库优化 (PRAGMA, 索引, 交易日历, 归档)
- 2026-03-15: 创建多层 CLAUDE.md 文档体系