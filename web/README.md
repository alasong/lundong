# A股热点轮动预测系统 - Web界面

## 快速启动

### 1. 启动 Web 界面

```bash
cd /home/song/lundong
.venv/bin/streamlit run web/app.py
```

或使用启动脚本：
```bash
./web/start.sh
```

### 2. 访问界面

浏览器打开: http://localhost:8501

---

## 功能模块

### 📊 数据管理

- **数据库状态**: 查看板块数据、个股数据量
- **数据采集**: 
  - 采集最新板块数据
  - 采集个股数据（指定日期范围）
- **数据操作**: 导出到 CSV、查看统计

### 🎯 策略管理

- **可用策略**: 11 个策略列表及说明
- **策略组合**: 
  - 选择多个策略
  - 设置策略权重
  - 选择信号合并方法

### 📈 回测分析

- **回测配置**: 
  - 设置日期范围
  - 设置初始资金
  - 选择策略
- **回测结果**:
  - 净值曲线图
  - 绩效指标表
  - 持仓分析

### 🔥 热点预测

- **生成预测**: 一键生成最新板块预测
- **预测结果**: TOP 10 板块排名
- **可视化**: 预测涨幅分布图
- **投资建议**: 短线/中线建议

### ⏰ 定时任务

- **任务配置**: 
  - 选择任务类型
  - 设置执行频率
- **当前任务**: 查看、执行、删除任务
- **调度器**: 启动定时任务调度器

---

## 定时任务调度器

### 启动调度器

```bash
./web/start_scheduler.sh
```

### 默认任务

| 任务 | 时间 | 说明 |
|------|------|------|
| 数据采集 | 每日 09:30 | 采集最新板块数据 |
| 预测生成 | 每日 15:30 | 生成板块预测 |
| 模型训练 | 每周六 10:00 | 重新训练模型 |

### 自定义任务

编辑 `web/scheduler.py` 添加新任务：

```python
scheduler.add_job(
    your_function,
    CronTrigger(hour=10, minute=0, day_of_week="mon-fri"),
    id="your_task",
    name="任务名称"
)
```

---

## 技术栈

| 组件 | 技术 |
|------|------|
| Web 框架 | Streamlit 1.55 |
| 可视化 | Plotly |
| 定时任务 | APScheduler |
| 数据库 | SQLite (WAL) |
| 预测模型 | XGBoost |

---

## 文件结构

```
web/
├── app.py              # Web 应用主程序
├── scheduler.py        # 定时任务调度器
├── start.sh            # 启动 Web 界面
├── start_scheduler.sh  # 启动调度器
└── README.md           # 本文档
```

---

## 常见问题

### Q: 页面加载慢？

A: 首次加载需要初始化模型，请耐心等待。

### Q: 预测失败？

A: 检查数据库是否有数据：
```bash
python -c "from src.data.database import get_database; print(get_database().get_statistics())"
```

### Q: 如何修改端口？

A: 
```bash
.venv/bin/streamlit run web/app.py --server.port 8502
```

---

## 下一步

1. 采集个股数据完善多策略
2. 配置定时任务自动运行
3. 添加更多可视化图表
4. 实现用户配置保存