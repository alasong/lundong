# Git Hooks 使用指南

## 安装

```bash
# 首次使用需要安装 hooks
bash scripts/install-hooks.sh
```

## 优化内容

### Pre-commit Hook（提交前检查）

**检查项目：**
1. Python 语法检查（超时 10 秒）
2. 敏感信息检查（API Key、Token 等）
3. 运行相关测试（超时 30 秒）

**智能测试映射：**
| 修改文件 | 运行测试 |
|---------|---------|
| predictor.py | test_prediction.py |
| predict_agent.py | test_prediction.py |
| analysis_agent.py | test_analysis.py |
| data_agent.py | test_data_collection.py |
| fast_collector.py | test_data_collection.py |
| database.py | test_database.py |
| main.py | test_e2e_pipeline.py |

**性能提升：**
- 优化前：5-10 分钟（运行所有测试）
- 优化后：5-30 秒（只运行相关测试）

### Pre-push Hook（推送前检查）

**检查项目：**
- 运行完整测试套件
- 超时限制：5 分钟
- 详细报告：显示每个测试结果

## 使用方法

### 正常提交流程

```bash
# 1. 修改文件
git add <files>

# 2. 提交（自动运行相关测试）
git commit -m "message"

# 3. 推送（自动运行完整测试）
git push
```

### 跳过 Hook

```bash
# 跳过提交 hook（不推荐）
git commit --no-verify -m "message"

# 跳过推送 hook（不推荐）
git push --no-verify
```

## 故障排除

### Hook 卡住怎么办？

```bash
# 杀死卡住的进程
pkill -f "git commit"
pkill -f "pre-commit"

# 使用 --no-verify 提交
git commit --no-verify -m "紧急提交"
```

### 测试失败怎么办？

```bash
# 1. 本地运行测试查看详细信息
.venv/bin/pytest tests/ -v

# 2. 修复问题后重新提交
git add <files>
git commit -m "修复问题"
```

### 如何禁用 Hook？

```bash
# 临时禁用 pre-commit
git commit --no-verify -m "message"

# 临时禁用 pre-push
git push --no-verify

# 永久删除 hook（不推荐）
rm .git/hooks/pre-commit
rm .git/hooks/pre-push
```

## 配置说明

### pytest.ini
```ini
[pytest]
timeout = 30           # 每个测试超时 30 秒
cache-dir = .pytest_cache  # 缓存目录
```

### 环境变量
```bash
# 延长超时时间（秒）
export PYTEST_TIMEOUT=60
```
