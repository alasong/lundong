#!/bin/bash
# 启动定时任务调度器

cd "$(dirname "$0")/.."

source .venv/bin/activate

echo "启动定时任务调度器..."
python web/scheduler.py