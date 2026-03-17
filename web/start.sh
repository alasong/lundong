#!/bin/bash
# A股热点轮动预测系统 - 启动脚本

echo "=========================================="
echo "A股热点轮动预测系统 - Web界面"
echo "=========================================="

cd "$(dirname "$0")/.."

# 激活虚拟环境
source .venv/bin/activate

# 启动 Streamlit
streamlit run web/app.py --server.port 8501 --server.address 0.0.0.0