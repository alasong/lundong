#!/bin/bash
# 预测模块压力测试快速脚本
# 用法：./run_stress_test.sh [all|feature|batch|e2e|concurrent]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "A 股热点轮动预测系统 - 压力测试"
echo "=============================================="
echo ""

# 检查虚拟环境
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${YELLOW}警告：未在虚拟环境中运行${NC}"
    echo "建议先激活虚拟环境：source .venv/bin/activate"
    echo ""
fi

# 检查依赖
echo "检查依赖..."
python -c "import pandas; import numpy; import joblib; from sklearn.ensemble import RandomForestRegressor" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "${RED}错误：缺少必要依赖${NC}"
    echo "请运行：pip install -r requirements.txt"
    exit 1
fi
echo -e "${GREEN}✓ 依赖检查通过${NC}"
echo ""

# 运行测试
TEST_TYPE=${1:-all}
echo "运行测试类型：$TEST_TYPE"
echo ""

python tests/stress_test_predictor.py --test $TEST_TYPE

echo ""
echo "=============================================="
echo -e "${GREEN}测试完成${NC}"
echo "=============================================="
