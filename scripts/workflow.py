#!/usr/bin/env python3
"""
项目自动化工作流脚本
用于 Claude Code hooks 和 CI/CD
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd: str, cwd: Path = None) -> bool:
    """运行命令并返回是否成功"""
    print(f"[RUN] {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd or Path.cwd())
    return result.returncode == 0

def check_syntax(file_path: str) -> bool:
    """检查 Python 文件语法"""
    if not file_path.endswith('.py'):
        return True
    return run_command(f"python -m py_compile {file_path}")

def run_tests(test_path: str = "tests/") -> bool:
    """运行测试"""
    return run_command(f"python -m pytest {test_path} -v --tb=short -x")

def run_lint() -> bool:
    """运行代码检查"""
    success = True
    # 类型检查 (可选)
    # success &= run_command("python -m mypy src/ --ignore-missing-imports")
    # 风格检查 (可选)
    # success &= run_command("python -m black src/ --check")
    return success

def train_model() -> bool:
    """训练模型"""
    return run_command("python src/main.py --mode train")

def collect_data() -> bool:
    """采集数据"""
    return run_command("python src/main.py --mode fast")

def run_prediction() -> bool:
    """执行预测"""
    return run_command("python src/main.py --mode full --top-n 10")

def main():
    """主入口"""
    if len(sys.argv) < 2:
        print("用法: python workflow.py <command> [args]")
        print("命令: syntax <file>, test, lint, train, collect, predict")
        sys.exit(1)

    command = sys.argv[1]

    if command == "syntax" and len(sys.argv) > 2:
        success = check_syntax(sys.argv[2])
    elif command == "test":
        success = run_tests()
    elif command == "lint":
        success = run_lint()
    elif command == "train":
        success = train_model()
    elif command == "collect":
        success = collect_data()
    elif command == "predict":
        success = run_prediction()
    else:
        print(f"未知命令: {command}")
        success = False

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()