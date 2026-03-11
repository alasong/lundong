#!/bin/bash
# Git Hooks 安装脚本
# 使用方法：bash scripts/install-hooks.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
HOOKS_DIR="$PROJECT_ROOT/.git/hooks"
HOOKS_SOURCE="$SCRIPT_DIR/hooks"

echo "======================================"
echo "安装 Git Hooks"
echo "======================================"

# 创建 hooks 目录
mkdir -p "$HOOKS_DIR"

# 复制 pre-commit hook
cp "$HOOKS_SOURCE/pre-commit" "$HOOKS_DIR/pre-commit"
chmod +x "$HOOKS_DIR/pre-commit"
echo "✓ 安装 pre-commit hook"

# 复制 pre-push hook
cp "$HOOKS_SOURCE/pre-push" "$HOOKS_DIR/pre-push"
chmod +x "$HOOKS_DIR/pre-push"
echo "✓ 安装 pre-push hook"

echo "======================================"
echo "Git Hooks 安装完成"
echo "======================================"
echo ""
echo "配置说明:"
echo "  - pre-commit: 只运行相关测试（超时 30 秒）"
echo "  - pre-push:   运行完整测试（超时 5 分钟）"
echo ""
echo "跳过 hook:"
echo "  git commit --no-verify -m \"message\""
echo "  git push --no-verify"
echo ""
