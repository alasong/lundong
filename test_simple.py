#!/usr/bin/env python
"""
测试脚本 - 验证简化后的系统
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from loguru import logger
from config import settings

def test_config():
    """测试配置加载"""
    print("=" * 50)
    print("测试配置加载")
    print("=" * 50)
    
    logger.info(f"Tushare Token: {'已配置' if settings.tushare_token else '未配置'}")
    logger.info(f"数据库：{settings.database_url}")
    logger.info(f"日志级别：{settings.log_level}")
    
    if not settings.tushare_token:
        print("⚠️  警告：TUSHARE_TOKEN 未配置")
        print("   请在.env 文件中设置 TUSHARE_TOKEN")
        return False
    
    return True


def test_data_agent():
    """测试数据采集 Agent"""
    print("\n" + "=" * 50)
    print("测试数据采集 Agent")
    print("=" * 50)
    
    from agents.data_agent import DataAgent
    
    try:
        agent = DataAgent()
        logger.info("DataAgent 初始化成功")
        return True
    except Exception as e:
        logger.error(f"DataAgent 初始化失败：{e}")
        return False


def test_analysis_agent():
    """测试分析 Agent"""
    print("\n" + "=" * 50)
    print("测试分析 Agent")
    print("=" * 50)
    
    from agents.analysis_agent import AnalysisAgent
    
    try:
        agent = AnalysisAgent()
        logger.info("AnalysisAgent 初始化成功")
        return True
    except Exception as e:
        logger.error(f"AnalysisAgent 初始化失败：{e}")
        return False


def test_predict_agent():
    """测试预测 Agent"""
    print("\n" + "=" * 50)
    print("测试预测 Agent")
    print("=" * 50)
    
    from agents.predict_agent import PredictAgent
    
    try:
        agent = PredictAgent()
        logger.info("PredictAgent 初始化成功")
        return True
    except Exception as e:
        logger.error(f"PredictAgent 初始化失败：{e}")
        return False


def test_runner():
    """测试运行器"""
    print("\n" + "=" * 50)
    print("测试运行器")
    print("=" * 50)
    
    from runner import SimpleRunner
    
    try:
        runner = SimpleRunner()
        logger.info("SimpleRunner 初始化成功")
        return True
    except Exception as e:
        logger.error(f"SimpleRunner 初始化失败：{e}")
        return False


def main():
    """主测试函数"""
    print("\n" + "=" * 60)
    print("A 股热点轮动预测系统 - 简化版测试")
    print("=" * 60)
    
    results = {
        "配置加载": test_config(),
        "DataAgent": test_data_agent(),
        "AnalysisAgent": test_analysis_agent(),
        "PredictAgent": test_predict_agent(),
        "SimpleRunner": test_runner()
    }
    
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    for name, passed in results.items():
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"{name}: {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("所有测试通过！系统已就绪。")
        print("\n下一步:")
        print("1. 确保.env 文件中配置了 TUSHARE_TOKEN")
        print("2. 运行：python src/main.py --mode data  (采集基础数据)")
        print("3. 运行：python src/main.py --mode daily --train  (每日工作流)")
    else:
        print("部分测试失败，请检查配置和依赖。")
    print("=" * 60 + "\n")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
