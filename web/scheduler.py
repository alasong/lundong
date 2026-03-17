"""
定时任务调度器
启动: .venv/bin/python web/scheduler.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from loguru import logger

scheduler = BlockingScheduler()


def job_collect_data():
    """数据采集任务"""
    logger.info(f"[{datetime.now()}] 开始数据采集...")
    try:
        from agents.data_agent import DataAgent

        agent = DataAgent()
        result = agent.execute(task="daily")
        if result.get("success"):
            logger.info(f"数据采集成功: {result.get('result', {})}")
        else:
            logger.error(f"数据采集失败: {result.get('error')}")
    except Exception as e:
        logger.error(f"数据采集异常: {e}")


def job_predict():
    """预测任务"""
    logger.info(f"[{datetime.now()}] 开始预测...")
    try:
        from agents.predict_agent import PredictAgent

        agent = PredictAgent()
        result = agent.execute(task="predict", horizon="all")
        if result.get("success"):
            top_10 = result.get("result", {}).get("top_10", [])
            logger.info(f"预测完成: TOP3 {[p.get('concept_name') for p in top_10[:3]]}")
        else:
            logger.error(f"预测失败: {result.get('error')}")
    except Exception as e:
        logger.error(f"预测异常: {e}")


def job_train():
    """模型训练任务"""
    logger.info(f"[{datetime.now()}] 开始模型训练...")
    try:
        from agents.predict_agent import PredictAgent

        agent = PredictAgent()
        result = agent.execute(task="train")
        if result.get("success"):
            logger.info(f"训练完成: {result.get('result', {})}")
        else:
            logger.error(f"训练失败: {result.get('error')}")
    except Exception as e:
        logger.error(f"训练异常: {e}")


def setup_jobs():
    """配置定时任务"""

    # 每日 09:30 数据采集
    scheduler.add_job(
        job_collect_data,
        CronTrigger(hour=9, minute=30, day_of_week="mon-fri"),
        id="collect_data",
        name="数据采集",
        replace_existing=True,
    )

    # 每日 15:30 预测
    scheduler.add_job(
        job_predict,
        CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
        id="predict",
        name="预测生成",
        replace_existing=True,
    )

    # 每周六 10:00 模型训练
    scheduler.add_job(
        job_train,
        CronTrigger(hour=10, minute=0, day_of_week="sat"),
        id="train",
        name="模型训练",
        replace_existing=True,
    )

    logger.info("定时任务配置完成:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name}: {job.trigger}")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("A股热点轮动预测系统 - 定时任务调度器")
    logger.info("=" * 60)

    setup_jobs()

    logger.info("调度器启动...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器停止")
        scheduler.shutdown()
