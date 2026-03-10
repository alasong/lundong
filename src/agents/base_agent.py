"""
Agent基类
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from loguru import logger
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class BaseAgent(ABC):
    """Agent基类"""

    def __init__(self, name: str):
        self.name = name
        self.status = "idle"
        self.last_run_time = None
        self.error_count = 0
        self.max_errors = 3

    @abstractmethod
    def run(self, *args, **kwargs) -> Dict[str, Any]:
        """执行Agent任务"""
        pass

    def execute(self, *args, **kwargs) -> Dict[str, Any]:
        """
        执行Agent任务（带错误处理）
        """
        self.status = "running"
        self.last_run_time = datetime.now()

        try:
            result = self.run(*args, **kwargs)
            self.status = "success"
            self.error_count = 0
            return {
                "success": True,
                "agent": self.name,
                "timestamp": self.last_run_time.isoformat(),
                "result": result
            }

        except Exception as e:
            self.status = "error"
            self.error_count += 1
            logger.error(f"Agent {self.name} 执行失败: {e}")

            return {
                "success": False,
                "agent": self.name,
                "timestamp": self.last_run_time.isoformat(),
                "error": str(e)
            }

    def get_status(self) -> Dict[str, Any]:
        """获取Agent状态"""
        return {
            "name": self.name,
            "status": self.status,
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None,
            "error_count": self.error_count
        }

    def reset(self):
        """重置Agent状态"""
        self.status = "idle"
        self.error_count = 0
        logger.info(f"Agent {self.name} 已重置")


class AgentResult:
    """Agent执行结果"""

    def __init__(
        self,
        success: bool,
        data: Any = None,
        message: str = "",
        metadata: Optional[Dict] = None
    ):
        self.success = success
        self.data = data
        self.message = message
        self.metadata = metadata or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "success": self.success,
            "data": self.data,
            "message": self.message,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat()
        }
