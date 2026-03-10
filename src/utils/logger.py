"""
日志工具模块
"""
import sys
from loguru import logger as _logger


def get_logger(name: str = None):
    """获取logger 实例
    
    Args:
        name: logger 名称
        
    Returns:
        logger 实例
    """
    return _logger


# 配置日志格式
_logger.remove()
_logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)

