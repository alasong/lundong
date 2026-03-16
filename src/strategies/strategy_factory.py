"""
策略工厂
根据配置创建策略实例
"""

from typing import Dict, List, Optional, Type
from loguru import logger
from strategies.base_strategy import BaseStrategy


class StrategyFactory:
    """策略工厂 - 创建和管理策略实例"""

    _strategy_registry: Dict[str, Type[BaseStrategy]] = {}

    @classmethod
    def register_strategy(cls, name: str, strategy_class: Type[BaseStrategy]):
        """
        注册策略类

        Args:
            name: 策略名称
            strategy_class: 策略类
        """
        cls._strategy_registry[name] = strategy_class
        logger.info(f"注册策略：{name}")

    @classmethod
    def create_strategy(
        cls, strategy_type: str, params: Optional[Dict] = None
    ) -> BaseStrategy:
        """
        创建策略实例

        Args:
            strategy_type: 策略类型
            params: 策略参数

        Returns:
            策略实例
        """
        if strategy_type not in cls._strategy_registry:
            raise ValueError(
                f"未知策略类型：{strategy_type}. "
                f"可用策略：{list(cls._strategy_registry.keys())}"
            )

        strategy_class = cls._strategy_registry[strategy_type]
        strategy = strategy_class(name=strategy_type, params=params)
        logger.info(f"创建策略实例：{strategy_type}")
        return strategy

    @classmethod
    def create_multiple_strategies(cls, config: Dict[str, Dict]) -> List[BaseStrategy]:
        """
        批量创建策略

        Args:
            config: 策略配置
                {
                    "hot_rotation": {"enabled": True, "params": {...}},
                    "momentum": {"enabled": True, "params": {...}},
                }

        Returns:
            策略实例列表
        """
        strategies = []
        for strategy_type, cfg in config.items():
            if not cfg.get("enabled", True):
                logger.info(f"跳过禁用的策略：{strategy_type}")
                continue

            try:
                strategy = cls.create_strategy(
                    strategy_type=strategy_type, params=cfg.get("params")
                )
                strategies.append(strategy)
            except Exception as e:
                logger.error(f"创建策略 {strategy_type} 失败：{e}")

        logger.info(f"成功创建 {len(strategies)} 个策略")
        return strategies

    @classmethod
    def get_available_strategies(cls) -> List[str]:
        """获取可用策略列表"""
        return list(cls._strategy_registry.keys())
