"""
多策略框架
支持灵活组合和扩展不同的交易策略
"""

from .base_strategy import BaseStrategy, StrategySignal
from .strategy_factory import StrategyFactory
from .multi_strategy import MultiStrategyPortfolio
from .hot_rotation import HotRotationStrategy
from .momentum import MomentumStrategy
from .mean_reversion import MeanReversionStrategy
from .value import ValueStrategy
from .growth import GrowthStrategy
from .event_driven import EventDrivenStrategy
from .capital_flow import CapitalFlowStrategy
from .quality import QualityStrategy
from .small_cap import SmallCapStrategy

__all__ = [
    "BaseStrategy",
    "StrategySignal",
    "StrategyFactory",
    "MultiStrategyPortfolio",
    "HotRotationStrategy",
    "MomentumStrategy",
    "MeanReversionStrategy",
    "ValueStrategy",
    "GrowthStrategy",
    "EventDrivenStrategy",
    "CapitalFlowStrategy",
    "QualityStrategy",
    "SmallCapStrategy",
]
