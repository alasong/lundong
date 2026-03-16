"""
多策略框架
支持灵活组合和扩展不同的交易策略
"""

from strategies.base_strategy import BaseStrategy, StrategySignal
from strategies.strategy_factory import StrategyFactory
from strategies.multi_strategy import MultiStrategyPortfolio
from strategies.hot_rotation import HotRotationStrategy
from strategies.momentum import MomentumStrategy
from strategies.mean_reversion import MeanReversionStrategy
from strategies.value import ValueStrategy
from strategies.growth import GrowthStrategy
from strategies.event_driven import EventDrivenStrategy
from strategies.capital_flow import CapitalFlowStrategy
from strategies.quality import QualityStrategy
from strategies.small_cap import SmallCapStrategy

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
