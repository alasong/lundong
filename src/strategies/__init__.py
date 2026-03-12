"""
策略库模块
包含多种交易策略实现
"""
from .mean_reversion import MeanReversionStrategy
from .momentum import MomentumStrategy
from .event_driven import EventDrivenStrategy
from .statistical_arbitrage import PairsTradingStrategy
from .multi_factor import MultiFactorStrategy
from .capital_flow import CapitalFlowStrategy

__all__ = [
    'MeanReversionStrategy',
    'MomentumStrategy',
    'EventDrivenStrategy',
    'PairsTradingStrategy',
    'MultiFactorStrategy',      # 多因子选股策略
    'CapitalFlowStrategy',      # 资金流策略
]
