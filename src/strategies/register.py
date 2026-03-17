"""
策略注册
在模块加载时自动注册所有策略
"""

from .strategy_factory import StrategyFactory

# 核心策略
from .hot_rotation import HotRotationStrategy
from .momentum import MomentumStrategy

# 均值回归策略
from .mean_reversion import MeanReversionStrategy

# 价值策略
from .value import ValueStrategy

# 成长策略
from .growth import GrowthStrategy

# 事件驱动策略
from .event_driven import EventDrivenStrategy

# 资金流策略
from .capital_flow import CapitalFlowStrategy

# 质量策略
from .quality import QualityStrategy

# 小市值策略
from .small_cap import SmallCapStrategy

# 行业轮动策略
from .sector_rotation import SectorRotationStrategy

# 高股息策略
from .dividend import DividendStrategy

# 注册所有策略
StrategyFactory.register_strategy("hot_rotation", HotRotationStrategy)
StrategyFactory.register_strategy("momentum", MomentumStrategy)
StrategyFactory.register_strategy("mean_reversion", MeanReversionStrategy)
StrategyFactory.register_strategy("value", ValueStrategy)
StrategyFactory.register_strategy("growth", GrowthStrategy)
StrategyFactory.register_strategy("event_driven", EventDrivenStrategy)
StrategyFactory.register_strategy("capital_flow", CapitalFlowStrategy)
StrategyFactory.register_strategy("quality", QualityStrategy)
StrategyFactory.register_strategy("small_cap", SmallCapStrategy)
StrategyFactory.register_strategy("sector_rotation", SectorRotationStrategy)
StrategyFactory.register_strategy("dividend", DividendStrategy)

# 预留其他策略注册位置
# from strategies.multi_factor import MultiFactorStrategy
# from strategies.statistical_arbitrage import StatisticalArbitrageStrategy
# StrategyFactory.register_strategy("multi_factor", MultiFactorStrategy)
# StrategyFactory.register_strategy("statistical_arbitrage", StatisticalArbitrageStrategy)
