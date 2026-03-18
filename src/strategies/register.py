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

# 打板策略
from .enhanced_dragon_head import EnhancedDragonHeadStrategy
from .first_limit import FirstLimitStrategy
from .one_to_two import OneToTwoStrategy

StrategyFactory.register_strategy("enhanced_dragon_head", EnhancedDragonHeadStrategy)
StrategyFactory.register_strategy("first_limit", FirstLimitStrategy)
StrategyFactory.register_strategy("one_to_two", OneToTwoStrategy)

# 预留其他策略注册位置
# from strategies.multi_factor import MultiFactorStrategy
# from strategies.statistical_arbitrage import StatisticalArbitrageStrategy
# StrategyFactory.register_strategy("multi_factor", MultiFactorStrategy)
# StrategyFactory.register_strategy("statistical_arbitrage", StatisticalArbitrageStrategy)

# 自动加载插件
try:
    from .plugin_loader import PluginLoader

    _plugin_loader = PluginLoader()
    _plugin_loader.load_all_plugins()
except ImportError:
    pass  # 插件加载器不可用时静默处理
