"""
策略基类
定义所有策略的统一接口
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
from loguru import logger


@dataclass
class StrategySignal:
    """策略信号 - 统一输出格式"""

    ts_code: str
    stock_name: str
    strategy_type: str
    signal_type: str  # buy/sell/hold
    weight: float  # 建议权重 0-1
    score: float  # 策略评分 0-100
    reason: str  # 信号原因
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "ts_code": self.ts_code,
            "stock_name": self.stock_name,
            "strategy_type": self.strategy_type,
            "signal_type": self.signal_type,
            "weight": self.weight,
            "score": self.score,
            "reason": self.reason,
            "metadata": self.metadata,
        }


class BaseStrategy(ABC):
    """策略基类 - 所有策略必须实现此接口"""

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        """
        初始化策略

        Args:
            name: 策略名称
            params: 策略参数
        """
        self.name = name
        self.params = params or {}
        self.signals: List[StrategySignal] = []
        logger.info(f"策略 {name} 初始化完成，参数：{self.params}")

    @abstractmethod
    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """
        生成交易信号

        Returns:
            策略信号列表
        """
        pass

    @abstractmethod
    def get_required_data(self) -> Dict[str, Any]:
        """
        获取策略所需数据

        Returns:
            数据需求描述
            {
                "concept_data": True,  # 需要板块数据
                "stock_data": True,    # 需要个股数据
                "history_days": 60,    # 需要历史天数
                "features": [...]      # 需要的特征
            }
        """
        pass

    def optimize_portfolio(
        self, signals: List[StrategySignal], **kwargs
    ) -> Dict[str, Any]:
        """
        组合优化（可选实现）

        Args:
            signals: 策略信号
            **kwargs: 其他参数

        Returns:
            优化后的组合
            {
                "portfolio": [...],
                "metrics": {...},
            }
        """
        logger.warning(f"{self.name} 未实现组合优化，使用默认等权组合")
        return self._equal_weight_portfolio(signals)

    def _equal_weight_portfolio(self, signals: List[StrategySignal]) -> Dict[str, Any]:
        """等权组合（默认实现）"""
        if not signals:
            return {"portfolio": [], "metrics": {}}

        n = len(signals)
        weight = 1.0 / n

        portfolio = []
        for sig in signals:
            if sig.signal_type == "buy":
                portfolio.append(
                    {
                        "ts_code": sig.ts_code,
                        "stock_name": sig.stock_name,
                        "weight": weight,
                        "score": sig.score,
                        "strategy": sig.strategy_type,
                    }
                )

        return {
            "portfolio": portfolio,
            "metrics": {
                "num_stocks": n,
                "avg_score": sum(s.score for s in signals) / n if n > 0 else 0,
            },
        }

    def validate_signals(self, signals: List[StrategySignal]) -> List[StrategySignal]:
        """
        验证信号有效性（可选实现）

        默认实现：过滤掉权重<=0 或评分<=0 的信号
        """
        valid = []
        for sig in signals:
            if sig.weight > 0 and sig.score > 0 and sig.signal_type == "buy":
                valid.append(sig)
        return valid

    def get_info(self) -> Dict[str, Any]:
        """获取策略信息"""
        return {
            "name": self.name,
            "params": self.params,
            "num_signals": len(self.signals),
        }
