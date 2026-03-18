"""
打板策略版本控制
Strategy version management for 打板 strategies
"""

from typing import Dict, Any
from datetime import datetime


class DabanStrategyVersion:
    """打板策略版本管理"""

    # 版本历史记录
    VERSION_HISTORY = {
        "1.0.0": {
            "release_date": "2026-03-17",
            "description": "初始版本 - 基于研究的首板、一进二策略实现",
            "features": [
                "首板策略: 识别首次涨停股票",
                "一进二策略: 连板延续交易机会",
                "龙头股策略: 板块龙头识别",
                "时段退出: 11:28止盈, 14:50强制平仓",
                "风险控制: -3%止损, 单股10%仓位限制",
            ],
            "parameters": {
                "first_limit": {
                    "limit_up_threshold": 0.095,
                    "limit_up_threshold_20": 0.195,
                    "min_volume_ratio": 3.0,
                    "max_volume_ratio": 15.0,
                    "min_turnover_amount": 1e4,
                    "min_market_cap": 1e5,
                    "max_market_cap": 1e8,
                    "min_price": 2.0,
                    "max_price": 50.0,
                    "first_limit_days": 180,
                    "top_n_stocks": 8,
                    "stop_loss_pct": -0.03,
                    "take_profit_pct": 0.015,
                },
                "one_to_two": {
                    "limit_up_threshold": 0.095,
                    "limit_up_threshold_20": 0.195,
                    "gap_open_min": 0.01,
                    "gap_open_max": 0.05,
                    "min_volume_ratio": 2.0,
                    "min_turnover_amount": 1e4,
                    "min_market_cap": 1e5,
                    "max_market_cap": 1e8,
                    "min_price": 2.0,
                    "max_price": 50.0,
                    "lookback_days": 30,
                    "top_n_stocks": 6,
                    "stop_loss_pct": -0.03,
                    "take_profit_pct": 0.025,
                },
            },
            "expected_performance": {
                "annual_return": "200-300%",
                "win_rate": "45-50%",
                "max_drawdown": "25-30%",
                "sharpe_ratio": "3.5-5.0",
            },
        }
    }

    CURRENT_VERSION = "1.0.0"

    @classmethod
    def get_current_version(cls) -> str:
        """获取当前版本"""
        return cls.CURRENT_VERSION

    @classmethod
    def get_version_info(cls, version: str = None) -> Dict[str, Any]:
        """获取版本信息"""
        if version is None:
            version = cls.CURRENT_VERSION

        if version not in cls.VERSION_HISTORY:
            raise ValueError(f"Version {version} not found")

        return cls.VERSION_HISTORY[version]

    @classmethod
    def get_current_parameters(cls) -> Dict[str, Any]:
        """获取当前版本参数"""
        return cls.get_version_info()["parameters"]

    @classmethod
    def get_expected_performance(cls) -> Dict[str, Any]:
        """获取预期性能指标"""
        return cls.get_version_info()["expected_performance"]

    @classmethod
    def log_version_usage(cls, strategy_name: str, version: str = None) -> None:
        """记录版本使用日志"""
        if version is None:
            version = cls.CURRENT_VERSION

        print(
            f"[VERSION] Strategy: {strategy_name}, Version: {version}, Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )


# 版本常量导出
DABAN_STRATEGY_VERSION = DabanStrategyVersion.CURRENT_VERSION
DABAN_STRATEGY_PARAMETERS = DabanStrategyVersion.get_current_parameters()
