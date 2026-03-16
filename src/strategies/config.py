"""
多策略配置
"""

# 策略配置
strategies_config = {
    "hot_rotation": {
        "enabled": True,
        "weight": 0.6,  # 策略权重
        "params": {
            "top_n_concepts": 10,
            "min_hotspot_score": 60,
            "stocks_per_concept": 5,
            "use_prediction": True,
        },
    },
    "momentum": {
        "enabled": True,
        "weight": 0.4,
        "params": {
            "momentum_window": 20,
            "volume_window": 20,
            "min_momentum": 0.05,
            "min_volume_ratio": 1.5,
            "top_n_stocks": 20,
        },
    },
}

# 多策略组合配置
multi_strategy_config = {
    "enabled": True,
    "combination_method": "weighted_score",  # weighted_score / voting
    "strategy_weights": {
        "hot_rotation": 0.6,
        "momentum": 0.4,
    },
    "portfolio_params": {
        "top_n_stocks": 10,
        "max_position": 0.10,
        "max_sector": 0.25,
    },
}
