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
    "enhanced_dragon_head": {
        "enabled": False,  # 默认禁用，需要手动启用
        "weight": 0.3,
        "params": {
            "strategy_type": "first_limit",  # first_limit, one_to_two, leader
            "limit_up_threshold": 0.095,
            "limit_up_threshold_20": 0.195,
            "min_volume_ratio": 3.0,
            "max_volume_ratio": 15.0,
            "min_turnover_amount": 5e8,
            "max_turnover_amount": 2e10,
            "min_market_cap": 7e9,
            "max_market_cap": 5.2e10,
            "min_price": 2.0,
            "max_price": 50.0,
            "first_limit_days": 180,
            "min_consecutive_limits": 1,
            "max_consecutive_limits": 5,
            "gap_open_min": 0.01,
            "gap_open_max": 0.06,
            "call_auction_volume_min": 0.03,
            "min_seal_order_ratio": 0.5,
            "min_macd_hist": 0.0,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "volume_weight": 0.25,
            "momentum_weight": 0.25,
            "sector_weight": 0.20,
            "fundamental_weight": 0.15,
            "technical_weight": 0.15,
            "top_n_stocks": 10,
            "max_position_per_stock": 0.10,
            "stop_loss_pct": -0.03,
            "take_profit_pct": 0.015,
        },
    },
    "first_limit": {
        "enabled": False,  # 默认禁用，需要手动启用
        "weight": 0.25,
        "params": {
            "limit_up_threshold": 0.095,
            "limit_up_threshold_20": 0.195,
            "min_volume_ratio": 3.0,
            "max_volume_ratio": 15.0,
            "min_turnover_amount": 1e4,  # 10K realistic for database
            "min_market_cap": 1e5,  # 100K realistic for database
            "max_market_cap": 1e8,  # 100M realistic for database
            "min_price": 2.0,
            "max_price": 50.0,
            "first_limit_days": 180,
            "top_n_stocks": 8,
            "stop_loss_pct": -0.03,
            "take_profit_pct": 0.015,
        },
    },
    "one_to_two": {
        "enabled": False,  # 默认禁用，需要手动启用
        "weight": 0.25,
        "params": {
            "limit_up_threshold": 0.095,
            "limit_up_threshold_20": 0.195,
            "gap_open_min": 0.01,
            "gap_open_max": 0.05,
            "min_volume_ratio": 2.0,
            "min_turnover_amount": 1e4,  # 10K realistic for database
            "min_market_cap": 1e5,  # 100K realistic for database
            "max_market_cap": 1e8,  # 100M realistic for database
            "min_price": 2.0,
            "max_price": 50.0,
            "lookback_days": 30,
            "top_n_stocks": 6,
            "stop_loss_pct": -0.03,
            "take_profit_pct": 0.025,
        },
    },
}

# 多策略组合配置
multi_strategy_config = {
    "enabled": True,
    "combination_method": "weighted_score",  # weighted_score / voting
    "strategy_weights": {
        "hot_rotation": 0.5,
        "momentum": 0.3,
        "enhanced_dragon_head": 0.2,  # 打板策略权重
    },
    "portfolio_params": {
        "top_n_stocks": 10,
        "max_position": 0.10,
        "max_sector": 0.25,
    },
}
