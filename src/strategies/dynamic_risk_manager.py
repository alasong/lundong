"""
Dynamic Risk Manager for Daban Strategy
动态风险控制管理器，提供实时止损、仓位管理和风险监控
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger


class DynamicRiskManager:
    """动态风险控制管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化风险控制管理器
        
        Args:
            config: 风险控制配置
                {
                    "base_stop_loss": -0.03,      # 基础止损
                    "base_take_profit": 0.02,     # 基础止盈
                    "max_position_per_stock": 0.10,  # 单股最大仓位
                    "max_total_position": 0.50,   # 总仓位上限
                    "volatility_multiplier": 1.5,  # 波动率倍数
                    "market_risk_factor": 0.8,    # 市场风险因子
                    "dynamic_adjustment": True,    # 是否动态调整
                }
        """
        self.config = config or self._get_default_config()
        self.market_state = {}
        self.position_tracker = {}
        logger.info("动态风险控制管理器初始化完成")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "base_stop_loss": -0.03,
            "base_take_profit": 0.02,
            "max_position_per_stock": 0.10,
            "max_total_position": 0.50,
            "volatility_multiplier": 1.5,
            "market_risk_factor": 0.8,
            "dynamic_adjustment": True,
            "time_based_exit": {
                "morning_take_profit_time": "11:28",
                "afternoon_force_exit_time": "14:50",
            },
            "emergency_conditions": {
                "max_daily_loss": -0.05,
                "max_consecutive_losses": 3,
                "market_volatility_threshold": 0.08,
            }
        }
    
    def update_market_state(self, market_data: Dict[str, Any]) -> None:
        """
        更新市场状态
        
        Args:
            market_data: 市场数据
                {
                    "market_phase": "启动期/高潮期/衰退期/冰点期",
                    "volatility_index": 22.5,
                    "advance_decline_ratio": 1.2,
                    "fear_greed_index": 55,
                    "market_return": 0.015,
                }
        """
        self.market_state = market_data
        logger.info(f"更新市场状态: {market_data.get('market_phase', 'unknown')}")
    
    def calculate_dynamic_stop_loss(self, stock_data: Dict[str, Any]) -> float:
        """
        计算动态止损
        
        Args:
            stock_data: 股票数据
            
        Returns:
            动态止损比例
        """
        base_stop_loss = self.config["base_stop_loss"]
        
        if not self.config["dynamic_adjustment"]:
            return base_stop_loss
        
        # 获取股票波动率
        volatility = stock_data.get("volatility", 0.03)
        
        # 获取市场风险因子
        market_risk = self._get_market_risk_factor()
        
        # 计算动态止损
        dynamic_stop_loss = base_stop_loss * (
            1 + 
            (volatility / 0.03 - 1) * 0.5 +  # 波动率调整
            (market_risk - 1) * 0.3          # 市场风险调整
        )
        
        # 限制止损范围
        min_stop_loss = -0.06  # 最大-6%
        max_stop_loss = -0.01  # 最小-1%
        
        return max(min_stop_loss, min(max_stop_loss, dynamic_stop_loss))
    
    def calculate_dynamic_take_profit(self, stock_data: Dict[str, Any]) -> float:
        """
        计算动态止盈
        
        Args:
            stock_data: 股票数据
            
        Returns:
            动态止盈比例
        """
        base_take_profit = self.config["base_take_profit"]
        
        if not self.config["dynamic_adjustment"]:
            return base_take_profit
        
        # 获取市场阶段
        market_phase = self.market_state.get("market_phase", "normal")
        
        # 市场阶段调整因子
        phase_factors = {
            "高潮期": 1.2,
            "启动期": 1.0,
            "衰退期": 0.8,
            "冰点期": 0.6,
            "normal": 1.0,
        }
        
        phase_factor = phase_factors.get(market_phase, 1.0)
        
        # 计算动态止盈
        dynamic_take_profit = base_take_profit * phase_factor
        
        # 限制止盈范围
        min_take_profit = 0.005  # 最小0.5%
        max_take_profit = 0.05   # 最大5%
        
        return max(min_take_profit, min(max_take_profit, dynamic_take_profit))
    
    def calculate_position_size(self, signal_score: float, stock_data: Dict[str, Any]) -> float:
        """
        计算仓位大小
        
        Args:
            signal_score: 信号评分 (0-100)
            stock_data: 股票数据
            
        Returns:
            仓位比例 (0-1)
        """
        base_position = self.config["max_position_per_stock"]
        
        # 基于信号评分调整
        score_factor = signal_score / 100.0
        
        # 基于波动率调整（高波动低仓位）
        volatility = stock_data.get("volatility", 0.03)
        volatility_factor = max(0.5, 1.0 - (volatility / 0.03 - 1) * 0.3)
        
        # 基于市场风险调整
        market_risk_factor = self._get_market_risk_factor()
        market_factor = max(0.5, 2.0 - market_risk_factor)
        
        # 计算最终仓位
        position_size = base_position * score_factor * volatility_factor * market_factor
        
        # 确保不超过最大仓位限制
        return min(position_size, self.config["max_position_per_stock"])
    
    def _get_market_risk_factor(self) -> float:
        """获取市场风险因子"""
        market_phase = self.market_state.get("market_phase", "normal")
        volatility_index = self.market_state.get("volatility_index", 20.0)
        fear_greed_index = self.market_state.get("fear_greed_index", 50)
        advance_decline_ratio = self.market_state.get("advance_decline_ratio", 1.0)
        
        # 市场阶段风险因子
        phase_risk = {
            "高潮期": 1.2,
            "启动期": 1.0,
            "衰退期": 1.3,
            "冰点期": 1.5,
            "normal": 1.0,
        }.get(market_phase, 1.0)
        
        # 波动率风险因子
        volatility_risk = 1.0 + (volatility_index - 20.0) / 20.0
        
        # 恐惧贪婪指数风险因子
        fear_greed_risk = 1.0 + (50.0 - fear_greed_index) / 50.0
        
        # 涨跌家数比风险因子
        adv_dec_risk = 1.0 + (1.0 - advance_decline_ratio) * 0.5
        
        # 综合风险因子
        market_risk = (
            phase_risk * 0.4 +
            volatility_risk * 0.3 +
            fear_greed_risk * 0.2 +
            adv_dec_risk * 0.1
        )
        
        return market_risk
    
    def check_emergency_conditions(self, current_pnl: float, consecutive_losses: int) -> bool:
        """
        检查紧急情况
        
        Args:
            current_pnl: 当前盈亏
            consecutive_losses: 连续亏损次数
            
        Returns:
            是否触发紧急条件
        """
        # 检查当日最大亏损
        if current_pnl <= self.config["emergency_conditions"]["max_daily_loss"]:
            logger.warning("触发当日最大亏损紧急条件")
            return True
        
        # 检查连续亏损
        if consecutive_losses >= self.config["emergency_conditions"]["max_consecutive_losses"]:
            logger.warning("触发连续亏损紧急条件")
            return True
        
        # 检查市场波动率
        market_volatility = self.market_state.get("volatility_index", 20.0) / 100.0
        if market_volatility >= self.config["emergency_conditions"]["market_volatility_threshold"]:
            logger.warning("触发市场高波动紧急条件")
            return True
        
        return False
    
    def get_time_based_exit_signal(self, current_time: str) -> str:
        """
        获取基于时间的退出信号
        
        Args:
            current_time: 当前时间 "HH:MM"
            
        Returns:
            退出信号 ("none", "partial_take_profit", "force_exit")
        """
        morning_time = self.config["time_based_exit"]["morning_take_profit_time"]
        afternoon_time = self.config["time_based_exit"]["afternoon_force_exit_time"]
        
        if current_time >= afternoon_time:
            return "force_exit"
        elif current_time >= morning_time:
            return "partial_take_profit"
        else:
            return "none"
    
    def apply_risk_management_to_signals(
        self, 
        signals: List[Dict[str, Any]], 
        current_time: str = None
    ) -> List[Dict[str, Any]]:
        """
        对信号应用风险管理
        
        Args:
            signals: 原始信号列表
            current_time: 当前时间 "HH:MM"
            
        Returns:
            应用风险管理后的信号列表
        """
        managed_signals = []
        
        for signal in signals:
            ts_code = signal.get("ts_code", "")
            score = signal.get("comprehensive_score", 50.0)
            
            # 创建股票数据字典
            stock_data = {
                "volatility": signal.get("features", {}).get("volatility", 0.03),
                "ts_code": ts_code,
            }
            
            # 计算动态止损止盈
            stop_loss = self.calculate_dynamic_stop_loss(stock_data)
            take_profit = self.calculate_dynamic_take_profit(stock_data)
            
            # 计算仓位大小
            position_size = self.calculate_position_size(score, stock_data)
            
            # 应用时间退出规则
            time_exit = "none"
            if current_time:
                time_exit = self.get_time_based_exit_signal(current_time)
            
            # 构建管理后的信号
            managed_signal = {
                **signal,
                "risk_managed": True,
                "dynamic_stop_loss": stop_loss,
                "dynamic_take_profit": take_profit,
                "position_size": position_size,
                "time_exit_signal": time_exit,
                "risk_level": self._calculate_risk_level(signal, stock_data),
            }
            
            managed_signals.append(managed_signal)
        
        logger.info(f"对 {len(signals)} 个信号应用风险管理")
        return managed_signals
    
    def _calculate_risk_level(self, signal: Dict[str, Any], stock_data: Dict[str, Any]) -> str:
        """计算风险等级"""
        stop_loss = self.calculate_dynamic_stop_loss(stock_data)
        market_risk = self._get_market_risk_factor()
        score = signal.get("comprehensive_score", 50.0)
        
        if stop_loss < -0.04 and market_risk > 1.3 and score < 60:
            return "high"
        elif stop_loss < -0.03 and market_risk > 1.1 and score < 70:
            return "medium"
        else:
            return "low"


# 便捷函数
def apply_dynamic_risk_management(
    signals: List[Dict[str, Any]],
    market_data: Dict[str, Any],
    current_time: str = None,
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    应用动态风险管理的便捷函数
    
    Args:
        signals: 信号列表
        market_data: 市场数据
        current_time: 当前时间
        config: 风险控制配置
        
    Returns:
        管理后的信号列表
    """
    risk_manager = DynamicRiskManager(config)
    risk_manager.update_market_state(market_data)
    return risk_manager.apply_risk_management_to_signals(signals, current_time)