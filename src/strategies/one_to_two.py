"""
One-to-Two (一进二) Strategy
Identifies continuation plays after first limit-up
Based on research findings with proper entry/exit rules
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class OneToTwoStrategy(BaseStrategy):
    """One-to-Two Strategy - Continuation plays after first limit-up"""

    def __init__(self, name: str = "one_to_two", params: Optional[Dict] = None):
        default_params = {
            # Core parameters from research
            "limit_up_threshold": 0.095,      # 9.5% for main board
            "limit_up_threshold_20": 0.195,   # 19.5% for STAR/ChiNext
            "gap_open_min": 0.01,            # Minimum 1% gap open
            "gap_open_max": 0.06,            # Maximum 6% gap open
            "min_volume_ratio": 2.0,         # Minimum 2x volume ratio vs previous day
            "min_turnover_amount": 1e4,      # 10K minimum turnover (realistic for database)
            "min_market_cap": 1e5,           # 100K minimum market cap (realistic)
            "max_market_cap": 1e8,           # 100M maximum market cap (realistic)
            "min_price": 2.0,                # 2 RMB minimum price
            "max_price": 50.0,               # 50 RMB maximum price
            "lookback_days": 30,             # Look back period for analysis
            "top_n_stocks": 10,              # Select top 10 stocks
            "stop_loss_pct": -0.03,          # -3% stop loss
            "take_profit_pct": 0.02,         # +2% take profit (higher than first limit)
        }
        default_params.update(params or {})
        
        super().__init__(name, default_params)
        self.db = None

    def _init_db(self):
        """Lazy load database connection"""
        if self.db is None:
            from data.database import get_database
            self.db = get_database()

    def get_required_data(self) -> Dict[str, Any]:
        """Required data for one-to-two strategy"""
        return {
            "concept_data": False,
            "stock_data": True,
            "history_days": 30,
            "features": ["close", "high", "low", "open", "vol", "amount", "pct_chg", "total_mv"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """Generate one-to-two signals with optimized performance"""
        logger.info("One-to-Two Strategy: Generating signals...")
        self._init_db()
        
        # Get latest date and previous date
        latest_date = self.db.get_latest_date()
        if not latest_date:
            logger.warning("Cannot get latest date")
            return []
        
        # Get dates for yesterday and day before yesterday
        prev_date = self._get_previous_trading_date(latest_date)
        if not prev_date:
            logger.warning("Cannot get previous trading date")
            return []
        
        prev_prev_date = self._get_previous_trading_date(prev_date)
        if not prev_prev_date:
            logger.warning("Cannot get day before previous trading date")
            return []
        
        # Get stock data for latest date (today)
        today_data = self.db.get_all_stock_data(latest_date)
        if today_data.empty:
            logger.warning("No today stock data available")
            return []
        
        # Get stock data for previous date (yesterday)
        yesterday_data = self.db.get_all_stock_data(prev_date)
        if yesterday_data.empty:
            logger.warning("No yesterday stock data available")
            return []
        
        # Get stock data for day before yesterday
        prev_prev_data = self.db.get_all_stock_data(prev_prev_date)
        if prev_prev_data.empty:
            logger.warning("No day before yesterday stock data available")
            return []
        
        # Create dictionaries for fast lookup
        today_dict = {row['ts_code']: row for _, row in today_data.iterrows()}
        yesterday_dict = {row['ts_code']: row for _, row in yesterday_data.iterrows()}
        prev_prev_dict = {row['ts_code']: row for _, row in prev_prev_data.iterrows()}
        
        # Find candidates that were first limit-up yesterday
        candidates = []
        for ts_code in yesterday_dict.keys():
            if ts_code not in today_dict:
                continue
                
            yesterday_row = yesterday_dict[ts_code]
            today_row = today_dict[ts_code]
            
            # Check if this was a first limit-up yesterday
            if self._was_first_limit_yesterday(ts_code, yesterday_row, prev_prev_dict, prev_prev_date):
                # Check if today shows continuation potential
                if self._has_continuation_potential(today_row, yesterday_row):
                    candidates.append((ts_code, today_row, yesterday_row))
        
        logger.info(f"Found {len(candidates)} one-to-two candidates")
        
        # Generate signals for candidates
        signals = []
        for ts_code, today_row, yesterday_row in candidates:
            score = self._calculate_score(today_row, yesterday_row)
            if score >= 65:  # Higher threshold for 1→2
                signal = self._create_signal(ts_code, today_row, yesterday_row, score)
                signals.append(signal)
        
        # Sort by score and limit to top N
        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[:self.params["top_n_stocks"]]
        self.signals = signals
        
        logger.info(f"One-to-Two Strategy: Generated {len(signals)} signals")
        return signals

    def _was_first_limit_yesterday(self, ts_code: str, yesterday_row: pd.Series, 
                                   prev_prev_dict: Dict, prev_prev_date: str) -> bool:
        """Check if stock was first limit-up yesterday"""
        # Get limit threshold based on stock type
        if str(ts_code).startswith(("688", "300")):
            limit_threshold = self.params["limit_up_threshold_20"]
        else:
            limit_threshold = self.params["limit_up_threshold"]
        
        # Check if yesterday was at or near limit-up
        pct_chg_yesterday = float(yesterday_row.get("pct_chg", 0))
        if pct_chg_yesterday < limit_threshold * 0.9:
            return False
        
        # Check if day before yesterday was NOT at limit-up (first limit condition)
        if ts_code in prev_prev_dict:
            pct_chg_prev_prev = float(prev_prev_dict[ts_code].get("pct_chg", 0))
            if pct_chg_prev_prev >= limit_threshold * 0.9:
                return False  # Was already at limit-up
        
        # Apply basic filters
        if not self._passes_basic_filters(yesterday_row):
            return False
        
        return True

    def _has_continuation_potential(self, today_row: pd.Series, yesterday_row: pd.Series) -> bool:
        """Check if today shows continuation potential"""
        # Calculate gap open
        yesterday_close = float(yesterday_row.get("close", 0))
        today_open = float(today_row.get("open", 0))
        
        if yesterday_close == 0:
            return False
        
        gap_open = (today_open - yesterday_close) / yesterday_close
        
        # Check gap open range (1-6% from research)
        if not (self.params["gap_open_min"] <= gap_open <= self.params["gap_open_max"]):
            return False
        
        # Check volume ratio vs yesterday
        today_volume = float(today_row.get("vol", 0))
        yesterday_volume = float(yesterday_row.get("vol", 0))
        
        if yesterday_volume == 0:
            volume_ratio = 1.0
        else:
            volume_ratio = today_volume / yesterday_volume
        
        if volume_ratio < self.params["min_volume_ratio"]:
            return False
        
        # Check today's price action (should be strong)
        today_pct_chg = float(today_row.get("pct_chg", 0))
        if today_pct_chg < 2.0:  # At least 2% up today
            return False
        
        return True

    def _passes_basic_filters(self, row: pd.Series) -> bool:
        """Apply basic filters to stock data"""
        # Market cap filter
        market_cap = float(row.get("total_mv", 0))
        if not (self.params["min_market_cap"] <= market_cap <= self.params["max_market_cap"]):
            return False
        
        # Price filter
        price = float(row.get("close", 0))
        if not (self.params["min_price"] <= price <= self.params["max_price"]):
            return False
        
        # Turnover filter
        turnover = float(row.get("amount", 0))
        if turnover < self.params["min_turnover_amount"]:
            return False
        
        return True

    def _calculate_score(self, today_row: pd.Series, yesterday_row: pd.Series) -> float:
        """Calculate comprehensive score for one-to-two candidate"""
        # Gap open score (0-100)
        yesterday_close = float(yesterday_row.get("close", 0))
        today_open = float(today_row.get("open", 0))
        if yesterday_close > 0:
            gap_open = (today_open - yesterday_close) / yesterday_close
            gap_score = min(100, max(0, (gap_open - self.params["gap_open_min"]) * 100))
        else:
            gap_score = 0
        
        # Volume ratio score (0-100)
        today_volume = float(today_row.get("vol", 0))
        yesterday_volume = float(yesterday_row.get("vol", 0))
        if yesterday_volume > 0:
            volume_ratio = today_volume / yesterday_volume
            volume_score = min(100, max(0, (volume_ratio - 1) * 33.3))
        else:
            volume_score = 0
        
        # Today momentum score (0-100)
        today_pct_chg = float(today_row.get("pct_chg", 0))
        momentum_score = min(100, max(0, today_pct_chg * 10))
        
        # Yesterday strength score (0-100)
        yesterday_pct_chg = float(yesterday_row.get("pct_chg", 0))
        yesterday_score = min(100, max(0, yesterday_pct_chg * 10))
        
        # Weighted composite score
        composite_score = (
            gap_score * 0.3 +
            volume_score * 0.25 +
            momentum_score * 0.25 +
            yesterday_score * 0.2
        )
        
        return min(100, max(0, composite_score))

    def _create_signal(self, ts_code: str, today_row: pd.Series, yesterday_row: pd.Series, score: float) -> StrategySignal:
        """Create strategy signal"""
        stock_name = today_row.get("name", ts_code)
        today_pct_chg = float(today_row.get("pct_chg", 0))
        yesterday_pct_chg = float(yesterday_row.get("pct_chg", 0))
        
        # Calculate gap open for reason
        yesterday_close = float(yesterday_row.get("close", 0))
        today_open = float(today_row.get("open", 0))
        if yesterday_close > 0:
            gap_open = (today_open - yesterday_close) / yesterday_close
        else:
            gap_open = 0
        
        # Determine signal type
        if today_pct_chg >= 5.0:  # Strong continuation
            signal_type = "buy"
        elif today_pct_chg >= 2.0:  # Moderate continuation
            signal_type = "watch"
        else:
            signal_type = "monitor"
        
        return StrategySignal(
            ts_code=ts_code,
            stock_name=stock_name,
            strategy_type="one_to_two",
            signal_type=signal_type,
            weight=min(1.0, score / 100.0),
            score=score,
            reason=f"一进二: 昨日涨幅{yesterday_pct_chg:.1%}, 今日跳空{gap_open:.1%}, 今日涨幅{today_pct_chg:.1%}",
            metadata={
                "yesterday_pct_chg": yesterday_pct_chg,
                "today_pct_chg": today_pct_chg,
                "gap_open": gap_open,
                "market_cap": today_row.get("total_mv", 0),
                "price": today_row.get("close", 0),
                "stop_loss_pct": self.params["stop_loss_pct"],
                "take_profit_pct": self.params["take_profit_pct"],
            },
        )

    def _get_previous_trading_date(self, date_str: str) -> Optional[str]:
        """Get previous trading date (simplified implementation)"""
        from datetime import datetime, timedelta
        
        try:
            date = datetime.strptime(date_str, "%Y%m%d")
            # Go back 1-3 days to account for weekends
            for i in range(1, 4):
                prev_date = date - timedelta(days=i)
                prev_date_str = prev_date.strftime("%Y%m%d")
                # In real implementation, check if it's a trading day
                # For now, just return the previous calendar date
                return prev_date_str
        except ValueError:
            logger.warning(f"Invalid date format: {date_str}")
            return None
        
        return None

    def optimize_portfolio(
        self, signals: List[StrategySignal], **kwargs
    ) -> Dict[str, Any]:
        """Optimize portfolio with risk management"""
        if not signals:
            return {"portfolio": [], "metrics": {}}
        
        portfolio = []
        total_score = sum(sig.score for sig in signals)
        
        for sig in signals:
            weight = min(
                sig.weight,
                0.10  # Max 10% per stock
            )
            normalized_weight = weight * (sig.score / total_score) if total_score > 0 else weight
            
            if normalized_weight > 0:
                portfolio.append({
                    "ts_code": sig.ts_code,
                    "stock_name": sig.stock_name,
                    "weight": normalized_weight,
                    "score": sig.score,
                    "strategy": sig.strategy_type,
                    "stop_loss": sig.metadata.get("stop_loss_pct", -0.03),
                    "take_profit": sig.metadata.get("take_profit_pct", 0.02),
                })
        
        metrics = {
            "num_stocks": len(portfolio),
            "avg_score": sum(p["score"] for p in portfolio) / len(portfolio) if portfolio else 0,
            "total_weight": sum(p["weight"] for p in portfolio),
        }
        
        return {
            "portfolio": portfolio,
            "metrics": metrics,
        }