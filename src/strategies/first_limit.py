"""
First Limit (首板) Strategy
Optimized implementation for identifying first-time limit-up stocks
Based on research findings with proper entry/exit rules
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class FirstLimitStrategy(BaseStrategy):
    """First Limit Strategy - Optimized for performance"""

    def __init__(self, name: str = "first_limit", params: Optional[Dict] = None):
        default_params = {
            # Core parameters from research
            "limit_up_threshold": 0.095,      # 9.5% for main board
            "limit_up_threshold_20": 0.195,   # 19.5% for STAR/ChiNext
            "min_volume_ratio": 3.0,         # Minimum 3x volume ratio
            "max_volume_ratio": 15.0,        # Maximum 15x volume ratio
            "min_turnover_amount": 5e8,      # 500M RMB minimum turnover
            "min_market_cap": 7e9,           # 7B RMB minimum market cap
            "max_market_cap": 5.2e10,        # 52B RMB maximum market cap
            "min_price": 2.0,                # 2 RMB minimum price
            "max_price": 50.0,               # 50 RMB maximum price
            "first_limit_days": 180,         # Look back 180 days for first limit
            "top_n_stocks": 10,              # Select top 10 stocks
            "stop_loss_pct": -0.03,          # -3% stop loss
            "take_profit_pct": 0.01,         # +1% take profit
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
        """Required data for first limit strategy"""
        return {
            "concept_data": False,
            "stock_data": True,
            "history_days": 200,
            "features": ["close", "high", "low", "open", "vol", "amount", "pct_chg", "total_mv"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """Generate first limit signals with optimized performance"""
        logger.info("First Limit Strategy: Generating signals...")
        self._init_db()
        
        # Get latest date
        latest_date = self.db.get_latest_date()
        if not latest_date:
            logger.warning("Cannot get latest date")
            return []
        
        # Get only latest stock data for initial filtering
        latest_data = self.db.get_all_stock_data(latest_date)
        if latest_data.empty:
            logger.warning("No latest stock data available")
            return []
        
        # Apply basic filters to reduce universe
        candidates = self._filter_candidates(latest_data)
        if candidates.empty:
            logger.warning("No candidates passed basic filters")
            return []
        
        logger.info(f"Found {len(candidates)} candidate stocks after basic filtering")
        
        # Get historical data for candidates only
        history_days = self.get_required_data()["history_days"]
        start_date = self._get_n_days_before(latest_date, history_days)
        
        signals = []
        for _, row in candidates.iterrows():
            ts_code = row["ts_code"]
            
            # Get historical data for this stock
            hist_data = self.db.get_stock_data(ts_code, start_date, latest_date)
            if len(hist_data) < 10:  # Need sufficient history
                continue
            
            # Check if this is a first limit candidate
            if self._is_first_limit_candidate(row, hist_data):
                score = self._calculate_score(row, hist_data)
                if score >= 60:  # Minimum score threshold
                    signal = self._create_signal(ts_code, row, score)
                    signals.append(signal)
        
        # Sort by score and limit to top N
        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[:self.params["top_n_stocks"]]
        self.signals = signals
        
        logger.info(f"First Limit Strategy: Generated {len(signals)} signals")
        return signals

    def _filter_candidates(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Apply basic filters to identify potential candidates"""
        # Convert to numeric
        for col in ['total_mv', 'amount', 'close', 'pct_chg']:
            if col in stock_data.columns:
                stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
        
        # Apply filters
        filtered = stock_data.copy()
        
        # Market cap filter
        filtered = filtered[
            (filtered['total_mv'] >= self.params["min_market_cap"]) & 
            (filtered['total_mv'] <= self.params["max_market_cap"])
        ]
        
        # Price filter
        filtered = filtered[
            (filtered['close'] >= self.params["min_price"]) & 
            (filtered['close'] <= self.params["max_price"])
        ]
        
        # Turnover filter
        filtered = filtered[filtered['amount'] >= self.params["min_turnover_amount"]]
        
        # Limit-up proximity filter - dynamic threshold based on stock type
        def get_limit_threshold(ts_code):
            ts_code_str = str(ts_code)
            if ts_code_str.startswith(("688", "300")):  # STAR Market or ChiNext
                return self.params["limit_up_threshold_20"]
            else:  # Main board
                return self.params["limit_up_threshold"]
        
        # Apply dynamic limit-up filter
        mask = []
        for idx, row in filtered.iterrows():
            threshold = get_limit_threshold(row["ts_code"])
            mask.append(row["pct_chg"] >= threshold * 0.8)  # At least 80% of limit
        
        filtered = filtered[mask]
        
        return filtered

    def _is_first_limit_candidate(self, latest_row: pd.Series, hist_data: pd.DataFrame) -> bool:
        """Check if stock is a first limit candidate"""
        # Get limit threshold based on stock type
        ts_code = latest_row["ts_code"]
        if str(ts_code).startswith(("688", "300")):
            limit_threshold = self.params["limit_up_threshold_20"]
        else:
            limit_threshold = self.params["limit_up_threshold"]
        
        # Check if current day is at or near limit-up
        pct_chg = float(latest_row.get("pct_chg", 0))
        if pct_chg < limit_threshold * 0.9:  # At least 90% of limit
            return False
        
        # Check volume ratio
        volume_ratio = self._calculate_volume_ratio(hist_data)
        if not (self.params["min_volume_ratio"] <= volume_ratio <= self.params["max_volume_ratio"]):
            return False
        
        # Check if this is first limit in lookback period
        lookback_days = min(self.params["first_limit_days"], len(hist_data))
        recent_data = hist_data.tail(lookback_days)
        
        limit_up_count = 0
        for _, row in recent_data.iterrows():
            if float(row.get("pct_chg", 0)) >= limit_threshold:
                limit_up_count += 1
        
        # Should be the first or very early limit-up
        return limit_up_count <= 2

    def _calculate_volume_ratio(self, stock_df: pd.DataFrame) -> float:
        """Calculate volume ratio vs 5-day average"""
        if len(stock_df) < 6:
            return 1.0
        
        latest_volume = stock_df["vol"].iloc[-1] if "vol" in stock_df.columns else stock_df["volume"].iloc[-1]
        avg_volume_5d = stock_df["vol"].iloc[-6:-1].mean() if "vol" in stock_df.columns else stock_df["volume"].iloc[-6:-1].mean()
        
        if avg_volume_5d == 0:
            return 1.0
        
        return latest_volume / avg_volume_5d

    def _calculate_score(self, latest_row: pd.Series, hist_data: pd.DataFrame) -> float:
        """Calculate comprehensive score for first limit candidate"""
        # Volume score (0-100)
        volume_ratio = self._calculate_volume_ratio(hist_data)
        volume_score = min(100, max(0, (volume_ratio - 1) * 25))
        
        # Momentum score (0-100)
        momentum_5d = self._calculate_momentum(hist_data, 5)
        momentum_10d = self._calculate_momentum(hist_data, 10)
        momentum_score = min(100, max(0, (momentum_5d + momentum_10d) * 50))
        
        # Proximity to limit score (0-100)
        pct_chg = float(latest_row.get("pct_chg", 0))
        limit_threshold = self.params["limit_up_threshold"]
        proximity_score = min(100, max(0, (pct_chg / limit_threshold) * 100))
        
        # Weighted composite score
        composite_score = (
            volume_score * 0.4 +
            momentum_score * 0.3 +
            proximity_score * 0.3
        )
        
        return min(100, max(0, composite_score))

    def _calculate_momentum(self, stock_df: pd.DataFrame, window: int) -> float:
        """Calculate momentum over specified window"""
        if len(stock_df) < window + 1:
            return 0.0
        
        start_price = stock_df["close"].iloc[-window-1]
        end_price = stock_df["close"].iloc[-1]
        
        if start_price == 0:
            return 0.0
        
        return (end_price - start_price) / start_price

    def _create_signal(self, ts_code: str, latest_row: pd.Series, score: float) -> StrategySignal:
        """Create strategy signal"""
        stock_name = latest_row.get("name", ts_code)
        pct_chg = float(latest_row.get("pct_chg", 0))
        volume_ratio = self._calculate_volume_ratio(pd.DataFrame([latest_row]))  # Approximate
        
        # Determine signal type
        limit_threshold = self.params["limit_up_threshold"]
        if pct_chg >= limit_threshold:
            signal_type = "buy"
        elif pct_chg >= limit_threshold * 0.95:
            signal_type = "watch"
        else:
            signal_type = "monitor"
        
        return StrategySignal(
            ts_code=ts_code,
            stock_name=stock_name,
            strategy_type="first_limit",
            signal_type=signal_type,
            weight=min(1.0, score / 100.0),
            score=score,
            reason=f"首板候选: 涨幅{pct_chg:.1%}, 成交量比{volume_ratio:.1f}x, 评分{score:.1f}",
            metadata={
                "pct_chg": pct_chg,
                "volume_ratio": volume_ratio,
                "market_cap": latest_row.get("total_mv", 0),
                "price": latest_row.get("close", 0),
                "stop_loss_pct": self.params["stop_loss_pct"],
                "take_profit_pct": self.params["take_profit_pct"],
            },
        )

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        """Get N trading days before given date"""
        from datetime import datetime, timedelta
        
        date = datetime.strptime(date_str, "%Y%m%d")
        # Approximate trading days
        calendar_days = int(n_days * 1.4)
        prev_date = date - timedelta(days=calendar_days)
        return prev_date.strftime("%Y%m%d")

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
                    "take_profit": sig.metadata.get("take_profit_pct", 0.01),
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