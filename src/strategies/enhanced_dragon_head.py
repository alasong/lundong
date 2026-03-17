"""
Enhanced Dragon Head (打板) Strategy
Based on comprehensive research of Chinese A-share limit-up trading strategies
Implements First Limit (首板), One-to-Two (一进二), and Leader Stock (龙头股) strategies
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class EnhancedDragonHeadStrategy(BaseStrategy):
    """Enhanced Dragon Head Strategy - Comprehensive 打板 implementation"""

    def __init__(self, name: str = "enhanced_dragon_head", params: Optional[Dict] = None):
        default_params = {
            # Core strategy parameters
            "strategy_type": "first_limit",  # "first_limit", "one_to_two", "leader"
            "limit_up_threshold": 0.095,     # 9.5% for main board (10% actual)
            "limit_up_threshold_20": 0.195,  # 19.5% for STAR/ChiNext (20% actual)
            
            # Entry conditions
            "min_volume_ratio": 3.0,         # Minimum volume ratio vs 5-day average
            "max_volume_ratio": 15.0,        # Maximum volume ratio (avoid excessive)
            "min_turnover_amount": 5e8,      # Minimum turnover: 500M RMB
            "max_turnover_amount": 2e10,     # Maximum turnover: 20B RMB
            "min_market_cap": 7e9,           # Minimum market cap: 7B RMB (70亿)
            "max_market_cap": 5.2e10,        # Maximum market cap: 52B RMB (520亿)
            "min_price": 2.0,                # Minimum price: 2 RMB
            "max_price": 50.0,               # Maximum price: 50 RMB
            
            # First Limit specific
            "first_limit_days": 180,         # Look back period for first limit
            "min_consecutive_limits": 1,     # Minimum consecutive limits
            "max_consecutive_limits": 5,     # Maximum consecutive limits (avoid overextended)
            
            # One-to-Two specific
            "gap_open_min": 0.01,            # Minimum gap open: 1%
            "gap_open_max": 0.06,            # Maximum gap open: 6%
            "call_auction_volume_min": 0.03, # Minimum call auction volume % of previous day
            
            # Technical indicators
            "min_seal_order_ratio": 0.5,     # Minimum seal order ratio
            "min_macd_hist": 0.0,            # Minimum MACD histogram (above zero preferred)
            "rsi_oversold": 30,              # RSI oversold threshold
            "rsi_overbought": 70,            # RSI overbought threshold
            
            # Scoring weights
            "volume_weight": 0.25,
            "momentum_weight": 0.25,
            "sector_weight": 0.20,
            "fundamental_weight": 0.15,
            "technical_weight": 0.15,
            
            # Portfolio parameters
            "top_n_stocks": 10,              # Number of stocks to select
            "max_position_per_stock": 0.10,  # Maximum position per stock (10%)
            
            # Risk management
            "stop_loss_pct": -0.03,          # Stop loss: -3%
            "take_profit_pct": 0.01,         # Take profit: +1% (for morning exit)
        }
        default_params.update(params or {})
        
        super().__init__(name, default_params)
        self.db = None
        self._cache = {}

    def _init_db(self):
        """Lazy load database connection"""
        if self.db is None:
            from data.database import get_database
            self.db = get_database()

    def get_required_data(self) -> Dict[str, Any]:
        """Required data for enhanced dragon head strategy"""
        return {
            "concept_data": True,
            "stock_data": True,
            "history_days": 200,  # Need 180+ days for first limit detection
            "features": [
                "close", "high", "low", "open", "vol", "amount", "pct_chg",
                "total_mv", "pe", "pb", "ma5", "ma10", "macd", "macd_signal", 
                "macd_hist", "rsi_6", "rsi_12", "bb_upper", "bb_lower", "bb_middle"
            ],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """Generate enhanced dragon head signals based on strategy type"""
        logger.info(f"Enhanced Dragon Head Strategy ({self.params['strategy_type']}): Generating signals...")
        self._init_db()
        
        signals = []
        
        # Get latest date and historical data
        latest_date = self.db.get_latest_date()
        if not latest_date:
            logger.warning("Cannot get latest date")
            return []
        
        history_days = self.get_required_data()["history_days"]
        start_date = self._get_n_days_before(latest_date, history_days)
        
        # Get latest date data first for initial filtering
        logger.info("Fetching latest stock data for initial filtering...")
        latest_stock_data = self.db.get_all_stock_data(latest_date)
        if latest_stock_data.empty:
            logger.warning("No latest stock data available")
            return []
        
        # Apply basic filters to reduce universe size
        filtered_stocks = self._apply_basic_universe_filters(latest_stock_data)
        if filtered_stocks.empty:
            logger.warning("No stocks passed basic filters")
            return []
        
        logger.info(f"Reduced universe from {len(latest_stock_data)} to {len(filtered_stocks)} stocks")
        
        # Get historical data only for filtered stocks
        all_stocks = filtered_stocks[["ts_code"]].drop_duplicates()
        stock_codes = all_stocks["ts_code"].tolist()
        
        # Fetch historical data for filtered stocks only
        logger.info(f"Fetching historical data for {len(stock_codes)} filtered stocks...")
        stock_data_list = []
        for ts_code in stock_codes:
            stock_df = self.db.get_stock_data(ts_code, start_date, latest_date)
            if not stock_df.empty:
                stock_data_list.append(stock_df)
        
        if not stock_data_list:
            logger.warning("Failed to fetch historical data for filtered stocks")
            return []
        
        stock_data = pd.concat(stock_data_list, ignore_index=True)

    def _apply_basic_universe_filters(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Apply basic filters to reduce universe size before detailed analysis"""
        if stock_data.empty:
            return stock_data
        
        # Convert columns to numeric if needed
        for col in ['total_mv', 'amount', 'close', 'pct_chg']:
            if col in stock_data.columns:
                stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
        
        # Apply market cap filter
        min_cap = self.params["min_market_cap"]
        max_cap = self.params["max_market_cap"]
        filtered = stock_data[
            (stock_data['total_mv'] >= min_cap) & 
            (stock_data['total_mv'] <= max_cap)
        ].copy()
        
        # Apply price filter
        min_price = self.params["min_price"]
        max_price = self.params["max_price"]
        filtered = filtered[
            (filtered['close'] >= min_price) & 
            (filtered['close'] <= max_price)
        ]
        
        # Apply minimum turnover filter
        min_turnover = self.params["min_turnover_amount"]
        filtered = filtered[filtered['amount'] >= min_turnover]
        
        # Apply limit-up proximity filter (only consider stocks near limit-up)
        limit_threshold = self.params["limit_up_threshold"]
        filtered = filtered[filtered['pct_chg'] >= limit_threshold * 0.8]
        
        logger.info(f"Basic filters: {len(stock_data)} → {len(filtered)} stocks")
        return filtered
        
        # Get sector/industry data (handle potential None returns)
        logger.info("Fetching sector data...")
        try:
            all_constituents = self.db.get_all_constituents()
            if all_constituents:
                constituent_df = pd.DataFrame(all_constituents)
                concept_data = self.db.get_all_concept_data(latest_date) or pd.DataFrame()
            else:
                constituent_df = pd.DataFrame()
                concept_data = pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to fetch sector data: {e}")
            constituent_df = pd.DataFrame()
            concept_data = pd.DataFrame()
        
        # Generate signals based on strategy type
        strategy_type = self.params["strategy_type"]
        
        if strategy_type == "first_limit":
            signals = self._generate_first_limit_signals(
                stock_data, all_stocks, constituent_df, concept_data, latest_date
            )
        elif strategy_type == "one_to_two":
            signals = self._generate_one_to_two_signals(
                stock_data, all_stocks, constituent_df, concept_data, latest_date
            )
        elif strategy_type == "leader":
            signals = self._generate_leader_signals(
                stock_data, all_stocks, constituent_df, concept_data, latest_date
            )
        else:
            logger.warning(f"Unknown strategy type: {strategy_type}, using first_limit")
            signals = self._generate_first_limit_signals(
                stock_data, all_stocks, constituent_df, concept_data, latest_date
            )
        
        # Sort by score and limit to top N
        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[:self.params["top_n_stocks"]]
        self.signals = signals
        
        logger.info(f"Enhanced Dragon Head Strategy: Generated {len(signals)} signals")
        return signals

    def _generate_first_limit_signals(
        self, stock_data: pd.DataFrame, all_stocks: pd.DataFrame, 
        constituent_df: pd.DataFrame, concept_data: pd.DataFrame, latest_date: str
    ) -> List[StrategySignal]:
        """Generate First Limit (首板) signals"""
        signals = []
        logger.info("Generating First Limit signals...")
        
        for ts_code in all_stocks["ts_code"].unique():
            stock_df = stock_data[stock_data["ts_code"] == ts_code].sort_values("trade_date")
            
            if len(stock_df) < 20:  # Need sufficient history
                continue
            
            # Get latest data
            latest_row = stock_df.iloc[-1]
            prev_row = stock_df.iloc[-2] if len(stock_df) >= 2 else None
            
            # Basic filters
            if not self._pass_basic_filters(latest_row):
                continue
            
            # Check if approaching or at limit-up
            pct_chg = latest_row.get("pct_chg", 0) or 0
            if pct_chg < self.params["limit_up_threshold"] * 0.8:  # At least 80% of limit
                continue
            
            # Check if this is first limit in specified period
            if not self._is_first_limit(stock_df, self.params["first_limit_days"]):
                continue
            
            # Check volume and turnover conditions
            volume_ratio = self._calculate_volume_ratio(stock_df)
            turnover_amount = latest_row.get("amount", 0) or 0
            
            if not (self.params["min_volume_ratio"] <= volume_ratio <= self.params["max_volume_ratio"]):
                continue
                
            if not (self.params["min_turnover_amount"] <= turnover_amount <= self.params["max_turnover_amount"]):
                continue
            
            # Calculate comprehensive score
            score = self._calculate_comprehensive_score(
                latest_row, stock_df, volume_ratio, constituent_df, concept_data
            )
            
            if score < 50:  # Minimum score threshold
                continue
            
            # Create signal
            signal = self._create_signal(
                ts_code, all_stocks, latest_row, score, volume_ratio, 
                "first_limit", f"首板策略: 涨幅{pct_chg:.1%}, 成交量比{volume_ratio:.1f}x"
            )
            signals.append(signal)
        
        return signals

    def _generate_one_to_two_signals(
        self, stock_data: pd.DataFrame, all_stocks: pd.DataFrame,
        constituent_df: pd.DataFrame, concept_data: pd.DataFrame, latest_date: str
    ) -> List[StrategySignal]:
        """Generate One-to-Two (一进二) signals"""
        signals = []
        logger.info("Generating One-to-Two signals...")
        
        for ts_code in all_stocks["ts_code"].unique():
            stock_df = stock_data[stock_data["ts_code"] == ts_code].sort_values("trade_date")
            
            if len(stock_df) < 3:  # Need at least 3 days
                continue
            
            # Get latest and previous data
            latest_row = stock_df.iloc[-1]
            prev_row = stock_df.iloc[-2]
            prev_prev_row = stock_df.iloc[-3]
            
            # Basic filters
            if not self._pass_basic_filters(latest_row):
                continue
            
            # Check if yesterday was first limit
            if not self._was_yesterday_first_limit(stock_df):
                continue
            
            # Check today's gap open
            if prev_row is not None:
                prev_close = prev_row.get("close", 0) or 0
                latest_open = latest_row.get("open", 0) or 0
                
                if prev_close > 0:
                    gap_open = (latest_open - prev_close) / prev_close
                    if not (self.params["gap_open_min"] <= gap_open <= self.params["gap_open_max"]):
                        continue
                else:
                    continue
            
            # Check call auction volume (approximated)
            call_auction_vol_ratio = self._estimate_call_auction_volume(stock_df)
            if call_auction_vol_ratio < self.params["call_auction_volume_min"]:
                continue
            
            # Calculate score
            volume_ratio = self._calculate_volume_ratio(stock_df)
            score = self._calculate_comprehensive_score(
                latest_row, stock_df, volume_ratio, constituent_df, concept_data
            )
            
            if score < 60:  # Higher threshold for 1→2
                continue
            
            # Create signal
            signal = self._create_signal(
                ts_code, all_stocks, latest_row, score, volume_ratio,
                "one_to_two", f"一进二策略: 跳空{gap_open:.1%}, 成交量比{volume_ratio:.1f}x"
            )
            signals.append(signal)
        
        return signals

    def _generate_leader_signals(
        self, stock_data: pd.DataFrame, all_stocks: pd.DataFrame,
        constituent_df: pd.DataFrame, concept_data: pd.DataFrame, latest_date: str
    ) -> List[StrategySignal]:
        """Generate Leader Stock (龙头股) signals"""
        signals = []
        logger.info("Generating Leader Stock signals...")
        
        # This is more complex and would require sector leadership analysis
        # For now, we'll use a simplified version based on momentum and sector strength
        
        for ts_code in all_stocks["ts_code"].unique():
            stock_df = stock_data[stock_data["ts_code"] == ts_code].sort_values("trade_date")
            
            if len(stock_df) < 30:  # Need longer history for leader identification
                continue
            
            latest_row = stock_df.iloc[-1]
            
            # Basic filters
            if not self._pass_basic_filters(latest_row):
                continue
            
            # Check momentum strength
            momentum_10d = self._calculate_momentum(stock_df, 10)
            momentum_20d = self._calculate_momentum(stock_df, 20)
            
            if momentum_10d < 0.05 or momentum_20d < 0.10:  # Strong momentum required
                continue
            
            # Check if in strong sector
            sector_strength = self._get_sector_strength(ts_code, constituent_df, concept_data)
            if sector_strength < 0.6:  # Sector must be strong
                continue
            
            # Calculate score with higher emphasis on momentum and sector
            volume_ratio = self._calculate_volume_ratio(stock_df)
            score = self._calculate_leader_score(
                latest_row, stock_df, volume_ratio, sector_strength, momentum_10d, momentum_20d
            )
            
            if score < 70:  # Highest threshold for leaders
                continue
            
            signal = self._create_signal(
                ts_code, all_stocks, latest_row, score, volume_ratio,
                "leader", f"龙头策略: 10日动量{momentum_10d:.1%}, 板块强度{sector_strength:.1f}"
            )
            signals.append(signal)
        
        return signals

    def _pass_basic_filters(self, row: pd.Series) -> bool:
        """Apply basic filters (price, market cap, etc.)"""
        market_cap = row.get("total_mv", 0) or 0
        price = row.get("close", 0) or 0
        
        if not (self.params["min_market_cap"] <= market_cap <= self.params["max_market_cap"]):
            return False
        
        if not (self.params["min_price"] <= price <= self.params["max_price"]):
            return False
        
        return True

    def _is_first_limit(self, stock_df: pd.DataFrame, lookback_days: int) -> bool:
        """Check if current limit-up is first in lookback period"""
        if len(stock_df) < lookback_days:
            lookback_days = len(stock_df)
        
        recent_data = stock_df.tail(lookback_days)
        limit_up_threshold = self._get_limit_threshold(recent_data.iloc[-1])
        
        # Count previous limit-ups
        limit_up_count = 0
        for idx, row in recent_data.iterrows():
            if row.get("pct_chg", 0) >= limit_up_threshold:
                limit_up_count += 1
        
        # Current day should be the only limit-up (or first in sequence)
        return limit_up_count <= self.params["max_consecutive_limits"]

    def _was_yesterday_first_limit(self, stock_df: pd.DataFrame) -> bool:
        """Check if yesterday was a first limit-up"""
        if len(stock_df) < 3:
            return False
        
        yesterday = stock_df.iloc[-2]
        day_before = stock_df.iloc[-3]
        
        limit_up_threshold = self._get_limit_threshold(yesterday)
        
        # Yesterday should be limit-up
        if yesterday.get("pct_chg", 0) < limit_up_threshold:
            return False
        
        # Day before should NOT be limit-up (first limit)
        if day_before.get("pct_chg", 0) >= limit_up_threshold:
            return False
        
        return True

    def _get_limit_threshold(self, row: pd.Series) -> float:
        """Get appropriate limit-up threshold based on stock type"""
        ts_code = row.get("ts_code", "")
        if ts_code.startswith(("688", "300")):  # STAR Market or ChiNext
            return self.params["limit_up_threshold_20"]
        else:  # Main board
            return self.params["limit_up_threshold"]

    def _calculate_volume_ratio(self, stock_df: pd.DataFrame) -> float:
        """Calculate volume ratio vs 5-day average"""
        if len(stock_df) < 6:
            return 1.0
        
        latest_volume = stock_df["vol"].iloc[-1] if "vol" in stock_df.columns else stock_df["volume"].iloc[-1]
        avg_volume_5d = stock_df["vol"].iloc[-6:-1].mean() if "vol" in stock_df.columns else stock_df["volume"].iloc[-6:-1].mean()
        
        if avg_volume_5d == 0:
            return 1.0
        
        return latest_volume / avg_volume_5d

    def _estimate_call_auction_volume(self, stock_df: pd.DataFrame) -> float:
        """Estimate call auction volume as percentage of previous day volume"""
        if len(stock_df) < 2:
            return 0.0
        
        # This is an approximation since we don't have intraday data
        # In real implementation, this would use actual call auction data
        latest_volume = stock_df["vol"].iloc[-1] if "vol" in stock_df.columns else stock_df["volume"].iloc[-1]
        prev_volume = stock_df["vol"].iloc[-2] if "vol" in stock_df.columns else stock_df["volume"].iloc[-2]
        
        if prev_volume == 0:
            return 0.0
        
        # Assume first 5 minutes represents ~10% of daily volume for estimation
        estimated_call_vol = latest_volume * 0.1
        return estimated_call_vol / prev_volume

    def _calculate_momentum(self, stock_df: pd.DataFrame, window: int) -> float:
        """Calculate momentum over specified window"""
        if len(stock_df) < window + 1:
            return 0.0
        
        start_price = stock_df["close"].iloc[-window-1]
        end_price = stock_df["close"].iloc[-1]
        
        if start_price == 0:
            return 0.0
        
        return (end_price - start_price) / start_price

    def _get_sector_strength(self, ts_code: str, constituent_df: pd.DataFrame, concept_data: pd.DataFrame) -> float:
        """Get sector strength for given stock"""
        if constituent_df.empty or concept_data.empty:
            return 0.5
        
        concept_row = constituent_df[constituent_df["stock_code"] == ts_code]
        if concept_row.empty:
            return 0.5
        
        concept_code = concept_row.iloc[0]["concept_code"]
        concept_perf = concept_data[concept_data["ts_code"] == concept_code]
        
        if concept_perf.empty:
            return 0.5
        
        latest_concept = concept_perf.iloc[-1]
        concept_chg = latest_concept.get("pct_change", 0) or 0
        
        # Normalize to 0-1 scale
        if concept_chg >= 0.05:
            return 1.0
        elif concept_chg >= -0.02:
            return 0.5 + concept_chg * 10
        else:
            return max(0.0, 0.3 + concept_chg * 5)

    def _calculate_comprehensive_score(
        self, latest_row: pd.Series, stock_df: pd.DataFrame, volume_ratio: float,
        constituent_df: pd.DataFrame, concept_data: pd.DataFrame
    ) -> float:
        """Calculate comprehensive score for first limit and one-to-two strategies"""
        # Volume score (0-100)
        volume_score = min(100, max(0, (volume_ratio - 1) * 25))
        
        # Momentum score (0-100)
        momentum_5d = self._calculate_momentum(stock_df, 5)
        momentum_10d = self._calculate_momentum(stock_df, 10)
        momentum_score = min(100, max(0, (momentum_5d + momentum_10d) * 100))
        
        # Sector score (0-100)
        ts_code = latest_row.get("ts_code", "")
        sector_score = self._get_sector_strength(ts_code, constituent_df, concept_data) * 100
        
        # Fundamental score (0-100)
        pe = latest_row.get("pe", 100) or 100
        pb = latest_row.get("pb", 10) or 10
        fundamental_score = 100 - min(100, max(0, (pe / 50 + pb / 5) * 25))
        
        # Technical score (0-100)
        macd_hist = latest_row.get("macd_hist", 0) or 0
        rsi_6 = latest_row.get("rsi_6", 50) or 50
        technical_score = 50
        
        if macd_hist > 0:
            technical_score += 25
        if 30 <= rsi_6 <= 70:
            technical_score += 25
        
        # Weighted composite score
        composite_score = (
            volume_score * self.params["volume_weight"] +
            momentum_score * self.params["momentum_weight"] +
            sector_score * self.params["sector_weight"] +
            fundamental_score * self.params["fundamental_weight"] +
            technical_score * self.params["technical_weight"]
        )
        
        return min(100, max(0, composite_score))

    def _calculate_leader_score(
        self, latest_row: pd.Series, stock_df: pd.DataFrame, volume_ratio: float,
        sector_strength: float, momentum_10d: float, momentum_20d: float
    ) -> float:
        """Calculate specialized score for leader stocks"""
        # Higher emphasis on momentum and sector strength for leaders
        momentum_score = min(100, max(0, (momentum_10d + momentum_20d) * 50))
        sector_score = min(100, max(0, sector_strength * 100))
        volume_score = min(100, max(0, (volume_ratio - 1) * 20))
        
        # Leaders should have sustained momentum
        composite_score = (
            momentum_score * 0.4 +
            sector_score * 0.3 +
            volume_score * 0.2 +
            10  # Base score for leaders
        )
        
        return min(100, max(0, composite_score))

    def _create_signal(
        self, ts_code: str, all_stocks: pd.DataFrame, latest_row: pd.Series,
        score: float, volume_ratio: float, strategy_subtype: str, reason: str
    ) -> StrategySignal:
        """Create standardized strategy signal"""
        stock_info = all_stocks[all_stocks["ts_code"] == ts_code]
        stock_name = stock_info["name"].iloc[0] if not stock_info.empty else ts_code
        
        # Determine signal type based on current status
        pct_chg = latest_row.get("pct_chg", 0) or 0
        limit_threshold = self._get_limit_threshold(latest_row)
        
        if pct_chg >= limit_threshold:
            signal_type = "buy"  # Already at limit-up
        elif pct_chg >= limit_threshold * 0.9:
            signal_type = "watch"  # Very close to limit-up
        else:
            signal_type = "monitor"  # Monitor for potential
        
        return StrategySignal(
            ts_code=ts_code,
            stock_name=stock_name,
            strategy_type=f"enhanced_dragon_head_{strategy_subtype}",
            signal_type=signal_type,
            weight=min(1.0, score / 100.0),
            score=score,
            reason=reason,
            metadata={
                "pct_chg": pct_chg,
                "volume_ratio": volume_ratio,
                "market_cap": latest_row.get("total_mv", 0),
                "price": latest_row.get("close", 0),
                "strategy_subtype": strategy_subtype,
                "stop_loss_pct": self.params["stop_loss_pct"],
                "take_profit_pct": self.params["take_profit_pct"],
                "max_position": self.params["max_position_per_stock"],
            },
        )

    def _get_n_days_before(self, date_str: str, n_days: int) -> str:
        """Get N trading days before given date"""
        from datetime import datetime, timedelta
        
        date = datetime.strptime(date_str, "%Y%m%d")
        # Approximate trading days (account for weekends)
        calendar_days = int(n_days * 1.4)  # Rough estimate
        prev_date = date - timedelta(days=calendar_days)
        return prev_date.strftime("%Y%m%d")

    def optimize_portfolio(
        self, signals: List[StrategySignal], **kwargs
    ) -> Dict[str, Any]:
        """Optimize portfolio with risk management for 打板 strategies"""
        if not signals:
            return {"portfolio": [], "metrics": {}}
        
        # Apply risk management specific to 打板 strategies
        filtered_signals = []
        total_score = sum(sig.score for sig in signals)
        
        for sig in signals:
            # Apply maximum position size
            weight = min(
                sig.weight,
                self.params["max_position_per_stock"]
            )
            
            # Adjust weight based on confidence and risk
            risk_adjusted_weight = weight * (sig.score / 100.0)
            
            if risk_adjusted_weight > 0:
                filtered_signals.append((sig, risk_adjusted_weight))
        
        # Normalize weights
        total_weight = sum(weight for _, weight in filtered_signals)
        if total_weight == 0:
            return {"portfolio": [], "metrics": {}}
        
        portfolio = []
        for sig, weight in filtered_signals:
            normalized_weight = weight / total_weight
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
            "avg_score": sum(p["score"] for p in portfolio) / len(portfolio),
            "total_weight": sum(p["weight"] for p in portfolio),
            "strategy_type": self.params["strategy_type"],
        }
        
        return {
            "portfolio": portfolio,
            "metrics": metrics,
        }