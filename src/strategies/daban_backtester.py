"""
打板策略回测框架
Specialized backtesting framework for Chinese A-share limit-up (打板) strategies
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from loguru import logger

from .base_strategy import BaseStrategy, StrategySignal


class DabanBacktester:
    """
    打板策略回测框架
    
    Features:
    1. Signal-based backtesting (compatible with existing strategy interface)
    2. Realistic transaction cost modeling
    3. Time-based exit rules (11:28 profit-taking, 14:50 mandatory close)
    4. Stop-loss and take-profit handling
    5. Performance metrics calculation
    """

    def __init__(self, initial_capital: float = 1000000.0, commission_rate: float = 0.0003):
        """
        Initialize backtester
        
        Args:
            initial_capital: Initial capital (default: 1M RMB)
            commission_rate: Commission rate (default: 0.03%)
        """
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.stamp_tax_rate = 0.001  # 0.1% stamp tax on sell
        self.results = {}
        
        logger.info(f"打板策略回测框架初始化完成，初始资金: {initial_capital:,.0f} RMB")

    def backtest_strategy(
        self,
        strategy: BaseStrategy,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        position_size: float = 0.10,
        max_positions: int = 10
    ) -> Dict[str, Any]:
        """
        Backtest a single 打板 strategy
        
        Args:
            strategy: Strategy instance
            stock_codes: List of stock codes to backtest
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            position_size: Position size per stock (default: 10%)
            max_positions: Maximum number of concurrent positions (default: 10)
            
        Returns:
            Backtest results dictionary
        """
        logger.info(f"开始回测策略: {strategy.name}")
        logger.info(f"回测期间: {start_date} - {end_date}")
        logger.info(f"股票数量: {len(stock_codes)}")
        
        portfolio = {}
        daily_values = []
        trades = []
        current_date = start_date
        
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        current_dt = start_dt
        while current_dt <= end_dt:
            current_date_str = current_dt.strftime("%Y%m%d")
            
            try:
                signals = strategy.generate_signals()
                valid_signals = strategy.validate_signals(signals)
            except Exception as e:
                logger.warning(f"信号生成失败 {current_date_str}: {e}")
                valid_signals = []
            
            today_trades = self._process_signals(
                valid_signals, 
                portfolio, 
                current_date_str,
                position_size,
                max_positions
            )
            trades.extend(today_trades)
            
            portfolio_value = self._calculate_portfolio_value(portfolio, current_date_str)
            daily_values.append({
                'date': current_date_str,
                'portfolio_value': portfolio_value,
                'cash': self.initial_capital - sum(p['cost'] for p in portfolio.values()),
                'positions': len(portfolio)
            })
            
            current_dt += timedelta(days=1)
        
        metrics = self._calculate_metrics(daily_values, trades)
        
        result = {
            'strategy_name': strategy.name,
            'start_date': start_date,
            'end_date': end_date,
            'initial_capital': self.initial_capital,
            'final_value': daily_values[-1]['portfolio_value'] if daily_values else self.initial_capital,
            'total_return': (daily_values[-1]['portfolio_value'] / self.initial_capital - 1) if daily_values else 0,
            'trades': trades,
            'daily_values': daily_values,
            'metrics': metrics
        }
        
        self.results[strategy.name] = result
        logger.info(f"策略 {strategy.name} 回测完成")
        return result

    def _process_signals(
        self,
        signals: List[StrategySignal],
        portfolio: Dict[str, Dict],
        current_date: str,
        position_size: float,
        max_positions: int
    ) -> List[Dict]:
        """
        Process trading signals and execute trades
        """
        trades = []
        
        sorted_signals = sorted(signals, key=lambda s: s.score, reverse=True)
        
        positions_to_buy = min(max_positions - len(portfolio), len(sorted_signals))
        for i in range(positions_to_buy):
            signal = sorted_signals[i]
            if signal.ts_code not in portfolio:
                trade = self._execute_buy(signal, portfolio, current_date, position_size)
                if trade:
                    trades.append(trade)
        
        exit_trades = self._check_exits(portfolio, current_date)
        trades.extend(exit_trades)
        
        return trades

    def _execute_buy(
        self,
        signal: StrategySignal,
        portfolio: Dict[str, Dict],
        current_date: str,
        position_size: float
    ) -> Optional[Dict]:
        """
        Execute buy trade
        """
        # Calculate position value
        position_value = self.initial_capital * position_size
        
        # Simulate getting current price (in real implementation, this would be actual market data)
        # For now, we'll assume we can get the price from the signal metadata
        price = signal.metadata.get('price', 10.0)  # Default to 10 RMB if not available
        
        # Calculate shares (round down to 100-share lots)
        shares = int((position_value / price) // 100 * 100)
        if shares == 0:
            return None
        
        # Calculate cost including commission
        gross_cost = shares * price
        commission = gross_cost * self.commission_rate
        total_cost = gross_cost + commission
        
        # Add to portfolio
        portfolio[signal.ts_code] = {
            'shares': shares,
            'entry_price': price,
            'entry_date': current_date,
            'cost': total_cost,
            'commission': commission,
            'stop_loss': signal.metadata.get('stop_loss_pct', -0.03),
            'take_profit': signal.metadata.get('take_profit_pct', 0.01),
            'score': signal.score
        }
        
        trade = {
            'date': current_date,
            'ts_code': signal.ts_code,
            'stock_name': signal.stock_name,
            'action': 'buy',
            'shares': shares,
            'price': price,
            'cost': total_cost,
            'commission': commission,
            'reason': signal.reason
        }
        
        logger.debug(f"买入: {signal.ts_code} {shares} 股 @ {price:.2f} RMB")
        return trade

    def _check_exits(self, portfolio: Dict[str, Dict], current_date: str) -> List[Dict]:
        """
        Check for exit conditions and execute sells
        """
        trades = []
        stocks_to_remove = []
        
        for ts_code, position in portfolio.items():
            # Get current price (simulated)
            entry_price = position['entry_price']
            current_price = self._simulate_current_price(entry_price, current_date, position['entry_date'])
            
            # Calculate current return
            current_return = (current_price - entry_price) / entry_price
            
            # Check exit conditions
            should_exit = False
            exit_reason = ""
            
            # Stop-loss
            if current_return <= position['stop_loss']:
                should_exit = True
                exit_reason = f"止损 ({current_return:.1%} <= {position['stop_loss']:.1%})"
            
            # Take-profit
            elif current_return >= position['take_profit']:
                should_exit = True
                exit_reason = f"止盈 ({current_return:.1%} >= {position['take_profit']:.1%})"
            
            # Time-based exit (simplified - in real implementation would check intraday timing)
            elif self._should_time_exit(current_date, position['entry_date']):
                should_exit = True
                exit_reason = "时间止损"
            
            if should_exit:
                trade = self._execute_sell(ts_code, position, current_price, current_date, exit_reason)
                if trade:
                    trades.append(trade)
                    stocks_to_remove.append(ts_code)
        
        # Remove exited positions
        for ts_code in stocks_to_remove:
            del portfolio[ts_code]
        
        return trades

    def _execute_sell(
        self,
        ts_code: str,
        position: Dict,
        current_price: float,
        current_date: str,
        reason: str
    ) -> Dict:
        """
        Execute sell trade
        """
        shares = position['shares']
        entry_price = position['entry_price']
        
        # Calculate proceeds
        gross_proceeds = shares * current_price
        commission = gross_proceeds * self.commission_rate
        stamp_tax = gross_proceeds * self.stamp_tax_rate
        net_proceeds = gross_proceeds - commission - stamp_tax
        
        # Calculate profit/loss
        cost = position['cost']
        profit_loss = net_proceeds - cost
        return_pct = profit_loss / cost if cost > 0 else 0
        
        trade = {
            'date': current_date,
            'ts_code': ts_code,
            'action': 'sell',
            'shares': shares,
            'price': current_price,
            'proceeds': net_proceeds,
            'commission': commission,
            'stamp_tax': stamp_tax,
            'profit_loss': profit_loss,
            'return_pct': return_pct,
            'reason': reason
        }
        
        logger.debug(f"卖出: {ts_code} {shares} 股 @ {current_price:.2f} RMB, 收益: {profit_loss:,.0f} RMB ({return_pct:.1%})")
        return trade

    def _simulate_current_price(self, entry_price: float, current_date: str, entry_date: str) -> float:
        """
        Simulate current price based on entry price and date
        In real implementation, this would fetch actual market data
        """
        # Simple simulation: assume some random movement
        days_held = (datetime.strptime(current_date, "%Y%m%d") - datetime.strptime(entry_date, "%Y%m%d")).days
        if days_held <= 0:
            return entry_price
        
        # Simulate price movement (this is just for demonstration)
        # In real backtesting, you'd get actual OHLCV data
        random_factor = np.random.normal(1.0, 0.02)  # 2% daily volatility
        simulated_price = entry_price * (random_factor ** days_held)
        return max(simulated_price, 0.01)  # Ensure positive price

    def _should_time_exit(self, current_date: str, entry_date: str) -> bool:
        """
        Check if time-based exit should trigger
        Simplified implementation - in real system would check intraday timing
        """
        days_held = (datetime.strptime(current_date, "%Y%m%d") - datetime.strptime(entry_date, "%Y%m%d")).days
        return days_held >= 2  # Exit after 2 days maximum

    def _calculate_portfolio_value(self, portfolio: Dict[str, Dict], current_date: str) -> float:
        """
        Calculate current portfolio value
        """
        total_value = self.initial_capital  # This is simplified
        
        # In real implementation, you'd calculate based on current positions and cash
        for ts_code, position in portfolio.items():
            current_price = self._simulate_current_price(position['entry_price'], current_date, position['entry_date'])
            position_value = position['shares'] * current_price
            total_value += (position_value - position['cost'])
        
        return total_value

    def _calculate_metrics(self, daily_values: List[Dict], trades: List[Dict]) -> Dict[str, Any]:
        """
        Calculate performance metrics
        """
        if not daily_values or len(daily_values) < 2:
            return {}
        
        # Extract daily returns
        values = [dv['portfolio_value'] for dv in daily_values]
        returns = [(values[i] - values[i-1]) / values[i-1] for i in range(1, len(values))]
        
        if not returns:
            return {}
        
        # Basic metrics
        total_return = (values[-1] / values[0]) - 1
        annualized_return = (1 + total_return) ** (252 / len(values)) - 1
        volatility = np.std(returns) * np.sqrt(252)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Max drawdown
        peak = values[0]
        max_drawdown = 0
        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # Trade statistics
        winning_trades = [t for t in trades if t.get('action') == 'sell' and t.get('profit_loss', 0) > 0]
        losing_trades = [t for t in trades if t.get('action') == 'sell' and t.get('profit_loss', 0) <= 0]
        win_rate = len(winning_trades) / len([t for t in trades if t.get('action') == 'sell']) if trades else 0
        
        avg_win = np.mean([t['profit_loss'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([abs(t['profit_loss']) for t in losing_trades]) if losing_trades else 1
        profit_factor = avg_win / avg_loss if avg_loss > 0 else float('inf')
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'total_trades': len([t for t in trades if t.get('action') == 'sell']),
            'avg_trade_return': np.mean([t['return_pct'] for t in trades if t.get('action') == 'sell']) if trades else 0
        }

    def compare_strategies(self, strategy_results: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Compare multiple strategy results
        """
        comparison = {}
        for name, results in strategy_results.items():
            if 'metrics' in results:
                comparison[name] = {
                    'total_return': results['metrics'].get('total_return', 0),
                    'sharpe_ratio': results['metrics'].get('sharpe_ratio', 0),
                    'max_drawdown': results['metrics'].get('max_drawdown', 0),
                    'win_rate': results['metrics'].get('win_rate', 0),
                    'total_trades': results['metrics'].get('total_trades', 0)
                }
        
        return comparison

    def generate_report(self, results: Dict[str, Any]) -> str:
        """
        Generate backtest report
        """
        report = f"""
打板策略回测报告
================

策略名称: {results.get('strategy_name', 'Unknown')}
回测期间: {results.get('start_date', 'N/A')} - {results.get('end_date', 'N/A')}
初始资金: {results.get('initial_capital', 0):,.0f} RMB
最终价值: {results.get('final_value', 0):,.0f} RMB
总收益率: {results.get('total_return', 0):.2%}

性能指标:
- 年化收益率: {results.get('metrics', {}).get('annualized_return', 0):.2%}
- 夏普比率: {results.get('metrics', {}).get('sharpe_ratio', 0):.2f}
- 最大回撤: {results.get('metrics', {}).get('max_drawdown', 0):.2%}
- 胜率: {results.get('metrics', {}).get('win_rate', 0):.2%}
- 总交易次数: {results.get('metrics', {}).get('total_trades', 0)}

交易统计:
- 盈利交易: {len([t for t in results.get('trades', []) if t.get('action') == 'sell' and t.get('profit_loss', 0) > 0])}
- 亏损交易: {len([t for t in results.get('trades', []) if t.get('action') == 'sell' and t.get('profit_loss', 0) <= 0])}
        """
        
        return report