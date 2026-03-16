"""
仓位管理模块
实现市场状态识别和动态仓位调整
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import SQLiteDatabase, get_database


class PositionManager:
    """
    仓位管理器

    功能：
    1. 市场状态识别 (BULL/BEAR/SIDEWAYS)
    2. 动态仓位计算
    3. 组合再平衡
    """

    # 市场状态
    BULL = 'BULL'
    BEAR = 'BEAR'
    SIDEWAYS = 'SIDEWAYS'

    def __init__(
        self,
        db: SQLiteDatabase = None,
        position_bull: float = 0.90,
        position_bear: float = 0.30,
        position_sideways: float = 0.60,
        ma_short: int = 20,
        ma_long: int = 60
    ):
        """
        初始化仓位管理器

        Args:
            db: 数据库实例
            position_bull: 牛市仓位
            position_bear: 熊市仓位
            position_sideways: 震荡市仓位
            ma_short: 短期均线周期
            ma_long: 长期均线周期
        """
        self.db = db or get_database()
        self.position_bull = position_bull
        self.position_bear = position_bear
        self.position_sideways = position_sideways
        self.ma_short = ma_short
        self.ma_long = ma_long

        logger.info(f"仓位管理器初始化: 牛市={position_bull}, 熊市={position_bear}, 震荡={position_sideways}")

    def detect_market_state(
        self,
        market_data: pd.DataFrame = None,
        benchmark_code: str = '000300.SH'
    ) -> Tuple[str, Dict]:
        """
        市场状态识别

        状态判断规则:
        - BULL: MA20 > MA60 * 1.02 且成交量放大
        - BEAR: MA20 < MA60 * 0.98 且成交量萎缩
        - SIDEWAYS: 其他情况

        Args:
            market_data: 市场数据 DataFrame (可选)
            benchmark_code: 基准代码

        Returns:
            (市场状态, 识别指标)
        """
        if market_data is None:
            market_data = self._get_benchmark_data(benchmark_code)

        if market_data is None or market_data.empty:
            logger.warning("无法获取市场数据，默认震荡市")
            return self.SIDEWAYS, {}

        # 确保数据按日期排序
        if 'trade_date' in market_data.columns:
            market_data = market_data.sort_values('trade_date')

        close = market_data['close']

        # 计算均线
        ma_short = close.rolling(self.ma_short).mean()
        ma_long = close.rolling(self.ma_long).mean()

        current_price = close.iloc[-1]
        current_ma_short = ma_short.iloc[-1]
        current_ma_long = ma_long.iloc[-1]

        # 计算成交量变化
        if 'volume' in market_data.columns:
            volume = market_data['volume']
            vol_short = volume.rolling(self.ma_short).mean()
            vol_long = volume.rolling(self.ma_long).mean()
            vol_ratio = vol_short.iloc[-1] / vol_long.iloc[-1] if vol_long.iloc[-1] > 0 else 1
        else:
            vol_ratio = 1.0

        # 计算波动率
        returns = close.pct_change().dropna()
        volatility = returns.iloc[-60:].std() * np.sqrt(252) if len(returns) >= 60 else 0.2

        # 判断市场状态
        if current_ma_short > current_ma_long * 1.02:
            # MA20 在 MA60 上方 2% 以上
            if vol_ratio > 1.1:  # 成交量放大
                state = self.BULL
            else:
                state = self.SIDEWAYS  # 量能不足
        elif current_ma_short < current_ma_long * 0.98:
            # MA20 在 MA60 下方 2% 以上
            if vol_ratio < 0.9:  # 成交量萎缩
                state = self.BEAR
            else:
                state = self.SIDEWAYS
        else:
            state = self.SIDEWAYS

        indicators = {
            'current_price': current_price,
            'ma_short': current_ma_short,
            'ma_long': current_ma_long,
            'ma_ratio': current_ma_short / current_ma_long if current_ma_long > 0 else 1,
            'vol_ratio': vol_ratio,
            'volatility': volatility,
            'trend': 'up' if current_ma_short > current_ma_long else 'down'
        }

        logger.info(f"市场状态识别: {state}, MA比例={indicators['ma_ratio']:.3f}, 量比={vol_ratio:.2f}")

        return state, indicators

    def _get_benchmark_data(
        self,
        benchmark_code: str,
        lookback_days: int = 120
    ) -> Optional[pd.DataFrame]:
        """获取基准数据"""
        try:
            end_date = self.db.get_latest_date()
            if end_date is None:
                return None

            start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")

            # 尝试从数据库获取
            df = self.db.get_stock_data(benchmark_code, start_date, end_date)

            if df.empty:
                # 尝试使用板块数据
                import sqlite3
                conn = sqlite3.connect('data/stock.db')
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT trade_date, close, vol as volume
                    FROM concept_daily
                    WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                    ORDER BY trade_date
                """, (benchmark_code, start_date, end_date))

                rows = cursor.fetchall()
                conn.close()

                if rows:
                    df = pd.DataFrame(rows, columns=['trade_date', 'close', 'volume'])

            return df

        except Exception as e:
            logger.error(f"获取基准数据失败: {e}")
            return None

    def calculate_position_size(
        self,
        market_state: str,
        prediction_confidence: float = 0.5,
        volatility: float = 0.2
    ) -> float:
        """
        计算仓位比例

        规则:
        - BULL: 基础仓位 90%，高置信度可加仓
        - BEAR: 基础仓位 30%，控制风险
        - SIDEWAYS: 基础仓位 60%

        Args:
            market_state: 市场状态
            prediction_confidence: 预测置信度 (0-1)
            volatility: 当前波动率

        Returns:
            建议仓位比例
        """
        # 基础仓位
        base_positions = {
            self.BULL: self.position_bull,
            self.BEAR: self.position_bear,
            self.SIDEWAYS: self.position_sideways
        }

        base = base_positions.get(market_state, self.position_sideways)

        # 置信度调整
        # 高置信度增加仓位 (最多 +10%)
        confidence_adj = (prediction_confidence - 0.5) * 0.2

        # 波动率调整
        # 高波动降低仓位 (最多 -15%)
        vol_adj = -max(0, (volatility - 0.2) / 0.3) * 0.15

        # 最终仓位
        adjusted = base + confidence_adj + vol_adj

        # 限制范围
        position = min(1.0, max(0.1, adjusted))

        logger.debug(f"仓位计算: 基础={base:.0%}, 置信度调整={confidence_adj:+.0%}, 波动率调整={vol_adj:+.0%}, 最终={position:.0%}")

        return position

    def calculate_individual_weights(
        self,
        stock_predictions: pd.DataFrame,
        total_position: float,
        max_single_weight: float = 0.10,
        min_single_weight: float = 0.02
    ) -> pd.DataFrame:
        """
        计算个股权重

        Args:
            stock_predictions: 股票预测 DataFrame
            total_position: 总仓位
            max_single_weight: 单股最大权重
            min_single_weight: 单股最小权重

        Returns:
            包含权重的 DataFrame
        """
        if stock_predictions.empty:
            return pd.DataFrame()

        df = stock_predictions.copy()

        # 确保有评分列
        if 'combined_score' not in df.columns:
            df['combined_score'] = 50

        # 基于评分计算权重
        scores = df['combined_score'].values
        weights = scores / scores.sum() if scores.sum() > 0 else np.ones(len(scores)) / len(scores)

        # 应用总仓位
        weights = weights * total_position

        # 应用约束
        weights = np.clip(weights, min_single_weight, max_single_weight)

        # 归一化
        weights = weights / weights.sum() * total_position

        df['weight'] = weights

        return df

    def rebalance(
        self,
        current_positions: Dict[str, float],
        target_positions: Dict[str, float],
        threshold: float = 0.01
    ) -> List[Dict]:
        """
        计算再平衡交易

        Args:
            current_positions: 当前持仓 {'stock_code': weight}
            target_positions: 目标持仓 {'stock_code': weight}
            threshold: 调整阈值 (权重变化小于此值不调整)

        Returns:
            交易列表 [{'stock': code, 'action': 'buy/sell', 'delta': weight_change}]
        """
        trades = []
        all_stocks = set(current_positions.keys()) | set(target_positions.keys())

        for stock in all_stocks:
            current = current_positions.get(stock, 0)
            target = target_positions.get(stock, 0)
            delta = target - current

            # 权重变化超过阈值才交易
            if abs(delta) > threshold:
                trades.append({
                    'stock': stock,
                    'action': 'buy' if delta > 0 else 'sell',
                    'delta': abs(delta),
                    'current_weight': current,
                    'target_weight': target
                })

        # 按调整幅度排序（大调整优先）
        trades.sort(key=lambda x: x['delta'], reverse=True)

        logger.info(f"再平衡交易数: {len(trades)}")

        return trades

    def calculate_risk_adjusted_position(
        self,
        base_position: float,
        portfolio_volatility: float,
        target_volatility: float = 0.15,
        max_drawdown_limit: float = 0.20
    ) -> float:
        """
        风险调整仓位

        根据组合波动率和最大回撤限制调整仓位

        Args:
            base_position: 基础仓位
            portfolio_volatility: 组合波动率
            target_volatility: 目标波动率
            max_drawdown_limit: 最大回撤限制

        Returns:
            调整后仓位
        """
        # 波动率调整
        if portfolio_volatility > target_volatility:
            vol_adj = target_volatility / portfolio_volatility
        else:
            vol_adj = 1.0

        # 回撤限制调整
        # 假设最大回撤约为波动率的 2-3 倍
        implied_max_dd = portfolio_volatility * 2.5
        if implied_max_dd > max_drawdown_limit:
            dd_adj = max_drawdown_limit / implied_max_dd
        else:
            dd_adj = 1.0

        # 综合调整
        risk_adj = min(vol_adj, dd_adj)
        adjusted_position = base_position * risk_adj

        return min(1.0, max(0.1, adjusted_position))

    def get_position_suggestion(
        self,
        market_state: str,
        prediction_confidence: float = 0.5,
        volatility: float = 0.2
    ) -> Dict:
        """
        获取仓位建议

        Args:
            market_state: 市场状态
            prediction_confidence: 预测置信度
            volatility: 波动率

        Returns:
            仓位建议字典
        """
        position = self.calculate_position_size(
            market_state, prediction_confidence, volatility
        )

        suggestions = {
            self.BULL: {
                'strategy': '进攻型',
                'description': '高仓位运行，把握上涨机会',
                'stop_loss_range': '8-12%',
                'take_profit_range': '15-25%'
            },
            self.BEAR: {
                'strategy': '防御型',
                'description': '低仓位防守，控制回撤',
                'stop_loss_range': '5-8%',
                'take_profit_range': '5-10%'
            },
            self.SIDEWAYS: {
                'strategy': '平衡型',
                'description': '中等仓位，波段操作',
                'stop_loss_range': '6-10%',
                'take_profit_range': '10-15%'
            }
        }

        suggestion = suggestions.get(market_state, suggestions[self.SIDEWAYS])

        return {
            'market_state': market_state,
            'suggested_position': position,
            'position_range': f"{max(0.1, position - 0.1):.0%} - {min(1.0, position + 0.1):.0%}",
            'strategy': suggestion['strategy'],
            'description': suggestion['description'],
            'stop_loss_range': suggestion['stop_loss_range'],
            'take_profit_range': suggestion['take_profit_range']
        }


def main():
    """测试函数"""
    print("=" * 70)
    print("仓位管理测试")
    print("=" * 70)

    pm = PositionManager()

    # 测试市场状态识别
    print("\n【市场状态识别】")
    state, indicators = pm.detect_market_state()
    print(f"当前市场状态: {state}")
    if indicators:
        print(f"MA 比例: {indicators['ma_ratio']:.3f}")
        print(f"波动率: {indicators['volatility']:.1%}")

    # 测试仓位计算
    print("\n【仓位计算】")
    for market_state in [PositionManager.BULL, PositionManager.BEAR, PositionManager.SIDEWAYS]:
        position = pm.calculate_position_size(market_state, prediction_confidence=0.6, volatility=0.18)
        print(f"{market_state}: {position:.0%}")

    # 测试仓位建议
    print("\n【仓位建议】")
    suggestion = pm.get_position_suggestion(state, prediction_confidence=0.6, volatility=0.18)
    print(f"市场状态: {suggestion['market_state']}")
    print(f"建议仓位: {suggestion['suggested_position']:.0%}")
    print(f"仓位区间: {suggestion['position_range']}")
    print(f"策略类型: {suggestion['strategy']}")
    print(f"止损区间: {suggestion['stop_loss_range']}")
    print(f"止盈区间: {suggestion['take_profit_range']}")


if __name__ == "__main__":
    main()