"""
市场状态识别模块
识别牛市、熊市、震荡市，并提供分场景策略建议
"""
import pandas as pd
import numpy as np
import sqlite3
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import SQLiteDatabase, get_database


class MarketRegimeDetector:
    """
    市场状态识别器

    市场状态：
    - bull: 牛市（上涨趋势，波动率低）
    - bear: 熊市（下跌趋势，波动率高）
    - sideways: 震荡市（无明显趋势）

    识别依据：
    1. 趋势：均线排列、MACD
    2. 波动率：VIX 类似指标
    3. 市场宽度：上涨股票占比
    """

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化市场状态识别器

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        logger.info("市场状态识别器初始化完成")

    def identify_regime(
        self,
        date: str = None,
        benchmark_code: str = '000300.SH'
    ) -> Dict:
        """
        识别当前市场状态

        Args:
            date: 基准日期
            benchmark_code: Benchmark 代码（默认沪深 300）

        Returns:
            市场状态字典
        """
        if date is None:
            date = self.db.get_latest_date()
            if date is None:
                return {'regime': 'unknown', 'confidence': 0}

        # 获取 Benchmark 数据
        df = self._get_benchmark_data(benchmark_code, date, lookback_days=250)

        if df.empty:
            logger.warning(f"无法获取 {benchmark_code} 数据")
            return {'regime': 'unknown', 'confidence': 0}

        # 计算识别指标
        indicators = self._calculate_indicators(df)

        # 判断市场状态
        regime, confidence = self._classify_regime(indicators)

        return {
            'regime': regime,
            'confidence': confidence,
            'indicators': indicators,
            'date': date,
            'regime_name': self._get_regime_name(regime)
        }

    def _get_benchmark_data(
        self,
        benchmark_code: str,
        date: str,
        lookback_days: int = 250
    ) -> pd.DataFrame:
        """获取 Benchmark 数据"""
        start_date = (datetime.strptime(date, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")

        # 尝试从 stock_daily 获取
        df = self.db.get_stock_data(benchmark_code, start_date, date)

        if df.empty:
            # 尝试从概念数据中获取（使用一个流动性好的板块作为基准）
            try:
                conn = sqlite3.connect('data/stock.db')
                cursor = conn.cursor()

                # 如果没有沪深 300，使用半导体板块作为替代（高流动性）
                alt_benchmarks = ['000300.SH', '399300.SZ', '885311.TI', '881101.TI']

                for code in alt_benchmarks:
                    cursor.execute("""
                        SELECT ts_code, trade_date, close, pct_change
                        FROM concept_daily
                        WHERE ts_code = ? AND trade_date <= ?
                        ORDER BY trade_date DESC
                        LIMIT ?
                    """, (code, date, lookback_days))

                    rows = cursor.fetchall()
                    if rows:
                        conn.close()
                        df = pd.DataFrame(rows, columns=['ts_code', 'trade_date', 'close', 'pct_chg'])
                        df = df.sort_values('trade_date')
                        logger.info(f"使用替代 Benchmark: {code}")
                        break

                conn.close()
            except Exception as e:
                logger.error(f"获取 Benchmark 数据失败：{e}")

        return df

    def _calculate_indicators(self, df: pd.DataFrame) -> Dict:
        """
        计算识别指标

        Args:
            df: 行情数据

        Returns:
            指标字典
        """
        closes = df['close'].values
        returns = df['pct_chg'].values / 100

        # 1. 趋势指标
        # 20 日、60 日、250 日均线
        ma20 = np.mean(closes[-20:]) if len(closes) >= 20 else closes[-1]
        ma60 = np.mean(closes[-60:]) if len(closes) >= 60 else closes[-1]
        ma250 = np.mean(closes[-250:]) if len(closes) >= 250 else closes[-1]

        current_price = closes[-1]

        # 均线排列
        bull_alignment = (current_price > ma20 > ma60 > ma250)
        bear_alignment = (current_price < ma20 < ma60 < ma250)

        # 2. 动量指标
        # RSI
        if len(returns) >= 14:
            gains = np.where(returns > 0, returns, 0)
            losses = np.where(returns < 0, -returns, 0)
            avg_gain = np.mean(gains[-14:])
            avg_loss = np.mean(losses[-14:])
            rs = avg_gain / avg_loss if avg_loss > 0 else 100
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 50

        # 250 日涨幅
        if len(closes) >= 250:
            return_250d = (closes[-1] / closes[-250] - 1) * 100
        else:
            return_250d = (closes[-1] / closes[0] - 1) * 100

        # 60 日涨幅
        if len(closes) >= 60:
            return_60d = (closes[-1] / closes[-60] - 1) * 100
        else:
            return_60d = (closes[-1] / closes[0] - 1) * 100

        # 3. 波动率指标
        if len(returns) >= 60:
            volatility_60d = np.std(returns[-60:]) * np.sqrt(252)
            volatility_250d = np.std(returns[-250:]) * np.sqrt(252)
        else:
            volatility_60d = np.std(returns) * np.sqrt(252) if len(returns) > 0 else 0
            volatility_250d = volatility_60d

        # 波动率分位数（简化）
        vol_percentile = np.searchsorted(np.sort([volatility_60d, volatility_250d]), volatility_60d) / 2 * 100

        # 4. 市场宽度代理指标（上涨天数占比）
        if len(returns) >= 60:
            up_ratio_60d = np.sum(returns[-60:] > 0) / 60
        else:
            up_ratio_60d = np.sum(returns > 0) / len(returns) if len(returns) > 0 else 0.5

        return {
            'ma20': ma20,
            'ma60': ma60,
            'ma250': ma250,
            'current_price': current_price,
            'bull_alignment': bull_alignment,
            'bear_alignment': bear_alignment,
            'rsi': rsi,
            'return_250d': return_250d,
            'return_60d': return_60d,
            'volatility_60d': volatility_60d,
            'volatility_250d': volatility_250d,
            'vol_percentile': vol_percentile,
            'up_ratio_60d': up_ratio_60d,
        }

    def _classify_regime(self, indicators: Dict) -> Tuple[str, float]:
        """
        根据指标分类市场状态

        Args:
            indicators: 指标字典

        Returns:
            (市场状态，置信度)
        """
        score = 0  # 正分为牛市倾向，负分为熊市倾向
        max_score = 0

        # 1. 均线排列（权重 30%）
        if indicators['bull_alignment']:
            score += 30
            max_score += 30
        elif indicators['bear_alignment']:
            score -= 30
            max_score += 30

        # 2. RSI（权重 20%）
        rsi = indicators['rsi']
        if rsi > 60:
            score += 20
            max_score += 20
        elif rsi < 40:
            score -= 20
            max_score += 20

        # 3. 250 日涨幅（权重 30%）
        return_250d = indicators['return_250d']
        if return_250d > 20:
            score += 30
            max_score += 30
        elif return_250d < -20:
            score -= 30
            max_score += 30

        # 4. 波动率（权重 20%）
        # 低波动率通常是牛市特征，高波动率是熊市特征
        vol = indicators['volatility_60d']
        if vol < 0.15:
            score += 20
            max_score += 20
        elif vol > 0.30:
            score -= 20
            max_score += 20

        # 判断市场状态
        if score > max_score * 0.3:
            regime = 'bull'
            confidence = score / max_score
        elif score < -max_score * 0.3:
            regime = 'bear'
            confidence = abs(score) / max_score
        else:
            regime = 'sideways'
            confidence = 1 - abs(score) / max_score

        return regime, min(confidence, 1.0)

    def _get_regime_name(self, regime: str) -> str:
        """获取市场状态中文名"""
        names = {
            'bull': '牛市',
            'bear': '熊市',
            'sideways': '震荡市',
            'unknown': '未知'
        }
        return names.get(regime, regime)

    def get_regime_history(
        self,
        start_date: str,
        end_date: str,
        benchmark_code: str = '000300.SH'
    ) -> pd.DataFrame:
        """
        获取历史市场状态

        Args:
            start_date: 开始日期
            end_date: 结束日期
            benchmark_code: Benchmark 代码

        Returns:
            历史状态 DataFrame
        """
        # 获取 Benchmark 数据
        df = self._get_benchmark_data(benchmark_code, end_date, lookback_days=500)

        if df.empty:
            return pd.DataFrame()

        # 筛选日期范围
        df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]

        if df.empty:
            return pd.DataFrame()

        # 滚动计算市场状态
        regimes = []
        dates = df['trade_date'].unique()

        for date in dates:
            # 使用该日期之前的数据进行识别
            hist_df = df[df['trade_date'] <= date]
            if len(hist_df) < 60:
                continue

            indicators = self._calculate_indicators(hist_df)
            regime, confidence = self._classify_regime(indicators)

            regimes.append({
                'trade_date': date,
                'regime': regime,
                'regime_name': self._get_regime_name(regime),
                'confidence': confidence,
                'close': hist_df[hist_df['trade_date'] == date]['close'].iloc[-1],
            })

        return pd.DataFrame(regimes)

    def get_strategy_suggestion(self, regime: str) -> Dict:
        """
        根据市场状态提供策略建议

        Args:
            regime: 市场状态

        Returns:
            策略建议字典
        """
        suggestions = {
            'bull': {
                'strategy': '进攻型',
                'position': '高仓位（80-100%）',
                'style': '成长股、高贝塔',
                'sectors': '科技、新能源、券商',
                'risk': '注意追高风险，适度止盈',
                'stop_loss': '放宽至 10-12%',
                'take_profit': '分批止盈 20-30%'
            },
            'bear': {
                'strategy': '防御型',
                'position': '低仓位（0-30%）',
                'style': '价值股、低贝塔、高股息',
                'sectors': '银行、公用事业、必需消费',
                'risk': '严格控制仓位，避免抄底',
                'stop_loss': '严格执行 5-8% 止损',
                'take_profit': '快进快出 5-10%'
            },
            'sideways': {
                'strategy': '平衡型',
                'position': '中等仓位（40-60%）',
                'style': '均衡配置，波段操作',
                'sectors': '消费、医药、龙头股',
                'risk': '高抛低吸，不追涨杀跌',
                'stop_loss': '8% 止损',
                'take_profit': '15-20% 止盈'
            },
            'unknown': {
                'strategy': '观望',
                'position': '等待明确信号',
                'style': 'N/A',
                'sectors': 'N/A',
                'risk': '保持低仓位观望',
                'stop_loss': 'N/A',
                'take_profit': 'N/A'
            }
        }

        return suggestions.get(regime, suggestions['unknown'])


def print_regime_report(regime_result: Dict):
    """打印市场状态报告"""
    print("\n" + "=" * 70)
    print("市场状态报告")
    print("=" * 70)

    regime = regime_result.get('regime', 'unknown')
    regime_name = regime_result.get('regime_name', '未知')
    confidence = regime_result.get('confidence', 0)

    print(f"\n当前状态：{regime_name} ({regime})")
    print(f"置信度：{confidence:.1%}")

    # 显示指标
    indicators = regime_result.get('indicators', {})
    if indicators:
        print("\n【识别指标】")
        print(f"  250 日涨幅：{indicators.get('return_250d', 0):.1f}%")
        print(f"  60 日涨幅：{indicators.get('return_60d', 0):.1f}%")
        print(f"  RSI: {indicators.get('rsi', 0):.1f}")
        print(f"  波动率：{indicators.get('volatility_60d', 0):.1%}")

    # 策略建议
    mrd = MarketRegimeDetector()
    suggestion = mrd.get_strategy_suggestion(regime)

    print("\n【策略建议】")
    print(f"  策略类型：{suggestion['strategy']}")
    print(f"  建议仓位：{suggestion['position']}")
    print(f"  风格偏好：{suggestion['style']}")
    print(f"  推荐板块：{suggestion['sectors']}")
    print(f"  风险提示：{suggestion['risk']}")
    print(f"  止损建议：{suggestion['stop_loss']}")
    print(f"  止盈建议：{suggestion['take_profit']}")

    print("\n" + "=" * 70)


def main():
    """测试函数"""
    print("=" * 70)
    print("市场状态识别测试")
    print("=" * 70)

    mrd = MarketRegimeDetector()

    # 识别当前市场状态
    regime_result = mrd.identify_regime()

    # 打印报告
    print_regime_report(regime_result)

    # 获取历史状态
    print("\n【历史市场状态（最近 10 个交易日）】")
    history = mrd.get_regime_history(
        start_date='20260101',
        end_date='20260311'
    )

    if not history.empty:
        print(history[['trade_date', 'regime_name', 'confidence']].tail(10).to_string())


if __name__ == "__main__":
    main()
