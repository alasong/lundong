"""
轮动策略模块
实现板块轮动触发条件和执行规则
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from loguru import logger
from dataclasses import dataclass
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class SignalType(Enum):
    """信号类型"""
    ENTER = 'enter'        # 买入信号
    EXIT = 'exit'          # 卖出信号
    ROTATE = 'rotate'      # 轮动信号
    HOLD = 'hold'          # 持有信号


@dataclass
class Signal:
    """交易信号"""
    type: SignalType
    concept_code: str
    concept_name: str = ''
    score: float = 0.0
    reason: str = ''
    target_concept: Optional[str] = None  # 轮动目标
    confidence: float = 0.0
    timestamp: str = ''


class RotationStrategy:
    """
    轮动策略执行器

    功能：
    1. 生成买入/卖出/轮动信号
    2. 基于热点衰减、预测差异、成交量异常等触发条件
    3. 支持多种轮动模式
    """

    def __init__(
        self,
        score_decay_threshold: float = 0.20,      # 热点评分衰减阈值
        prediction_diff_threshold: float = 0.15,   # 预测差异阈值
        volume_surge_threshold: float = 2.0,       # 成交量放大倍数
        rsi_overbought: float = 70,                # RSI 超买线
        rsi_oversold: float = 30,                  # RSI 超卖线
        min_confidence: float = 0.3                # 最小信号置信度
    ):
        """
        初始化轮动策略

        Args:
            score_decay_threshold: 热点评分衰减阈值（触发退出）
            prediction_diff_threshold: 预测得分差异阈值（触发轮动）
            volume_surge_threshold: 成交量放大倍数（触发追涨）
            rsi_overbought: RSI 超买线
            rsi_oversold: RSI 超卖线
            min_confidence: 最小信号置信度
        """
        self.score_decay_threshold = score_decay_threshold
        self.prediction_diff_threshold = prediction_diff_threshold
        self.volume_surge_threshold = volume_surge_threshold
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.min_confidence = min_confidence

        logger.info(f"轮动策略初始化: 衰减阈值={score_decay_threshold}, 预测差异={prediction_diff_threshold}")

    def generate_signals(
        self,
        hotspot_scores: pd.DataFrame,
        predictions: pd.DataFrame,
        market_state: str = 'SIDEWAYS',
        current_positions: Optional[List[Dict]] = None
    ) -> List[Signal]:
        """
        生成具体交易信号

        Args:
            hotspot_scores: 热点评分 DataFrame
                - concept_code, concept_name, score, prev_score, volume_ratio, rsi
            predictions: 预测得分 DataFrame
                - concept_code, pred_1d, pred_5d, pred_20d, combined_score
            market_state: 市场状态 ('BULL', 'BEAR', 'SIDEWAYS')
            current_positions: 当前持仓列表

        Returns:
            交易信号列表
        """
        signals = []
        timestamp = datetime.now().strftime('%Y%m%d %H:%M:%S')

        if hotspot_scores.empty:
            logger.warning("热点评分为空，无法生成信号")
            return signals

        # 确保 prev_score 列存在
        if 'prev_score' not in hotspot_scores.columns:
            hotspot_scores = hotspot_scores.copy()
            hotspot_scores['prev_score'] = hotspot_scores.get('score', 50)

        for _, row in hotspot_scores.iterrows():
            concept_code = row.get('concept_code', '')
            concept_name = row.get('concept_name', '')
            current_score = row.get('score', 0)
            prev_score = row.get('prev_score', current_score)
            volume_ratio = row.get('volume_ratio', 1.0)
            rsi = row.get('rsi', 50)

            # 获取预测数据
            pred_row = predictions[predictions['concept_code'] == concept_code]
            pred_score = pred_row['combined_score'].iloc[0] if not pred_row.empty else 50

            # 1. 检查退出条件：热点评分衰减
            exit_signal = self._check_exit_condition(
                concept_code, concept_name, current_score, prev_score, timestamp
            )
            if exit_signal:
                signals.append(exit_signal)
                continue  # 已触发退出，不再检查其他条件

            # 2. 检查轮动条件：存在更好的标的
            rotate_signal = self._check_rotation_condition(
                concept_code, concept_name, pred_score, predictions, timestamp
            )
            if rotate_signal:
                signals.append(rotate_signal)
                continue

            # 3. 检查追涨条件：成交量异常放大
            enter_signal = self._check_chase_condition(
                concept_code, concept_name, current_score, volume_ratio, rsi,
                market_state, timestamp
            )
            if enter_signal:
                signals.append(enter_signal)

        # 按置信度排序
        signals.sort(key=lambda x: x.confidence, reverse=True)

        logger.info(f"生成 {len(signals)} 个交易信号")
        return signals

    def _check_exit_condition(
        self,
        concept_code: str,
        concept_name: str,
        current_score: float,
        prev_score: float,
        timestamp: str
    ) -> Optional[Signal]:
        """
        检查退出条件

        触发条件：
        1. 热点评分衰减 > 阈值
        2. 评分绝对值过低 (< 30)
        """
        # 计算评分衰减
        if prev_score > 0:
            score_decay = (prev_score - current_score) / prev_score
        else:
            score_decay = 0

        # 评分衰减超过阈值
        if score_decay > self.score_decay_threshold:
            return Signal(
                type=SignalType.EXIT,
                concept_code=concept_code,
                concept_name=concept_name,
                score=current_score,
                reason=f"热点衰减: {score_decay:.1%} > {self.score_decay_threshold:.1%}",
                confidence=min(score_decay / self.score_decay_threshold, 1.0),
                timestamp=timestamp
            )

        # 评分过低
        if current_score < 30:
            return Signal(
                type=SignalType.EXIT,
                concept_code=concept_code,
                concept_name=concept_name,
                score=current_score,
                reason=f"评分过低: {current_score:.1f} < 30",
                confidence=0.8,
                timestamp=timestamp
            )

        return None

    def _check_rotation_condition(
        self,
        concept_code: str,
        concept_name: str,
        current_pred_score: float,
        predictions: pd.DataFrame,
        timestamp: str
    ) -> Optional[Signal]:
        """
        检查轮动条件

        触发条件：
        1. 存在预测得分显著更高的板块
        """
        if predictions.empty:
            return None

        # 找到预测得分最高的板块
        best_pred = predictions.nlargest(1, 'combined_score').iloc[0]
        best_concept = best_pred.get('concept_code', '')
        best_score = best_pred.get('combined_score', 0)
        best_name = best_pred.get('concept_name', best_concept)

        # 如果当前板块就是最佳板块，不轮动
        if best_concept == concept_code:
            return None

        # 计算预测差异
        if current_pred_score > 0:
            pred_diff = (best_score - current_pred_score) / current_pred_score
        else:
            pred_diff = best_score / 100

        # 预测差异超过阈值
        if pred_diff > self.prediction_diff_threshold:
            return Signal(
                type=SignalType.ROTATE,
                concept_code=concept_code,
                concept_name=concept_name,
                score=current_pred_score,
                target_concept=best_concept,
                reason=f"轮动至 {best_name}: 预测差异 {pred_diff:.1%}",
                confidence=min(pred_diff / self.prediction_diff_threshold, 1.0),
                timestamp=timestamp
            )

        return None

    def _check_chase_condition(
        self,
        concept_code: str,
        concept_name: str,
        current_score: float,
        volume_ratio: float,
        rsi: float,
        market_state: str,
        timestamp: str
    ) -> Optional[Signal]:
        """
        检查追涨条件

        触发条件：
        1. 成交量异常放大 (> 阈值)
        2. 热点评分较高 (> 60)
        3. RSI 未超买
        4. 市场状态不是熊市
        """
        # 熊市不追涨
        if market_state == 'BEAR':
            return None

        # 成交量放大且评分较高
        if volume_ratio >= self.volume_surge_threshold and current_score > 60:
            # RSI 超买则降低置信度
            confidence = volume_ratio / self.volume_surge_threshold
            reason = f"成交量放大 {volume_ratio:.1f}x"

            if rsi > self.rsi_overbought:
                reason += f" (RSI 超买: {rsi:.1f})"
                confidence *= 0.5  # 超买降低置信度
            elif rsi > 60:
                reason += f" (RSI 偏高: {rsi:.1f})"

            if confidence >= self.min_confidence:
                return Signal(
                    type=SignalType.ENTER,
                    concept_code=concept_code,
                    concept_name=concept_name,
                    score=current_score,
                    reason=reason,
                    confidence=min(confidence, 1.0),
                    timestamp=timestamp
                )

        return None

    def generate_rsi_signals(
        self,
        rsi_data: pd.DataFrame,
        market_state: str = 'SIDEWAYS'
    ) -> List[Signal]:
        """
        基于 RSI 生成轮动信号

        Args:
            rsi_data: RSI 数据 DataFrame
                - concept_code, concept_name, rsi
            market_state: 市场状态

        Returns:
            信号列表
        """
        signals = []
        timestamp = datetime.now().strftime('%Y%m%d %H:%M:%S')

        if rsi_data.empty:
            return signals

        for _, row in rsi_data.iterrows():
            concept_code = row.get('concept_code', '')
            concept_name = row.get('concept_name', '')
            rsi = row.get('rsi', 50)

            # RSI 超买 - 轮动信号
            if rsi > self.rsi_overbought:
                signals.append(Signal(
                    type=SignalType.ROTATE,
                    concept_code=concept_code,
                    concept_name=concept_name,
                    score=rsi,
                    reason=f"RSI 超买: {rsi:.1f} > {self.rsi_overbought}",
                    confidence=(rsi - self.rsi_overbought) / (100 - self.rsi_overbought),
                    timestamp=timestamp
                ))

            # RSI 超卖 - 买入信号（牛市/震荡市）
            elif rsi < self.rsi_oversold and market_state != 'BEAR':
                signals.append(Signal(
                    type=SignalType.ENTER,
                    concept_code=concept_code,
                    concept_name=concept_name,
                    score=rsi,
                    reason=f"RSI 超卖: {rsi:.1f} < {self.rsi_oversold}",
                    confidence=(self.rsi_oversold - rsi) / self.rsi_oversold,
                    timestamp=timestamp
                ))

        return signals

    def prioritize_signals(
        self,
        signals: List[Signal],
        market_state: str
    ) -> List[Signal]:
        """
        根据市场状态对信号进行优先级排序

        Args:
            signals: 信号列表
            market_state: 市场状态

        Returns:
            排序后的信号列表
        """
        # 市场状态权重
        state_weights = {
            'BULL': {'ENTER': 1.2, 'ROTATE': 1.0, 'EXIT': 0.8, 'HOLD': 0.5},
            'BEAR': {'ENTER': 0.5, 'ROTATE': 0.8, 'EXIT': 1.2, 'HOLD': 1.0},
            'SIDEWAYS': {'ENTER': 1.0, 'ROTATE': 1.0, 'EXIT': 1.0, 'HOLD': 0.8}
        }

        weights = state_weights.get(market_state, state_weights['SIDEWAYS'])

        # 调整置信度
        for signal in signals:
            signal.confidence *= weights.get(signal.type.value, 1.0)

        # 按调整后置信度排序
        signals.sort(key=lambda x: x.confidence, reverse=True)

        return signals

    def get_signal_summary(self, signals: List[Signal]) -> Dict:
        """获取信号统计摘要"""
        if not signals:
            return {
                'total': 0,
                'enter': 0,
                'exit': 0,
                'rotate': 0,
                'hold': 0,
                'avg_confidence': 0
            }

        type_counts = {t.value: 0 for t in SignalType}
        total_confidence = 0

        for signal in signals:
            type_counts[signal.type.value] = type_counts.get(signal.type.value, 0) + 1
            total_confidence += signal.confidence

        return {
            'total': len(signals),
            'enter': type_counts.get('enter', 0),
            'exit': type_counts.get('exit', 0),
            'rotate': type_counts.get('rotate', 0),
            'hold': type_counts.get('hold', 0),
            'avg_confidence': total_confidence / len(signals)
        }


def main():
    """测试函数"""
    print("=" * 70)
    print("轮动策略测试")
    print("=" * 70)

    # 创建模拟数据
    hotspot_scores = pd.DataFrame([
        {'concept_code': '881101.TI', 'concept_name': '银行', 'score': 75, 'prev_score': 80, 'volume_ratio': 1.5, 'rsi': 55},
        {'concept_code': '881102.TI', 'concept_name': '房地产', 'score': 45, 'prev_score': 70, 'volume_ratio': 0.8, 'rsi': 45},
        {'concept_code': '881103.TI', 'concept_name': '新能源', 'score': 85, 'prev_score': 82, 'volume_ratio': 2.5, 'rsi': 72},
        {'concept_code': '881104.TI', 'concept_name': '半导体', 'score': 90, 'prev_score': 88, 'volume_ratio': 3.0, 'rsi': 68},
    ])

    predictions = pd.DataFrame([
        {'concept_code': '881101.TI', 'combined_score': 70},
        {'concept_code': '881102.TI', 'combined_score': 50},
        {'concept_code': '881103.TI', 'combined_score': 88},
        {'concept_code': '881104.TI', 'combined_score': 95},
    ])

    # 创建策略
    strategy = RotationStrategy()

    # 生成信号
    signals = strategy.generate_signals(
        hotspot_scores=hotspot_scores,
        predictions=predictions,
        market_state='BULL'
    )

    # 打印结果
    print(f"\n生成信号数: {len(signals)}")
    print("\n信号详情:")
    for signal in signals:
        print(f"  [{signal.type.value.upper()}] {signal.concept_name}")
        print(f"    原因: {signal.reason}")
        print(f"    置信度: {signal.confidence:.2f}")
        if signal.target_concept:
            print(f"    目标: {signal.target_concept}")

    # 信号摘要
    summary = strategy.get_signal_summary(signals)
    print(f"\n信号摘要: {summary}")


if __name__ == "__main__":
    main()