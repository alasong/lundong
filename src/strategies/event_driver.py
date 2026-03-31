"""
事件驱动策略模块
实现财报季、政策事件等事件驱动策略
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
from dataclasses import dataclass
import sys
import os

from ..data.database import SQLiteDatabase, get_database


@dataclass
class EventSignal:
    """事件信号"""
    event_type: str
    concept_code: str
    concept_name: str = ''
    signal_type: str = 'increase_weight'  # increase_weight, decrease_weight, hold
    factor: float = 1.0
    reason: str = ''
    start_date: str = ''
    end_date: str = ''


class EventDriver:
    """
    事件驱动策略

    功能：
    1. 财报季事件驱动
    2. 政策事件驱动
    3. 经济数据发布驱动
    """

    # 财报季时间表 (月, 日)
    EARNINGS_SEASONS = [
        {'name': '年报预告', 'start': (1, 1), 'end': (1, 31), 'impact': 'high'},
        {'name': '年报披露', 'start': (1, 15), 'end': (4, 30), 'impact': 'high'},
        {'name': '一季报', 'start': (4, 1), 'end': (4, 30), 'impact': 'medium'},
        {'name': '半年报预告', 'start': (7, 1), 'end': (7, 31), 'impact': 'high'},
        {'name': '半年报披露', 'start': (7, 15), 'end': (8, 31), 'impact': 'high'},
        {'name': '三季报', 'start': (10, 1), 'end': (10, 31), 'impact': 'medium'},
    ]

    # 政策事件日历
    POLICY_EVENTS = {
        'central_bank_meeting': {
            'name': '央行会议',
            'frequency': 'monthly',
            'beneficiaries': ['银行', '券商', '保险'],
            'impact': 'high'
        },
        'two_sessions': {
            'name': '两会',
            'period': [(3, 3), (3, 15)],
            'beneficiaries': ['国企改革', '新能源', '科技', '基建'],
            'impact': 'high'
        },
        'economic_data_release': {
            'name': '经济数据发布',
            'frequency': 'monthly',
            'beneficiaries': [],
            'impact': 'medium'
        },
        'financial_stability_report': {
            'name': '金融稳定报告',
            'frequency': 'quarterly',
            'beneficiaries': ['金融'],
            'impact': 'medium'
        }
    }

    # 政策受益板块映射
    POLICY_BENEFICIARIES = {
        '货币政策宽松': ['银行', '券商', '地产', '基建'],
        '产业政策扶持': ['新能源', '半导体', '人工智能', '生物医药'],
        '改革预期': ['国企改革', '混改', '军工'],
        '消费刺激': ['汽车', '家电', '消费电子', '食品饮料'],
        '基建投资': ['建筑', '建材', '工程机械', '钢铁']
    }

    def __init__(
        self,
        earnings_weight_factor: float = 1.2,
        policy_weight_factor: float = 1.3,
        signal_validity_days: int = 10
    ):
        """
        初始化事件驱动策略

        Args:
            earnings_weight_factor: 财报季权重因子
            policy_weight_factor: 政策事件权重因子
            signal_validity_days: 信号有效天数
        """
        self.earnings_weight_factor = earnings_weight_factor
        self.policy_weight_factor = policy_weight_factor
        self.signal_validity_days = signal_validity_days

        logger.info(f"事件驱动策略初始化: 财报因子={earnings_weight_factor}, 政策因子={policy_weight_factor}")

    def check_upcoming_events(self, date: str = None) -> List[Dict]:
        """
        检查近期事件

        Args:
            date: 基准日期 (YYYYMMDD)

        Returns:
            事件列表
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')

        dt = datetime.strptime(date, '%Y%m%d')
        month, day = dt.month, dt.day

        events = []

        # 检查财报季
        for season in self.EARNINGS_SEASONS:
            start_m, start_d = season['start']
            end_m, end_d = season['end']

            # 判断是否在财报季期间
            in_season = False
            if start_m == end_m:
                in_season = (month == start_m and start_d <= day <= end_d)
            elif start_m < end_m:
                in_season = (month > start_m or (month == start_m and day >= start_d)) and \
                           (month < end_m or (month == end_m and day <= end_d))
            else:  # 跨年情况
                in_season = (month > start_m or (month == start_m and day >= start_d)) or \
                           (month < end_m or (month == end_m and day <= end_d))

            if in_season:
                events.append({
                    'type': 'earnings_season',
                    'name': season['name'],
                    'period': f"{start_m}月{start_d}日-{end_m}月{end_d}日",
                    'impact': season['impact'],
                    'beneficiaries': []  # 业绩预增板块
                })

        # 检查政策事件
        # 两会期间 (3月3日-3月15日)
        if month == 3 and 3 <= day <= 15:
            events.append({
                'type': 'policy_event',
                'name': '两会',
                'period': '3月3日-3月15日',
                'impact': 'high',
                'beneficiaries': self.POLICY_EVENTS['two_sessions']['beneficiaries']
            })

        # 央行会议（每月中旬）
        if 10 <= day <= 20:
            events.append({
                'type': 'policy_event',
                'name': '央行会议',
                'period': f'{month}月中旬',
                'impact': 'medium',
                'beneficiaries': self.POLICY_EVENTS['central_bank_meeting']['beneficiaries']
            })

        logger.info(f"日期 {date} 检测到 {len(events)} 个事件")

        return events

    def generate_event_signals(
        self,
        events: List[Dict],
        predictions: pd.DataFrame,
        concept_map: Optional[Dict[str, str]] = None
    ) -> List[EventSignal]:
        """
        基于事件生成信号

        Args:
            events: 事件列表
            predictions: 预测 DataFrame
                - concept_code, combined_score, earnings_forecast (可选)
            concept_map: 板块代码到名称的映射

        Returns:
            事件信号列表
        """
        signals = []
        today = datetime.now().strftime('%Y%m%d')
        valid_until = (datetime.now() + timedelta(days=self.signal_validity_days)).strftime('%Y%m%d')

        for event in events:
            event_type = event.get('type', '')
            event_name = event.get('name', '')
            beneficiaries = event.get('beneficiaries', [])
            impact = event.get('impact', 'medium')

            # 根据影响力确定权重因子
            if impact == 'high':
                factor = self.earnings_weight_factor if event_type == 'earnings_season' else self.policy_weight_factor
            else:
                factor = 1.1  # 中等影响力

            if event_type == 'earnings_season':
                # 财报季: 关注业绩预增板块
                for _, pred in predictions.iterrows():
                    concept_code = pred.get('concept_code', '')
                    concept_name = pred.get('concept_name', concept_map.get(concept_code, '') if concept_map else '')
                    earnings_forecast = pred.get('earnings_forecast', 'neutral')

                    if earnings_forecast == 'positive':
                        signals.append(EventSignal(
                            event_type='earnings_season',
                            concept_code=concept_code,
                            concept_name=concept_name,
                            signal_type='increase_weight',
                            factor=factor,
                            reason=f"{event_name}: 业绩预增",
                            start_date=today,
                            end_date=valid_until
                        ))
                    elif earnings_forecast == 'negative':
                        signals.append(EventSignal(
                            event_type='earnings_season',
                            concept_code=concept_code,
                            concept_name=concept_name,
                            signal_type='decrease_weight',
                            factor=0.8,
                            reason=f"{event_name}: 业绩预警",
                            start_date=today,
                            end_date=valid_until
                        ))

            elif event_type == 'policy_event':
                # 政策事件: 关注受益板块
                for beneficiary in beneficiaries:
                    # 查找对应板块
                    matched = self._match_concept(beneficiary, predictions, concept_map)
                    for concept_code, concept_name in matched:
                        signals.append(EventSignal(
                            event_type='policy_event',
                            concept_code=concept_code,
                            concept_name=concept_name,
                            signal_type='increase_weight',
                            factor=factor,
                            reason=f"{event_name} 受益板块",
                            start_date=today,
                            end_date=valid_until
                        ))

        # 去重（同一板块取最大因子）
        signal_map = {}
        for sig in signals:
            key = (sig.event_type, sig.concept_code)
            if key not in signal_map or sig.factor > signal_map[key].factor:
                signal_map[key] = sig

        logger.info(f"生成 {len(signal_map)} 个事件信号")

        return list(signal_map.values())

    def _match_concept(
        self,
        keyword: str,
        predictions: pd.DataFrame,
        concept_map: Optional[Dict] = None
    ) -> List[Tuple[str, str]]:
        """匹配板块"""
        matched = []

        if 'concept_name' in predictions.columns:
            # 直接从预测数据匹配
            for _, row in predictions.iterrows():
                name = str(row.get('concept_name', ''))
                if keyword in name:
                    matched.append((row['concept_code'], name))

        if concept_map:
            # 从映射字典匹配
            for code, name in concept_map.items():
                if keyword in name:
                    matched.append((code, name))

        return matched

    def apply_event_signals(
        self,
        predictions: pd.DataFrame,
        signals: List[EventSignal]
    ) -> pd.DataFrame:
        """
        应用事件信号到预测结果

        Args:
            predictions: 预测 DataFrame
            signals: 事件信号列表

        Returns:
            调整后的预测 DataFrame
        """
        if predictions.empty or not signals:
            return predictions

        df = predictions.copy()

        # 创建信号映射
        signal_map = {}
        for sig in signals:
            signal_map[sig.concept_code] = (sig.signal_type, sig.factor)

        # 应用信号
        def adjust_score(row):
            concept_code = row.get('concept_code', '')
            if concept_code in signal_map:
                signal_type, factor = signal_map[concept_code]
                if signal_type == 'increase_weight':
                    return row['combined_score'] * factor
                elif signal_type == 'decrease_weight':
                    return row['combined_score'] * factor
            return row['combined_score']

        df['adjusted_score'] = df.apply(adjust_score, axis=1)

        return df

    def get_event_calendar(self, year: int = None) -> pd.DataFrame:
        """
        获取事件日历

        Args:
            year: 年份

        Returns:
            事件日历 DataFrame
        """
        if year is None:
            year = datetime.now().year

        events = []

        # 财报季
        for season in self.EARNINGS_SEASONS:
            start_m, start_d = season['start']
            end_m, end_d = season['end']

            events.append({
                'date': f"{year}-{start_m:02d}-{start_d:02d}",
                'end_date': f"{year}-{end_m:02d}-{end_d:02d}",
                'event': season['name'],
                'type': 'earnings_season',
                'impact': season['impact']
            })

        # 政策事件
        events.append({
            'date': f"{year}-03-03",
            'end_date': f"{year}-03-15",
            'event': '两会',
            'type': 'policy_event',
            'impact': 'high'
        })

        # 每月央行会议
        for month in range(1, 13):
            events.append({
                'date': f"{year}-{month:02d}-15",
                'end_date': f"{year}-{month:02d}-15",
                'event': '央行会议',
                'type': 'policy_event',
                'impact': 'medium'
            })

        df = pd.DataFrame(events)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        return df

    def get_active_signals(
        self,
        signals: List[EventSignal],
        current_date: str = None
    ) -> List[EventSignal]:
        """
        获取当前有效的信号

        Args:
            signals: 信号列表
            current_date: 当前日期

        Returns:
            有效信号列表
        """
        if current_date is None:
            current_date = datetime.now().strftime('%Y%m%d')

        active_signals = []
        current_dt = datetime.strptime(current_date, '%Y%m%d')

        for sig in signals:
            if sig.start_date and sig.end_date:
                start_dt = datetime.strptime(sig.start_date, '%Y%m%d')
                end_dt = datetime.strptime(sig.end_date, '%Y%m%d')

                if start_dt <= current_dt <= end_dt:
                    active_signals.append(sig)

        return active_signals


def main():
    """测试函数"""
    print("=" * 70)
    print("事件驱动策略测试")
    print("=" * 70)

    driver = EventDriver()

    # 测试事件检测
    print("\n【事件检测】")
    test_dates = ['20260315', '20260415', '20260801']
    for date in test_dates:
        events = driver.check_upcoming_events(date)
        print(f"\n日期 {date}:")
        for event in events:
            print(f"  - {event['name']} ({event['type']})")

    # 测试信号生成
    print("\n【信号生成】")
    predictions = pd.DataFrame([
        {'concept_code': '881101.TI', 'concept_name': '银行', 'combined_score': 70, 'earnings_forecast': 'positive'},
        {'concept_code': '881102.TI', 'concept_name': '新能源', 'combined_score': 80, 'earnings_forecast': 'positive'},
        {'concept_code': '881103.TI', 'concept_name': '半导体', 'combined_score': 75, 'earnings_forecast': 'neutral'},
        {'concept_code': '881104.TI', 'concept_name': '房地产', 'combined_score': 50, 'earnings_forecast': 'negative'},
    ])

    events = driver.check_upcoming_events('20260315')
    signals = driver.generate_event_signals(events, predictions)

    print(f"\n生成信号数: {len(signals)}")
    for sig in signals:
        print(f"  [{sig.signal_type}] {sig.concept_name}: 因子={sig.factor:.2f}, 原因={sig.reason}")

    # 测试应用信号
    print("\n【应用信号】")
    adjusted = driver.apply_event_signals(predictions, signals)
    print("\n调整前后对比:")
    for _, row in adjusted.iterrows():
        print(f"  {row['concept_name']}: {row['combined_score']:.1f} -> {row['adjusted_score']:.1f}")

    # 测试事件日历
    print("\n【事件日历（2026年）】")
    calendar = driver.get_event_calendar(2026)
    print(calendar.head(10).to_string())


if __name__ == "__main__":
    main()