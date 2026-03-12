#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据质量验证模块
负责验证数据完整性、检测异常值、校验数据一致性
"""
import sys
import os
# 添加 src 目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
from database import get_database


class DataValidator:
    """
    数据质量验证器

    功能：
    1. 数据完整性检查 - 检测缺失数据
    2. 异常值检测 - 识别价格/成交量异常
    3. 数据一致性校验 - 复权校验、停牌校验
    4. 数据质量报告 - 生成质量评分
    """

    def __init__(self, db=None):
        """
        初始化验证器

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        logger.info("数据验证器初始化完成")

    def check_data_completeness(
        self,
        table_name: str = 'stock_daily',
        start_date: str = None,
        end_date: str = None
    ) -> Dict:
        """
        检查数据完整性

        Args:
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            完整性检查结果
        """
        logger.info(f"检查 {table_name} 数据完整性...")

        conn = sqlite3.connect('data/stock.db')

        # 获取所有股票代码
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT ts_code FROM {table_name}")
        stocks = [row[0] for row in cursor.fetchall()]

        if not stocks:
            return {'error': '未找到数据'}

        # 获取日期范围
        if start_date is None or end_date is None:
            cursor.execute(f"""
                SELECT MIN(trade_date), MAX(trade_date) FROM {table_name}
            """)
            row = cursor.fetchone()
            start_date = row[0] or '20200101'
            end_date = row[1] or datetime.now().strftime('%Y%m%d')

        # 计算预期交易日数量（约 250 天/年）
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        expected_days = int((end - start).days * 5 / 7)  # 约 250 个交易日/年

        results = []
        for ts_code in stocks:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
            """, (ts_code, start_date, end_date))

            actual_days = cursor.fetchone()[0]
            completeness = actual_days / expected_days * 100 if expected_days > 0 else 0

            results.append({
                'ts_code': ts_code,
                'expected_days': expected_days,
                'actual_days': actual_days,
                'completeness': round(completeness, 2),
                'missing_days': max(0, expected_days - actual_days)
            })

        conn.close()

        # 统计
        df = pd.DataFrame(results)
        summary = {
            'total_stocks': len(df),
            'avg_completeness': round(df['completeness'].mean(), 2),
            'min_completeness': round(df['completeness'].min(), 2),
            'max_completeness': round(df['completeness'].max(), 2),
            'low_completeness_stocks': len(df[df['completeness'] < 80]),
            'details': df.sort_values('completeness').head(20).to_dict('records')
        }

        logger.info(f"完整性检查完成：{summary['total_stocks']} 只股票，"
                   f"平均完整度 {summary['avg_completeness']}%")

        return summary

    def detect_price_anomalies(
        self,
        ts_code: str = None,
        threshold_pct: float = 20.0
    ) -> List[Dict]:
        """
        检测价格异常

        Args:
            ts_code: 股票代码（None 表示检查所有）
            threshold_pct: 异常阈值（单日涨跌幅超过该值）

        Returns:
            异常记录列表
        """
        logger.info(f"检测价格异常（阈值：{threshold_pct}%）...")

        conn = sqlite3.connect('data/stock.db')
        cursor = conn.cursor()

        if ts_code:
            stocks = [ts_code]
        else:
            cursor.execute("SELECT DISTINCT ts_code FROM stock_daily")
            stocks = [row[0] for row in cursor.fetchall()]

        anomalies = []

        for stock in stocks:
            cursor.execute("""
                SELECT ts_code, trade_date, close, pct_chg
                FROM stock_daily
                WHERE ts_code = ? AND ABS(pct_chg) > ?
                ORDER BY trade_date DESC
                LIMIT 10
            """, (stock, threshold_pct))

            for row in cursor.fetchall():
                anomalies.append({
                    'ts_code': row[0],
                    'trade_date': row[1],
                    'close': row[2],
                    'pct_chg': row[3],
                    'anomaly_type': 'price_surge' if row[3] > 0 else 'price_drop'
                })

        conn.close()

        logger.info(f"发现 {len(anomalies)} 条价格异常记录")
        return anomalies

    def detect_volume_anomalies(
        self,
        ts_code: str = None,
        threshold_ratio: float = 5.0
    ) -> List[Dict]:
        """
        检测成交量异常

        Args:
            ts_code: 股票代码
            threshold_ratio: 异常阈值（成交量/均量）

        Returns:
            异常记录列表
        """
        logger.info(f"检测成交量异常（阈值：{threshold_ratio}x）...")

        conn = sqlite3.connect('data/stock.db')
        cursor = conn.cursor()

        if ts_code:
            stocks = [ts_code]
        else:
            cursor.execute("SELECT DISTINCT ts_code FROM stock_daily")
            stocks = [row[0] for row in cursor.fetchall()]

        anomalies = []

        for stock in stocks:
            # 获取 20 日均量
            cursor.execute("""
                SELECT AVG(vol) FROM stock_daily
                WHERE ts_code = ? AND vol > 0
                ORDER BY trade_date DESC
                LIMIT 20
            """, (stock,))
            avg_vol = cursor.fetchone()[0]

            if avg_vol and avg_vol > 0:
                # 检测异常放量
                cursor.execute("""
                    SELECT ts_code, trade_date, vol, amount
                    FROM stock_daily
                    WHERE ts_code = ? AND vol > ?
                    ORDER BY trade_date DESC
                    LIMIT 10
                """, (stock, avg_vol * threshold_ratio))

                for row in cursor.fetchall():
                    vol_ratio = row[2] / avg_vol if avg_vol > 0 else 0
                    anomalies.append({
                        'ts_code': row[0],
                        'trade_date': row[1],
                        'vol': row[2],
                        'amount': row[3],
                        'vol_ratio': round(vol_ratio, 2),
                        'anomaly_type': 'volume_surge'
                    })

        conn.close()

        logger.info(f"发现 {len(anomalies)} 条成交量异常记录")
        return anomalies

    def check_data_consistency(self) -> Dict:
        """
        检查数据一致性

        Returns:
            一致性检查结果
        """
        logger.info("检查数据一致性...")

        conn = sqlite3.connect('data/stock.db')
        cursor = conn.cursor()

        issues = []

        # 1. 检查涨跌停价格是否合理
        cursor.execute("""
            SELECT ts_code, trade_date, close, pre_close, pct_chg
            FROM stock_daily
            WHERE pre_close > 0 AND pre_close IS NOT NULL
        """)

        for row in cursor.fetchall():
            ts_code, trade_date, close, pre_close, pct_chg = row

            # 计算理论涨跌幅
            if pre_close > 0:
                calc_pct = (close / pre_close - 1) * 100

                # 检查是否超过涨跌停限制（10% 或 20%）
                if abs(calc_pct) > 25:  # 留一些缓冲
                    issues.append({
                        'type': 'price_limit_breach',
                        'ts_code': ts_code,
                        'trade_date': trade_date,
                        'close': close,
                        'pre_close': pre_close,
                        'calc_pct': round(calc_pct, 2),
                        'reported_pct': pct_chg
                    })

        # 2. 检查停牌数据
        cursor.execute("""
            SELECT ts_code, COUNT(*) as suspended_days
            FROM stock_daily
            WHERE vol = 0 OR vol IS NULL
            GROUP BY ts_code
            HAVING suspended_days > 5
        """)

        suspensions = cursor.fetchall()
        for row in suspensions:
            issues.append({
                'type': 'long_suspension',
                'ts_code': row[0],
                'suspended_days': row[1]
            })

        # 3. 检查 OHLC 关系
        cursor.execute("""
            SELECT ts_code, trade_date, open, high, low, close
            FROM stock_daily
            WHERE high < low OR high < open OR high < close
               OR low > open OR low > close
        """)

        invalid_ohlc = cursor.fetchall()
        for row in invalid_ohlc:
            issues.append({
                'type': 'invalid_ohlc',
                'ts_code': row[0],
                'trade_date': row[1],
                'open': row[2],
                'high': row[3],
                'low': row[4],
                'close': row[5]
            })

        conn.close()

        summary = {
            'total_issues': len(issues),
            'by_type': {},
            'issues': issues[:50]  # 限制返回数量
        }

        # 按类型统计
        for issue in issues:
            issue_type = issue['type']
            if issue_type not in summary['by_type']:
                summary['by_type'][issue_type] = 0
            summary['by_type'][issue_type] += 1

        logger.info(f"一致性检查完成：发现 {len(issues)} 个问题")
        return summary

    def check_missing_dates(
        self,
        table_name: str = 'stock_daily',
        recent_days: int = 5
    ) -> List[str]:
        """
        检查近期是否有缺失的交易日

        Args:
            table_name: 表名
            recent_days: 检查最近多少天

        Returns:
            缺失数据的股票列表
        """
        logger.info(f"检查近 {recent_days} 个交易日数据完整性...")

        conn = sqlite3.connect('data/stock.db')
        cursor = conn.cursor()

        # 获取最新日期
        cursor.execute(f"""
            SELECT MAX(trade_date) FROM {table_name}
        """)
        latest_date = cursor.fetchone()[0]

        if not latest_date:
            return []

        # 计算检查起始日期（约 recent_days 个交易日前）
        latest = datetime.strptime(latest_date, '%Y%m%d')
        start = (latest - timedelta(days=recent_days * 2)).strftime('%Y%m%d')

        # 获取每只股票的记录数
        cursor.execute(f"""
            SELECT ts_code, COUNT(*) as cnt
            FROM {table_name}
            WHERE trade_date BETWEEN ? AND ?
            GROUP BY ts_code
            HAVING cnt < ?
        """, (start, latest_date, recent_days))

        missing = [row[0] for row in cursor.fetchall()]

        conn.close()

        logger.info(f"发现 {len(missing)} 只股票近期数据缺失")
        return missing

    def generate_quality_report(self) -> Dict:
        """
        生成数据质量综合报告

        Returns:
            质量报告
        """
        logger.info("生成数据质量综合报告...")

        # 1. 完整性检查
        completeness = self.check_data_completeness()

        # 2. 异常检测
        price_anomalies = self.detect_price_anomalies()
        volume_anomalies = self.detect_volume_anomalies()

        # 3. 一致性检查
        consistency = self.check_data_consistency()

        # 4. 计算综合评分
        score = 100

        # 完整度扣分
        if completeness.get('avg_completeness', 100) < 90:
            score -= (90 - completeness['avg_completeness']) * 0.5

        # 异常扣分
        score -= min(len(price_anomalies) * 0.1, 10)
        score -= min(len(volume_anomalies) * 0.1, 10)

        # 一致性问题扣分
        score -= min(consistency.get('total_issues', 0) * 0.5, 20)

        score = max(0, min(100, score))

        report = {
            'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'overall_score': round(score, 1),
            'completeness': completeness,
            'price_anomalies_count': len(price_anomalies),
            'volume_anomalies_count': len(volume_anomalies),
            'consistency_issues': consistency,
            'recommendations': []
        }

        # 生成建议
        if completeness.get('avg_completeness', 100) < 90:
            report['recommendations'].append("建议补充缺失的历史数据")

        if len(price_anomalies) > 10:
            report['recommendations'].append("建议核查价格异常数据")

        if consistency.get('total_issues', 0) > 5:
            report['recommendations'].append("建议修复数据一致性问题")

        if not report['recommendations']:
            report['recommendations'].append("数据质量良好，无需特别处理")

        logger.info(f"质量报告生成完成：综合评分 {score:.1f}/100")
        return report


def print_quality_report(report: Dict):
    """打印质量报告"""
    print("\n" + "=" * 70)
    print("数据质量综合报告")
    print("=" * 70)

    print(f"\n报告时间：{report['report_date']}")
    print(f"综合评分：{report['overall_score']:.1f}/100")

    print("\n【完整性检查】")
    comp = report['completeness']
    print(f"  股票总数：{comp.get('total_stocks', 'N/A')}")
    print(f"  平均完整度：{comp.get('avg_completeness', 'N/A')}%")
    print(f"  最低完整度：{comp.get('min_completeness', 'N/A')}%")
    print(f"  完整度<80%: {comp.get('low_completeness_stocks', 'N/A')} 只")

    print("\n【异常检测】")
    print(f"  价格异常：{report['price_anomalies_count']} 条")
    print(f"  成交量异常：{report['volume_anomalies_count']} 条")

    print("\n【一致性问题】")
    cons = report['consistency_issues']
    print(f"  问题总数：{cons.get('total_issues', 0)}")
    by_type = cons.get('by_type', {})
    for issue_type, count in by_type.items():
        print(f"    - {issue_type}: {count}")

    print("\n【建议】")
    for rec in report.get('recommendations', []):
        print(f"  - {rec}")

    print("=" * 70)


def main():
    """测试函数"""
    print("=" * 70)
    print("数据质量验证工具")
    print("=" * 70)

    validator = DataValidator()

    # 生成综合报告
    report = validator.generate_quality_report()
    print_quality_report(report)

    # 显示详细异常
    print("\n【价格异常 TOP10】")
    anomalies = validator.detect_price_anomalies(threshold_pct=15)
    for a in anomalies[:10]:
        print(f"  {a['ts_code']} {a['trade_date']}: {a['pct_chg']:.2f}% ({a['anomaly_type']})")

    print("\n【成交量异常 TOP10】")
    anomalies = validator.detect_volume_anomalies(threshold_ratio=5)
    for a in anomalies[:10]:
        print(f"  {a['ts_code']} {a['trade_date']}: {a['vol_ratio']:.1f}x 均量")


if __name__ == "__main__":
    main()
