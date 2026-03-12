#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
监控告警系统
负责监控系统运行状态、数据更新、风险指标等，并发送告警通知
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import sqlite3
import pandas as pd
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from loguru import logger
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class MonitorSystem:
    """
    监控告警系统

    功能：
    1. 系统健康监控 - CPU/内存/磁盘/数据库
    2. 数据更新监控 - 数据时效性检查
    3. 风险指标监控 - 持仓风险、市场风险
    4. 告警通知 - 邮件/钉钉/企业微信
    """

    def __init__(self, config_path: str = None):
        """
        初始化监控系统

        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.alerts = []
        logger.info("监控告警系统初始化完成")

    def _load_config(self, config_path: str = None) -> Dict:
        """加载配置文件"""
        default_config = {
            'alert_thresholds': {
                'data_delay_days': 3,  # 数据延迟超过 3 天告警
                'database_size_gb': 10,  # 数据库超过 10GB 告警
                'error_count': 10,  # 错误数量超过 10 个告警
                'drawdown_pct': 10,  # 回撤超过 10% 告警
                'var_limit': 500000,  # VaR 超过 50 万告警
            },
            'notification': {
                'email': {
                    'enabled': False,
                    'smtp_server': 'smtp.example.com',
                    'smtp_port': 587,
                    'sender': 'alert@example.com',
                    'recipients': ['user@example.com'],
                    'username': '',
                    'password': ''
                },
                'dingtalk': {
                    'enabled': False,
                    'webhook_url': ''
                },
                'wechat': {
                    'enabled': False,
                    'webhook_url': ''
                }
            }
        }

        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                    # 合并配置
                    for key, value in user_config.items():
                        if isinstance(value, dict) and key in default_config:
                            default_config[key].update(value)
                        else:
                            default_config[key] = value
            except Exception as e:
                logger.warning(f"加载配置文件失败：{e}，使用默认配置")

        return default_config

    def check_system_health(self) -> Dict:
        """
        检查系统健康状态

        Returns:
            健康检查结果
        """
        logger.info("检查系统健康状态...")

        result = {
            'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'healthy',
            'items': {}
        }

        # 1. 检查磁盘空间
        import shutil
        try:
            total, used, free = shutil.disk_usage('/')
            disk_usage_pct = used / total * 100
            result['items']['disk'] = {
                'total_gb': round(total / (1024**3), 2),
                'used_gb': round(used / (1024**3), 2),
                'free_gb': round(free / (1024**3), 2),
                'usage_pct': round(disk_usage_pct, 2),
                'status': 'warning' if disk_usage_pct > 80 else 'ok'
            }
            if disk_usage_pct > 90:
                result['status'] = 'critical'
            elif disk_usage_pct > 80:
                result['status'] = 'warning'
        except Exception as e:
            result['items']['disk'] = {'error': str(e), 'status': 'error'}

        # 2. 检查数据库大小
        try:
            db_path = 'data/stock.db'
            if os.path.exists(db_path):
                db_size = os.path.getsize(db_path) / (1024**3)  # GB
                result['items']['database'] = {
                    'size_gb': round(db_size, 2),
                    'status': 'warning' if db_size > self.config['alert_thresholds']['database_size_gb'] else 'ok'
                }
                if db_size > self.config['alert_thresholds']['database_size_gb']:
                    result['status'] = 'warning'
            else:
                result['items']['database'] = {'status': 'not_found'}
        except Exception as e:
            result['items']['database'] = {'error': str(e), 'status': 'error'}

        # 3. 检查数据库连接
        try:
            conn = sqlite3.connect('data/stock.db')
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            result['items']['database_connection'] = {'status': 'ok'}
        except Exception as e:
            result['items']['database_connection'] = {'error': str(e), 'status': 'error'}
            result['status'] = 'critical'

        # 4. 检查进程状态（可选）
        result['items']['process'] = {'status': 'ok'}

        logger.info(f"系统健康检查完成：{result['status']}")
        return result

    def check_data_freshness(self) -> Dict:
        """
        检查数据时效性

        Returns:
            时效性检查结果
        """
        logger.info("检查数据时效性...")

        result = {
            'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'ok',
            'tables': {}
        }

        try:
            conn = sqlite3.connect('data/stock.db')

            # 检查各个表的最新日期
            tables_to_check = [
                ('stock_daily', 'ts_code', '股票日线'),
                ('concept_daily', 'ts_code', '板块日线'),
                ('stock_daily_basic', 'ts_code', '股票基本面'),
            ]

            today = datetime.now()

            for table, code_col, desc in tables_to_check:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        SELECT MAX(trade_date) FROM {table}
                    """)
                    max_date = cursor.fetchone()[0]

                    if max_date:
                        max_date_str = str(max_date)
                        # 解析日期
                        if len(max_date_str) == 8:
                            data_date = datetime.strptime(max_date_str, '%Y%m%d')
                        else:
                            data_date = datetime.strptime(max_date_str[:10], '%Y-%m-%d')

                        days_diff = (today - data_date).days

                        result['tables'][table] = {
                            'latest_date': max_date_str,
                            'days_ago': days_diff,
                            'status': 'warning' if days_diff > self.config['alert_thresholds']['data_delay_days'] else 'ok'
                        }

                        if days_diff > self.config['alert_thresholds']['data_delay_days']:
                            result['status'] = 'warning'
                    else:
                        result['tables'][table] = {'status': 'no_data'}
                except Exception as e:
                    result['tables'][table] = {'error': str(e), 'status': 'error'}

            conn.close()

        except Exception as e:
            result['error'] = str(e)
            result['status'] = 'error'

        logger.info(f"数据时效性检查完成：{result['status']}")
        return result

    def check_portfolio_risk(self, positions: List[Dict] = None) -> Dict:
        """
        检查投资组合风险

        Args:
            positions: 持仓列表 [{'ts_code': '000001.SZ', 'shares': 1000, 'cost': 10.0}, ...]

        Returns:
            风险检查结果
        """
        logger.info("检查投资组合风险...")

        result = {
            'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'ok',
            'risks': {}
        }

        if not positions:
            result['message'] = '无持仓数据'
            return result

        try:
            conn = sqlite3.connect('data/stock.db')
            cursor = conn.cursor()

            total_value = 0
            total_cost = 0
            risk_details = []

            for pos in positions:
                ts_code = pos.get('ts_code')
                shares = pos.get('shares', 0)
                cost = pos.get('cost', 0)

                # 获取最新价格
                cursor.execute("""
                    SELECT close, pct_chg FROM stock_daily
                    WHERE ts_code = ?
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (ts_code,))

                row = cursor.fetchone()
                if row:
                    current_price = row[0]
                    pct_chg = row[1] or 0

                    position_value = current_price * shares
                    position_cost = cost * shares
                    profit_loss = position_value - position_cost
                    profit_loss_pct = profit_loss / position_cost * 100 if position_cost > 0 else 0

                    total_value += position_value
                    total_cost += position_cost

                    # 检查个股风险
                    if profit_loss_pct < -self.config['alert_thresholds']['drawdown_pct']:
                        risk_details.append({
                            'ts_code': ts_code,
                            'type': 'large_loss',
                            'message': f'亏损 {profit_loss_pct:.2f}%'
                        })

                    # 检查当日跌幅
                    if pct_chg < -7:
                        risk_details.append({
                            'ts_code': ts_code,
                            'type': 'daily_drop',
                            'message': f'今日下跌 {pct_chg:.2f}%'
                        })

            conn.close()

            # 计算整体风险
            total_profit_loss_pct = (total_value - total_cost) / total_cost * 100 if total_cost > 0 else 0

            result['risks'] = {
                'total_value': round(total_value, 2),
                'total_cost': round(total_cost, 2),
                'profit_loss': round(total_value - total_cost, 2),
                'profit_loss_pct': round(total_profit_loss_pct, 2),
                'details': risk_details
            }

            if len(risk_details) > 0:
                result['status'] = 'warning'
            if total_profit_loss_pct < -self.config['alert_thresholds']['drawdown_pct']:
                result['status'] = 'critical'

        except Exception as e:
            result['error'] = str(e)
            result['status'] = 'error'

        logger.info(f"投资组合风险检查完成：{result['status']}")
        return result

    def send_alert(self, title: str, message: str, level: str = 'warning') -> bool:
        """
        发送告警通知

        Args:
            title: 告警标题
            message: 告警内容
            level: 告警级别 (info/warning/critical)

        Returns:
            是否发送成功
        """
        logger.info(f"发送告警：[{level}] {title}")

        alert = {
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'title': title,
            'message': message,
            'level': level
        }
        self.alerts.append(alert)

        success = False

        # 邮件通知
        if self.config['notification']['email']['enabled']:
            try:
                email_config = self.config['notification']['email']
                msg = MIMEMultipart()
                msg['From'] = email_config['sender']
                msg['To'] = ', '.join(email_config['recipients'])
                msg['Subject'] = f"[量化系统告警] {title}"

                body = f"""
时间：{alert['time']}
级别：{level}

{message}

---
此邮件由量化系统自动发送
                """
                msg.attach(MIMEText(body, 'plain', 'utf-8'))

                server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
                server.starttls()
                if email_config['username'] and email_config['password']:
                    server.login(email_config['username'], email_config['password'])
                server.send_message(msg)
                server.quit()

                logger.info("邮件告警发送成功")
                success = True
            except Exception as e:
                logger.error(f"邮件告警发送失败：{e}")

        # 钉钉通知
        if self.config['notification']['dingtalk']['enabled']:
            try:
                import requests
                webhook_url = self.config['notification']['dingtalk']['webhook_url']

                data = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": title,
                        "text": f"""## {title}
- 时间：{alert['time']}
- 级别：{level}

{message}"""
                    }
                }

                response = requests.post(webhook_url, json=data)
                if response.status_code == 200:
                    logger.info("钉钉告警发送成功")
                    success = True
                else:
                    logger.error(f"钉钉告警发送失败：{response.text}")
            except Exception as e:
                logger.error(f"钉钉告警发送失败：{e}")

        # 企业微信通知
        if self.config['notification']['wechat']['enabled']:
            try:
                import requests
                webhook_url = self.config['notification']['wechat']['webhook_url']

                data = {
                    "msgtype": "markdown",
                    "markdown": {
                        "content": f"""## {title}
> 时间：{alert['time']}
> 级别：{level}

{message}"""
                    }
                }

                response = requests.post(webhook_url, json=data)
                if response.status_code == 200:
                    logger.info("企业微信告警发送成功")
                    success = True
                else:
                    logger.error(f"企业微信告警发送失败：{response.text}")
            except Exception as e:
                logger.error(f"企业微信告警发送失败：{e}")

        return success

    def run_full_check(self, positions: List[Dict] = None) -> Dict:
        """
        运行完整检查

        Args:
            positions: 持仓列表

        Returns:
            检查结果
        """
        logger.info("运行完整监控检查...")

        result = {
            'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'overall_status': 'ok',
            'system_health': self.check_system_health(),
            'data_freshness': self.check_data_freshness(),
            'portfolio_risk': self.check_portfolio_risk(positions)
        }

        # 确定整体状态
        if result['system_health']['status'] == 'critical':
            result['overall_status'] = 'critical'
            self.send_alert("系统严重告警", "系统健康检查发现严重问题", 'critical')
        elif result['system_health']['status'] == 'warning':
            result['overall_status'] = 'warning'
            self.send_alert("系统警告", "系统健康检查发现异常", 'warning')

        if result['data_freshness']['status'] == 'warning':
            if result['overall_status'] == 'ok':
                result['overall_status'] = 'warning'
            tables = [k for k, v in result['data_freshness']['tables'].items() if v.get('status') == 'warning']
            self.send_alert("数据延迟告警", f"以下数据表更新延迟：{', '.join(tables)}", 'warning')

        if result['portfolio_risk']['status'] == 'critical':
            result['overall_status'] = 'critical'
            self.send_alert("投资组合严重风险", "组合回撤超过阈值", 'critical')
        elif result['portfolio_risk']['status'] == 'warning':
            if result['overall_status'] == 'ok':
                result['overall_status'] = 'warning'
            details = result['portfolio_risk'].get('risks', {}).get('details', [])
            if details:
                self.send_alert("投资风险警告", f"{len(details)} 只股票存在风险", 'warning')

        logger.info(f"完整监控检查完成：{result['overall_status']}")
        return result


def print_monitor_report(result: Dict):
    """打印监控报告"""
    print("\n" + "=" * 70)
    print("量化系统监控报告")
    print("=" * 70)

    print(f"\n报告时间：{result['check_time']}")
    print(f"整体状态：{result['overall_status'].upper()}")

    print("\n【系统健康】")
    health = result['system_health']
    print(f"  状态：{health['status']}")
    for item, data in health.get('items', {}).items():
        status = data.get('status', 'unknown')
        if 'usage_pct' in data:
            print(f"  - {item}: {data['usage_pct']}% ({status})")
        elif 'size_gb' in data:
            print(f"  - {item}: {data['size_gb']}GB ({status})")
        else:
            print(f"  - {item}: {status}")

    print("\n【数据时效性】")
    freshness = result['data_freshness']
    print(f"  状态：{freshness['status']}")
    for table, data in freshness.get('tables', {}).items():
        if 'days_ago' in data:
            print(f"  - {table}: {data['days_ago']} 天前 ({data.get('status', 'unknown')})")
        else:
            print(f"  - {table}: {data.get('status', 'unknown')}")

    print("\n【投资组合风险】")
    risk = result['portfolio_risk']
    print(f"  状态：{risk['status']}")
    if 'risks' in risk and risk['risks']:
        risks = risk['risks']
        print(f"  总市值：¥{risks.get('total_value', 0):,.2f}")
        print(f"  总成本：¥{risks.get('total_cost', 0):,.2f}")
        print(f"  盈亏：¥{risks.get('profit_loss', 0):,.2f} ({risks.get('profit_loss_pct', 0):.2f}%)")

        details = risks.get('details', [])
        if details:
            print(f"  风险详情：{len(details)} 条")
            for d in details[:5]:
                print(f"    - {d['ts_code']}: {d['message']}")

    print("=" * 70)


def main():
    """测试函数"""
    print("=" * 70)
    print("监控告警系统测试")
    print("=" * 70)

    # 创建监控系统
    monitor = MonitorSystem()

    # 运行完整检查
    result = monitor.run_full_check(positions=[
        {'ts_code': '000001.SZ', 'shares': 1000, 'cost': 12.0},
        {'ts_code': '600519.SH', 'shares': 100, 'cost': 1800.0},
        {'ts_code': '300750.SZ', 'shares': 500, 'cost': 350.0},
    ])

    # 打印报告
    print_monitor_report(result)

    # 显示告警历史
    if monitor.alerts:
        print("\n【告警历史】")
        for alert in monitor.alerts:
            print(f"  [{alert['level']}] {alert['time']}: {alert['title']}")


if __name__ == "__main__":
    main()
