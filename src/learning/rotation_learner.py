#!/usr/bin/env python
"""
热点轮动规律学习模块
从网络学习 + 自主分析数据，总结热点轮动规律
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json
import talib
from utils.logger import get_logger

logger = get_logger(__name__)


class RotationLearner:
    """热点轮动学习器"""
    
    def __init__(self, data_dir: str = None):
        """初始化
        
        Args:
            data_dir: 数据目录，默认使用 data/raw
        """
        if data_dir is None:
            self.data_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))), 'data', 'raw'
            )
        else:
            self.data_dir = data_dir
        
        self.industries_df = None
        self.concepts_df = None
        self.history_cache = {}
        
        # 学习到的规律
        self.learned_patterns = {
            'momentum_effect': None,  # 动量效应
            'reversal_effect': None,  # 反转效应
            'sector_rotation': None,  # 板块轮动周期
            'money_flow': None,  # 资金流向规律
            'correlation_matrix': None,  # 板块相关性
            'leading_lagging': None,  # 领先滞后关系
        }
    
    def load_industry_list(self) -> pd.DataFrame:
        """加载行业列表"""
        file_path = os.path.join(self.data_dir, 'eastmoney_industries.csv')
        if os.path.exists(file_path):
            self.industries_df = pd.read_csv(file_path)
            logger.info(f"加载行业列表：{len(self.industries_df)} 个")
            return self.industries_df
        else:
            logger.warning(f"行业列表文件不存在：{file_path}")
            return None
    
    def load_concept_list(self) -> pd.DataFrame:
        """加载概念列表"""
        file_path = os.path.join(self.data_dir, 'eastmoney_concepts.csv')
        if os.path.exists(file_path):
            self.concepts_df = pd.read_csv(file_path)
            logger.info(f"加载概念列表：{len(self.concepts_df)} 个")
            return self.concepts_df
        else:
            logger.warning(f"概念列表文件不存在：{file_path}")
            return None
    
    def load_industry_history(self, industry_code: str) -> pd.DataFrame:
        """加载单个行业的历史数据"""
        if industry_code in self.history_cache:
            return self.history_cache[industry_code]
        
        file_path = os.path.join(self.data_dir, f'industry_{industry_code}.csv')
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            # 标准化日期格式
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            elif '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期'])
            
            self.history_cache[industry_code] = df
            logger.debug(f"加载行业历史：{industry_code}, {len(df)} 条")
            return df
        else:
            logger.debug(f"行业历史文件不存在：{file_path}")
            return None
    
    def load_all_industry_history(self) -> Dict[str, pd.DataFrame]:
        """加载所有行业历史数据"""
        if self.industries_df is None:
            self.load_industry_list()
        
        logger.info("开始加载所有行业历史数据...")
        loaded = 0
        
        for idx, row in self.industries_df.iterrows():
            code = row['industry_code']
            df = self.load_industry_history(code)
            if df is not None and len(df) > 0:
                loaded += 1
        
        logger.info(f"加载完成：{loaded}/{len(self.industries_df)} 个行业")
        return self.history_cache
    
    def learn_momentum_effect(self, lookback_days: int = 20, 
                             forward_days: int = 5) -> Dict:
        """学习动量效应：过去表现好的板块未来是否继续表现好
        
        Args:
            lookback_days: 回看天数（动量计算周期）
            forward_days: 前瞻天数（验证周期）
            
        Returns:
            动量效应分析结果
        """
        logger.info(f"学习动量效应（回看={lookbackback_days}天，前瞻={forward_days}天）...")
        
        if not self.history_cache:
            self.load_all_industry_history()
        
        momentum_results = []
        
        # 遍历所有行业
        for code, df in self.history_cache.items():
            if len(df) < lookback_days + forward_days + 10:
                continue
            
            # 计算动量（过去 N 天的涨幅）
            # 使用收盘价计算
            close_col = 'close' if 'close' in df.columns else ('收盘' if '收盘' in df.columns else None)
            if close_col is None:
                continue
            
            df = df.sort_values('trade_date' if 'trade_date' in df.columns else '日期', 
                               ascending=False).reset_index(drop=True)
            
            # 滚动计算动量
            for i in range(len(df) - lookback_days - forward_days):
                # 回看期
                lookback_start = i + forward_days
                lookback_end = lookback_start + lookback_days
                
                if lookback_end > len(df):
                    continue
                
                # 前瞻期
                forward_start = i
                forward_end = i + forward_days
                
                # 计算动量（回看期收益率）
                lookback_return = (df.loc[lookback_start - 1, close_col] / 
                                  df.loc[lookback_end - 1, close_col] - 1) * 100
                
                # 计算前瞻期收益率
                forward_return = (df.loc[forward_start, close_col] / 
                                 df.loc[forward_end - 1, close_col] - 1) * 100
                
                momentum_results.append({
                    'industry_code': code,
                    'date': df.loc[i, 'trade_date' if 'trade_date' in df.columns else '日期'],
                    'momentum': lookback_return,
                    'forward_return': forward_return,
                })
        
        if not momentum_results:
            logger.warning("没有足够的数据进行动量效应分析")
            return None
        
        momentum_df = pd.DataFrame(momentum_results)
        
        # 分组成动量组合
        momentum_df['momentum_rank'] = pd.qcut(momentum_df['momentum'].rank(method='first'), 
                                               q=5, labels=['低', '中低', '中', '中高', '高'])
        
        # 计算各组合的未来收益
        grouped = momentum_df.groupby('momentum_rank')['forward_return'].agg(['mean', 'std', 'count'])
        
        # 计算动量效应强度（高动量组合 - 低动量组合）
        high_momentum_return = momentum_df[momentum_df['momentum_rank'] == '高']['forward_return'].mean()
        low_momentum_return = momentum_df[momentum_df['momentum_rank'] == '低']['forward_return'].mean()
        momentum_strength = high_momentum_return - low_momentum_return
        
        # 相关性分析
        correlation = momentum_df['momentum'].corr(momentum_df['forward_return'])
        
        result = {
            'momentum_strength': momentum_strength,
            'correlation': correlation,
            'grouped_returns': grouped.to_dict(),
            'high_momentum_return': high_momentum_return,
            'low_momentum_return': low_momentum_return,
            'sample_size': len(momentum_df),
            'interpretation': self._interpret_momentum(momentum_strength, correlation)
        }
        
        self.learned_patterns['momentum_effect'] = result
        logger.info(f"动量效应强度：{momentum_strength:.2f}%, 相关性：{correlation:.3f}")
        
        return result
    
    def _interpret_momentum(self, strength: float, correlation: float) -> str:
        """解释动量效应结果"""
        if abs(strength) < 0.5:
            return "动量效应不明显，过去表现与未来表现关联较弱"
        elif strength > 0:
            return f"存在正向动量效应（强度{strength:.2f}%），过去强势板块未来倾向于继续强势"
        else:
            return f"存在反转效应（强度{abs(strength):.2f}%），过去强势板块未来倾向于反转走弱"
    
    def learn_reversal_effect(self, lookback_days: int = 60,
                             forward_days: int = 20) -> Dict:
        """学习反转效应：过去表现差的板块未来是否会反转
        
        Args:
            lookback_days: 回看天数
            forward_days: 前瞻天数
            
        Returns:
            反转效应分析结果
        """
        logger.info(f"学习反转效应（回看={lookbackback_days}天，前瞻={forward_days}天）...")
        
        if not self.history_cache:
            self.load_all_industry_history()
        
        reversal_results = []
        
        for code, df in self.history_cache.items():
            if len(df) < lookback_days + forward_days + 10:
                continue
            
            close_col = 'close' if 'close' in df.columns else ('收盘' if '收盘' in df.columns else None)
            if close_col is None:
                continue
            
            df = df.sort_values('trade_date' if 'trade_date' in df.columns else '日期',
                               ascending=False).reset_index(drop=True)
            
            for i in range(len(df) - lookback_days - forward_days):
                lookback_start = i + forward_days
                lookback_end = lookback_start + lookback_days
                forward_start = i
                forward_end = i + forward_days
                
                if lookback_end > len(df):
                    continue
                
                # 计算长期收益率（回看期）
                lookback_return = (df.loc[lookback_start - 1, close_col] / 
                                  df.loc[lookback_end - 1, close_col] - 1) * 100
                
                # 计算前瞻期收益率
                forward_return = (df.loc[forward_start, close_col] / 
                                 df.loc[forward_end - 1, close_col] - 1) * 100
                
                reversal_results.append({
                    'industry_code': code,
                    'date': df.loc[i, 'trade_date' if 'trade_date' in df.columns else '日期'],
                    'long_term_return': lookback_return,
                    'forward_return': forward_return,
                })
        
        if not reversal_results:
            logger.warning("没有足够的数据进行反转效应分析")
            return None
        
        reversal_df = pd.DataFrame(reversal_results)
        
        # 长期收益分组
        reversal_df['return_rank'] = pd.qcut(reversal_df['long_term_return'].rank(method='first'),
                                             q=5, labels=['差', '中下', '中', '中上', '好'])
        
        grouped = reversal_df.groupby('return_rank')['forward_return'].agg(['mean', 'std', 'count'])
        
        # 计算反转效应
        losers_return = reversal_df[reversal_df['return_rank'] == '差']['forward_return'].mean()
        winners_return = reversal_df[reversal_df['return_rank'] == '好']['forward_return'].mean()
        reversal_strength = losers_return - winners_return
        
        # 相关性
        correlation = reversal_df['long_term_return'].corr(reversal_df['forward_return'])
        
        result = {
            'reversal_strength': reversal_strength,
            'correlation': correlation,
            'grouped_returns': grouped.to_dict(),
            'losers_return': losers_return,
            'winners_return': winners_return,
            'sample_size': len(reversal_df),
            'interpretation': self._interpret_reversal(reversal_strength, correlation)
        }
        
        self.learned_patterns['reversal_effect'] = result
        logger.info(f"反转效应强度：{reversal_strength:.2f}%, 相关性：{correlation:.3f}")
        
        return result
    
    def _interpret_reversal(self, strength: float, correlation: float) -> str:
        """解释反转效应结果"""
        if abs(strength) < 0.5:
            return "反转效应不明显"
        elif strength > 0:
            return f"存在显著反转效应（强度{strength:.2f}%），过去弱势板块未来倾向于反弹"
        else:
            return f"存在正向延续效应（强度{abs(strength):.2f}%），板块表现具有持续性"
    
    def learn_sector_rotation_cycle(self) -> Dict:
        """学习板块轮动周期"""
        logger.info("学习板块轮动周期...")
        
        if not self.history_cache:
            self.load_all_industry_history()
        
        # 计算各行业的月度收益率矩阵
        monthly_returns = {}
        
        for code, df in self.history_cache.items():
            close_col = 'close' if 'close' in df.columns else ('收盘' if '收盘' in df.columns else None)
            if close_col is None or len(df) < 60:
                continue
            
            df = df.copy()
            date_col = 'trade_date' if 'trade_date' in df.columns else '日期'
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.sort_values(date_col)
            
            # 计算月度收益率
            df['month'] = df[date_col].dt.to_period('M')
            monthly = df.groupby('month')[close_col].last()
            monthly_return = monthly.pct_change() * 100
            
            monthly_returns[code] = monthly_return.dropna()
        
        if len(monthly_returns) < 10:
            logger.warning("数据不足以分析轮动周期")
            return None
        
        # 将月度收益率对齐到 DataFrame
        df_returns = pd.DataFrame(monthly_returns)
        
        # 计算滚动窗口表现（3 个月）
        window = 3
        rolling_performance = df_returns.rolling(window=window).mean()
        
        # 找出每个时期的领先板块
        leading_sectors = []
        for date in rolling_performance.index[window:]:
            row = rolling_performance.loc[date].dropna()
            if len(row) > 0:
                top_sector = row.idxmax()
                leading_sectors.append({
                    'date': date,
                    'leading_sector': top_sector,
                    'return': row[top_sector]
                })
        
        # 分析轮动模式
        sector_appearance = {}
        for item in leading_sectors:
            sector = item['leading_sector']
            sector_appearance[sector] = sector_appearance.get(sector, 0) + 1
        
        # 找出最常领先的板块
        top_leaders = sorted(sector_appearance.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # 计算轮动周期（使用自相关）
        avg_return = df_returns.mean(axis=1)
        autocorr = pd.Series([avg_return.autocorr(lag=i) for i in range(1, 13)])
        
        # 找出显著的周期
        significant_lags = np.where(autocorr.abs() > 0.3)[0] + 1
        
        result = {
            'top_leading_sectors': top_leaders,
            'significant_cycle_months': significant_lags.tolist(),
            'autocorrelation': autocorr.tolist(),
            'interpretation': self._interpret_cycle(significant_lags, top_leaders)
        }
        
        self.learned_patterns['sector_rotation'] = result
        logger.info(f"识别到显著周期：{significant_lags.tolist()} 个月")
        
        return result
    
    def _interpret_cycle(self, significant_lags: np.ndarray, top_leaders: list) -> str:
        """解释轮动周期结果"""
        if len(significant_lags) == 0:
            return "未检测到显著的周期性轮动模式"
        
        primary_cycle = significant_lags[0]
        interpretation = f"检测到板块轮动周期约为 {primary_cycle} 个月"
        
        if len(top_leaders) > 0:
            leaders_str = ", ".join([f"{code}({count}次)" for code, count in top_leaders[:5]])
            interpretation += f"\n最常领先的板块：{leaders_str}"
        
        return interpretation
    
    def learn_money_flow_pattern(self) -> Dict:
        """学习资金流向规律"""
        logger.info("学习资金流向规律...")
        
        if not self.history_cache:
            self.load_all_industry_history()
        
        flow_patterns = []
        
        for code, df in self.history_cache.items():
            vol_col = 'volume' if 'volume' in df.columns else ('成交量' if '成交量' in df.columns else None)
            close_col = 'close' if 'close' in df.columns else ('收盘' if '收盘' in df.columns else None)
            
            if vol_col is None or close_col is None:
                continue
            
            df = df.copy()
            df = df.sort_values('trade_date' if 'trade_date' in df.columns else '日期', 
                               ascending=False).reset_index(drop=True)
            
            # 计算量价关系
            # 量增价升：成交量增加且价格上涨
            # 量价背离：成交量增加但价格下跌
            
            for i in range(1, len(df) - 5):
                prev_vol = df.loc[i, vol_col]
                curr_vol = df.loc[i-1, vol_col]
                vol_change = (curr_vol - prev_vol) / prev_vol * 100 if prev_vol > 0 else 0
                
                prev_close = df.loc[i, close_col]
                curr_close = df.loc[i-1, close_col]
                price_change = (curr_close - prev_close) / prev_close * 100
                
                # 未来 5 天收益
                future_return = (df.loc[max(0, i-5), close_col] / prev_close - 1) * 100
                
                if vol_change > 10:  # 成交量显著增加
                    pattern_type = "量增"
                    if price_change > 1:
                        pattern_type += "价升"
                        outcome = "量增价升"
                    elif price_change < -1:
                        pattern_type += "价跌"
                        outcome = "量增价跌"
                    else:
                        outcome = "量增价平"
                    
                    flow_patterns.append({
                        'industry_code': code,
                        'date': df.loc[i, 'trade_date' if 'trade_date' in df.columns else '日期'],
                        'pattern': outcome,
                        'vol_change': vol_change,
                        'price_change': price_change,
                        'future_return': future_return,
                    })
        
        if not flow_patterns:
            logger.warning("没有足够的数据进行资金流向分析")
            return None
        
        flow_df = pd.DataFrame(flow_patterns)
        
        # 分析不同量价模式的未来收益
        pattern_outcomes = flow_df.groupby('pattern')['future_return'].agg(['mean', 'std', 'count'])
        
        # 找出最赚钱的模式
        best_pattern = pattern_outcomes['mean'].idxmax()
        best_return = pattern_outcomes.loc[best_pattern, 'mean']
        
        result = {
            'pattern_outcomes': pattern_outcomes.to_dict(),
            'best_pattern': best_pattern,
            'best_return': best_return,
            'sample_size': len(flow_df),
            'interpretation': self._interpret_money_flow(best_pattern, best_return)
        }
        
        self.learned_patterns['money_flow'] = result
        logger.info(f"最佳量价模式：{best_pattern}, 预期收益：{best_return:.2f}%")
        
        return result
    
    def _interpret_money_flow(self, pattern: str, return_val: float) -> str:
        """解释资金流向结果"""
        if pattern == "量增价升":
            return f"量增价升模式后，未来 5 天平均收益 {return_val:.2f}%，建议跟随趋势"
        elif pattern == "量增价跌":
            return f"量增价跌模式后，未来 5 天平均收益 {return_val:.2f}%，可能面临反转"
        else:
            return f"资金流向模式预期收益：{return_val:.2f}%"
    
    def learn_sector_correlation(self) -> pd.DataFrame:
        """学习板块间的相关性矩阵"""
        logger.info("计算板块相关性矩阵...")
        
        if not self.history_cache:
            self.load_all_industry_history()
        
        # 构建收益率矩阵
        returns_dict = {}
        
        for code, df in self.history_cache.items():
            close_col = 'close' if 'close' in df.columns else ('收盘' if '收盘' in df.columns else None)
            if close_col is None or len(df) < 30:
                continue
            
            df = df.copy()
            df = df.sort_values('trade_date' if 'trade_date' in df.columns else '日期')
            
            # 计算日收益率
            returns = df[close_col].pct_change() * 100
            returns_dict[code] = returns
        
        if len(returns_dict) < 5:
            logger.warning("数据不足以计算相关性矩阵")
            return None
        
        # 对齐并计算相关性
        df_returns = pd.DataFrame(returns_dict)
        correlation_matrix = df_returns.corr()
        
        self.learned_patterns['correlation_matrix'] = correlation_matrix
        logger.info(f"相关性矩阵维度：{correlation_matrix.shape}")
        
        return correlation_matrix
    
    def learn_leading_lagging_relationship(self) -> Dict:
        """学习板块间的领先 - 滞后关系"""
        logger.info("学习领先 - 滞后关系...")
        
        if self.learned_patterns['correlation_matrix'] is None:
            self.learn_sector_correlation()
        
        correlation_matrix = self.learned_patterns['correlation_matrix']
        if correlation_matrix is None:
            return None
        
        # 计算互相关，找出领先滞后关系
        if not self.history_cache:
            self.load_all_industry_history()
        
        leading_lagging = []
        
        # 随机选择一些板块对进行分析（避免计算量过大）
        codes = list(self.history_cache.keys())[:20]  # 限制在前 20 个
        
        for i, code1 in enumerate(codes):
            for code2 in codes[i+1:]:
                df1 = self.history_cache[code1]
                df2 = self.history_cache[code2]
                
                close_col1 = 'close' if 'close' in df1.columns else ('收盘' if '收盘' in df1.columns else None)
                close_col2 = 'close' if 'close' in df2.columns else ('收盘' if '收盘' in df2.columns else None)
                
                if close_col1 is None or close_col2 is None:
                    continue
                
                # 计算收益率
                ret1 = df1[close_col1].pct_change()
                ret2 = df2[close_col2].pct_change()
                
                # 计算交叉相关性（-5 到 +5 天）
                cross_corr = []
                for lag in range(-5, 6):
                    if lag < 0:
                        corr = ret1.corr(ret2.shift(lag))
                    elif lag > 0:
                        corr = ret1.shift(lag).corr(ret2)
                    else:
                        corr = ret1.corr(ret2)
                    cross_corr.append(corr)
                
                # 找出最大相关性的滞后
                max_corr = max(cross_corr, key=abs)
                max_lag = cross_corr.index(max_corr) - 5
                
                if abs(max_corr) > 0.3:  # 显著相关性
                    if max_lag > 0:
                        relationship = f"{code1} 领先 {code2} {max_lag}天"
                    elif max_lag < 0:
                        relationship = f"{code2} 领先 {code1} {abs(max_lag)}天"
                    else:
                        relationship = f"{code1} 与 {code2} 同步"
                    
                    leading_lagging.append({
                        'pair': f"{code1}-{code2}",
                        'max_correlation': max_corr,
                        'lag_days': max_lag,
                        'relationship': relationship
                    })
        
        # 按相关性排序
        leading_lagging.sort(key=lambda x: abs(x['max_correlation']), reverse=True)
        
        result = {
            'relationships': leading_lagging[:20],  # 保留前 20 个
            'interpretation': self._interpret_leading_lagging(leading_lagging[:10])
        }
        
        self.learned_patterns['leading_lagging'] = result
        logger.info(f"识别到 {len(leading_lagging)} 组领先 - 滞后关系")
        
        return result
    
    def _interpret_leading_lagging(self, relationships: list) -> str:
        """解释领先滞后关系"""
        if not relationships:
            return "未检测到显著的领先滞后关系"
        
        interpretation = "板块领先滞后关系:\n"
        for rel in relationships[:5]:
            interpretation += f"  - {rel['relationship']} (相关性：{rel['max_correlation']:.3f})\n"
        
        return interpretation
    
    def generate_summary_report(self) -> str:
        """生成学习总结报告"""
        report = []
        report.append("="*70)
        report.append("热点轮动规律学习报告")
        report.append("="*70)
        report.append(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # 数据加载情况
        report.append("一、数据加载情况")
        report.append("-"*70)
        if self.industries_df is not None:
            report.append(f"  行业数量：{len(self.industries_df)}")
        if self.concepts_df is not None:
            report.append(f"  概念数量：{len(self.concepts_df)}")
        report.append(f"  加载的历史数据：{len(self.history_cache)} 个板块")
        report.append("")
        
        # 动量效应
        if self.learned_patterns['momentum_effect']:
            report.append("二、动量效应")
            report.append("-"*70)
            eff = self.learned_patterns['momentum_effect']
            report.append(f"  动量效应强度：{eff['momentum_strength']:.2f}%")
            report.append(f"  相关性：{eff['correlation']:.3f}")
            report.append(f"  结论：{eff['interpretation']}")
            report.append("")
        
        # 反转效应
        if self.learned_patterns['reversal_effect']:
            report.append("三、反转效应")
            report.append("-"*70)
            eff = self.learned_patterns['reversal_effect']
            report.append(f"  反转效应强度：{eff['reversal_strength']:.2f}%")
            report.append(f"  相关性：{eff['correlation']:.3f}")
            report.append(f"  结论：{eff['interpretation']}")
            report.append("")
        
        # 轮动周期
        if self.learned_patterns['sector_rotation']:
            report.append("四、板块轮动周期")
            report.append("-"*70)
            rot = self.learned_patterns['sector_rotation']
            report.append(f"  显著周期：{rot['significant_cycle_months']} 个月")
            report.append(f"  领先板块：{rot['top_leading_sectors'][:5]}")
            report.append(f"  结论：{rot['interpretation']}")
            report.append("")
        
        # 资金流向
        if self.learned_patterns['money_flow']:
            report.append("五、资金流向规律")
            report.append("-"*70)
            flow = self.learned_patterns['money_flow']
            report.append(f"  最佳模式：{flow['best_pattern']}")
            report.append(f"  预期收益：{flow['best_return']:.2f}%")
            report.append(f"  结论：{flow['interpretation']}")
            report.append("")
        
        # 相关性
        if self.learned_patterns['correlation_matrix'] is not None:
            report.append("六、板块相关性")
            report.append("-"*70)
            report.append(f"  相关性矩阵维度：{self.learned_patterns['correlation_matrix'].shape}")
            report.append("")
        
        # 领先滞后
        if self.learned_patterns['leading_lagging']:
            report.append("七、领先 - 滞后关系")
            report.append("-"*70)
            ll = self.learned_patterns['leading_lagging']
            report.append(f"  识别到的关系：{len(ll['relationships'])} 组")
            report.append(f"  结论：{ll['interpretation']}")
            report.append("")
        
        # 综合结论
        report.append("八、综合结论与策略建议")
        report.append("-"*70)
        report.append(self._generate_strategy_recommendations())
        report.append("")
        
        report.append("="*70)
        report.append("报告结束")
        report.append("="*70)
        
        return "\n".join(report)
    
    def _generate_strategy_recommendations(self) -> str:
        """生成策略建议"""
        recommendations = []
        
        # 基于动量效应
        momentum = self.learned_patterns.get('momentum_effect')
        if momentum and momentum['momentum_strength'] > 1:
            recommendations.append(
                f"1. 动量策略：过去{20}天表现强势的板块，建议继续持有，预期超额收益{momentum['momentum_strength']:.2f}%"
            )
        elif momentum:
            recommendations.append(
                "1. 动量策略：动量效应不显著，不建议追涨"
            )
        
        # 基于反转效应
        reversal = self.learned_patterns.get('reversal_effect')
        if reversal and reversal['reversal_strength'] > 1:
            recommendations.append(
                f"2. 反转策略：过去{60}天表现弱势的板块，建议关注反弹机会，预期收益{reversal['reversal_strength']:.2f}%"
            )
        
        # 基于轮动周期
        rotation = self.learned_patterns.get('sector_rotation')
        if rotation and rotation['significant_cycle_months']:
            cycle = rotation['significant_cycle_months'][0]
            recommendations.append(
                f"3. 轮动策略：检测到约{cycle}个月的轮动周期，建议在周期底部提前布局"
            )
        
        # 基于资金流向
        money_flow = self.learned_patterns.get('money_flow')
        if money_flow:
            recommendations.append(
                f"4. 量价策略：{money_flow['best_pattern']} 模式最有效，预期收益{money_flow['best_return']:.2f}%"
            )
        
        return "\n".join(recommendations) if recommendations else "数据不足以生成策略建议"
    
    def save_learned_patterns(self, save_path: str = None):
        """保存学习到的规律"""
        if save_path is None:
            save_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))), 'data', 'learned_patterns.json'
            )
        
        # 将 DataFrame 转换为可序列化格式
        serializable_patterns = {}
        for key, value in self.learned_patterns.items():
            if isinstance(value, pd.DataFrame):
                serializable_patterns[key] = {
                    'type': 'DataFrame',
                    'data': value.to_json()
                }
            elif isinstance(value, pd.Series):
                serializable_patterns[key] = {
                    'type': 'Series',
                    'data': value.to_json()
                }
            else:
                serializable_patterns[key] = value
        
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(serializable_patterns, f, ensure_ascii=False, indent=2)
        
        logger.info(f"学习规律已保存：{save_path}")


def main():
    """主函数"""
    print("\n" + "="*70)
    print("热点轮动规律学习器")
    print("="*70)
    
    learner = RotationLearner()
    
    # 加载数据
    print("\n[1/6] 加载行业列表...")
    learner.load_industry_list()
    
    print("[2/6] 加载概念列表...")
    learner.load_concept_list()
    
    print("[3/6] 加载历史数据...")
    learner.load_all_industry_history()
    
    # 学习规律
    print("\n[4/6] 学习动量效应和反转效应...")
    learner.learn_momentum_effect(lookback_days=20, forward_days=5)
    learner.learn_reversal_effect(lookback_days=60, forward_days=20)
    
    print("[5/6] 学习板块轮动周期...")
    learner.learn_sector_rotation_cycle()
    
    print("[6/6] 学习资金流向和板块相关性...")
    learner.learn_money_flow_pattern()
    learner.learn_sector_correlation()
    learner.learn_leading_lagging_relationship()
    
    # 生成报告
    print("\n" + "="*70)
    print("生成学习报告")
    print("="*70)
    
    report = learner.generate_summary_report()
    print(report)
    
    # 保存结果
    learner.save_learned_patterns()
    
    # 保存报告
    report_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), 'docs', 'rotation_learning_report.md')
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# 热点轮动规律学习报告\n\n")
        f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("```\n")
        f.write(report)
        f.write("\n```\n")
    
    print(f"\n学习报告已保存：{report_path}")
    print("="*70)


if __name__ == "__main__":
    main()
