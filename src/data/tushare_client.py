"""
Tushare 客户端模块 - 专用于东方财富板块数据
"""
import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class TushareClient:
    """Tushare 客户端 - 东方财富板块数据"""
    
    def __init__(self, token: str, max_retries: int = 3, retry_delay: float = 1.0):
        """初始化 Tushare 客户端
        
        Args:
            token: Tushare API token
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
        """
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        logger.info("Tushare 客户端初始化完成")
    
    def get_concept_list(self) -> pd.DataFrame:
        """获取东方财富概念列表
        
        Returns:
            DataFrame containing concept data with columns:
            - name: 概念名称
            - code: 概念代码
        """
        logger.info("获取东方财富概念列表...")
        df = self.pro.dc_concept()
        
        if df is not None and len(df) > 0:
            logger.info(f"获取成功：{len(df)} 个概念")
            # 标准化列名
            if 'name' in df.columns:
                df = df.rename(columns={'name': 'concept_name'})
            return df
        else:
            logger.warning("概念列表返回空数据")
            return pd.DataFrame()
    
    def get_index_list(self) -> pd.DataFrame:
        """获取东方财富行业列表
        
        Returns:
            DataFrame containing industry data
        """
        logger.info("获取东方财富行业列表...")
        df = self.pro.dc_index()
        
        if df is not None and len(df) > 0:
            logger.info(f"获取成功：{len(df)} 个行业")
            # 标准化列名
            if 'name' in df.columns:
                df = df.rename(columns={'name': 'industry_name'})
            return df
        else:
            logger.warning("行业列表返回空数据")
            return pd.DataFrame()
    
    def get_concept_members(self, concept_code: str, 
                           trade_date: str = None) -> pd.DataFrame:
        """获取概念成分股
        
        Args:
            concept_code: 概念代码 (e.g., 'BK1184.DC')
            trade_date: 交易日期 (YYYYMMDD 格式，默认最新)
            
        Returns:
            DataFrame containing constituent stocks
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"获取概念成分股：{concept_code}")
        df = self.pro.dc_member(ts_code=concept_code, trade_date=trade_date)
        
        if df is not None and len(df) > 0:
            logger.info(f"获取成功：{len(df)} 只成分股")
            return df
        else:
            logger.warning(f"成分股返回空数据：{concept_code}")
            return pd.DataFrame()
    
    def get_index_history(self, ts_code: str, 
                         start_date: str = '20200101',
                         end_date: str = None) -> pd.DataFrame:
        """获取指数/板块历史行情
        
        Args:
            ts_code: 指数代码 (e.g., 'BK1184.DC')
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD，默认今天)
            
        Returns:
            DataFrame with historical OHLCV data
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"获取指数行情：{ts_code} ({start_date} to {end_date})")
        
        for i in range(self.max_retries):
            try:
                # 尝试 dc_daily 接口
                df = self.pro.dc_daily(ts_code=ts_code, 
                                      start_date=start_date, 
                                      end_date=end_date)
                
                if df is not None and len(df) > 0:
                    logger.info(f"dc_daily 获取成功：{len(df)} 条")
                    return df
                
                # 如果 dc_daily 返回空，尝试 index_daily 接口
                logger.warning(f"dc_daily 返回空数据，尝试 index_daily...")
                df = self.pro.index_daily(ts_code=ts_code,
                                         start_date=start_date,
                                         end_date=end_date)
                
                if df is not None and len(df) > 0:
                    logger.info(f"index_daily 获取成功：{len(df)} 条")
                    return df
                
                logger.warning(f"行情返回空数据：{ts_code}")
                return pd.DataFrame()
                
            except Exception as e:
                if i < self.max_retries - 1:
                    logger.warning(f"获取失败，重试中 ({i+1}/{self.max_retries}): {str(e)[:80]}")
                    time.sleep(self.retry_delay * (i + 1))
                else:
                    logger.error(f"获取最终失败：{str(e)[:100]}")
                    return pd.DataFrame()
        
        return pd.DataFrame()
    
    def get_index_basic(self, ts_code: str = None) -> pd.DataFrame:
        """获取指数基本信息
        
        Args:
            ts_code: 指数代码（可选）
            
        Returns:
            DataFrame with index basic info
        """
        logger.info(f"获取指数基本信息...")
        try:
            df = self.pro.index_basic(market='DC', ts_code=ts_code)
            if df is not None and len(df) > 0:
                logger.info(f"获取成功：{len(df)} 个")
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            logger.warning(f"获取失败：{str(e)}")
            return pd.DataFrame()
