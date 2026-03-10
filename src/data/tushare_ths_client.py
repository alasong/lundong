"""
Tushare 客户端 - 同花顺行业数据
"""
import tushare as ts
import pandas as pd
import time
from typing import Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class TushareTHSClient:
    """Tushare 同花顺数据客户端"""
    
    def __init__(self, token: str, max_retries: int = 3):
        """初始化
        
        Args:
            token: Tushare API token
            max_retries: 最大重试次数
        """
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.max_retries = max_retries
        
        logger.info("Tushare 同花顺客户端初始化完成")
    
    def get_ths_indices(self) -> pd.DataFrame:
        """获取同花顺指数列表
        
        Returns:
            DataFrame with columns:
            - ts_code: 指数代码
            - name: 指数名称
            - count: 成分股数量
            - exchange: 交易所
            - list_date: 上市日期
            - type: 类型
        """
        logger.info("获取同花顺指数列表...")
        
        for i in range(self.max_retries):
            try:
                df = self.pro.ths_index()
                if df is not None and len(df) > 0:
                    logger.info(f"获取成功：{len(df)} 个指数")
                    return df
                else:
                    logger.warning("返回空数据")
                    return pd.DataFrame()
            except Exception as e:
                if i < self.max_retries - 1:
                    logger.warning(f"获取失败，重试中 ({i+1}/{self.max_retries}): {str(e)[:80]}")
                    time.sleep(1.0 * (i + 1))
                else:
                    logger.error(f"最终失败：{str(e)[:100]}")
                    raise
        
        return pd.DataFrame()
    
    def get_ths_industries(self, level: int = 1) -> pd.DataFrame:
        """获取同花顺行业分类
        
        Args:
            level: 行业级别 (1=一级行业，2=二级行业)
            
        Returns:
            DataFrame with industry indices
        """
        logger.info(f"获取同花顺{level}级行业分类...")
        
        df = self.get_ths_indices()
        if len(df) == 0:
            return pd.DataFrame()
        
        # 筛选行业指数
        # 一级行业：881xxx, 二级行业：882xxx
        prefix = '881' if level == 1 else '882'
        industries = df[df['ts_code'].str.startswith(prefix, na=False)].copy()
        
        logger.info(f"筛选后：{len(industries)} 个{level}级行业")
        
        return industries
    
    def get_ths_history(self, ts_code: str, 
                       start_date: str = '20200101',
                       end_date: str = None) -> Optional[pd.DataFrame]:
        """获取同花顺指数历史行情
        
        Args:
            ts_code: 指数代码 (e.g., '881101.TI')
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (默认今天)
            
        Returns:
            DataFrame with OHLCV data
        """
        if end_date is None:
            end_date = pd.Timestamp.now().strftime('%Y%m%d')
        
        logger.info(f"获取 {ts_code} 历史行情 ({start_date} - {end_date})...")
        
        for i in range(self.max_retries):
            try:
                # 尝试 ths_daily 接口
                df = self.pro.ths_daily(ts_code=ts_code, 
                                       start_date=start_date, 
                                       end_date=end_date)
                
                if df is not None and len(df) > 0:
                    logger.info(f"获取成功：{len(df)} 条")
                    return df
                
                # 如果 ths_daily 返回空，尝试 index_daily
                # 注意：需要将代码格式从 .TI 改为 .THS
                ths_code = ts_code.replace('.TI', '.THS')
                df = self.pro.index_daily(ts_code=ths_code,
                                         start_date=start_date,
                                         end_date=end_date)
                
                if df is not None and len(df) > 0:
                    logger.info(f"index_daily 获取成功：{len(df)} 条")
                    return df
                
                logger.warning(f"返回空数据")
                return None
                
            except Exception as e:
                if i < self.max_retries - 1:
                    logger.warning(f"获取失败，重试中 ({i+1}/{self.max_retries}): {str(e)[:80]}")
                    time.sleep(1.0 * (i + 1))
                else:
                    logger.error(f"最终失败：{str(e)[:100]}")
                    return None
        
        return None
    
    def get_ths_members(self, ts_code: str) -> Optional[pd.DataFrame]:
        """获取同花顺指数成分股
        
        Args:
            ts_code: 指数代码
            
        Returns:
            DataFrame with constituent stocks
        """
        logger.info(f"获取 {ts_code} 成分股...")
        
        try:
            # Tushare 可能没有直接的成分股接口 for 同花顺
            # 需要用 index_member
            df = self.pro.index_member(index_code=ts_code)
            
            if df is not None and len(df) > 0:
                logger.info(f"获取成功：{len(df)} 只成分股")
                return df
            else:
                logger.warning(f"返回空数据")
                return None
        except Exception as e:
            logger.error(f"获取失败：{str(e)[:100]}")
            return None


def main():
    """测试函数"""
    from core.settings import settings
    
    client = TushareTHSClient(token=settings.tushare_token)
    
    # 1. 获取同花顺指数列表
    print("\n[1/3] 获取同花顺指数列表...")
    indices = client.get_ths_indices()
    if len(indices) > 0:
        print(f"  总数：{len(indices)} 个")
    
    # 2. 获取一级行业
    print("\n[2/3] 获取同花顺一级行业...")
    industries = client.get_ths_industries(level=1)
    if len(industries) > 0:
        print(f"  一级行业：{len(industries)} 个")
        print(f"  前 20 个:")
        for i, (idx, row) in enumerate(industries.head(20).iterrows()):
            print(f"    {i+1:2d}. {row['name']:20s} ({row['ts_code']})")
    
    # 3. 测试获取历史数据
    print("\n[3/3] 测试历史数据...")
    if len(industries) > 0:
        test_code = industries.iloc[0]['ts_code']
        test_name = industries.iloc[0]['name']
        print(f"  测试：{test_name} ({test_code})")
        
        hist = client.get_ths_history(test_code, start_date='20240101')
        if hist is not None:
            print(f"  ✓ 获取 {len(hist)} 条历史数据")
        else:
            print(f"  ✗ 获取失败")


if __name__ == "__main__":
    main()
