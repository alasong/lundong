"""
核心配置模块
"""
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class Settings:
    """配置类"""
    
    def __init__(self):
        # Tushare 配置
        self.tushare_token = os.getenv('TUSHARE_TOKEN', 'your_token_here')
        
        # 数据库配置
        self.db_path = os.getenv('DB_PATH', 'data/stocks.db')
        
        # 日志配置
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')

# 全局配置实例
settings = Settings()
