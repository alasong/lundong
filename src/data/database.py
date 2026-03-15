#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SQLite 数据库管理模块
支持高并发写入、实时去重、增量更新
"""
import os
import sys
import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Tuple, Any
from loguru import logger
from contextlib import contextmanager
from queue import Queue
from threading import Lock
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class SQLiteDatabase:
    """
    SQLite 数据库管理类

    特性:
    - WAL 模式支持高并发写入
    - 连接池管理数据库连接
    - 批量插入优化性能
    - 唯一约束实现实时去重
    - 支持增量更新和断点续传
    """

    # 表名白名单 - 防止 SQL 注入
    VALID_TABLES = frozenset([
        'concept_daily',
        'concept_info',
        'industry_info',
        'collect_task',
        'stock_daily',
        'concept_constituent',
        'stock_factors',
        'trade_calendar',
        'concept_daily_archive',
        'stock_daily_archive',
        'stock_daily_basic',
    ])

    def __init__(self, db_path: str = None, pool_size: int = 5):
        """
        初始化数据库

        Args:
            db_path: 数据库文件路径
            pool_size: 连接池大小
        """
        if db_path is None:
            # 使用配置中的数据库路径
            db_url = settings.database_url
            if db_url.startswith("sqlite:///"):
                db_path = db_url.replace("sqlite:///", "")
            else:
                db_path = "data/stock.db"

        # 确保目录存在
        db_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(db_dir, exist_ok=True)

        self.db_path = db_path
        self.pool_size = pool_size
        self._pool: Queue = Queue(maxsize=pool_size)
        self._pool_lock = Lock()
        self._initialized = False

        # 初始化数据库
        self._init_db()

    def _init_db(self):
        """初始化数据库（WAL 模式支持高并发）"""
        logger.info(f"初始化数据库：{self.db_path}")

        # 创建连接进行初始化
        conn = sqlite3.connect(self.db_path)
        try:
            # WAL 模式 - 读写不阻塞
            conn.execute("PRAGMA journal_mode=WAL")
            # 平衡性能和安全性
            conn.execute("PRAGMA synchronous=NORMAL")
            # 128MB 缓存 (优化：增大缓存)
            conn.execute("PRAGMA cache_size=-128000")
            # 内存临时表
            conn.execute("PRAGMA temp_store=MEMORY")
            # 设置忙碌超时（10 秒，优化：增加超时）
            conn.execute("PRAGMA busy_timeout=10000")
            # 256MB 内存映射 (优化：启用 mmap 加速读取)
            conn.execute("PRAGMA mmap_size=268435456")
            # 每500页自动checkpoint (优化：更频繁checkpoint减少WAL大小)
            conn.execute("PRAGMA wal_autocheckpoint=500")
            # WAL文件上限100MB (优化：限制WAL文件大小)
            conn.execute("PRAGMA journal_size_limit=104857600")

            conn.commit()

            # 创建表结构
            self._create_tables(conn)

            # 更新统计信息（优化查询计划）
            self._analyze_tables(conn)

            logger.info("数据库初始化完成（WAL 模式 + 优化配置）")
        finally:
            conn.close()

        # 预填充连接池
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self._pool.put(conn)

        self._initialized = True

    def _create_connection(self) -> sqlite3.Connection:
        """创建并配置数据库连接"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # 应用优化的 PRAGMA 配置
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-128000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("PRAGMA wal_autocheckpoint=500")
        conn.execute("PRAGMA journal_size_limit=104857600")
        return conn

    def _create_tables(self, conn: sqlite3.Connection):
        """创建数据库表结构"""
        logger.info("创建数据表...")

        # 板块行情数据表（完整字段）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS concept_daily (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                avg_price REAL,
                change REAL,
                pct_change REAL,
                vol REAL,
                turnover_rate REAL,
                amount REAL,
                change_pct REAL,
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 板块列表元数据
        conn.execute("""
            CREATE TABLE IF NOT EXISTS concept_info (
                ts_code TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                count INTEGER,
                exchange TEXT,
                list_date TEXT,
                updated_at TEXT
            )
        """)

        # 行业分类元数据
        conn.execute("""
            CREATE TABLE IF NOT EXISTS industry_info (
                ts_code TEXT PRIMARY KEY,
                name TEXT,
                level INTEGER,
                parent_code TEXT,
                updated_at TEXT
            )
        """)

        # 采集任务队列（支持断点续传）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS collect_task (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        # ===== 个股相关表结构 =====

        # 个股行情数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_daily (
                ts_code TEXT NOT NULL,          -- 个股代码 (000001.SZ)
                trade_date TEXT NOT NULL,       -- 交易日期
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                change REAL,
                pct_chg REAL,
                vol REAL,
                amount REAL,
                turnover_rate REAL,             -- 换手率
                pe REAL,                        -- 市盈率
                pb REAL,                        -- 市净率
                ps REAL,                        -- 市销率
                total_mv REAL,                  -- 总市值
                circ_mv REAL,                   -- 流通市值
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 板块 - 个股成分关系表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS concept_constituent (
                concept_code TEXT NOT NULL,     -- 板块代码
                stock_code TEXT NOT NULL,       -- 个股代码
                stock_name TEXT,
                weight REAL,                    -- 权重
                is_core INTEGER DEFAULT 1,      -- 是否核心成分股
                listed_date TEXT,               -- 上市日期
                updated_at TEXT,
                PRIMARY KEY (concept_code, stock_code)
            )
        """)

        # 个股因子表 (用于筛选)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_factors (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                market_cap REAL,                -- 市值
                pe_ttm REAL,                    -- 市盈率
                pb_ttm REAL,                    -- 市净率
                ps_ttm REAL,                    -- 市销率
                momentum_20d REAL,              -- 20 日动量
                momentum_60d REAL,              -- 60 日动量
                volatility_20d REAL,            -- 20 日波动率
                avg_turnover_20d REAL,          -- 20 日平均换手率
                avg_amount_20d REAL,            -- 20 日平均成交额
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 创建索引加速查询
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_date ON concept_daily(trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ts_code ON concept_daily(ts_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_concept_date ON concept_daily(ts_code, trade_date)")

        # 个股权威引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_trade_date ON stock_daily(trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_ts_code ON stock_daily(ts_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_date ON stock_daily(ts_code, trade_date)")

        # 成分股索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_constituent_concept ON concept_constituent(concept_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_constituent_stock ON concept_constituent(stock_code)")

        # ===== 优化：覆盖索引（避免回表查询）=====

        # 板块行情覆盖索引（常用查询字段）
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_concept_daily_cover
            ON concept_daily(ts_code, trade_date, pct_change, vol, amount, close)
        """)

        # 个股行情覆盖索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_daily_cover
            ON stock_daily(ts_code, trade_date, pct_chg, total_mv, pe, pb, amount)
        """)

        # 成分股权重排序索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_constituent_weight
            ON concept_constituent(concept_code, weight DESC)
        """)

        # ===== 优化：交易日历表 =====
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_calendar (
                trade_date TEXT PRIMARY KEY,
                is_open INTEGER DEFAULT 1,
                pre_trade_date TEXT,
                next_trade_date TEXT
            )
        """)

        # ===== 优化：历史数据归档表 =====
        conn.execute("""
            CREATE TABLE IF NOT EXISTS concept_daily_archive (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                avg_price REAL,
                change REAL,
                pct_change REAL,
                vol REAL,
                turnover_rate REAL,
                amount REAL,
                change_pct REAL,
                PRIMARY KEY (ts_code, trade_date)
            ) WITHOUT ROWID
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_daily_archive (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                change REAL,
                pct_chg REAL,
                vol REAL,
                amount REAL,
                turnover_rate REAL,
                pe REAL,
                pb REAL,
                ps REAL,
                total_mv REAL,
                circ_mv REAL,
                PRIMARY KEY (ts_code, trade_date)
            ) WITHOUT ROWID
        """)

        # 归档表索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_archive_concept_date ON concept_daily_archive(trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_archive_stock_date ON stock_daily_archive(trade_date)")

        conn.commit()
        logger.info("数据表创建完成（含覆盖索引、交易日历、归档表）")

    def _analyze_tables(self, conn: sqlite3.Connection):
        """更新表统计信息（优化查询计划）"""
        logger.info("更新表统计信息...")
        try:
            conn.execute("ANALYZE concept_daily")
            conn.execute("ANALYZE stock_daily")
            conn.execute("ANALYZE concept_constituent")
            conn.execute("ANALYZE concept_daily_archive")
            conn.execute("ANALYZE stock_daily_archive")
            conn.commit()
            logger.info("表统计信息更新完成")
        except Exception as e:
            logger.warning(f"更新统计信息失败: {e}")

    @contextmanager
    def get_connection(self):
        """从连接池获取连接（上下文管理器）"""
        conn = None
        try:
            conn = self._pool.get(timeout=30)
            yield conn
        finally:
            if conn:
                self._pool.put(conn)

    @staticmethod
    def _validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
        """
        验证标识符（表名/列名）安全性，防止 SQL 注入

        Args:
            identifier: 要验证的标识符
            identifier_type: 标识符类型（用于错误消息）

        Returns:
            验证通过的标识符

        Raises:
            ValueError: 标识符不合法时抛出
        """
        import re

        if not identifier:
            raise ValueError(f"{identifier_type} 不能为空")

        # 只允许字母、数字、下划线，且不能以数字开头
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(
                f"无效的 {identifier_type}: '{identifier}'。"
                f"只允许字母、数字、下划线，且不能以数字开头"
            )

        # 长度限制
        if len(identifier) > 128:
            raise ValueError(f"{identifier_type} 长度不能超过 128 字符")

        return identifier

    def _validate_table_name(self, table: str) -> str:
        """
        验证表名（白名单检查）

        Args:
            table: 表名

        Returns:
            验证通过的表名

        Raises:
            ValueError: 表名不在白名单中时抛出
        """
        if table not in self.VALID_TABLES:
            raise ValueError(
                f"无效的表名: '{table}'。"
                f"允许的表: {', '.join(sorted(self.VALID_TABLES))}"
            )
        return table

    def batch_insert(
        self,
        table: str,
        data: List[Dict[str, Any]],
        replace: bool = True,
        batch_size: int = 10000
    ) -> int:
        """
        批量插入数据（优化：事务批量提交）

        Args:
            table: 表名（必须是白名单中的有效表名）
            data: 数据列表，每个元素为 dict
            replace: True=覆盖重复，False=跳过重复
            batch_size: 每批提交的记录数

        Returns:
            插入/更新的记录数

        Raises:
            ValueError: 表名或列名不合法时抛出
        """
        # 输入验证
        if not data:
            return 0

        if not data[0]:
            raise ValueError("数据字典不能为空")

        # 安全检查：验证表名（白名单）
        table = self._validate_table_name(table)

        # 安全检查：验证列名（防止 SQL 注入）
        columns = list(data[0].keys())
        for col in columns:
            self._validate_identifier(col, "列名")

        with self.get_connection() as conn:
            try:
                placeholders = ','.join(['?' for _ in columns])
                col_names = ','.join(columns)

                if replace:
                    sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
                else:
                    sql = f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})"

                rows = [[row.get(col) for col in columns] for row in data]
                total_count = 0

                # 优化：事务批量提交
                conn.execute("BEGIN TRANSACTION")
                try:
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i + batch_size]
                        cursor = conn.executemany(sql, batch)
                        total_count += cursor.rowcount
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e

                mode = "覆盖" if replace else "跳过"
                logger.debug(f"批量插入 {table}: {total_count} 条记录 ({mode}模式, 批次大小={batch_size})")
                return total_count

            except Exception as e:
                logger.error(f"批量插入失败：{e}")
                raise

    def batch_insert_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        replace: bool = True
    ) -> int:
        """
        批量插入 DataFrame 数据

        Args:
            table: 表名
            df: pandas DataFrame
            replace: True=覆盖重复，False=跳过重复

        Returns:
            插入/更新的记录数
        """
        if df.empty:
            return 0

        # 转换为 dict 列表
        data = df.to_dict(orient='records')
        return self.batch_insert(table, data, replace)

    def query(
        self,
        sql: str,
        params: tuple = None
    ) -> List[Tuple]:
        """
        查询数据

        Args:
            sql: SQL 语句
            params: 参数元组

        Returns:
            查询结果列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()

    def query_to_dataframe(
        self,
        sql: str,
        params: tuple = None
    ) -> pd.DataFrame:
        """
        查询数据并返回 DataFrame

        Args:
            sql: SQL 语句
            params: 参数元组

        Returns:
            pandas DataFrame
        """
        with self.get_connection() as conn:
            return pd.read_sql_query(sql, conn, params=params)

    def execute(
        self,
        sql: str,
        params: tuple = None
    ) -> int:
        """
        执行 SQL 语句（UPDATE/DELETE）

        Args:
            sql: SQL 语句
            params: 参数元组

        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            return cursor.rowcount

    def get_missing_dates(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        获取缺失的交易日期（优化：使用交易日历表）

        Args:
            ts_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            缺失的日期列表
        """
        # 优先使用交易日历表
        sql_check = "SELECT COUNT(*) FROM trade_calendar WHERE is_open = 1"
        result = self.query(sql_check)

        if result and result[0][0] > 0:
            # 使用交易日历表（高效准确）
            sql = """
                SELECT trade_date FROM trade_calendar
                WHERE trade_date >= ? AND trade_date <= ? AND is_open = 1
                EXCEPT
                SELECT trade_date FROM concept_daily WHERE ts_code = ?
            """
            results = self.query(sql, (start_date, end_date, ts_code))
            return [r[0] for r in results]
        else:
            # 回退到简单日期生成（跳过周末）
            logger.warning("交易日历表为空，使用简单日期生成")
            sql = """
                SELECT DISTINCT trade_date
                FROM concept_daily
                WHERE ts_code = ?
                  AND trade_date BETWEEN ? AND ?
            """
            results = self.query(sql, (ts_code, start_date, end_date))
            existing_dates = set(row[0] for row in results)

            from datetime import timedelta
            start = datetime.strptime(start_date, "%Y%m%d")
            end = datetime.strptime(end_date, "%Y%m%d")

            all_dates = []
            current = start
            while current <= end:
                # 跳过周末
                if current.weekday() < 5:
                    date_str = current.strftime("%Y%m%d")
                    if date_str not in existing_dates:
                        all_dates.append(date_str)
                current += timedelta(days=1)

            return all_dates

    def has_data(
        self,
        ts_code: str,
        trade_date: str
    ) -> bool:
        """
        检查数据是否存在

        Args:
            ts_code: 板块代码
            trade_date: 交易日期

        Returns:
            是否存在
        """
        sql = """
            SELECT 1 FROM concept_daily
            WHERE ts_code = ? AND trade_date = ?
            LIMIT 1
        """
        results = self.query(sql, (ts_code, trade_date))
        return len(results) > 0

    def get_latest_date(self, ts_code: str = None) -> Optional[str]:
        """
        获取最新交易日期

        Args:
            ts_code: 板块代码，如果为 None 则返回所有板块的最新日期

        Returns:
            最新日期字符串 (YYYYMMDD)
        """
        if ts_code:
            sql = """
                SELECT MAX(trade_date) FROM concept_daily
                WHERE ts_code = ?
            """
            results = self.query(sql, (ts_code,))
        else:
            sql = "SELECT MAX(trade_date) FROM concept_daily"
            results = self.query(sql)

        if results and results[0][0]:
            return str(results[0][0])
        return None

    def get_data_range(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取指定范围的数据

        Args:
            ts_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pandas DataFrame
        """
        sql = """
            SELECT * FROM concept_daily
            WHERE ts_code = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date
        """
        return self.query_to_dataframe(sql, (ts_code, start_date, end_date))

    def get_all_concept_data(self, trade_date: str = None) -> pd.DataFrame:
        """
        获取所有板块数据

        Args:
            trade_date: 交易日期，如果为 None 则返回所有数据

        Returns:
            pandas DataFrame
        """
        if trade_date:
            sql = """
                SELECT * FROM concept_daily
                WHERE trade_date = ?
                ORDER BY ts_code
            """
            return self.query_to_dataframe(sql, (trade_date,))
        else:
            sql = "SELECT * FROM concept_daily ORDER BY ts_code, trade_date"
            return self.query_to_dataframe(sql)

    def save_concept_info(self, info: Dict[str, Any]):
        """保存板块信息"""
        info['updated_at'] = datetime.now().strftime("%Y%m%d")
        self.batch_insert('concept_info', [info], replace=True)

    def save_industry_info(self, info: Dict[str, Any]):
        """保存行业信息"""
        info['updated_at'] = datetime.now().strftime("%Y%m%d")
        self.batch_insert('industry_info', [info], replace=True)

    def save_concept_daily(
        self,
        ts_code: str,
        data: Dict[str, Any],
        replace: bool = True
    ):
        """
        保存板块日线数据

        Args:
            ts_code: 板块代码
            data: 数据字典
            replace: 是否覆盖已有数据
        """
        data['ts_code'] = ts_code
        self.batch_insert('concept_daily', [data], replace)

    def save_concept_daily_batch(
        self,
        df: pd.DataFrame,
        replace: bool = True
    ):
        """
        批量保存板块日线数据

        Args:
            df: DataFrame (必须包含 ts_code, trade_date 等列)
            replace: 是否覆盖已有数据
        """
        self.batch_insert_dataframe('concept_daily', df, replace)

    def create_collect_task(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> int:
        """
        创建采集任务

        Args:
            ts_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            任务 ID
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = """
            INSERT INTO collect_task (ts_code, start_date, end_date, status, created_at, updated_at)
            VALUES (?, ?, ?, 'pending', ?, ?)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (ts_code, start_date, end_date, now, now))
            conn.commit()
            return cursor.lastrowid

    def update_task_status(
        self,
        task_id: int,
        status: str,
        error_message: str = None
    ):
        """
        更新任务状态

        Args:
            task_id: 任务 ID
            status: 新状态 (pending/running/done/failed)
            error_message: 错误信息
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if error_message:
            sql = """
                UPDATE collect_task
                SET status = ?, error_message = ?, updated_at = ?
                WHERE id = ?
            """
            self.execute(sql, (status, error_message, now, task_id))
        else:
            sql = """
                UPDATE collect_task
                SET status = ?, updated_at = ?
                WHERE id = ?
            """
            self.execute(sql, (status, now, task_id))

    def get_pending_tasks(self, limit: int = 100) -> List[Dict]:
        """
        获取待处理的任务

        Args:
            limit: 数量限制

        Returns:
            任务列表
        """
        sql = """
            SELECT id, ts_code, start_date, end_date
            FROM collect_task
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT ?
        """
        results = self.query(sql, (limit,))
        return [
            {
                'id': r[0],
                'ts_code': r[1],
                'start_date': r[2],
                'end_date': r[3]
            }
            for r in results
        ]

    def export_to_csv(
        self,
        query: str,
        params: tuple,
        output_path: str
    ):
        """
        将查询结果导出为 CSV（兼容下游分析流程）

        Args:
            query: SQL 查询语句
            params: 查询参数
            output_path: 输出文件路径
        """
        df = self.query_to_dataframe(query, params)
        df.to_csv(output_path, index=False)
        logger.info(f"导出数据到 CSV: {output_path} ({len(df)} 条记录)")

    def get_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        stats = {}

        # 总记录数
        sql = "SELECT COUNT(*) FROM concept_daily"
        result = self.query(sql)
        stats['total_records'] = result[0][0] if result else 0

        # 板块数量
        sql = "SELECT COUNT(DISTINCT ts_code) FROM concept_daily"
        result = self.query(sql)
        stats['concept_count'] = result[0][0] if result else 0

        # 日期范围
        sql = "SELECT MIN(trade_date), MAX(trade_date) FROM concept_daily"
        result = self.query(sql)
        if result and result[0][0]:
            stats['date_range'] = (str(result[0][0]), str(result[0][1]))
        else:
            stats['date_range'] = (None, None)

        # 检查重复
        sql = """
            SELECT COUNT(*) - COUNT(DISTINCT ts_code || trade_date) as dup_count
            FROM concept_daily
        """
        result = self.query(sql)
        stats['duplicates'] = result[0][0] if result else 0

        return stats

    def vacuum(self):
        """清理数据库空间"""
        logger.info("清理数据库空间...")
        with self.get_connection() as conn:
            conn.execute("VACUUM")
            conn.commit()
        logger.info("数据库清理完成")

    def close(self):
        """关闭数据库连接"""
        logger.info("关闭数据库连接...")
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Exception:
                pass
        self._initialized = False

    # ==================== 个股数据操作方法 ====================

    def save_stock_daily(
        self,
        ts_code: str,
        data: Dict[str, Any],
        replace: bool = True
    ):
        """
        保存个股日线数据

        Args:
            ts_code: 个股代码
            data: 数据字典
            replace: 是否覆盖已有数据
        """
        data['ts_code'] = ts_code
        self.batch_insert('stock_daily', [data], replace)

    def save_stock_daily_batch(
        self,
        df: pd.DataFrame,
        replace: bool = True
    ):
        """
        批量保存个股日线数据

        Args:
            df: DataFrame (必须包含 ts_code, trade_date 等列)
            replace: 是否覆盖已有数据
        """
        self.batch_insert_dataframe('stock_daily', df, replace)

    def get_stock_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取个股数据

        Args:
            ts_code: 个股代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pandas DataFrame
        """
        sql = """
            SELECT * FROM stock_daily
            WHERE ts_code = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date
        """
        return self.query_to_dataframe(sql, (ts_code, start_date, end_date))

    def get_all_stock_data(self, trade_date: str = None) -> pd.DataFrame:
        """
        获取所有个股数据

        Args:
            trade_date: 交易日期，如果为 None 则返回所有数据

        Returns:
            pandas DataFrame
        """
        if trade_date:
            sql = """
                SELECT * FROM stock_daily
                WHERE trade_date = ?
                ORDER BY ts_code
            """
            return self.query_to_dataframe(sql, (trade_date,))
        else:
            sql = "SELECT * FROM stock_daily ORDER BY ts_code, trade_date"
            return self.query_to_dataframe(sql)

    def get_stock_missing_dates(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        获取个股缺失的交易日期（优化：使用交易日历表）

        Args:
            ts_code: 个股代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            缺失的日期列表
        """
        # 优先使用交易日历表
        sql_check = "SELECT COUNT(*) FROM trade_calendar WHERE is_open = 1"
        result = self.query(sql_check)

        if result and result[0][0] > 0:
            # 使用交易日历表（高效准确）
            sql = """
                SELECT trade_date FROM trade_calendar
                WHERE trade_date >= ? AND trade_date <= ? AND is_open = 1
                EXCEPT
                SELECT trade_date FROM stock_daily WHERE ts_code = ?
            """
            results = self.query(sql, (start_date, end_date, ts_code))
            return [r[0] for r in results]
        else:
            # 回退到简单日期生成（跳过周末）
            sql = """
                SELECT DISTINCT trade_date
                FROM stock_daily
                WHERE ts_code = ?
                  AND trade_date BETWEEN ? AND ?
            """
            results = self.query(sql, (ts_code, start_date, end_date))
            existing_dates = set(row[0] for row in results)

            from datetime import timedelta
            start = datetime.strptime(start_date, "%Y%m%d")
            end = datetime.strptime(end_date, "%Y%m%d")

            all_dates = []
            current = start
            while current <= end:
                if current.weekday() < 5:
                    date_str = current.strftime("%Y%m%d")
                    if date_str not in existing_dates:
                        all_dates.append(date_str)
                current += timedelta(days=1)

            return all_dates

    # ==================== 成分股操作方法 ====================

    def save_concept_constituents(
        self,
        concept_code: str,
        constituents: List[Dict[str, Any]]
    ):
        """
        保存板块成分股

        Args:
            concept_code: 板块代码
            constituents: 成分股列表，每个 dict 包含 stock_code, stock_name, weight 等
        """
        updated_at = datetime.now().strftime("%Y%m%d")
        data = []
        for stock in constituents:
            record = {
                'concept_code': concept_code,
                'stock_code': stock.get('stock_code'),
                'stock_name': stock.get('stock_name', ''),
                'weight': stock.get('weight'),
                'is_core': stock.get('is_core', 1),
                'listed_date': stock.get('listed_date'),
                'updated_at': updated_at
            }
            data.append(record)
        self.batch_insert('concept_constituent', data, replace=True)

    def get_concept_constituents(self, concept_code: str) -> List[Dict[str, Any]]:
        """
        获取板块成分股

        Args:
            concept_code: 板块代码

        Returns:
            成分股列表
        """
        sql = """
            SELECT stock_code, stock_name, weight, is_core, listed_date
            FROM concept_constituent
            WHERE concept_code = ?
            ORDER BY weight DESC NULLS LAST
        """
        results = self.query(sql, (concept_code,))
        return [
            {
                'stock_code': r[0],
                'stock_name': r[1],
                'weight': r[2],
                'is_core': bool(r[3]) if r[3] is not None else True,
                'listed_date': r[4]
            }
            for r in results
        ]

    def get_all_constituents(self) -> List[Dict[str, Any]]:
        """
        获取所有成分股（去重）

        Returns:
            所有成分股列表
        """
        sql = """
            SELECT DISTINCT stock_code, stock_name
            FROM concept_constituent
            ORDER BY stock_code
        """
        results = self.query(sql)
        return [
            {
                'stock_code': r[0],
                'stock_name': r[1]
            }
            for r in results
        ]

    def get_constituent_stocks(
        self,
        concept_codes: List[str],
        limit_per_concept: int = None
    ) -> pd.DataFrame:
        """
        获取多个板块的成分股

        Args:
            concept_codes: 板块代码列表
            limit_per_concept: 每个板块限制的数量

        Returns:
            pandas DataFrame
        """
        if not concept_codes:
            return pd.DataFrame()

        placeholders = ','.join(['?' for _ in concept_codes])
        if limit_per_concept:
            sql = f"""
                SELECT concept_code, stock_code, stock_name, weight, is_core
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY concept_code ORDER BY weight DESC NULLS LAST) as rn
                    FROM concept_constituent
                    WHERE concept_code IN ({placeholders})
                )
                WHERE rn <= ?
            """
            params = tuple(concept_codes) + (limit_per_concept,)
        else:
            sql = f"""
                SELECT concept_code, stock_code, stock_name, weight, is_core
                FROM concept_constituent
                WHERE concept_code IN ({placeholders})
            """
            params = tuple(concept_codes)

        return self.query_to_dataframe(sql, params)

    # ==================== 个股因子操作方法 ====================

    def save_stock_factors(
        self,
        ts_code: str,
        factors: Dict[str, Any]
    ):
        """
        保存个股因子

        Args:
            ts_code: 个股代码
            factors: 因子数据字典
        """
        factors['ts_code'] = ts_code
        self.batch_insert('stock_factors', [factors], replace=True)

    def get_stock_factors(
        self,
        ts_code: str,
        trade_date: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取个股因子

        Args:
            ts_code: 个股代码
            trade_date: 交易日期

        Returns:
            因子数据字典
        """
        if trade_date:
            sql = """
                SELECT * FROM stock_factors
                WHERE ts_code = ? AND trade_date = ?
            """
            results = self.query(sql, (ts_code, trade_date))
        else:
            sql = """
                SELECT * FROM stock_factors
                WHERE ts_code = ?
                ORDER BY trade_date DESC
                LIMIT 1
            """
            results = self.query(sql, (ts_code,))

        if results:
            cols = ['ts_code', 'trade_date', 'market_cap', 'pe_ttm', 'pb_ttm', 'ps_ttm',
                    'momentum_20d', 'momentum_60d', 'volatility_20d', 'avg_turnover_20d', 'avg_amount_20d']
            return dict(zip(cols, results[0]))
        return None

    # ==================== 统计方法 ====================

    def get_stock_statistics(self) -> Dict[str, Any]:
        """获取个股数据库统计信息"""
        stats = {}

        # 总记录数
        sql = "SELECT COUNT(*) FROM stock_daily"
        result = self.query(sql)
        stats['total_records'] = result[0][0] if result else 0

        # 个股数量
        sql = "SELECT COUNT(DISTINCT ts_code) FROM stock_daily"
        result = self.query(sql)
        stats['stock_count'] = result[0][0] if result else 0

        # 日期范围
        sql = "SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily"
        result = self.query(sql)
        if result and result[0][0]:
            stats['date_range'] = (str(result[0][0]), str(result[0][1]))
        else:
            stats['date_range'] = (None, None)

        return stats

    # ==================== 交易日历操作方法 ====================

    def init_trade_calendar(self, start_date: str = "20100101", end_date: str = None):
        """
        初始化交易日历（需要从 Tushare 获取）

        Args:
            start_date: 开始日期
            end_date: 结束日期，默认到今天
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"初始化交易日历: {start_date} - {end_date}")

        try:
            import tushare as ts
            from core.settings import settings as core_settings

            ts.set_token(core_settings.tushare_token)
            pro = ts.pro_api()

            # 获取交易日历
            cal = pro.trade_cal(
                exchange='SSE',
                start_date=start_date,
                end_date=end_date
            )

            if cal.empty:
                logger.warning("未获取到交易日历数据")
                return

            # 准备数据
            records = []
            for _, row in cal.iterrows():
                records.append({
                    'trade_date': row['cal_date'],
                    'is_open': row.get('is_open', 1),
                    'pre_trade_date': row.get('pretrade_date'),
                    'next_trade_date': None  # 后续计算
                })

            # 批量插入
            self.batch_insert('trade_calendar', records, replace=True)

            # 更新 next_trade_date
            self._update_trade_calendar_links()

            logger.info(f"交易日历初始化完成: {len(records)} 条记录")

        except Exception as e:
            logger.error(f"初始化交易日历失败: {e}")
            raise

    def _update_trade_calendar_links(self):
        """更新交易日历的前后交易日关联"""
        sql = """
            UPDATE trade_calendar SET next_trade_date = (
                SELECT trade_date FROM trade_calendar t2
                WHERE t2.trade_date > trade_calendar.trade_date
                  AND t2.is_open = 1
                ORDER BY t2.trade_date LIMIT 1
            )
            WHERE is_open = 1
        """
        self.execute(sql)

    def get_trade_dates(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> List[str]:
        """
        获取交易日列表

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表
        """
        if start_date and end_date:
            sql = """
                SELECT trade_date FROM trade_calendar
                WHERE trade_date >= ? AND trade_date <= ? AND is_open = 1
                ORDER BY trade_date
            """
            results = self.query(sql, (start_date, end_date))
        elif start_date:
            sql = """
                SELECT trade_date FROM trade_calendar
                WHERE trade_date >= ? AND is_open = 1
                ORDER BY trade_date
            """
            results = self.query(sql, (start_date,))
        elif end_date:
            sql = """
                SELECT trade_date FROM trade_calendar
                WHERE trade_date <= ? AND is_open = 1
                ORDER BY trade_date
            """
            results = self.query(sql, (end_date,))
        else:
            sql = """
                SELECT trade_date FROM trade_calendar
                WHERE is_open = 1
                ORDER BY trade_date
            """
            results = self.query(sql)

        return [r[0] for r in results]

    def is_trade_date(self, date: str) -> bool:
        """
        检查是否为交易日

        Args:
            date: 日期

        Returns:
            是否为交易日
        """
        sql = "SELECT is_open FROM trade_calendar WHERE trade_date = ?"
        result = self.query(sql, (date,))
        return result and result[0][0] == 1

    def get_prev_trade_date(self, date: str) -> Optional[str]:
        """获取前一个交易日"""
        sql = "SELECT pre_trade_date FROM trade_calendar WHERE trade_date = ? AND is_open = 1"
        result = self.query(sql, (date,))
        return result[0][0] if result and result[0][0] else None

    def get_next_trade_date(self, date: str) -> Optional[str]:
        """获取后一个交易日"""
        sql = "SELECT next_trade_date FROM trade_calendar WHERE trade_date = ? AND is_open = 1"
        result = self.query(sql, (date,))
        return result[0][0] if result and result[0][0] else None

    # ==================== 数据归档操作方法 ====================

    def archive_old_data(
        self,
        before_date: str,
        tables: List[str] = None
    ) -> Dict[str, int]:
        """
        归档历史数据

        Args:
            before_date: 归档此日期之前的数据
            tables: 要归档的表列表，默认 ['concept_daily', 'stock_daily']

        Returns:
            各表归档记录数
        """
        if tables is None:
            tables = ['concept_daily', 'stock_daily']

        results = {}
        logger.info(f"开始归档 {before_date} 之前的数据...")

        for table in tables:
            archive_table = f"{table}_archive"

            # 检查归档表是否存在
            check_sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{archive_table}'"
            if not self.query(check_sql):
                logger.warning(f"归档表 {archive_table} 不存在，跳过")
                continue

            # 迁移数据到归档表
            insert_sql = f"""
                INSERT OR IGNORE INTO {archive_table}
                SELECT * FROM {table} WHERE trade_date < ?
            """
            with self.get_connection() as conn:
                cursor = conn.execute(insert_sql, (before_date,))
                archived_count = cursor.rowcount
                conn.commit()

            # 删除已归档的数据
            delete_sql = f"DELETE FROM {table} WHERE trade_date < ?"
            deleted_count = self.execute(delete_sql, (before_date,))

            results[table] = {'archived': archived_count, 'deleted': deleted_count}
            logger.info(f"{table}: 归档 {archived_count} 条, 删除 {deleted_count} 条")

        # 清理空间
        self.vacuum()

        return results

    def restore_archived_data(
        self,
        start_date: str,
        end_date: str,
        tables: List[str] = None
    ) -> Dict[str, int]:
        """
        从归档表恢复数据

        Args:
            start_date: 开始日期
            end_date: 结束日期
            tables: 要恢复的表列表

        Returns:
            各表恢复记录数
        """
        if tables is None:
            tables = ['concept_daily', 'stock_daily']

        results = {}

        for table in tables:
            archive_table = f"{table}_archive"

            # 从归档表恢复
            insert_sql = f"""
                INSERT OR REPLACE INTO {table}
                SELECT * FROM {archive_table}
                WHERE trade_date >= ? AND trade_date <= ?
            """
            with self.get_connection() as conn:
                cursor = conn.execute(insert_sql, (start_date, end_date))
                restored_count = cursor.rowcount
                conn.commit()

            results[table] = restored_count
            logger.info(f"{table}: 恢复 {restored_count} 条")

        return results

    def get_archive_statistics(self) -> Dict[str, Any]:
        """获取归档统计信息"""
        stats = {}

        # 概念板块归档统计
        sql = "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM concept_daily_archive"
        result = self.query(sql)
        stats['concept_archive'] = {
            'count': result[0][0] if result else 0,
            'date_range': (result[0][1], result[0][2]) if result and result[0][1] else (None, None)
        }

        # 个股归档统计
        sql = "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM stock_daily_archive"
        result = self.query(sql)
        stats['stock_archive'] = {
            'count': result[0][0] if result else 0,
            'date_range': (result[0][1], result[0][2]) if result and result[0][1] else (None, None)
        }

        return stats

    # ==================== 性能统计方法 ====================

    def get_performance_stats(self) -> Dict[str, Any]:
        """获取数据库性能统计"""
        stats = {}

        # 页面统计
        result = self.query("PRAGMA page_count")
        stats['page_count'] = result[0][0] if result else 0

        result = self.query("PRAGMA page_size")
        stats['page_size'] = result[0][0] if result else 4096

        stats['db_size_mb'] = round(
            stats['page_count'] * stats['page_size'] / 1024 / 1024, 2
        )

        # WAL 状态
        try:
            result = self.query("PRAGMA wal_checkpoint(PASSIVE)")
            stats['wal_busy'] = result[0][0] if result else 0
            stats['wal_log'] = result[0][1] if result else 0
            stats['wal_checkpointed'] = result[0][2] if result else 0
        except:
            stats['wal_status'] = 'unavailable'

        # 缓存状态
        try:
            result = self.query("PRAGMA cache_size")
            stats['cache_size'] = result[0][0] if result else 0
        except:
            pass

        # 主表记录数
        stats['concept_daily_count'] = self.query("SELECT COUNT(*) FROM concept_daily")[0][0]
        stats['stock_daily_count'] = self.query("SELECT COUNT(*) FROM stock_daily")[0][0]

        # 数据日期范围
        result = self.query("SELECT MIN(trade_date), MAX(trade_date) FROM concept_daily")
        if result and result[0][0]:
            stats['concept_date_range'] = (result[0][0], result[0][1])

        result = self.query("SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily")
        if result and result[0][0]:
            stats['stock_date_range'] = (result[0][0], result[0][1])

        return stats

    def optimize_database(self):
        """优化数据库（重建索引、更新统计、清理空间）"""
        logger.info("开始优化数据库...")

        with self.get_connection() as conn:
            # 重建索引
            conn.execute("REINDEX concept_daily")
            conn.execute("REINDEX stock_daily")
            conn.execute("REINDEX concept_constituent")

            # 更新统计信息
            conn.execute("ANALYZE")

            # 完整 checkpoint
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            conn.commit()

        # 清理空间
        self.vacuum()

        logger.info("数据库优化完成")
_db_instance: Optional[SQLiteDatabase] = None


def get_database(db_path: str = None, pool_size: int = 5) -> SQLiteDatabase:
    """获取数据库单例实例"""
    global _db_instance
    if _db_instance is None:
        _db_instance = SQLiteDatabase(db_path, pool_size)
    return _db_instance


def init_database(db_path: str = None, pool_size: int = 5) -> SQLiteDatabase:
    """强制初始化新的数据库实例"""
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
    _db_instance = SQLiteDatabase(db_path, pool_size)
    return _db_instance
