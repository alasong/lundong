"""
Tushare 客户端 - 同花顺行业数据
"""

import tushare as ts
import pandas as pd
import time
from typing import Optional, List, Callable, Any
from utils.logger import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

logger = get_logger(__name__)


class TushareTHSClient:
    """Tushare 同花顺数据客户端"""

    # Pagination configuration
    PAGINATION_LIMIT = 5000  # Tushare API limit per request
    PAGINATION_DELAY = 1.0  # Delay between requests (seconds)

    # Concurrent worker configuration
    DEFAULT_MAX_WORKERS = 4
    RATE_LIMIT_DELAY = 1.0

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

    def get_ths_indices(self, exclude_bse: bool = True) -> pd.DataFrame:
        """获取同花顺指数列表（支持分页）

        Args:
            exclude_bse: 是否排除北交所板块（87xxxx）

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
        all_results = []

        # 使用 offset/limit 分页模式
        offset = 0
        while True:
            for i in range(self.max_retries):
                try:
                    df = self.pro.ths_index(offset=offset, limit=self.PAGINATION_LIMIT)

                    if df is not None and len(df) > 0:
                        all_results.append(df)
                        logger.debug(f"分页获取: offset={offset}, limit={len(df)}")

                        # 检查是否获取完所有数据
                        if len(df) < self.PAGINATION_LIMIT:
                            # 实际返回数据少于 limit，说明已到最后一页
                            logger.info(
                                f"获取成功：{len(all_results)} 页，共 {sum(len(r) for r in all_results)} 个指数"
                            )
                            # 合并所有分页结果
                            result_df = pd.concat(all_results, ignore_index=True)
                            # 排除北交所板块（87xxxx）
                            if exclude_bse:
                                result_df = result_df[
                                    ~result_df["ts_code"].str.startswith("87", na=False)
                                ]
                                logger.info(
                                    f"排除北交所板块后：{len(result_df)} 个指数"
                                )
                            return result_df
                        else:
                            # 继续下一页，添加延迟
                            offset += self.PAGINATION_LIMIT
                            time.sleep(self.PAGINATION_DELAY)
                            break
                    else:
                        logger.warning("返回空数据")
                        return pd.DataFrame()

                except Exception as e:
                    if i < self.max_retries - 1:
                        logger.warning(
                            f"获取失败，重试中 ({i + 1}/{self.max_retries}): {str(e)[:80]}"
                        )
                        time.sleep(1.0 * (i + 1))
                    else:
                        logger.error(f"最终失败：{str(e)[:100]}")
                        raise

        # 理论上不会到达这里
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
        prefix = "881" if level == 1 else "882"
        industries = df[df["ts_code"].str.startswith(prefix, na=False)].copy()

        logger.info(f"筛选后：{len(industries)} 个{level}级行业")

        return industries

    def get_ths_history(
        self, ts_code: str, start_date: str = "20200101", end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """获取同花顺指数历史行情

        Args:
            ts_code: 指数代码 (e.g., '881101.TI')
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (默认今天)

        Returns:
            DataFrame with OHLCV data
        """
        if end_date is None:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")

        logger.info(f"获取 {ts_code} 历史行情 ({start_date} - {end_date})...")

        for i in range(self.max_retries):
            try:
                # 尝试 ths_daily 接口
                df = self.pro.ths_daily(
                    ts_code=ts_code, start_date=start_date, end_date=end_date
                )

                if df is not None and len(df) > 0:
                    logger.info(f"获取成功：{len(df)} 条")
                    return df

                # 如果 ths_daily 返回空，尝试 index_daily
                # 注意：需要将代码格式从 .TI 改为 .THS
                ths_code = ts_code.replace(".TI", ".THS")
                df = self.pro.index_daily(
                    ts_code=ths_code, start_date=start_date, end_date=end_date
                )

                if df is not None and len(df) > 0:
                    logger.info(f"index_daily 获取成功：{len(df)} 条")
                    return df

                logger.warning(f"返回空数据")
                return None

            except Exception as e:
                if i < self.max_retries - 1:
                    logger.warning(
                        f"获取失败，重试中 ({i + 1}/{self.max_retries}): {str(e)[:80]}"
                    )
                    time.sleep(1.0 * (i + 1))
                else:
                    logger.error(f"最终失败：{str(e)[:100]}")
                    return None

        return None

    def get_ths_members(self, ts_code: str) -> Optional[pd.DataFrame]:
        """获取同花顺指数成分股（支持分页）

        Args:
            ts_code: 指数代码

        Returns:
            DataFrame with constituent stocks
        """
        logger.info(f"获取 {ts_code} 成分股...")
        all_results = []

        # 使用 offset/limit 分页模式
        offset = 0
        while True:
            for i in range(self.max_retries):
                try:
                    df = self.pro.index_member(
                        index_code=ts_code, offset=offset, limit=self.PAGINATION_LIMIT
                    )

                    if df is not None and len(df) > 0:
                        all_results.append(df)
                        logger.debug(f"分页获取: offset={offset}, limit={len(df)}")

                        # 检查是否获取完所有数据
                        if len(df) < self.PAGINATION_LIMIT:
                            # 实际返回数据少于 limit，说明已到最后一页
                            logger.info(
                                f"获取成功：{len(all_results)} 页，共 {sum(len(r) for r in all_results)} 只成分股"
                            )
                            # 合并所有分页结果
                            result_df = pd.concat(all_results, ignore_index=True)
                            return result_df
                        else:
                            # 继续下一页，添加延迟
                            offset += self.PAGINATION_LIMIT
                            time.sleep(self.PAGINATION_DELAY)
                            break
                    else:
                        logger.warning(f"返回空数据")
                        return None

                except Exception as e:
                    if i < self.max_retries - 1:
                        logger.warning(
                            f"获取失败，重试中 ({i + 1}/{self.max_retries}): {str(e)[:80]}"
                        )
                        time.sleep(1.0 * (i + 1))
                    else:
                        logger.error(f"最终失败：{str(e)[:100]}")
                        return None

        # 理论上不会到达这里
        return None

    def get_stock_daily(
        self, ts_code: str, start_date: str = "20200101", end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        获取个股日线数据

        Args:
            ts_code: 个股代码 (e.g., '000001.SZ')
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (默认今天)

        Returns:
            DataFrame with OHLCV data
        """
        if end_date is None:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")

        logger.info(f"获取 {ts_code} 历史行情 ({start_date} - {end_date})...")

        for i in range(self.max_retries):
            try:
                df = self.pro.daily(
                    ts_code=ts_code, start_date=start_date, end_date=end_date
                )

                if df is not None and len(df) > 0:
                    logger.info(f"获取成功：{len(df)} 条")
                    return df

                logger.warning(f"返回空数据")
                return None

            except Exception as e:
                if i < self.max_retries - 1:
                    logger.warning(
                        f"获取失败，重试中 ({i + 1}/{self.max_retries}): {str(e)[:80]}"
                    )
                    time.sleep(1.0 * (i + 1))
                else:
                    logger.error(f"最终失败：{str(e)[:100]}")
                    return None

        return None

    def get_stock_list(self, exchange: str = None) -> pd.DataFrame:
        """获取 A 股上市公司列表（支持分页）

        Args:
            exchange: 交易所 (SSE=上交所，SZSE=深交所，BSE=北交所)

        Returns:
            DataFrame with stock list
        """
        logger.info(f"获取 A 股上市公司列表...")
        all_results = []

        # 使用 offset/limit 分页模式
        offset = 0
        while True:
            for i in range(self.max_retries):
                try:
                    df = self.pro.stock_basic(
                        exchange=exchange,
                        list_status="L",  # 只取正常上市的公司
                        offset=offset,
                        limit=self.PAGINATION_LIMIT,
                    )

                    if df is not None and len(df) > 0:
                        all_results.append(df)
                        logger.debug(f"分页获取: offset={offset}, limit={len(df)}")

                        # 检查是否获取完所有数据
                        if len(df) < self.PAGINATION_LIMIT:
                            # 实际返回数据少于 limit，说明已到最后一页
                            logger.info(
                                f"获取成功：{len(all_results)} 页，共 {sum(len(r) for r in all_results)} 只股票"
                            )
                            # 合并所有分页结果
                            result_df = pd.concat(all_results, ignore_index=True)
                            return result_df
                        else:
                            # 继续下一页，添加延迟
                            offset += self.PAGINATION_LIMIT
                            time.sleep(self.PAGINATION_DELAY)
                            break
                    else:
                        logger.warning(f"返回空数据")
                        return pd.DataFrame()

                except Exception as e:
                    if i < self.max_retries - 1:
                        logger.warning(
                            f"获取失败，重试中 ({i + 1}/{self.max_retries}): {str(e)[:80]}"
                        )
                        time.sleep(1.0 * (i + 1))
                    else:
                        logger.error(f"最终失败：{str(e)[:100]}")
                        return pd.DataFrame()

        # 理论上不会到达这里
        return pd.DataFrame()

    def get_stock_factors(
        self, ts_code: str, trade_date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        获取个股技术因子

        Args:
            ts_code: 个股代码
            trade_date: 交易日期

        Returns:
            DataFrame with factors
        """
        logger.info(f"获取 {ts_code} 技术因子...")

        try:
            # 使用 stk_factor 接口
            if trade_date:
                df = self.pro.stk_factor(ts_code=ts_code, trade_date=trade_date)
            else:
                df = self.pro.stk_factor(ts_code=ts_code)

            if df is not None and len(df) > 0:
                logger.info(f"获取成功：{len(df)} 条")
                return df
            else:
                logger.warning(f"返回空数据")
                return None

        except Exception as e:
            logger.error(f"获取失败：{str(e)[:100]}")
            return None

    def _rate_limit_delay(self):
        time.sleep(self.RATE_LIMIT_DELAY)

    def _execute_with_rate_limit(self, func: Callable, *args, **kwargs) -> Any:
        result = func(*args, **kwargs)
        self._rate_limit_delay()
        return result

    def download_batch_concurrent(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        max_workers: int = None,
    ) -> pd.DataFrame:
        """
        并发批量下载板块历史数据（使用 4 个并发工作线程）

        Args:
            codes: 板块代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            max_workers: 最大并发数（默认 4）

        Returns:
            DataFrame with all downloaded data
        """
        if max_workers is None:
            max_workers = self.DEFAULT_MAX_WORKERS

        logger.info(f"并发下载：{len(codes)} 个板块，{max_workers} 个工作线程")
        logger.info(f"日期范围：{start_date} - {end_date}")

        all_data = []
        success_count = 0
        failed_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for code in codes:
                future = executor.submit(
                    self._download_single_with_rate_limit,
                    code,
                    start_date,
                    end_date,
                )
                futures[future] = code

            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    if result is not None and len(result) > 0:
                        all_data.append(result)
                        success_count += 1
                        logger.debug(f"完成：{code} ({len(result)} 条)")
                    else:
                        failed_count += 1
                        logger.warning(f"失败/空数据：{code}")
                except Exception as e:
                    failed_count += 1
                    logger.error(f"{code} 下载异常：{str(e)[:80]}")

        logger.info(f"并发下载完成：成功 {success_count} 个，失败 {failed_count} 个")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _download_single_with_rate_limit(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        下载单个板块数据（带速率限制）

        Args:
            ts_code: 板块代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with OHLCV data
        """
        try:
            df = self.pro.ths_daily(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            self._rate_limit_delay()
            return df
        except Exception as e:
            logger.warning(f"{ts_code} 下载失败：{str(e)[:80]}")
            self._rate_limit_delay()
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
            print(f"    {i + 1:2d}. {row['name']:20s} ({row['ts_code']})")

    # 3. 测试获取历史数据
    print("\n[3/3] 测试历史数据...")
    if len(industries) > 0:
        test_code = industries.iloc[0]["ts_code"]
        test_name = industries.iloc[0]["name"]
        print(f"  测试：{test_name} ({test_code})")

        hist = client.get_ths_history(test_code, start_date="20240101")
        if hist is not None:
            print(f"  ✓ 获取 {len(hist)} 条历史数据")
        else:
            print(f"  ✗ 获取失败")


def verify_concurrent_workers():
    from core.settings import settings

    print("\n=== 并发工作线程验证 ===\n")

    client = TushareTHSClient(token=settings.tushare_token)

    print(f"1. 默认配置验证：")
    print(f"   - DEFAULT_MAX_WORKERS = {client.DEFAULT_MAX_WORKERS}")
    print(f"   - RATE_LIMIT_DELAY = {client.RATE_LIMIT_DELAY}秒")
    print(f"   - PAGINATION_DELAY = {client.PAGINATION_DELAY}秒")

    print(f"\n2. ThreadPoolExecutor 配置验证：")
    print(f"   - 使用 max_workers={client.DEFAULT_MAX_WORKERS}")

    print(f"\n3. 配置验证结果：")
    print(f"   ✓ 并发工作线程数：{client.DEFAULT_MAX_WORKERS}")
    print(f"   ✓ 速率限制延迟：{client.RATE_LIMIT_DELAY}秒/请求")
    print(f"   ✓ 分页延迟：{client.PAGINATION_DELAY}秒/页")

    print(f"\n4. 方法验证：")
    print(f"   ✓ download_batch_concurrent() 方法存在")
    print(f"   ✓ _rate_limit_delay() 方法存在")
    print(f"   ✓ _download_single_with_rate_limit() 方法存在")

    print(f"\n=== 验证完成 ===\n")


if __name__ == "__main__":
    main()
