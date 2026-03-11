"""
预测 Agent - 简化版
负责统一预测（1 日/5 日/20 日）
"""
import pandas as pd
from typing import Optional, Dict, Any
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.base_agent import BaseAgent
from models.predictor import UnifiedPredictor
from config import settings
from data.name_mapper import load_name_mapping, get_block_name


class PredictAgent(BaseAgent):
    """预测 Agent - 简化版"""

    def __init__(self):
        super().__init__("PredictAgent")
        self.predictor = UnifiedPredictor()
        self.model_result = None

        # 尝试加载已存在的模型
        self.model_result = self.predictor.load_model()
        if self.model_result:
            logger.info("已加载预训练模型")

    def run(
        self,
        task: str = "predict",
        horizon: str = "all",
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行预测任务

        Args:
            task: 任务类型 predict/train
            horizon: 预测周期 all/1d/5d/20d（预留，当前默认使用 all）
            data: 输入数据
        """
        # horizon 参数预留用于未来单周期预测
        if task == "train":
            return self._train_models(data, **kwargs)
        elif task == "predict":
            return self._predict(data, **kwargs)
        else:
            raise ValueError(f"未知任务类型：{task}")

    def _train_models(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """训练模型（流式加载优化版）"""
        logger.info("开始训练模型...")

        # 使用流式加载训练数据
        logger.info("从数据库流式加载训练数据...")

        try:
            from data.database import get_database
            db = get_database()

            # 分批次从数据库读取数据并处理
            features = self._stream_prepare_features(db, n_jobs=32)

            if features is None or features.empty:
                return {"success": False, "error": "无概念数据"}

            logger.info(f"特征准备完成：{len(features)} 条样本")

            # 训练模型（使用所有 CPU 核心）
            self.model_result = self.predictor.train(features, n_jobs=-1)

            return {
                "success": True,
                "metrics": self.model_result["metrics"]
            }

        except Exception as e:
            logger.error(f"训练失败：{e}")
            return {"success": False, "error": str(e)}

    def _predict(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """执行预测"""
        logger.info("开始预测...")

        if data is None:
            data = self._load_latest_data()

        concept_data = data.get("concept")
        if concept_data is None or concept_data.empty:
            return {"success": False, "error": "无概念数据"}

        # 准备特征（默认 32 线程高并发）
        features = self.predictor.prepare_features(concept_data, n_jobs=32)
        if features.empty:
            logger.warning("特征为空，可能数据不足")
            # 返回简化预测（使用近期表现）
            return self._simple_prediction(concept_data)

        # 预测（带置信度评估）
        predictions = self.predictor.predict(self.model_result, features, with_confidence=True)

        if predictions.empty:
            return {"success": False, "error": "预测失败"}

        # 整理结果
        result = self._format_predictions(predictions)

        return {
            "success": True,
            "result": result
        }

    def _simple_prediction(self, concept_data: pd.DataFrame) -> Dict:
        """简化预测（当模型不可用时）"""
        logger.info("使用简化预测（基于近期表现）")

        # 获取最新日期
        latest_date = concept_data["trade_date"].max()
        latest = concept_data[concept_data["trade_date"] == latest_date]

        # 计算近期动量
        results = []
        for _, row in latest.iterrows():
            concept_code = row["concept_code"]
            concept_hist = concept_data[concept_data["concept_code"] == concept_code].sort_values("trade_date")

            if len(concept_hist) >= 5:
                recent_5d = concept_hist.tail(5)["pct_chg"].sum()
                recent_10d = concept_hist.tail(10)["pct_chg"].sum() if len(concept_hist) >= 10 else recent_5d
            else:
                recent_5d = 0
                recent_10d = 0

            results.append({
                "concept_code": concept_code,
                "concept_name": row.get("name", ""),
                "pred_1d": row.get("pct_chg", 0),
                "pred_5d": recent_5d,
                "pred_20d": recent_10d,
                "combined_score": recent_5d
            })

        result_df = pd.DataFrame(results).nlargest(20, "combined_score")

        return {
            "predictions": result_df.to_dict("records"),
            "note": "简化预测（基于近期动量）"
        }

    def _format_predictions(self, predictions: pd.DataFrame) -> Dict:
        """格式化预测结果 - 优化可读性（支持置信度）"""
        # 按综合评分排序
        ranked = predictions.nlargest(50, "combined_score")

        # 添加板块名称（如果只有 code）
        top_predictions = []
        for _, row in ranked.head(20).iterrows():
            pred = {
                "rank": int(row.name) if hasattr(row, 'name') else 0,
                "concept_code": row.get("concept_code", ""),
                "concept_name": row.get("concept_name", row.get("name", "")),
                "combined_score": round(row.get("combined_score", 0), 2),
                "pred_1d": round(row.get("pred_1d", 0), 2),
                "pred_5d": round(row.get("pred_5d", 0), 2),
                "pred_20d": round(row.get("pred_20d", 0), 2),
            }
            # 添加置信度信息（如果存在）
            if "confidence" in row:
                pred["confidence"] = round(row.get("confidence", 0), 3)
            if "confidence_level" in row:
                pred["confidence_level"] = row.get("confidence_level", "")
            top_predictions.append(pred)

        # 检查是否有高置信度的预测
        if "confidence_level" in ranked.columns:
            high_conf_count = (ranked["confidence_level"] == "高").sum()
            logger.info(f"高置信度预测数量：{high_conf_count}")

        return {
            "predictions": ranked.to_dict("records"),
            "top_10": top_predictions[:10],
            "top_20": top_predictions,
            "model_used": True,
            "confidence_available": "confidence" in ranked.columns
        }

    def _load_training_data(self) -> Dict[str, pd.DataFrame]:
        """
        加载训练数据（支持同花顺数据格式）- 从数据库读取
        """
        data = {}

        # 首先尝试从数据库读取
        try:
            from data.database import get_database
            db = get_database()

            # 从数据库加载概念数据
            df = db.get_all_concept_data()

            if not df.empty:
                # 重命名字段以匹配系统期望的格式
                if 'pct_change' in df.columns:
                    df = df.rename(columns={'pct_change': 'pct_chg'})
                if 'ts_code' in df.columns:
                    df = df.rename(columns={'ts_code': 'concept_code'})

                # 添加 name 字段（如果不存在）
                if 'name' not in df.columns:
                    df['name'] = df['concept_code']

                data["concept"] = df
                logger.info(f"从数据库加载训练数据：{len(df):,} 条记录")
                return data
        except Exception as e:
            logger.warning(f"从数据库加载数据失败：{e}")

        # 回退到文件读取
        raw_dir = settings.raw_data_dir

        if not os.path.exists(raw_dir):
            return data

        # 加载同花顺行业/概念数据 (ths_*_TI.csv 格式)
        ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

        if ths_files:
            # 使用并行读取加速
            from joblib import Parallel, delayed

            def load_single_file(filepath):
                try:
                    df = pd.read_csv(filepath, dtype={
                        'concept_code': str,
                        'trade_date': str,
                        'pct_chg': float,
                        'vol': float,
                        'close': float
                    })
                    # 重命名字段以匹配系统期望的格式
                    if 'pct_change' in df.columns:
                        df = df.rename(columns={'pct_change': 'pct_chg'})
                    if 'ts_code' in df.columns:
                        df = df.rename(columns={'ts_code': 'concept_code'})

                    # 处理 name 字段 - 从文件名提取或使用 code
                    filename = os.path.basename(filepath)
                    if 'name' not in df.columns:
                        # 尝试从文件名提取：ths_881101_TI.csv -> 881101
                        if filename.startswith('ths_') and '_TI.csv' in filename:
                            code_part = filename.replace('ths_', '').replace('_TI.csv', '')
                            df['name'] = f"板块_{code_part}"
                        else:
                            df['name'] = df['concept_code']
                    elif df['name'].iloc[0] == df['concept_code'].iloc[0]:
                        # 如果 name 等于 code，尝试从文件名获取更好的名称
                        if filename.startswith('ths_') and '_TI.csv' in filename:
                            code_part = filename.replace('ths_', '').replace('_TI.csv', '')
                            df['name'] = f"板块_{code_part}"

                    return df
                except Exception as e:
                    logger.warning(f"加载文件 {filepath} 失败：{e}")
                    return None

            # 并行加载所有文件
            dfs = Parallel(n_jobs=-1, backend="threading")(
                delayed(load_single_file)(os.path.join(raw_dir, f))
                for f in ths_files
            )
            dfs = [df for df in dfs if df is not None]

            if dfs:
                data["concept"] = pd.concat(dfs, ignore_index=True)
                logger.info(f"加载了 {len(ths_files)} 个同花顺数据文件用于训练，共 {len(data['concept'])} 条记录")

        return data

    def _load_latest_data(self, recent_days: int = 60) -> Dict[str, pd.DataFrame]:
        """
        加载最新数据（优先从数据库加载）- 优化版

        Args:
            recent_days: 加载最近 N 天的数据（默认 60 天，确保有足够数据用于特征计算）
        """
        data = {}

        # 优先从数据库加载
        try:
            from data.database import SQLiteDatabase
            db = SQLiteDatabase()

            df = db.get_all_concept_data()

            if not df.empty:
                # 重命名字段以匹配系统期望的格式
                if 'pct_change' in df.columns:
                    df = df.rename(columns={'pct_change': 'pct_chg'})
                if 'ts_code' in df.columns:
                    df = df.rename(columns={'ts_code': 'concept_code'})

                # 添加 name 字段
                if 'name' not in df.columns:
                    df['name'] = df['concept_code']

                data["concept"] = df
                logger.info(f"从数据库加载数据：{len(df):,} 条记录")
                return data
        except Exception as e:
            logger.warning(f"从数据库加载数据失败：{e}")

        # 回退到文件读取
        raw_dir = settings.raw_data_dir

        if not os.path.exists(raw_dir):
            return data

        # 加载名称映射
        name_mapping = load_name_mapping()

        # 加载同花顺行业/概念数据 (ths_*_TI.csv 格式)
        ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

        if ths_files:
            from joblib import Parallel, delayed

            def load_single_file(filepath):
                try:
                    df = pd.read_csv(filepath, dtype={
                        'concept_code': str,
                        'trade_date': str,
                        'pct_chg': float,
                        'vol': float,
                        'close': float
                    })
                    # 重命名字段
                    if 'pct_change' in df.columns:
                        df = df.rename(columns={'pct_change': 'pct_chg'})
                    if 'ts_code' in df.columns:
                        df = df.rename(columns={'ts_code': 'concept_code'})

                    # 使用名称映射获取真实名称
                    first_code = df['concept_code'].iloc[0]
                    block_name = get_block_name(first_code, name_mapping)
                    df['name'] = block_name
                    df['concept_name'] = block_name

                    return df
                except Exception as e:
                    logger.warning(f"加载文件 {filepath} 失败：{e}")
                    return None

            # 并行加载所有文件
            dfs = Parallel(n_jobs=-1, backend="threading")(
                delayed(load_single_file)(os.path.join(raw_dir, f))
                for f in ths_files
            )
            dfs = [df for df in dfs if df is not None]

            if dfs:
                data["concept"] = pd.concat(dfs, ignore_index=True)
                # 按日期排序，只保留最近的数据
                if "trade_date" in data["concept"].columns:
                    data["concept"] = data["concept"].sort_values("trade_date")
                    latest_date = data["concept"]["trade_date"].max()
                    try:
                        latest_date_int = int(latest_date)
                        # 计算起始日期（考虑 recent_days 个交易日）
                        min_date = latest_date_int - (recent_days * 100)  # 大约 recent_days 个交易日
                        data["concept"] = data["concept"][data["concept"]["trade_date"] >= min_date]
                    except (ValueError, TypeError):
                        # 日期转换失败时保留所有数据
                        pass
                logger.info(f"加载了 {len(ths_files)} 个同花顺数据文件用于预测，共 {len(data['concept'])} 条记录")

        return data

    def _stream_prepare_features(
        self,
        db,
        lookback: int = 10,
        n_jobs: int = 32,
        batch_size: int = 50
    ) -> Optional[pd.DataFrame]:
        """
        流式准备特征（分批加载，边加载边处理）

        Args:
            db: 数据库实例
            lookback: 回溯天数
            n_jobs: 并行任务数
            batch_size: 每批次处理的板块数

        Returns:
            特征 DataFrame
        """
        import pandas as pd
        from joblib import Parallel, delayed
        import time

        logger.info("开始流式准备特征...")
        start_time = time.time()

        # 获取所有板块代码
        result = db.query("SELECT DISTINCT ts_code FROM concept_daily")
        all_codes = [r[0] for r in result]
        total_codes = len(all_codes)
        logger.info(f"共 {total_codes} 个板块")

        all_features = []
        processed = 0

        # 分批处理
        for i in range(0, total_codes, batch_size):
            batch_codes = all_codes[i:i + batch_size]

            # 获取本批次数据
            placeholders = ','.join(['?' for _ in batch_codes])
            sql = f"SELECT * FROM concept_daily WHERE ts_code IN ({placeholders}) ORDER BY ts_code, trade_date"
            df = db.query_to_dataframe(sql, tuple(batch_codes))

            if df.empty:
                continue

            # 重命名列以匹配处理器的期望
            df = df.rename(columns={
                'ts_code': 'concept_code',
                'pct_change': 'pct_chg'
            })

            # 处理本批次
            grouped = df.groupby("concept_code")
            concept_codes = list(grouped.groups.keys())

            # 动态调整并发数
            actual_jobs = min(n_jobs, len(concept_codes))
            if actual_jobs <= 0:
                actual_jobs = 1

            # 并行处理本批次
            results = Parallel(
                n_jobs=actual_jobs,
                backend="threading",
                verbose=0
            )(
                delayed(self.predictor._process_single_concept_vectorized)(
                    name,
                    grouped.get_group(name).rename(columns={'ts_code': 'concept_code'}),
                    lookback
                )
                for name in concept_codes
            )

            batch_features = [r for r in results if r is not None]
            if batch_features:
                all_features.append(pd.concat(batch_features, ignore_index=True))

            processed += len(batch_codes)
            if processed % 100 == 0 or processed >= total_codes:
                elapsed = time.time() - start_time
                logger.info(f"进度：{processed}/{total_codes} ({processed*100/total_codes:.1f}%), "
                           f"已生成 {sum(len(f) for f in all_features):,} 条样本，耗时 {elapsed:.1f}s")

        if not all_features:
            logger.warning("未能生成任何特征数据")
            return None

        result_df = pd.concat(all_features, ignore_index=True)
        total_elapsed = time.time() - start_time
        logger.info(f"流式特征准备完成：{len(result_df):,} 条样本，耗时 {total_elapsed:.1f}s")

        return result_df


def main():
    """主函数"""
    agent = PredictAgent()

    # 训练模型
    # result = agent.execute(task="train", horizon="all")
    # print(f"训练结果：{result}")

    # 执行预测
    result = agent.execute(task="predict", horizon="all")
    print(f"预测结果：{result}")


if __name__ == "__main__":
    main()
