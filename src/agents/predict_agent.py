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
            horizon: 预测周期 all/1d/5d/20d
            data: 输入数据
        """
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
        """训练模型"""
        logger.info("开始训练模型...")

        if data is None:
            data = self._load_training_data()

        concept_data = data.get("concept")
        if concept_data is None or concept_data.empty:
            return {"success": False, "error": "无概念数据"}

        # 准备特征
        features = self.predictor.prepare_features(concept_data)
        logger.info(f"特征准备完成：{len(features)} 条样本")

        # 训练模型
        self.model_result = self.predictor.train(features)

        return {
            "success": True,
            "metrics": self.model_result["metrics"]
        }

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

        # 准备特征
        features = self.predictor.prepare_features(concept_data)
        if features.empty:
            logger.warning("特征为空，可能数据不足")
            # 返回简化预测（使用近期表现）
            return self._simple_prediction(concept_data)

        # 预测
        predictions = self.predictor.predict(self.model_result, features)

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
        """格式化预测结果"""
        # 按综合评分排序
        ranked = predictions.nlargest(50, "combined_score")

        return {
            "predictions": ranked.to_dict("records"),
            "top_10": ranked.head(10).to_dict("records"),
            "model_used": True
        }

    def _load_training_data(self) -> Dict[str, pd.DataFrame]:
        """
        加载训练数据（支持同花顺数据格式）
        """
        data = {}
        raw_dir = settings.raw_data_dir

        if not os.path.exists(raw_dir):
            return data

        # 加载同花顺行业/概念数据 (ths_*_TI.csv 格式)
        ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

        if ths_files:
            dfs = []
            for f in ths_files:
                try:
                    df = pd.read_csv(os.path.join(raw_dir, f))
                    # 重命名字段以匹配系统期望的格式
                    df = df.rename(columns={
                        "pct_change": "pct_chg",
                        "ts_code": "concept_code"
                    })
                    # 添加 name 字段
                    if "name" not in df.columns:
                        df["name"] = df["concept_code"]
                    dfs.append(df)
                except Exception as e:
                    logger.warning(f"加载文件 {f} 失败：{e}")

            if dfs:
                data["concept"] = pd.concat(dfs, ignore_index=True)
                logger.info(f"加载了 {len(ths_files)} 个同花顺数据文件用于训练，共 {len(data['concept'])} 条记录")

        return data

    def _load_latest_data(self, recent_days: int = 30) -> Dict[str, pd.DataFrame]:
        """
        加载最新数据（支持同花顺数据格式）

        Args:
            recent_days: 加载最近 N 天的数据
        """
        data = {}
        raw_dir = settings.raw_data_dir

        if not os.path.exists(raw_dir):
            return data

        # 加载同花顺行业/概念数据 (ths_*_TI.csv 格式)
        ths_files = [f for f in os.listdir(raw_dir) if f.endswith("_TI.csv")]

        if ths_files:
            dfs = []
            for f in ths_files:
                try:
                    df = pd.read_csv(os.path.join(raw_dir, f))
                    # 重命名字段以匹配系统期望的格式
                    df = df.rename(columns={
                        "pct_change": "pct_chg",
                        "ts_code": "concept_code"
                    })
                    # 添加 name 字段
                    if "name" not in df.columns:
                        df["name"] = df["concept_code"]
                    dfs.append(df)
                except Exception as e:
                    logger.warning(f"加载文件 {f} 失败：{e}")

            if dfs:
                data["concept"] = pd.concat(dfs, ignore_index=True)
                # 按日期排序，只保留最近的数据
                if "trade_date" in data["concept"].columns:
                    data["concept"] = data["concept"].sort_values("trade_date")
                    latest_date = data["concept"]["trade_date"].max()
                    try:
                        latest_date_int = int(latest_date)
                        min_date = latest_date_int - 10000  # 大约 30 个交易日
                        data["concept"] = data["concept"][data["concept"]["trade_date"] >= min_date]
                    except:
                        pass
                logger.info(f"加载了 {len(ths_files)} 个同花顺数据文件用于预测，共 {len(data['concept'])} 条记录")

        return data


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
