"""
分析Agent
负责热点识别和轮动分析
"""
import pandas as pd
from typing import Optional, Dict, Any, List
from loguru import logger
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.base_agent import BaseAgent, AgentResult
from analysis.hotspot_detector import HotspotDetector
from analysis.rotation_analyzer import RotationAnalyzer
from analysis.pattern_learner import PatternLearner
from config import settings


class AnalysisAgent(BaseAgent):
    """分析Agent"""

    def __init__(self):
        super().__init__("AnalysisAgent")
        self.hotspot_detector = HotspotDetector()
        self.rotation_analyzer = RotationAnalyzer()
        self.pattern_learner = PatternLearner()

    def run(
        self,
        task: str = "hotspot",
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行分析任务

        Args:
            task: 任务类型 hotspot/rotation/pattern/all
            data: 输入数据字典
        """
        if task == "hotspot":
            return self._analyze_hotspot(data, **kwargs)
        elif task == "rotation":
            return self._analyze_rotation(data, **kwargs)
        elif task == "pattern":
            return self._learn_patterns(data, **kwargs)
        elif task == "all":
            return self._full_analysis(data, **kwargs)
        else:
            raise ValueError(f"未知任务类型: {task}")

    def _analyze_hotspot(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """分析热点"""
        logger.info("开始热点分析")

        if data is None:
            data = self._load_latest_data()

        concept_data = data.get("concept")
        moneyflow_data = data.get("moneyflow")
        limit_data = data.get("limit")

        if concept_data is None or concept_data.empty:
            return {"error": "无概念板块数据"}

        # 计算热点评分
        scores = self.hotspot_detector.compute_hotspot_score(
            concept_data=concept_data,
            moneyflow_data=moneyflow_data,
            limit_data=limit_data,
            historical_data=data.get("concept_history")
        )

        # 添加排名字段
        if not scores.empty and "hotspot_score" in scores.columns:
            scores["rank"] = scores.groupby("trade_date")["hotspot_score"].rank(
                ascending=False, method="min"
            )

        # 识别热点
        hotspots = self.hotspot_detector.identify_hotspots(
            scores_df=scores,
            top_n=10,
            min_score=60.0
        )

        # 检测新出现的热点
        emergence = self.hotspot_detector.detect_hotspot_emergence(scores) if not scores.empty else pd.DataFrame()

        return {
            "hotspot_scores": scores.to_dict("records"),
            "top_hotspots": hotspots.to_dict("records"),
            "emerging_hotspots": emergence.to_dict("records") if not emergence.empty else []
        }

    def _analyze_rotation(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """分析轮动"""
        logger.info("开始轮动分析")

        if data is None:
            data = self._load_latest_data()

        concept_data = data.get("concept")

        if concept_data is None or concept_data.empty:
            return {"error": "无概念板块数据"}

        # 计算相关性矩阵
        corr_matrix = self.rotation_analyzer.compute_correlation_matrix(
            price_data=concept_data,
            window=20
        )

        # 计算领涨滞后矩阵
        lead_lag_matrix = self.rotation_analyzer.compute_lead_lag_matrix(
            price_data=concept_data,
            max_lag=5
        )

        # 计算轮动强度指数
        rsi = self.rotation_analyzer.compute_rotation_strength_index(
            price_data=concept_data,
            window=20
        )

        # 计算轮动路径
        hotspot_scores = data.get("hotspot_scores")
        if hotspot_scores is not None:
            rotation_paths = self.rotation_analyzer.compute_rotation_path(hotspot_scores)
            rotation_patterns = self.rotation_analyzer.compute_rotation_patterns(rotation_paths)
        else:
            rotation_paths = pd.DataFrame()
            rotation_patterns = {}

        # 识别轮动信号
        signals = []
        if hotspot_scores is not None:
            signals = self.rotation_analyzer.identify_rotation_signal(
                hotspot_scores=hotspot_scores,
                correlation_matrix=corr_matrix,
                lead_lag_matrix=lead_lag_matrix
            )

        return {
            "correlation_matrix": corr_matrix.to_dict(),
            "lead_lag_matrix": lead_lag_matrix.to_dict(),
            "rotation_strength": rsi.to_dict() if isinstance(rsi, pd.Series) else rsi,
            "rotation_paths": rotation_paths.to_dict("records") if not rotation_paths.empty else [],
            "rotation_patterns": rotation_patterns,
            "rotation_signals": signals
        }

    def _learn_patterns(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """学习规律"""
        logger.info("开始规律学习")

        if data is None:
            data = self._load_latest_data()

        hotspot_scores = data.get("hotspot_scores")
        rotation_paths = data.get("rotation_paths")

        if hotspot_scores is None:
            return {"error": "无热点评分数据"}

        # 学习轮动规则
        rotation_paths_df = rotation_paths if rotation_paths is not None and not rotation_paths.empty else pd.DataFrame()
        rules = self.pattern_learner.learn_rotation_rules(
            hotspot_scores=hotspot_scores,
            rotation_paths=rotation_paths_df
        )

        # 学习市场环境规则
        market_data = data.get("market")
        if market_data is not None:
            market_rules = self.pattern_learner.learn_market_context_rules(
                hotspot_scores=hotspot_scores,
                market_data=market_data
            )
            rules.update(market_rules)

        # 保存规律
        self.pattern_learner.save_patterns(rules, "rotation_rules.json")

        return rules

    def _full_analysis(
        self,
        data: Optional[Dict[str, pd.DataFrame]] = None,
        **kwargs
    ) -> Dict:
        """完整分析"""
        logger.info("开始完整分析")

        if data is None:
            data = self._load_latest_data()

        results = {}

        # 热点分析
        hotspot_result = self._analyze_hotspot(data)
        results["hotspot"] = hotspot_result

        # 更新数据
        if "hotspot_scores" in hotspot_result:
            data["hotspot_scores"] = pd.DataFrame(hotspot_result["hotspot_scores"])

        # 轮动分析
        rotation_result = self._analyze_rotation(data)
        results["rotation"] = rotation_result

        # 规律学习
        pattern_result = self._learn_patterns(data)
        results["patterns"] = pattern_result

        return results

    def _load_latest_data(self, recent_days: int = 60) -> Dict[str, pd.DataFrame]:
        """
        加载最新数据（支持同花顺数据格式）- 优化版
        使用并行读取加速

        Args:
            recent_days: 加载最近 N 天的数据（默认 60 天，确保有足够数据用于特征计算）
        """
        data = {}
        raw_dir = settings.raw_data_dir

        if not os.path.exists(raw_dir):
            return data

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
                # 按日期排序，只保留最近的数据
                if "trade_date" in data["concept"].columns:
                    data["concept"] = data["concept"].sort_values("trade_date")
                    latest_date = data["concept"]["trade_date"].max()
                    # 转换为整数进行比较
                    try:
                        latest_date_int = int(latest_date)
                        # 计算起始日期（考虑 recent_days 个交易日）
                        min_date = latest_date_int - (recent_days * 100)  # 大约 recent_days 个交易日
                        data["concept"] = data["concept"][data["concept"]["trade_date"] >= min_date]
                    except:
                        pass
                logger.info(f"加载了 {len(ths_files)} 个同花顺数据文件，共 {len(data['concept'])} 条记录")

        return data


def main():
    """主函数"""
    agent = AnalysisAgent()
    result = agent.execute(task="all")
    print(f"分析结果: {result}")


if __name__ == "__main__":
    main()
