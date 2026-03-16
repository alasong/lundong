"""
热点轮动策略
基于板块热点评分和 XGBoost 预测的轮动策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from strategies.base_strategy import BaseStrategy, StrategySignal


class HotRotationStrategy(BaseStrategy):
    """热点轮动策略 - 基于板块热点评分和预测"""

    def __init__(self, name: str = "hot_rotation", params: Optional[Dict] = None):
        default_params = {
            "top_n_concepts": 10,  # 选择 TOP N 板块
            "min_hotspot_score": 60,  # 最小热点评分
            "stocks_per_concept": 5,  # 每个板块选股数量
            "use_prediction": True,  # 是否使用模型预测
        }
        default_params.update(params or {})

        super().__init__(name, default_params)

        # 延迟导入，避免循环依赖
        self.analysis_agent = None
        self.predict_agent = None
        self.screener = None
        self.stock_predictor = None

    def _init_components(self):
        """懒加载组件"""
        if self.analysis_agent is None:
            from agents.analysis_agent import AnalysisAgent
            from agents.predict_agent import PredictAgent
            from data.stock_screener import StockScreener
            from models.stock_predictor import StockPredictor

            self.analysis_agent = AnalysisAgent()
            self.predict_agent = PredictAgent()
            self.screener = StockScreener()
            self.stock_predictor = StockPredictor()

    def get_required_data(self) -> Dict[str, Any]:
        """需要板块数据和个股数据"""
        return {
            "concept_data": True,
            "stock_data": True,
            "history_days": 60,
            "features": ["price", "moneyflow", "limit_count"],
        }

    def generate_signals(self, **kwargs) -> List[StrategySignal]:
        """生成热点轮动信号"""
        logger.info("热点轮动策略：生成信号...")
        self._init_components()

        signals = []

        # Step 1: 热点分析
        logger.info("Step 1: 热点分析...")
        analysis_result = self.analysis_agent.execute(task="hotspot")

        if not analysis_result.get("success", False):
            logger.warning("热点分析失败")
            return []

        top_hotspots = analysis_result.get("result", {}).get("top_hotspots", [])
        if not top_hotspots:
            logger.warning("未识别到热点板块")
            return []

        # Step 2: 板块预测
        if self.params.get("use_prediction", True):
            logger.info("Step 2: 板块预测...")
            predict_result = self.predict_agent.execute(task="predict", horizon="all")

            if predict_result.get("success", False):
                concept_predictions = predict_result.get("result", {}).get(
                    "concept_predictions", pd.DataFrame()
                )
            else:
                concept_predictions = pd.DataFrame()
        else:
            concept_predictions = pd.DataFrame()

        # Step 3: 从热点板块中选股
        logger.info("Step 3: 选股...")
        concept_codes = [
            h["concept_code"] for h in top_hotspots[: self.params["top_n_concepts"]]
        ]

        screened = self.screener.screen_stocks(
            concept_codes=concept_codes,
            concept_ranking=concept_predictions,
            top_n_per_concept=self.params["stocks_per_concept"],
        )

        if screened.empty:
            logger.warning("选股结果为空")
            return []

        # Step 4: 个股预测和评分
        logger.info("Step 4: 个股评分...")
        stock_codes = screened["ts_code"].unique().tolist()

        for _, row in screened.iterrows():
            ts_code = row["ts_code"]
            stock_name = row.get("stock_name", ts_code)
            concept_code = row.get("concept_code", "")

            # 综合评分（流动性 + 估值 + 市值 + 板块排名）
            score = self._calculate_stock_score(row, concept_predictions)

            signal = StrategySignal(
                ts_code=ts_code,
                stock_name=stock_name,
                strategy_type="hot_rotation",
                signal_type="buy",
                weight=score / 100.0,
                score=score,
                reason=f"热点板块：{concept_code}, 评分：{score:.1f}",
                metadata={
                    "concept_code": concept_code,
                    "liquidity_score": row.get("liquidity_score", 50),
                    "valuation_score": row.get("valuation_score", 50),
                    "hotspot_rank": next(
                        (
                            h.get("rank", 99)
                            for h in top_hotspots
                            if h.get("concept_code") == concept_code
                        ),
                        99,
                    ),
                },
            )
            signals.append(signal)

        # 按评分排序
        signals.sort(key=lambda s: s.score, reverse=True)
        self.signals = signals

        logger.info(f"热点轮动策略：生成 {len(signals)} 个信号")
        return signals

    def _calculate_stock_score(
        self, row: pd.Series, concept_predictions: pd.DataFrame
    ) -> float:
        """
        计算个股综合评分

        评分组成:
        - 流动性评分 (30%)
        - 估值评分 (20%)
        - 市值评分 (10%)
        - 板块排名评分 (40%)
        """
        # 基础评分
        liquidity = row.get("liquidity_score", 50)
        valuation = row.get("valuation_score", 50)
        market_cap = row.get("market_cap_score", 50)

        # 板块排名评分
        concept_code = row.get("concept_code", "")
        if not concept_predictions.empty and "ts_code" in concept_predictions.columns:
            stock_pred = concept_predictions[
                concept_predictions["ts_code"] == row.get("ts_code")
            ]
            if not stock_pred.empty:
                pred_1d = stock_pred["pred_1d"].iloc[0]
                sector_rank_score = min(100, max(0, 50 + pred_1d * 10))
            else:
                sector_rank_score = 50
        else:
            sector_rank_score = 50

        # 加权综合评分
        total_score = (
            liquidity * 0.30
            + valuation * 0.20
            + market_cap * 0.10
            + sector_rank_score * 0.40
        )

        return min(100, max(0, total_score))

    def optimize_portfolio(
        self, signals: List[StrategySignal], **kwargs
    ) -> Dict[str, Any]:
        """使用组合优化器进行优化"""
        if not signals:
            return {"portfolio": [], "metrics": {}}

        # 延迟导入
        from portfolio.optimizer import PortfolioOptimizer

        # 转换为 DataFrame
        df_data = []
        for sig in signals:
            df_data.append(
                {
                    "ts_code": sig.ts_code,
                    "stock_name": sig.stock_name,
                    "concept_code": sig.metadata.get("concept_code", ""),
                    "combined_score": sig.score,
                }
            )

        stock_df = pd.DataFrame(df_data)

        # 调用优化器
        optimizer = PortfolioOptimizer()
        result = optimizer.optimize(
            stock_predictions=stock_df,
            top_n_stocks=kwargs.get("top_n_stocks", 10),
            max_position=kwargs.get("max_position", 0.10),
            max_sector=kwargs.get("max_sector", 0.25),
        )

        return result
