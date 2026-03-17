"""
热点轮动策略
基于板块热点评分和 XGBoost 预测的轮动策略
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
from .base_strategy import BaseStrategy, StrategySignal


class HotRotationStrategy(BaseStrategy):
    """热点轮动策略 - 基于板块热点评分和预测"""

    def __init__(self, name: str = "hot_rotation", params: Optional[Dict] = None):
        default_params = {
            "top_n_concepts": 10,
            "min_hotspot_score": 50,  # 降低阈值：60 -> 50
            "stocks_per_concept": 5,
            "use_prediction": True,
            "use_fallback": True,  # 启用降级模式
        }
        default_params.update(params or {})

        super().__init__(name, default_params)

        self.analysis_agent = None
        self.predict_agent = None
        self.screener = None
        self.stock_predictor = None

    def _init_components(self):
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
        top_hotspots = []
        concept_predictions = pd.DataFrame()

        # Step 1: 板块预测（先执行，用于降级）
        if self.params.get("use_prediction", True):
            logger.info("Step 1: 板块预测...")
            predict_result = self.predict_agent.execute(task="predict", horizon="all")
            if predict_result.get("success", False):
                result_data = predict_result.get("result", {})
                # 兼容多种键名
                concept_predictions = pd.DataFrame(
                    result_data.get(
                        "predictions",
                        result_data.get(
                            "top_10", result_data.get("concept_predictions", [])
                        ),
                    )
                )
                if not concept_predictions.empty:
                    # 确保有 concept_code 列
                    if (
                        "concept_code" not in concept_predictions.columns
                        and "ts_code" in concept_predictions.columns
                    ):
                        concept_predictions = concept_predictions.rename(
                            columns={"ts_code": "concept_code"}
                        )

        # Step 2: 热点分析
        logger.info("Step 2: 热点分析...")
        analysis_result = self.analysis_agent.execute(task="hotspot")

        if analysis_result.get("success", False):
            top_hotspots = analysis_result.get("result", {}).get("top_hotspots", [])

        # 降级逻辑：如果热点分析失败或为空，用板块预测代替
        if not top_hotspots:
            if self.params.get("use_fallback", True) and not concept_predictions.empty:
                logger.warning("热点分析失败，启用降级模式：直接用板块预测")
                top_hotspots = self._fallback_from_predictions(concept_predictions)
            else:
                logger.warning("未识别到热点板块且降级失败")
                return []

        # Step 3: 从热点板块中选股
        logger.info("Step 3: 选股...")
        concept_codes = [
            h.get("concept_code") or h.get("ts_code", "")
            for h in top_hotspots[: self.params["top_n_concepts"]]
            if h.get("concept_code") or h.get("ts_code")
        ]

        if not concept_codes:
            logger.warning("没有有效的板块代码")
            return []

        try:
            screened = self.screener.screen_stocks(
                concept_codes=concept_codes,
                concept_ranking=concept_predictions,
                top_n_per_concept=self.params["stocks_per_concept"],
            )
        except Exception as e:
            logger.warning(f"选股失败: {e}，使用降级模式")
            return self._fallback_signals(concept_predictions)

        if screened.empty:
            logger.warning("选股结果为空，使用降级模式")
            return self._fallback_signals(concept_predictions)

        # Step 4: 个股评分
        logger.info("Step 4: 个股评分...")
        for _, row in screened.iterrows():
            ts_code = row["ts_code"]
            stock_name = row.get("stock_name", ts_code)
            concept_code = row.get("concept_code", "")
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
                },
            )
            signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        self.signals = signals
        logger.info(f"热点轮动策略：生成 {len(signals)} 个信号")
        return signals

    def _fallback_from_predictions(
        self, concept_predictions: pd.DataFrame
    ) -> List[Dict]:
        """从板块预测生成热点（降级模式）"""
        if concept_predictions.empty:
            return []

        top_concepts = concept_predictions.nlargest(
            self.params["top_n_concepts"],
            "combined_score"
            if "combined_score" in concept_predictions.columns
            else "pred_1d",
        )

        hotspots = []
        for _, row in top_concepts.iterrows():
            hotspots.append(
                {
                    "concept_code": row.get("concept_code") or row.get("ts_code", ""),
                    "concept_name": row.get("name", ""),
                    "hotspot_score": row.get(
                        "combined_score", row.get("pred_1d", 0) * 20 + 50
                    ),
                    "rank": len(hotspots) + 1,
                }
            )

        logger.info(f"降级模式：从预测生成 {len(hotspots)} 个热点板块")
        return hotspots

    def _fallback_signals(
        self, concept_predictions: pd.DataFrame
    ) -> List[StrategySignal]:
        """降级模式：直接从预测生成信号"""
        if concept_predictions.empty:
            return []

        signals = []
        top = concept_predictions.nlargest(
            10,
            "combined_score"
            if "combined_score" in concept_predictions.columns
            else "pred_1d",
        )

        for _, row in top.iterrows():
            score = min(100, max(0, 50 + row.get("pred_1d", 0) * 100))
            signal = StrategySignal(
                ts_code=row.get("concept_code") or row.get("ts_code", ""),
                stock_name=row.get("name", ""),
                strategy_type="hot_rotation_fallback",
                signal_type="buy",
                weight=score / 100.0,
                score=score,
                reason=f"降级模式：预测涨幅 {row.get('pred_1d', 0):.2%}",
                metadata={"fallback": True},
            )
            signals.append(signal)

        return signals

    def _calculate_stock_score(
        self, row: pd.Series, concept_predictions: pd.DataFrame
    ) -> float:
        liquidity = row.get("liquidity_score", 50)
        valuation = row.get("valuation_score", 50)
        market_cap = row.get("market_cap_score", 50)

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
        if not signals:
            return {"portfolio": [], "metrics": {}}

        from portfolio.optimizer import PortfolioOptimizer

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

        optimizer = PortfolioOptimizer()
        result = optimizer.optimize(
            stock_predictions=stock_df,
            top_n_stocks=kwargs.get("top_n_stocks", 10),
            max_position=kwargs.get("max_position", 0.10),
            max_sector=kwargs.get("max_sector", 0.25),
        )

        return result
