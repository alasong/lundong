"""
组合构建 Agent
整合热点轮动预测 + 个股筛选 + 组合优化
"""
import pandas as pd
from typing import Optional, Dict, Any, List
from loguru import logger
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.base_agent import BaseAgent
from data.stock_screener import StockScreener
from models.stock_predictor import StockPredictor
from portfolio.optimizer import PortfolioOptimizer
from data.database import get_database


class PortfolioAgent(BaseAgent):
    """组合构建 Agent - 整合热点轮动 + 个股筛选 + 组合优化"""

    def __init__(self):
        super().__init__("PortfolioAgent")
        self.screener = StockScreener()
        self.stock_predictor = StockPredictor()
        self.optimizer = PortfolioOptimizer()
        self.db = get_database()

        # 加载预训练模型
        self.stock_model = self.stock_predictor.load_model()
        if self.stock_model:
            logger.info("已加载个股预测模型")

    def run(
        self,
        task: str = "build",
        concept_codes: List[str] = None,
        concept_predictions: pd.DataFrame = None,
        top_n_stocks: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行组合构建任务

        Args:
            task: 任务类型
                - build: 构建组合
                - screen: 仅筛选股票
                - predict: 仅个股预测
                - optimize: 仅组合优化
            concept_codes: 目标板块代码列表
            concept_predictions: 板块预测结果 (可选)
            top_n_stocks: 最终持仓股票数量

        Returns:
            组合构建结果
        """
        if task == "build":
            return self._build_portfolio(concept_codes, concept_predictions, top_n_stocks, **kwargs)
        elif task == "screen":
            return self._screen_stocks(concept_codes, **kwargs)
        elif task == "predict":
            return self._predict_stocks(concept_codes, **kwargs)
        elif task == "optimize":
            return self._optimize(concept_codes, concept_predictions, **kwargs)
        else:
            raise ValueError(f"未知任务类型：{task}")

    def _build_portfolio(
        self,
        concept_codes: List[str],
        concept_predictions: pd.DataFrame = None,
        top_n_stocks: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """
        完整流程：筛选 → 预测 → 优化

        返回最终投资组合
        """
        logger.info("开始构建投资组合...")

        # 过滤：只保留有成分股的板块
        available_concepts = self._get_concepts_with_constituents(concept_codes)
        if not available_concepts:
            logger.warning("所有板块都没有成分股数据")
            return {"success": False, "error": "无成分股数据"}

        if len(available_concepts) < len(concept_codes):
            logger.info(f"只有 {len(available_concepts)} 个板块有成分股数据：{available_concepts}")

        # Step 1: 筛选股票
        logger.info("Step 1: 筛选股票...")
        screened_stocks = self.screener.screen_stocks(
            concept_codes=available_concepts,
            concept_ranking=concept_predictions,
            top_n_per_concept=5  # 每个板块选 5 只
        )

        if screened_stocks.empty:
            logger.warning("筛选结果为空")
            return {"success": False, "error": "无候选股票"}

        logger.info(f"筛选出 {len(screened_stocks)} 只股票")

        # Step 2: 获取个股历史数据用于预测
        logger.info("Step 2: 准备个股数据...")
        stock_codes = screened_stocks['stock_code'].unique().tolist()
        stock_data = self._get_stock_data(stock_codes)

        if stock_data.empty:
            logger.warning("无法获取个股数据")
            return {"success": False, "error": "无个股数据"}

        # Step 3: 准备特征
        logger.info("Step 3: 准备特征...")
        features = self.stock_predictor.prepare_features(stock_data, n_jobs=16)

        if features.empty:
            logger.warning("特征为空")
            return {"success": False, "error": "特征为空"}

        # Step 4: 个股预测
        logger.info("Step 4: 个股预测...")
        if self.stock_model is None:
            logger.info("无预训练模型，使用简化预测")
            predictions = self._simple_stock_prediction(screened_stocks)
        else:
            predictions = self.stock_predictor.predict(self.stock_model, features)

        if predictions.empty:
            logger.warning("预测结果为空")
            return {"success": False, "error": "预测失败"}

        # 合并 concept_code 列（从筛选结果中）
        # 使用 ts_code 对齐，先删除 predictions 中可能重复的列
        screened_stocks = screened_stocks.rename(columns={'stock_code': 'ts_code'})

        # 要合并的列
        merge_cols = ['ts_code', 'concept_code']
        if 'concept_name' in screened_stocks.columns:
            merge_cols.append('concept_name')
        if 'stock_name' in screened_stocks.columns:
            merge_cols.append('stock_name')

        # 删除 predictions 中的 stock_name 列（如果存在），避免重复
        if 'stock_name' in predictions.columns:
            predictions = predictions.drop(columns=['stock_name'])

        predictions = predictions.merge(screened_stocks[merge_cols].drop_duplicates(), on='ts_code', how='left')
        logger.info(f"合并后预测数据：{len(predictions)} 条，列：{predictions.columns.tolist()}")

        # Step 5: 组合优化
        logger.info("Step 5: 组合优化...")
        result = self.optimizer.optimize(
            stock_predictions=predictions,
            concept_predictions=concept_predictions,
            top_n_stocks=top_n_stocks,
            max_position=kwargs.get('max_position', 0.10),
            max_sector=kwargs.get('max_sector', 0.25)
        )

        logger.info(f"组合构建完成：{len(result.get('portfolio', []))} 只股票")

        return {
            "success": True,
            "portfolio": result.get('portfolio', []),
            "metrics": result.get('metrics', {}),
            "risk_analysis": result.get('risk_analysis', {}),
            "all_predictions": predictions.to_dict('records')
        }

    def _get_concepts_with_constituents(self, concept_codes: List[str]) -> List[str]:
        """
        获取有成分股数据的板块代码

        Args:
            concept_codes: 传入的板块代码列表

        Returns:
            有成分股数据的板块代码列表
        """
        # 直接查询数据库
        df = self.db.get_constituent_stocks(concept_codes)
        if df.empty:
            return []
        return df['concept_code'].unique().tolist()

    def _screen_stocks(
        self,
        concept_codes: List[str],
        **kwargs
    ) -> Dict[str, Any]:
        """仅筛选股票"""
        logger.info("筛选股票...")

        result = self.screener.screen_stocks(concept_codes, **kwargs)

        if result.empty:
            return {"success": False, "error": "无候选股票"}

        return {
            "success": True,
            "stocks": result.to_dict('records'),
            "count": len(result)
        }

    def _predict_stocks(
        self,
        concept_codes: List[str],
        **kwargs
    ) -> Dict[str, Any]:
        """仅个股预测"""
        logger.info("预测个股...")

        # 先筛选
        screened = self.screener.screen_stocks(concept_codes, **kwargs)
        if screened.empty:
            return {"success": False, "error": "无候选股票"}

        # 准备数据
        stock_codes = screened['stock_code'].unique().tolist()
        stock_data = self._get_stock_data(stock_codes)

        if stock_data.empty:
            return {"success": False, "error": "无个股数据"}

        # 准备特征
        features = self.stock_predictor.prepare_features(stock_data, n_jobs=16)

        if features.empty:
            return {"success": False, "error": "特征为空"}

        # 预测
        if self.stock_model is None:
            predictions = self._simple_stock_prediction(screened)
        else:
            predictions = self.stock_predictor.predict(self.stock_model, features)

        if predictions.empty:
            return {"success": False, "error": "预测失败"}

        return {
            "success": True,
            "predictions": predictions.to_dict('records'),
            "top_10": predictions.nlargest(10, 'combined_score').to_dict('records')
        }

    def _optimize(
        self,
        concept_codes: List[str],
        concept_predictions: pd.DataFrame = None,
        **kwargs
    ) -> Dict[str, Any]:
        """仅组合优化（需要先有预测数据）"""
        logger.info("组合优化...")

        # 这里假设已经有预测数据，实际应该先获取
        # 简化处理：先筛选和预测
        pred_result = self._predict_stocks(concept_codes, **kwargs)

        if not pred_result.get('success'):
            return pred_result

        predictions = pd.DataFrame(pred_result.get('predictions', []))

        # 优化
        result = self.optimizer.optimize(
            stock_predictions=predictions,
            concept_predictions=concept_predictions,
            top_n_stocks=kwargs.get('top_n_stocks', 10)
        )

        return {
            "success": True,
            "portfolio": result.get('portfolio', []),
            "metrics": result.get('metrics', {})
        }

    def _get_stock_data(
        self,
        stock_codes: List[str],
        lookback_days: int = 60
    ) -> pd.DataFrame:
        """获取个股历史数据"""
        end_date = self.db.get_latest_date()
        if end_date is None:
            return pd.DataFrame()

        from datetime import timedelta
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")

        all_data = []
        for code in stock_codes:
            df = self.db.get_stock_data(code, start_date, end_date)
            if not df.empty:
                all_data.append(df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _simple_stock_prediction(
        self,
        screened_stocks: pd.DataFrame
    ) -> pd.DataFrame:
        """
        简化个股预测（当模型不可用时）

        基于筛选得分和板块预测
        """
        logger.info("使用简化预测...")

        predictions = screened_stocks.copy()

        # 使用综合得分作为预测代理
        if 'combined_score' in predictions.columns:
            predictions['pred_1d'] = predictions['combined_score'] * 0.02
            predictions['pred_5d'] = predictions['combined_score'] * 0.08
            predictions['pred_20d'] = predictions['combined_score'] * 0.15
        else:
            predictions['pred_1d'] = 1.0
            predictions['pred_5d'] = 3.0
            predictions['pred_20d'] = 5.0
            predictions['combined_score'] = 50

        return predictions


def main():
    """测试函数"""
    agent = PortfolioAgent()

    # 测试构建组合
    test_concepts = ['881101.TI', '881102.TI']

    print("\n[测试] 构建投资组合...")
    result = agent.run(task="build", concept_codes=test_concepts, top_n_stocks=10)

    if result.get('success'):
        print(f"\n组合构建成功!")
        print(f"持仓数量：{len(result['portfolio'])}")
        print("\n持仓明细:")
        for pos in result['portfolio'][:5]:
            print(f"  {pos['ts_code']} {pos['stock_name']}: {pos['weight']:.1%}")

        print("\n预期指标:")
        metrics = result.get('metrics', {})
        print(f"  预期收益：{metrics.get('expected_return', 0):.1%}")
        print(f"  预期波动率：{metrics.get('expected_volatility', 0):.1%}")
        print(f"  夏普比率：{metrics.get('sharpe', 0):.2f}")
    else:
        print(f"组合构建失败：{result.get('error', '未知错误')}")


if __name__ == "__main__":
    main()
