"""
组合优化器
基于风险平价和 Black-Litterman 模型构建投资组合
集成策略层：仓位管理、风险管理、事件驱动
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
from loguru import logger
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.database import SQLiteDatabase, get_database
from config import settings


class PortfolioOptimizer:
    """组合优化器 - 决定最终持仓权重"""

    def __init__(self, db: SQLiteDatabase = None):
        """
        初始化优化器

        Args:
            db: 数据库实例
        """
        self.db = db or get_database()
        logger.info("组合优化器初始化完成")

    def optimize(
        self,
        stock_predictions: pd.DataFrame,
        concept_predictions: pd.DataFrame = None,
        max_position: float = 0.10,      # 单股上限 10%
        max_sector: float = 0.25,         # 单板块上限 25%
        target_risk: float = 0.15,        # 目标年化波动率 15%
        min_position: float = 0.02,       # 单股下限 2%
        top_n_stocks: int = 10            # 持仓股票数量
    ) -> Dict[str, Any]:
        """
        组合优化

        Args:
            stock_predictions: 个股预测 DataFrame
                - ts_code, stock_name, concept_code
                - pred_1d, pred_5d, pred_20d, combined_score
            concept_predictions: 板块预测 DataFrame (可选，用于 Black-Litterman)
            max_position: 单只股票最大权重
            max_sector: 单个板块最大权重
            target_risk: 目标组合波动率
            min_position: 单只股票最小权重
            top_n_stocks: 最终持仓股票数量

        Returns:
            {
                'portfolio': [
                    {'ts_code': 'xxx', 'weight': 0.08, 'concept': 'yyy', ...},
                    ...
                ],
                'metrics': {
                    'expected_return': 0.25,
                    'expected_volatility': 0.15,
                    'sharpe': 1.67,
                    'max_drawdown': 0.12,
                    'sector_concentration': 0.35,
                },
                'risk_analysis': {...}
            }
        """
        logger.info("开始组合优化...")

        # 统一列名：stock_code -> ts_code
        if 'stock_code' in stock_predictions.columns and 'ts_code' not in stock_predictions.columns:
            stock_predictions = stock_predictions.rename(columns={'stock_code': 'ts_code'})

        # 确保 combined_score 存在
        if 'combined_score' not in stock_predictions.columns:
            if 'stock_score' in stock_predictions.columns:
                stock_predictions['combined_score'] = stock_predictions['stock_score']
            else:
                stock_predictions['combined_score'] = 50.0

        # Step 1: 选择候选股票 (按综合得分 TOP N)
        candidates = stock_predictions.nlargest(top_n_stocks * 2, 'combined_score')

        if candidates.empty:
            logger.warning("无候选股票")
            return self._empty_result()

        logger.info(f"候选股票：{len(candidates)} 只")
        logger.info(f"候选股票列名：{candidates.columns.tolist()}")

        # Step 2: 计算相关性矩阵
        corr_matrix = self._calculate_correlation(candidates['ts_code'].tolist())

        if corr_matrix is None or corr_matrix.empty:
            logger.warning("无法计算相关性矩阵，使用等权组合")
            return self._equal_weight_portfolio(candidates, max_position, max_sector)

        # Step 3: 计算波动率
        volatilities = self._calculate_volatilities(candidates['ts_code'].tolist())

        # Step 4: 风险平价优化
        weights_rp = self._risk_parity_optimization(corr_matrix, volatilities)

        # Step 5: 如果有板块预测，使用 Black-Litterman 调整
        if concept_predictions is not None and not concept_predictions.empty:
            weights = self._black_litterman_adjust(
                weights_rp,
                candidates,
                concept_predictions,
                corr_matrix,
                volatilities
            )
        else:
            weights = weights_rp

        # Step 6: 应用约束条件
        weights = self._apply_constraints(
            weights,
            candidates,
            max_position,
            max_sector,
            min_position
        )

        # Step 7: 计算组合指标
        metrics = self._calculate_portfolio_metrics(weights, corr_matrix, volatilities)

        # Step 8: 构建最终组合
        portfolio = self._build_portfolio(weights, candidates)

        logger.info(f"组合构建完成：{len(portfolio)} 只股票")
        logger.info(f"预期年化收益：{metrics['expected_return']:.1%}, 波动率：{metrics['expected_volatility']:.1%}")

        return {
            'portfolio': portfolio,
            'metrics': metrics,
            'risk_analysis': self._analyze_risk(weights, corr_matrix, candidates)
        }

    def _calculate_correlation(self, stock_codes: List[str]) -> Optional[pd.DataFrame]:
        """计算个股相关性矩阵"""
        logger.info("计算相关性矩阵...")

        # 获取过去 60 日收益率数据
        end_date = self.db.get_latest_date()
        if end_date is None:
            return None

        from datetime import timedelta
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=90)).strftime("%Y%m%d")

        # 先获取所有数据，按日期对齐
        returns_by_date = {}

        for code in stock_codes:
            df = self.db.get_stock_data(code, start_date, end_date)
            if len(df) < 20:
                continue
            df = df.sort_values('trade_date')
            # 使用日期作为索引
            for _, row in df.iterrows():
                date = row['trade_date']
                if date not in returns_by_date:
                    returns_by_date[date] = {}
                returns_by_date[date][code] = row['pct_chg'] / 100

        if len(returns_by_date) < 20:
            logger.warning(f"有效交易日不足：{len(returns_by_date)}")
            return None

        # 转换为 DataFrame，自动按日期对齐
        df_returns = pd.DataFrame(returns_by_date).T

        # 只保留所有股票都有数据的日期（dropna 会删除任何有 NaN 的行）
        df_returns = df_returns.dropna(axis=0, how='any')

        if len(df_returns) < 20 or len(df_returns.columns) < 2:
            logger.warning(f"对齐后数据不足：{len(df_returns)} 天，{len(df_returns.columns)} 只股票")
            return None

        # 计算相关性
        corr_matrix = df_returns.corr()

        logger.info(f"相关性矩阵维度：{corr_matrix.shape}，基于 {len(df_returns)} 个交易日")
        return corr_matrix

    def _calculate_volatilities(self, stock_codes: List[str]) -> Dict[str, float]:
        """计算个股波动率"""
        volatilities = {}

        end_date = self.db.get_latest_date()
        if end_date is None:
            return {code: 0.25 for code in stock_codes}  # 默认 25% 波动率

        from datetime import timedelta
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")

        for code in stock_codes:
            df = self.db.get_stock_data(code, start_date, end_date)
            if len(df) < 20:
                volatilities[code] = 0.25
                continue

            daily_vol = df['pct_chg'].std() / 100
            annual_vol = daily_vol * np.sqrt(252)
            volatilities[code] = annual_vol

        return volatilities

    def _risk_parity_optimization(
        self,
        corr_matrix: pd.DataFrame,
        volatilities: Dict[str, float]
    ) -> pd.Series:
        """
        风险平价优化

        每只股票对组合风险的贡献相等
        """
        n_assets = len(corr_matrix.columns)

        if n_assets == 0:
            return pd.Series(dtype=float)

        # 初始等权
        weights = np.ones(n_assets) / n_assets

        # 构建协方差矩阵
        vol_vector = np.array([volatilities.get(code, 0.25) for code in corr_matrix.columns])
        cov_matrix = np.outer(vol_vector, vol_vector) * corr_matrix.values

        # 迭代优化风险平价
        for _ in range(100):
            # 计算风险贡献
            portfolio_vol = np.sqrt(weights @ cov_matrix @ weights)
            marginal_risk = cov_matrix @ weights
            risk_contrib = weights * marginal_risk / portfolio_vol

            # 调整权重使风险贡献相等
            target_risk_contrib = np.ones(n_assets) / n_assets
            weights = weights * (target_risk_contrib / (risk_contrib + 1e-8))
            weights = weights / weights.sum()  # 归一化

        return pd.Series(weights, index=corr_matrix.columns)

    def _black_litterman_adjust(
        self,
        weights_rp: pd.Series,
        candidates: pd.DataFrame,
        concept_predictions: pd.DataFrame,
        corr_matrix: pd.DataFrame,
        volatilities: Dict[str, float]
    ) -> pd.Series:
        """
        Black-Litterman 调整

        将板块预测观点融入权重
        """
        # 提取板块预测
        concept_pred_map = concept_predictions.set_index('concept_code')['combined_score'].to_dict()

        # 为每只股票计算观点强度
        views = []
        for idx, row in candidates.iterrows():
            concept = row.get('concept_code')
            if concept in concept_pred_map:
                view_strength = concept_pred_map[concept] / 100  # 归一化
                views.append(view_strength)
            else:
                views.append(0)

        views = np.array(views)

        # 调整权重 (观点越强，权重越高)
        adjusted_weights = weights_rp.values * (1 + views)
        adjusted_weights = adjusted_weights / adjusted_weights.sum()

        return pd.Series(adjusted_weights, index=weights_rp.index)

    def _apply_constraints(
        self,
        weights: pd.Series,
        candidates: pd.DataFrame,
        max_position: float,
        max_sector: float,
        min_position: float
    ) -> pd.Series:
        """应用约束条件"""
        # 1. 单个股权重约束
        weights = weights.clip(min_position, max_position)

        # 2. 板块集中度约束
        # 使用 ts_code 对齐权重
        candidates_with_weights = candidates.copy()
        candidates_with_weights = candidates_with_weights.set_index('ts_code')
        candidates_with_weights['weight'] = weights

        # 重置索引以便 groupby
        candidates_with_weights = candidates_with_weights.reset_index()
        sector_weights = candidates_with_weights.groupby('concept_code')['weight'].sum()

        for sector, total_weight in sector_weights.items():
            if total_weight > max_sector:
                # 按比例缩减
                sector_mask = candidates_with_weights['concept_code'] == sector
                scale = max_sector / total_weight
                # 获取该板块的股票代码
                sector_codes = candidates_with_weights[sector_mask]['ts_code'].tolist()
                for code in sector_codes:
                    if code in weights.index:
                        weights[code] *= scale

        # 重新归一化
        weights = weights / weights.sum()

        return weights

    def _calculate_portfolio_metrics(
        self,
        weights: pd.Series,
        corr_matrix: pd.DataFrame,
        volatilities: Dict[str, float]
    ) -> Dict:
        """计算组合预期指标"""
        # 权重向量
        w = weights.values

        # 波动率向量
        vol_vector = np.array([volatilities.get(code, 0.25) for code in weights.index])

        # 协方差矩阵
        cov_matrix = np.outer(vol_vector, vol_vector) * corr_matrix.values

        # 组合波动率
        portfolio_vol = np.sqrt(w @ cov_matrix @ w)

        # 预期收益 (简单使用预测得分)
        # 实际应该使用更复杂的预期收益模型
        expected_return = portfolio_vol * 1.5  # 假设 Sharpe=1.5

        # 夏普比率
        sharpe = expected_return / portfolio_vol if portfolio_vol > 0 else 0

        # 最大回撤估计
        max_drawdown = portfolio_vol * 2.5  # 粗略估计

        return {
            'expected_return': expected_return,
            'expected_volatility': portfolio_vol,
            'sharpe': sharpe,
            'max_drawdown': max_drawdown
        }

    def _analyze_risk(
        self,
        weights: pd.Series,
        corr_matrix: pd.DataFrame,
        candidates: pd.DataFrame
    ) -> Dict:
        """风险分析"""
        # 板块集中度
        # 使用 ts_code 对齐权重
        candidates_with_weights = candidates.copy()
        candidates_with_weights = candidates_with_weights.set_index('ts_code')
        candidates_with_weights['weight'] = weights

        # 重置索引以便 groupby
        candidates_with_weights = candidates_with_weights.reset_index()
        sector_weights = candidates_with_weights.groupby('concept_code')['weight'].sum()
        sector_concentration = sector_weights.max() if not sector_weights.empty else 0.0

        # 相关性分析
        avg_correlation = corr_matrix.values[np.triu_indices(len(corr_matrix), k=1)].mean()

        return {
            'sector_concentration': sector_concentration,
            'avg_correlation': avg_correlation,
            'weights_distribution': {
                'min': weights.min(),
                'max': weights.max(),
                'mean': weights.mean(),
            }
        }

    def _build_portfolio(
        self,
        weights: pd.Series,
        candidates: pd.DataFrame
    ) -> List[Dict]:
        """构建最终组合"""
        portfolio = []

        for ts_code, weight in weights.items():
            if weight < 0.01:  # 忽略权重<1% 的股票
                continue

            row = candidates[candidates['ts_code'] == ts_code]
            if row.empty:
                continue

            row = row.iloc[0]

            # 获取板块信息，concept_name 不存在时使用 concept_code
            concept_code = row.get('concept_code', '')
            concept_name = row.get('concept_name', '') or concept_code

            portfolio.append({
                'ts_code': ts_code,
                'stock_name': row.get('stock_name', ''),
                'concept_code': concept_code,
                'concept_name': concept_name,
                'weight': round(weight, 4),
                'pred_1d': round(row.get('pred_1d', 0), 2),
                'pred_5d': round(row.get('pred_5d', 0), 2),
                'pred_20d': round(row.get('pred_20d', 0), 2),
                'combined_score': round(row.get('combined_score', 0), 2)
            })

        # 按权重排序
        portfolio.sort(key=lambda x: x['weight'], reverse=True)

        return portfolio

    def _equal_weight_portfolio(
        self,
        candidates: pd.DataFrame,
        max_position: float,
        max_sector: float
    ) -> Dict:
        """等权组合"""
        n_stocks = min(10, len(candidates))
        weight = 1.0 / n_stocks

        portfolio = []
        for _, row in candidates.head(n_stocks).iterrows():
            portfolio.append({
                'ts_code': row['ts_code'],
                'stock_name': row.get('stock_name', ''),
                'weight': weight,
                'pred_1d': row.get('pred_1d', 0),
                'pred_5d': row.get('pred_5d', 0),
            })

        return {
            'portfolio': portfolio,
            'metrics': {
                'expected_return': 0.10,
                'expected_volatility': 0.20,
                'sharpe': 0.5,
                'max_drawdown': 0.25
            },
            'method': 'equal_weight'
        }

    def _empty_result(self) -> Dict:
        """空结果"""
        return {
            'portfolio': [],
            'metrics': {
                'expected_return': 0,
                'expected_volatility': 0,
                'sharpe': 0,
                'max_drawdown': 0
            },
            'risk_analysis': {}
        }

    def optimize_with_strategy(
        self,
        stock_predictions: pd.DataFrame,
        concept_predictions: pd.DataFrame = None,
        market_state: str = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        集成策略层的组合优化

        流程:
        1. 识别市场状态
        2. 根据市场状态调整仓位
        3. 应用事件信号
        4. 执行组合优化
        5. 应用风险管理约束

        Args:
            stock_predictions: 个股预测 DataFrame
            concept_predictions: 板块预测 DataFrame
            market_state: 市场状态 (可选，自动检测)
            **kwargs: 其他优化参数

        Returns:
            优化结果，包含策略信息
        """
        logger.info("开始策略集成组合优化...")

        # 导入策略组件
        try:
            from strategy.position_manager import PositionManager
            from strategy.enhanced_risk_manager import EnhancedRiskManager
            from strategy.event_driver import EventDriver
        except ImportError:
            logger.warning("策略模块导入失败，使用基础优化")
            return self.optimize(stock_predictions, concept_predictions, **kwargs)

        # 1. 市场状态识别
        pos_manager = PositionManager(self.db)
        if market_state is None:
            market_state, indicators = pos_manager.detect_market_state()

        logger.info(f"市场状态: {market_state}")

        # 2. 计算仓位
        prediction_confidence = self._calc_confidence(stock_predictions)
        total_position = pos_manager.calculate_position_size(
            market_state,
            prediction_confidence=prediction_confidence
        )

        logger.info(f"建议仓位: {total_position:.0%}")

        # 3. 事件驱动调整
        event_driver = EventDriver()
        events = event_driver.check_upcoming_events()

        if events and concept_predictions is not None:
            signals = event_driver.generate_event_signals(events, concept_predictions)
            concept_predictions = event_driver.apply_event_signals(concept_predictions, signals)
            logger.info(f"应用 {len(signals)} 个事件信号")

        # 4. 执行基础优化
        result = self.optimize(stock_predictions, concept_predictions, **kwargs)

        if not result['portfolio']:
            return result

        # 5. 应用仓位调整
        for pos in result['portfolio']:
            pos['weight'] = pos['weight'] * total_position

        # 重新归一化
        total_weight = sum(p['weight'] for p in result['portfolio'])
        if total_weight > 0:
            for pos in result['portfolio']:
                pos['weight'] = pos['weight'] / total_weight * total_position

        # 6. 风险管理信息
        risk_manager = EnhancedRiskManager(self.db)
        risk_report = risk_manager.get_risk_report(
            positions=[{
                'ts_code': p['ts_code'],
                'stock_name': p['stock_name'],
                'cost_price': 100,  # 假设成本
                'shares': 100,
                'current_price': 100
            } for p in result['portfolio']],
            current_prices={p['ts_code']: 100 for p in result['portfolio']},
            market_state=market_state
        )

        # 添加策略信息到结果
        result['strategy_info'] = {
            'market_state': market_state,
            'total_position': total_position,
            'prediction_confidence': prediction_confidence,
            'events': len(events),
            'position_suggestion': pos_manager.get_position_suggestion(
                market_state, prediction_confidence
            )
        }

        result['risk_report'] = {
            'total_value': risk_report['total_value'],
            'alert_count': risk_report['alert_count'],
            'critical_count': risk_report['critical_count']
        }

        # 更新指标
        result['metrics']['total_position'] = total_position
        result['metrics']['market_state'] = market_state

        logger.info(f"策略集成优化完成: 仓位={total_position:.0%}, 持仓={len(result['portfolio'])}只")

        return result

    def _calc_confidence(self, stock_predictions: pd.DataFrame) -> float:
        """计算预测置信度"""
        if stock_predictions.empty:
            return 0.5

        if 'combined_score' not in stock_predictions.columns:
            return 0.5

        scores = stock_predictions['combined_score'].values

        # 基于得分分布计算置信度
        mean_score = np.mean(scores)
        std_score = np.std(scores)

        # 高均值 + 低标准差 = 高置信度
        confidence = (mean_score / 100) * (1 - std_score / 50)
        confidence = max(0.1, min(1.0, confidence))

        return confidence


def main():
    """测试函数"""
    # 构造测试数据
    stock_predictions = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ'],
        'stock_name': ['平安银行', '万科 A', '浦发银行', '招商银行', '五粮液'],
        'concept_code': ['881101.TI', '881102.TI', '881101.TI', '881101.TI', '881103.TI'],
        'pred_1d': [1.5, 1.2, 1.3, 1.8, 2.0],
        'pred_5d': [5.0, 4.5, 4.8, 6.0, 7.0],
        'combined_score': [85, 80, 82, 90, 95]
    })

    concept_predictions = pd.DataFrame({
        'concept_code': ['881101.TI', '881102.TI', '881103.TI'],
        'combined_score': [85, 80, 95]
    })

    optimizer = PortfolioOptimizer()

    # 测试基础优化
    print("\n=== 基础组合优化 ===")
    result = optimizer.optimize(stock_predictions, concept_predictions)

    print(f"\n持仓数量：{len(result['portfolio'])}")
    print("\n持仓明细:")
    for pos in result['portfolio']:
        print(f"  {pos['ts_code']} {pos['stock_name']}: {pos['weight']:.1%}")

    print(f"\n预期指标:")
    print(f"  预期收益：{result['metrics']['expected_return']:.1%}")
    print(f"  预期波动率：{result['metrics']['expected_volatility']:.1%}")
    print(f"  夏普比率：{result['metrics']['sharpe']:.2f}")
    print(f"  最大回撤：{result['metrics']['max_drawdown']:.1%}")

    # 测试策略集成优化
    print("\n=== 策略集成优化 ===")
    try:
        result_with_strategy = optimizer.optimize_with_strategy(
            stock_predictions,
            concept_predictions,
            market_state='BULL'
        )

        print(f"\n市场状态: {result_with_strategy['strategy_info']['market_state']}")
        print(f"建议仓位: {result_with_strategy['strategy_info']['total_position']:.0%}")
        print(f"预测置信度: {result_with_strategy['strategy_info']['prediction_confidence']:.2f}")

        print(f"\n持仓明细 (策略调整后):")
        for pos in result_with_strategy['portfolio']:
            print(f"  {pos['ts_code']} {pos['stock_name']}: {pos['weight']:.1%}")

    except Exception as e:
        print(f"策略集成测试失败: {e}")


if __name__ == "__main__":
    main()
