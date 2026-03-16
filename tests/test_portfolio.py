"""
组合构建系统测试
测试热点轮动 + 个股筛选 + 组合优化完整流程
"""
import sys
import os

# 添加 src 目录到路径
src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')
sys.path.insert(0, src_dir)

from data.database import SQLiteDatabase, get_database
from data.stock_collector import StockCollector
from data.stock_screener import StockScreener
from models.stock_predictor import StockPredictor
from portfolio.optimizer import PortfolioOptimizer
from agents.portfolio_agent import PortfolioAgent


def test_database_extension():
    """测试数据库扩展"""
    print("\n" + "=" * 70)
    print("测试 1: 数据库扩展")
    print("=" * 70)

    db = get_database()

    # 测试表是否存在
    stats = db.get_stock_statistics()
    print(f"股票数据统计：{stats}")

    # 测试成分股查询
    test_concept = '881101.TI'
    constituents = db.get_concept_constituents(test_concept)
    print(f"板块 {test_concept} 成分股数量：{len(constituents)}")

    print("\n✓ 数据库扩展测试完成")


def test_stock_screener():
    """测试个股筛选器"""
    print("\n" + "=" * 70)
    print("测试 2: 个股筛选器")
    print("=" * 70)

    screener = StockScreener()

    # 测试筛选
    test_concepts = ['881101.TI', '881102.TI']
    result = screener.get_top_stocks(test_concepts, top_n=5)

    if not result.empty:
        print(f"\n筛选结果：{len(result)} 只股票")
        print(result[['stock_code', 'stock_name', 'concept_code', 'stock_score']].to_string())
    else:
        print("筛选结果为空（可能是因为没有个股数据）")

    print("\n✓ 个股筛选器测试完成")


def test_portfolio_optimizer():
    """测试组合优化器"""
    print("\n" + "=" * 70)
    print("测试 3: 组合优化器")
    print("=" * 70)

    optimizer = PortfolioOptimizer()

    # 构造测试数据
    import pandas as pd
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

    result = optimizer.optimize(stock_predictions, concept_predictions)

    print(f"\n组合优化结果：{len(result.get('portfolio', []))} 只股票")
    print("\n持仓明细:")
    for pos in result.get('portfolio', [])[:5]:
        print(f"  {pos['ts_code']} {pos['stock_name']}: {pos['weight']:.1%}")

    print("\n预期指标:")
    metrics = result.get('metrics', {})
    print(f"  预期收益：{metrics.get('expected_return', 0):.1%}")
    print(f"  预期波动率：{metrics.get('expected_volatility', 0):.1%}")
    print(f"  夏普比率：{metrics.get('sharpe', 0):.2f}")

    print("\n✓ 组合优化器测试完成")


def test_portfolio_agent():
    """测试组合 Agent 完整流程"""
    print("\n" + "=" * 70)
    print("测试 4: 组合 Agent 完整流程")
    print("=" * 70)

    agent = PortfolioAgent()

    # 测试构建组合
    test_concepts = ['881101.TI', '881102.TI']

    print("\n构建投资组合...")
    result = agent.run(task="build", concept_codes=test_concepts, top_n_stocks=10)

    if result.get('success'):
        print(f"\n✓ 组合构建成功!")
        print(f"持仓数量：{len(result['portfolio'])}")

        if result['portfolio']:
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
        print("（这可能是因为缺乏个股数据或个股预测模型）")

    print("\n✓ 组合 Agent 测试完成")


def main():
    """运行所有测试"""
    print("\n" + "=" * 70)
    print("组合构建系统测试")
    print("=" * 70)

    # 运行测试
    test_database_extension()
    test_stock_screener()
    test_portfolio_optimizer()
    test_portfolio_agent()

    print("\n" + "=" * 70)
    print("所有测试完成!")
    print("=" * 70)


if __name__ == "__main__":
    main()
