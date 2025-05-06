from typing import Any, Dict, List

import pandas as pd

from paper_engine_strategy._types import File
from paper_engine_strategy.model.source_model.spot_prices import SpotPrices
from paper_engine_strategy.model.strategy_latest import StrategyLatest
from paper_engine_strategy.strategy.base import BaseStrategy
from paper_engine_strategy.strategy.portfolio_optimization.po import PortfolioOptimization
import paper_engine_strategy.strategy.portfolio_optimization.helpers.data_models as dm
import paper_engine_strategy.strategy.portfolio_optimization.helpers.tc_optimization as tc

class POHurstExpStrategy(BaseStrategy):
    """Base Strategy."""

    strategy_id: int
    params: Dict[str, Any]

    @classmethod
    def setup(cls, strategy_config: Dict[str, Any]) -> "BaseStrategy":
        """Setup portfolio_optimization params."""
        res = cls()
        functional_constraints = dm.Functional_Constraints(
            Take_Profit=float(strategy_config.get("take_profit", 0)),  # NOT USED
            Stop_Loss=float(strategy_config.get("stop_loss", 0)),  # NOT USED
            Capital_at_Risk=float(strategy_config.get("capital_at_risk", 0)),  # NOT USED
            Hurst_Filter=dm.hurst_filter[strategy_config.get("hurst_filter", "MODERATE")],
            RSIFilter=dm.rsi_filter[strategy_config.get("rsi_filter", "STANDARD")],
            Hurst_Exponents_Period=int(strategy_config.get("hurst_exponents_period", 270)),
            MACD_Short_Window=int(strategy_config.get("macd_short_window", 12)),
            MACD_Long_Window=int(strategy_config.get("macd_long_window", 26)),
            Bollinger_Window=int(strategy_config.get("bollinger_window", 20)),
            RSI_Window=int(strategy_config.get("rsi_window", 5)),
        )

        rebalance_constraints = dm.Rebalance_Constraints(
            Turnover_Constraint=float(strategy_config.get("turnover_constraint", 0)),  # NOT USED
            Transaction_Cost=float(strategy_config.get("transaction_cost", 0.01)),
            distance_method=tc.distance_method[strategy_config.get("distance_method", "NORMALIZED_EUCLIDEAN")],
        )

        params = {
            "best_delta": 0.235,
            "mom_type": dm.momentum_type[strategy_config.get("momentum_type", "MACD")],
            "mean_rev_type": dm.mean_rev_type[strategy_config.get("mean_rev_type", "RSI")],
            "rebalancing_period": dm.rebalancing_period[strategy_config.get("rebalancing_period", "DAILY")],
            "functional_constraints": functional_constraints,
            "rebalance_constraints": rebalance_constraints,
            "mom_days": int(strategy_config.get("mom_days", 30)),
        }
        res.params = params
        return res

    def get_weights(self, records: List[SpotPrices], prev_weights: List[StrategyLatest]) -> File:
        """Pick portfolio according to thresholds."""
        closes = self.records_2_df(records)
        prev_wgts = [[p.asset_id, p.datadate, p.weight] for p in prev_weights]

        params = self.params
        p = PortfolioOptimization(
            **params,
            closes=closes,
            previous_weights=prev_wgts
        )
        strategy_records = p.get_weights()

        return strategy_records

    @staticmethod
    def records_2_df(records: List[SpotPrices]) -> pd.DataFrame:
        short_records = [{
            "date": r.open_time,
            "symbol": r.symbol,
            "close": float(r.close_price),
        } for r in records]
        df = pd.DataFrame(short_records)
        pivoted = df.pivot(index='date', columns='symbol', values='close')
        pivoted = pivoted.dropna(axis=1, how="any")
        return pivoted

