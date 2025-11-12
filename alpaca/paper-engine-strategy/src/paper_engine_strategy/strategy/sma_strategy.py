from typing import Any, Dict, List

import pandas as pd

from paper_engine_strategy._types import File
from paper_engine_strategy.model.source_model.spot_prices import SpotPrices
from paper_engine_strategy.model.strategy_latest import StrategyLatest
from paper_engine_strategy.strategy.base import BaseStrategy
from paper_engine_strategy.strategy.simple_moving_average.sma import SMA

class SMAStrategy(BaseStrategy):
    """Base Strategy."""

    strategy_id: int
    params: Dict[str, Any]

    @classmethod
    def setup(cls, strategy_config: Dict[str, Any]) -> "BaseStrategy":
        """Setup simple_moving_average params."""

        res = cls()
        params = {
            "symbol": strategy_config.get("symbol", "ETHUSD"),
            "sma_window": int(strategy_config.get("sma_window", 20)),
        }
        res.params = params
        return res

    def get_weights(self, records: List[SpotPrices], prev_weights: List[List]) -> File:
        """Pick portfolio according to thresholds."""
        closes = self.records_2_df(records)

        params = self.params
        p = SMA(
            **params,
            closes=closes,
            previous_weights=prev_weights
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

