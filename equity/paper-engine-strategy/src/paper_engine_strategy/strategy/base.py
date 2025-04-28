"""Base portfolio_optimization model."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from paper_engine_strategy._types import File
from paper_engine_strategy.model.source_model.spot_prices import SpotPrices


class BaseStrategy(ABC):
    """Base Strategy."""

    strategy_id: int

    @classmethod
    @abstractmethod
    def setup(cls, strategy_config: Dict[str, Any]) -> "BaseStrategy":
        """Setup portfolio_optimization params."""
        pass

    @abstractmethod
    def get_weights(self, records: List[SpotPrices], prev_weights: List[List]) -> File:
        """Pick portfolio according to thresholds."""
        pass
