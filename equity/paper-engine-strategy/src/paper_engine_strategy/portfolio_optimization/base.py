"""Base portfolio_optimization model."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from paper_engine_strategy._types import File


class BaseStrategy(ABC):
    """Base Strategy."""

    # TODO: ADD PO STRAT PARAMS
    strategy_id: int

    @classmethod
    @abstractmethod
    def setup(cls, strat_params: Dict[str, Any]) -> "BaseStrategy":
        """Setup portfolio_optimization params."""
        pass

    @abstractmethod
    def get_weights(self, records: List[List]) -> File:
        """Pick portfolio according to thresholds."""
        pass
