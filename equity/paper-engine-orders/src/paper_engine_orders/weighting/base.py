"""Base weighting model."""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List

from paper_engine_orders._types import File
from paper_engine_orders.broker.base import Broker


class BaseWeight(ABC):
    """Base weighting class."""

    capital: Decimal
    strategy_records: List
    broker: Broker

    target_weights: Dict
    real_weights: Dict
    quantities: Dict

    total_value: Decimal

    @classmethod
    @abstractmethod
    def setup(
        cls,
        broker: Broker,
        capital: Decimal,
        strategy_records: List,
        current_positions: Dict,
    ) -> "BaseWeight":
        """Initializes weighting class."""

    @abstractmethod
    def get_target_weights(self, records: List) -> None:
        """Get target weights for the weighting scheme."""

    @abstractmethod
    def get_weights(self) -> None:
        """Get weights (as attributes)."""

    @abstractmethod
    def get_orders_params(self) -> Dict:
        """Get order parameters."""

    @abstractmethod
    def get_orders_records(self, portfolio_id: int) -> File:
        """Get position entity records."""
