"""Base broker interaction model."""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict


class Broker(ABC):
    """Broker base class."""

    @abstractmethod
    def get_portfolio_value(self, side: str) -> Decimal:
        """Get information about the available assets to trade."""

    @abstractmethod
    def get_all_positions(self) -> Dict:
        """Get all positions."""

    @abstractmethod
    def get_current_weights(self):
        """Get current weights."""
