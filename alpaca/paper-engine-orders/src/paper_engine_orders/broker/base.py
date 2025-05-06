"""Base broker interaction model."""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Dict, List, Tuple


class Broker(ABC):
    """Broker base class."""

    @abstractmethod
    def check_tradable(self, symbols) -> List[str]:
        """Get information about the available assets to trade."""

    def check_shortable(self, symbols: List[str]) -> List[str]:
        """Get shortable tickers."""

    @abstractmethod
    def get_latest_book(self, symbols: List[str]) -> Tuple[Dict, Dict]:
        """Get latest ask prices."""

    @abstractmethod
    def get_positions(self) -> Dict:
        """Get all positions."""

    @abstractmethod
    def close_positions(self, portfolio_id: int, tickers: List[str]) -> List:
        """Close (liquidate) all positions."""

    @abstractmethod
    def submit_order(self, order_type: str, config: Dict) -> None:
        """Submit order to broker."""

    @staticmethod
    @abstractmethod
    def buy_params(ticker: str, quantity: Decimal) -> Dict:
        """Generate buy order params."""

    @staticmethod
    @abstractmethod
    def sell_params(ticker: str, quantity: Decimal) -> Dict:
        """Generate sell order params."""
