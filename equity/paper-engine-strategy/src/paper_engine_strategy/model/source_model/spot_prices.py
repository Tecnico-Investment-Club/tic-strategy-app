"""Kline model."""

from datetime import datetime
from decimal import Decimal
from typing import List, Tuple

from paper_engine_strategy.model.source_model.base import SourceState


class SpotPrices(SourceState):
    """Kline class."""

    id: int
    symbol: str
    open_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal

    @classmethod
    def from_source(cls, record: List) -> "SpotPrices":
        """Build record object."""
        res = cls()

        res.id = record[0]
        res.symbol = record[1]
        res.open_time = record[2]
        res.open_price = record[3]
        res.high_price = record[4]
        res.low_price = record[5]
        res.close_price = record[6]

        return res

    @property
    def key(self) -> Tuple:
        """Object key."""
        return (self.id,)

    def as_tuple(self) -> Tuple:
        """Get object as tuple."""
        return (
            self.id,
            self.symbol,
            self.open_time,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
        )

    def __repr__(self) -> str:
        return f"{self.symbol}, {self.open_time}"
