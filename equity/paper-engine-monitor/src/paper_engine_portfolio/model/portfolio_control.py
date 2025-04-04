"""Portfolio Control data model."""

from datetime import datetime
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_portfolio._types import Key, Keys, Message, Record
from paper_engine_portfolio.model.base import EventLog, State
from paper_engine_portfolio.model.event_type import EventType

logger = logging.getLogger(__name__)


class PortfolioControl(State):
    """Portfolio Control state."""

    portfolio_id: int  # entity key
    last_monitor_ts: Optional[datetime] = None

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = f"{self.portfolio_id}, {self.last_monitor_ts}"

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.portfolio_id,)

    @classmethod
    def from_source(cls, record: Record) -> "PortfolioControl":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.portfolio_id = record[0]  # entity key
        res.last_monitor_ts = record[1] if record[1] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "PortfolioControl":
        """Creates object from target record."""
        res = cls()
        res.portfolio_id = record[0]
        res.last_monitor_ts = record[1]
        _ = record[2]  # hash
        res.event_id = record[3]
        res.delivery_id = record[4]

        return res

    @classmethod
    def removal_instance(
        cls, event_id: int, delivery_id: int, key: Key
    ) -> "PortfolioControl":
        """Creates an empty object instance (for removal event logs)."""
        res = cls()
        res.event_id = event_id
        res.delivery_id = delivery_id
        (res.portfolio_id,) = key

        return res

    @staticmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""
        return [(r[0],) for r in records]

    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
        return (
            self.portfolio_id,
            self.last_monitor_ts,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
