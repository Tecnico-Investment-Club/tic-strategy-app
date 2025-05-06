"""Orders Control data model."""

from datetime import datetime
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Record
from paper_engine_orders.model.base import State

logger = logging.getLogger(__name__)


class OrdersControl(State):
    """Orders Control state."""

    portfolio_id: int  # entity key
    last_read_delivery_id: Optional[int] = None
    last_decision_datadate: Optional[datetime] = None
    last_rebal_ts: Optional[datetime] = None

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.portfolio_id}, "
            f"{self.last_read_delivery_id}, "
            f"{self.last_decision_datadate}, "
            f"{self.last_rebal_ts}"
        )

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.portfolio_id,)

    @classmethod
    def from_source(cls, record: Record) -> "OrdersControl":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.portfolio_id = record[0]  # entity key
        res.last_read_delivery_id = record[1] if record[1] else None
        res.last_decision_datadate = record[2] if record[2] else None
        res.last_rebal_ts = record[3] if record[3] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "OrdersControl":
        """Creates object from target record."""
        res = cls()
        res.portfolio_id = record[0]
        res.last_read_delivery_id = record[1]
        res.last_decision_datadate = record[2]
        res.last_rebal_ts = record[3]
        _ = record[4]  # hash
        res.event_id = record[5]
        res.delivery_id = record[6]

        return res

    @classmethod
    def removal_instance(
        cls, event_id: int, delivery_id: int, key: Key
    ) -> "OrdersControl":
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
            self.last_read_delivery_id,
            self.last_decision_datadate,
            self.last_rebal_ts,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
