"""Orders Config data model."""

from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Record
from paper_engine_orders.model.base import State

logger = logging.getLogger(__name__)


class OrdersConfig(State):
    """Orders Config state."""

    portfolio_id: int  # entity key
    strategy_id: Optional[int] = None
    portfolio_name: Optional[str] = None
    account_id: Optional[str] = None

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.portfolio_id}, "
            f"{self.strategy_id}, "
            f"{self.portfolio_name}, "
            f"{self.account_id}"
        )

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.portfolio_id,)

    @classmethod
    def from_source(cls, record: Record) -> "OrdersConfig":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.portfolio_id = record[0]  # entity key
        res.strategy_id = record[1] if record[1] else None
        res.portfolio_name = record[2] if record[2] else None
        res.account_id = record[3] if record[3] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "OrdersConfig":
        """Creates object from target record."""
        res = cls()
        res.portfolio_id = record[0]
        res.strategy_id = record[1]
        res.portfolio_name = record[2]
        res.account_id = record[3]
        _ = record[4]  # hash
        res.event_id = record[5]
        res.delivery_id = record[6]

        return res

    @classmethod
    def removal_instance(
        cls, event_id: int, delivery_id: int, key: Key
    ) -> "OrdersConfig":
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
            self.strategy_id,
            self.portfolio_name,
            self.account_id,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
