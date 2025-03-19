"""Orders Latest data model."""

from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Record
from paper_engine_orders.model.base import State

logger = logging.getLogger(__name__)


class OrdersLatest(State):
    """Orders state."""

    portfolio_id: int  # entity key
    side: int
    asset_id_type: str
    asset_id: str
    order_ts: datetime
    target_wgt: Optional[Decimal] = None
    real_wgt: Optional[Decimal] = None
    quantity: Optional[Decimal] = None
    notional: Optional[Decimal] = None

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.portfolio_id}, "
            f"{self.side}, "
            f"{self.asset_id_type}, "
            f"{self.asset_id}, "
            f"{self.order_ts}, "
            f"{self.target_wgt}, "
            f"{self.real_wgt}, "
            f"{self.quantity}, "
            f"{self.notional}"
        )

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.portfolio_id, self.asset_id)

    @classmethod
    def from_source(cls, record: Record) -> "OrdersLatest":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.portfolio_id = record[0]  # entity key
        res.side = record[1]
        res.asset_id_type = record[2]
        res.asset_id = record[3]
        res.order_ts = record[4]
        res.target_wgt = record[5] if record[5] else None
        res.real_wgt = record[6] if record[6] else None
        res.quantity = record[7] if record[7] else None
        res.notional = record[8] if record[8] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "OrdersLatest":
        """Creates object from target record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.portfolio_id = record[0]  # entity key
        res.side = record[1]  # entity key
        res.asset_id_type = record[2]
        res.asset_id = record[3]
        res.order_ts = record[4]
        res.target_wgt = record[5] if record[5] else None
        res.real_wgt = record[6] if record[6] else None
        res.quantity = record[7] if record[7] else None
        res.notional = record[8] if record[8] else None
        _ = record[9]  # hash
        res.event_id = record[10]
        res.delivery_id = record[11]

        return res

    @classmethod
    def removal_instance(
        cls, event_id: int, delivery_id: int, key: Key
    ) -> "OrdersLatest":
        """Creates an empty object instance (for removal event logs)."""
        res = cls()
        res.event_id = event_id
        res.delivery_id = delivery_id
        (res.portfolio_id, res.asset_id) = key

        return res

    @staticmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""
        return [(r[0], r[3]) for r in records]

    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
        return (
            self.portfolio_id,
            self.side,
            self.asset_id_type,
            self.asset_id,
            self.order_ts,
            self.target_wgt,
            self.real_wgt,
            self.quantity,
            self.notional,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
