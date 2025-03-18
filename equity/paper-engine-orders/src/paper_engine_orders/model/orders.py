"""Orders data model."""

from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Message, Record
from paper_engine_orders.model.base import EventLog, State
from paper_engine_orders.model.event_type import EventType

logger = logging.getLogger(__name__)


class Orders(State):
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
        return (self.portfolio_id, self.asset_id, self.order_ts)

    @classmethod
    def from_source(cls, record: Record) -> "Orders":
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
    def from_target(cls, record: Tuple) -> "Orders":
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
    def removal_instance(cls, event_id: int, delivery_id: int, key: Key) -> "Orders":
        """Creates an empty object instance (for removal event logs)."""
        res = cls()
        res.event_id = event_id
        res.delivery_id = delivery_id
        (res.portfolio_id, res.asset_id, res.order_ts) = key

        return res

    @staticmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""
        return [(r[0], r[3], r[4]) for r in records]

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


class OrdersLog(EventLog):
    """Orders event log."""

    curr: Orders
    prev: Optional[Orders] = None

    @property
    def mask(self) -> str:
        """Event log record change mask."""
        if (
            self.prev is None  # EventType.CREATE
            or self.curr is None  # EventType.REMOVE
            or self.event_type == EventType.CREATE
            or self.event_type == EventType.REMOVE
            or self.event_type == EventType.RECREATE
        ):
            return "111111111"

        if self.event_type == EventType.POINTLESS_AMEND:
            return "000000000"

        res = ""
        res += "1" if self.curr.portfolio_id != self.prev.portfolio_id else "0"
        res += "1" if self.curr.side != self.prev.side else "0"
        res += "1" if self.curr.asset_id_type != self.prev.asset_id_type else "0"
        res += "1" if self.curr.asset_id != self.prev.asset_id else "0"
        res += "1" if self.curr.order_ts != self.prev.order_ts else "0"
        res += "1" if self.curr.target_wgt != self.prev.target_wgt else "0"
        res += "1" if self.curr.real_wgt != self.prev.real_wgt else "0"
        res += "1" if self.curr.quantity != self.prev.quantity else "0"
        res += "1" if self.curr.notional != self.prev.notional else "0"

        return res

    @property
    def topic(self) -> str:
        """Returns object topic to publish."""
        return (
            f"orders."
            f"{self.event_type}."
            f"{self.curr.portfolio_id}."
            f"{self.curr.asset_id}."
            f"{self.curr.order_ts}"
        )

    @property
    def message(self) -> Message:
        """Returns object message to publish."""
        msg: Message = {
            "v": 1,
            "event_type": self.event_type,
        }

        if self.event_type == EventType.CREATE:
            msg.update(
                {
                    "portfolio_id": self.curr.portfolio_id,
                    "side": self.curr.side,
                    "asset_id_type": self.curr.asset_id_type,
                    "asset_id": self.curr.asset_id,
                    "order_ts": self.curr.order_ts,
                    "target_wgt": self.curr.target_wgt,
                    "real_wgt": self.curr.real_wgt,
                    "quantity": self.curr.quantity,
                    "notional": self.curr.notional,
                    "hash": self.curr.hash,
                    "event_id": self.curr.event_id,
                    "delivery_id": self.curr.delivery_id,
                }
            )
        elif self.event_type == EventType.AMEND:
            assert self.prev is not None
            msg.update(
                {
                    "curr_portfolio_id": self.curr.portfolio_id,
                    "curr_side": self.curr.side,
                    "curr_asset_id_type": self.curr.asset_id_type,
                    "curr_asset_id": self.curr.asset_id,
                    "curr_order_ts": self.curr.order_ts,
                    "curr_target_wgt": self.curr.target_wgt,
                    "curr_real_wgt": self.curr.real_wgt,
                    "curr_quantity": self.curr.quantity,
                    "curr_notional": self.curr.notional,
                    "curr_hash": self.curr.hash,
                    "curr_event_id": self.curr.event_id,
                    "curr_delivery_id": self.curr.delivery_id,
                    "prev_portfolio_id": self.prev.portfolio_id,
                    "prev_side": self.prev.side,
                    "prev_asset_id_type": self.prev.asset_id_type,
                    "prev_asset_id": self.prev.asset_id,
                    "prev_order_ts": self.prev.order_ts,
                    "prev_target_wgt": self.prev.target_wgt,
                    "prev_real_wgt": self.prev.real_wgt,
                    "prev_quantity": self.prev.quantity,
                    "prev_notional": self.prev.notional,
                    "prev_hash": self.prev.hash,
                    "prev_event_id": self.prev.event_id,
                    "prev_delivery_id": self.prev.delivery_id,
                }
            )
        elif self.event_type == EventType.REMOVE:
            msg.update(
                {
                    "portfolio_id": self.curr.portfolio_id,
                    "asset_id": self.curr.asset_id,
                    "order_ts": self.curr.order_ts,
                    "event_id": self.curr.event_id,
                    "delivery_id": self.curr.delivery_id,
                }
            )
        else:
            pass

        return msg
