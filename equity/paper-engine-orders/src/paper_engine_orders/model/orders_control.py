"""Orders Control data model."""

from datetime import datetime
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Message, Record
from paper_engine_orders.model.base import EventLog, State
from paper_engine_orders.model.event_type import EventType

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


class OrdersControlLog(EventLog):
    """Orders Control event log."""

    curr: OrdersControl
    prev: Optional[OrdersControl] = None

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
            return "1111"

        if self.event_type == EventType.POINTLESS_AMEND:
            return "0000"

        res = ""
        res += "1" if self.curr.portfolio_id != self.prev.portfolio_id else "0"
        res += (
            "1"
            if self.curr.last_read_delivery_id != self.prev.last_read_delivery_id
            else "0"
        )
        res += (
            "1"
            if self.curr.last_decision_datadate != self.prev.last_decision_datadate
            else "0"
        )
        res += "1" if self.curr.last_rebal_ts != self.prev.last_rebal_ts else "0"

        return res

    @property
    def topic(self) -> str:
        """Returns object topic to publish."""
        return f"orders_control.{self.event_type}.{self.curr.portfolio_id}"

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
                    "last_read_delivery_id": self.curr.last_read_delivery_id,
                    "last_decision_datadate": self.curr.last_decision_datadate,
                    "last_rebal_ts": self.curr.last_rebal_ts,
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
                    "curr_last_read_delivery_id": self.curr.last_read_delivery_id,
                    "curr_last_decision_datadate": self.curr.last_decision_datadate,
                    "curr_last_rebal_ts": self.curr.last_rebal_ts,
                    "curr_hash": self.curr.hash,
                    "curr_event_id": self.curr.event_id,
                    "curr_delivery_id": self.curr.delivery_id,
                    "prev_portfolio_id": self.prev.portfolio_id,
                    "prev_last_read_delivery_id": self.prev.last_read_delivery_id,
                    "prev_last_decision_datadate": self.prev.last_decision_datadate,
                    "prev_last_rebal_ts": self.prev.last_rebal_ts,
                    "prev_hash": self.prev.hash,
                    "prev_event_id": self.prev.event_id,
                    "prev_delivery_id": self.prev.delivery_id,
                }
            )
        elif self.event_type == EventType.REMOVE:
            msg.update(
                {
                    "portfolio_id": self.curr.portfolio_id,
                    "event_id": self.curr.event_id,
                    "delivery_id": self.curr.delivery_id,
                }
            )
        else:
            pass

        return msg
