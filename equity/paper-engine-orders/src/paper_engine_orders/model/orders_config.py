"""Orders Config data model."""

from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Message, Record
from paper_engine_orders.model.base import EventLog, State
from paper_engine_orders.model.event_type import EventType

logger = logging.getLogger(__name__)


class OrdersConfig(State):
    """Orders Config state."""

    portfolio_id: int  # entity key
    strategy_id: Optional[int] = None
    portfolio_type: Optional[str] = None
    rebal_freq: Optional[str] = None
    adjust: Optional[bool] = None
    wgt_method: Optional[str] = None
    portfolio_hash: Optional[str] = None
    account_id: Optional[str] = None

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.portfolio_id}, "
            f"{self.strategy_id}, "
            f"{self.portfolio_type}, "
            f"{self.rebal_freq}, "
            f"{self.adjust}, "
            f"{self.wgt_method}, "
            f"{self.portfolio_hash}, "
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
        res.portfolio_type = record[2] if record[2] else None
        res.rebal_freq = record[3] if record[3] else None
        res.adjust = record[4] if record[4] else None
        res.wgt_method = record[5] if record[5] else None
        res.portfolio_hash = record[6] if record[6] else None
        res.account_id = record[7] if record[7] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "OrdersConfig":
        """Creates object from target record."""
        res = cls()
        res.portfolio_id = record[0]
        res.strategy_id = record[1]
        res.portfolio_type = record[2]
        res.rebal_freq = record[3]
        res.adjust = record[4]
        res.wgt_method = record[5]
        res.portfolio_hash = record[6]
        res.account_id = record[7]
        _ = record[8]  # hash
        res.event_id = record[9]
        res.delivery_id = record[10]

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
            self.portfolio_type,
            self.rebal_freq,
            self.adjust,
            self.wgt_method,
            self.portfolio_hash,
            self.account_id,
            self.hash,
            self.event_id,
            self.delivery_id,
        )


class OrdersConfigLog(EventLog):
    """Orders Config event log."""

    curr: OrdersConfig
    prev: Optional[OrdersConfig] = None

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
            return "11111111"

        if self.event_type == EventType.POINTLESS_AMEND:
            return "00000000"

        res = ""
        res += "1" if self.curr.portfolio_id != self.prev.portfolio_id else "0"
        res += "1" if self.curr.strategy_id != self.prev.strategy_id else "0"
        res += "1" if self.curr.portfolio_type != self.prev.portfolio_type else "0"
        res += "1" if self.curr.rebal_freq != self.prev.rebal_freq else "0"
        res += "1" if self.curr.adjust != self.prev.adjust else "0"
        res += "1" if self.curr.wgt_method != self.prev.wgt_method else "0"
        res += "1" if self.curr.portfolio_hash != self.prev.portfolio_hash else "0"
        res += "1" if self.curr.account_id != self.prev.account_id else "0"

        return res

    @property
    def topic(self) -> str:
        """Returns object topic to publish."""
        return f"orders_config.{self.event_type}.{self.curr.portfolio_id}"

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
                    "strategy_id": self.curr.strategy_id,
                    "portfolio_type": self.curr.portfolio_type,
                    "rebal_freq": self.curr.rebal_freq,
                    "adjust": self.curr.adjust,
                    "wgt_method": self.curr.wgt_method,
                    "portfolio_hash": self.curr.portfolio_hash,
                    "account_id": self.curr.account_id,
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
                    "curr_strategy_id": self.curr.strategy_id,
                    "curr_portfolio_type": self.curr.portfolio_type,
                    "curr_rebal_freq": self.curr.rebal_freq,
                    "curr_adjust": self.curr.adjust,
                    "curr_wgt_method": self.curr.wgt_method,
                    "curr_portfolio_hash": self.curr.portfolio_hash,
                    "curr_account_id": self.curr.account_id,
                    "curr_hash": self.curr.hash,
                    "curr_event_id": self.curr.event_id,
                    "curr_delivery_id": self.curr.delivery_id,
                    "prev_portfolio_id": self.prev.portfolio_id,
                    "prev_strategy_id": self.prev.strategy_id,
                    "prev_portfolio_type": self.prev.portfolio_type,
                    "prev_rebal_freq": self.prev.rebal_freq,
                    "prev_adjust": self.prev.adjust,
                    "prev_wgt_method": self.prev.wgt_method,
                    "prev_portfolio_hash": self.prev.portfolio_hash,
                    "prev_account_id": self.prev.account_id,
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
