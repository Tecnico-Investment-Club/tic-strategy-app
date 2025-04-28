"""Strategy data model."""

from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_strategy._types import Key, Keys, Record
from paper_engine_strategy.model.base import State

logger = logging.getLogger(__name__)


class Strategy(State):
    """Strategy state."""

    strategy_id: int  # entity key
    asset_id_type: str
    asset_id: str
    datadate: datetime
    decision_ts: Optional[datetime] = None
    factor: Optional[Decimal] = None
    decision: Optional[int] = None

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.strategy_id}, "
            f"{self.asset_id_type}, "
            f"{self.asset_id}, "
            f"{self.datadate}, "
            f"{self.decision_ts}, "
            f"{self.factor}, "
            f"{self.decision}"
        )

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (
            self.strategy_id,
            self.asset_id_type,
            self.asset_id,
            self.datadate,
        )

    @classmethod
    def from_source(cls, record: Record) -> "Strategy":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.strategy_id = record[0]  # entity key
        res.asset_id_type = record[1]
        res.asset_id = record[2]
        res.datadate = record[3]
        res.decision_ts = record[4] if record[4] else None
        res.factor = record[5] if record[5] else None
        res.decision = record[6] if record[6] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "Strategy":
        """Creates object from target record."""
        res = cls()
        res.strategy_id = record[0]
        res.asset_id_type = record[1]
        res.asset_id = record[2]
        res.datadate = record[3]
        res.decision_ts = record[4]
        res.factor = record[5]
        res.decision = record[6]
        _ = record[7]  # hash
        res.event_id = record[8]
        res.delivery_id = record[9]

        return res

    @classmethod
    def removal_instance(cls, event_id: int, delivery_id: int, key: Key) -> "Strategy":
        """Creates an empty object instance (for removal event logs)."""
        res = cls()
        res.event_id = event_id
        res.delivery_id = delivery_id
        (res.strategy_id, res.asset_id_type, res.asset_id, res.datadate) = key

        return res

    @staticmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""
        return [(r[0], r[1], r[2], r[3]) for r in records]

    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
        return (
            self.strategy_id,
            self.asset_id_type,
            self.asset_id,
            self.datadate,
            self.decision_ts,
            self.factor,
            self.decision,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
