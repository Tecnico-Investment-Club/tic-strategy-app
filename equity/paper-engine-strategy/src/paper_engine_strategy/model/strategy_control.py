"""Strategy Control data model."""

from datetime import datetime
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_strategy._types import Key, Keys, Record
from paper_engine_strategy.model.base import State

logger = logging.getLogger(__name__)


class StrategyControl(State):
    """Strategy Control state."""

    strategy_id: int  # entity key
    last_decision_date: Optional[datetime]

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = f"{self.strategy_id}, " f"{self.last_decision_date}"

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.strategy_id,)

    @classmethod
    def from_source(cls, record: Record) -> "StrategyControl":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.strategy_id = record[0]  # entity key
        res.last_decision_date = record[1] if record[1] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "StrategyControl":
        """Creates object from target record."""
        res = cls()
        res.strategy_id = record[0]
        res.last_decision_date = record[1]
        _ = record[2]  # hash
        res.event_id = record[3]
        res.delivery_id = record[4]

        return res

    @classmethod
    def removal_instance(
        cls, event_id: int, delivery_id: int, key: Key
    ) -> "StrategyControl":
        """Creates an empty object instance (for removal event logs)."""
        res = cls()
        res.event_id = event_id
        res.delivery_id = delivery_id
        (res.strategy_id,) = key

        return res

    @staticmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""
        return [(r[0],) for r in records]

    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
        return (
            self.strategy_id,
            self.last_decision_date,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
