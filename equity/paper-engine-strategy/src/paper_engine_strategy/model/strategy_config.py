"""Strategy Config data model."""

from hashlib import sha256
import json
import logging
from typing import List, Optional, Tuple

from paper_engine_strategy._types import Key, Keys, Record
from paper_engine_strategy.model.base import State

logger = logging.getLogger(__name__)


class StrategyConfig(State):
    """Strategy Config state."""

    # TODO: ADD PO STRAT PARAMS

    strategy_id: int  # entity key
    strategy_name: Optional[str]
    strategy_type: Optional[str]
    alpha: Optional[str]
    factor: Optional[str]
    signal_lifetime: Optional[str]
    top_threshold: Optional[int]
    top_threshold_type: Optional[str]
    bottom_threshold: Optional[int]
    bottom_threshold_type: Optional[str]
    strategy_hash: Optional[str]

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.strategy_id}, "
            f"{self.strategy_name}, "
            f"{self.strategy_type}, "
            f"{self.alpha}, "
            f"{self.factor}, "
            f"{self.signal_lifetime}, "
            f"{self.top_threshold}, "
            f"{self.top_threshold_type}, "
            f"{self.bottom_threshold}, "
            f"{self.bottom_threshold_type},"
            f"{self.strategy_hash}"
        )

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.strategy_id,)

    @classmethod
    def from_source(cls, record: Record) -> "StrategyConfig":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.strategy_id = record[0]  # entity key
        res.strategy_name = record[1] if record[1] else None
        res.strategy_type = record[2] if record[2] else None
        res.alpha = record[3] if record[3] else None
        res.factor = json.dumps(record[4], sort_keys=True) if record[4] else None
        res.signal_lifetime = record[5] if record[5] else None
        res.top_threshold = record[6] if record[6] else None
        res.top_threshold_type = record[7] if record[7] else None
        res.bottom_threshold = record[8] if record[8] else None
        res.bottom_threshold_type = record[9] if record[9] else None
        res.strategy_hash = record[10] if record[10] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "StrategyConfig":
        """Creates object from target record."""
        res = cls()
        res.strategy_id = record[0]
        res.strategy_name = record[1]
        res.strategy_type = record[2]
        res.alpha = record[3]
        res.factor = record[4]
        res.signal_lifetime = record[5]
        res.top_threshold = record[6]
        res.top_threshold_type = record[7]
        res.bottom_threshold = record[8]
        res.bottom_threshold_type = record[9]
        res.strategy_hash = record[10]
        _ = record[11]  # hash
        res.event_id = record[12]
        res.delivery_id = record[13]

        return res

    @classmethod
    def removal_instance(
        cls, event_id: int, delivery_id: int, key: Key
    ) -> "StrategyConfig":
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
            self.strategy_name,
            self.strategy_type,
            self.alpha,
            self.factor,
            self.signal_lifetime,
            self.top_threshold,
            self.top_threshold_type,
            self.bottom_threshold,
            self.bottom_threshold_type,
            self.strategy_hash,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
