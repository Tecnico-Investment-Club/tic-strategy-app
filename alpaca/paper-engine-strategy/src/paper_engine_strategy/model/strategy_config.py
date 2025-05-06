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

    strategy_id: int  # entity key
    strategy_type: Optional[str]
    asset_type: Optional[str]
    interval: Optional[str]
    lookback: Optional[int]
    strategy_config: Optional[str]
    strategy_hash: Optional[str]

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.strategy_id}, "
            f"{self.strategy_type}, "
            f"{self.asset_type}, "
            f"{self.interval}, "
            f"{self.lookback}, "
            f"{self.strategy_config}, "
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
        res.strategy_type = record[1] if record[1] else None
        res.asset_type = record[2] if record[2] else None
        res.interval = record[3] if record[3] else None
        res.lookback = record[4] if record[4] else None
        res.strategy_config = json.dumps(record[5], sort_keys=True) if record[5] else None
        res.strategy_hash = record[6] if record[6] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "StrategyConfig":
        """Creates object from target record."""
        res = cls()
        res.strategy_id = record[0]
        res.strategy_type = record[1]
        res.asset_type = record[2]
        res.interval = record[3]
        res.lookback = record[4]
        res.strategy_config = record[5]
        res.strategy_hash = record[6]
        _ = record[7]  # hash
        res.event_id = record[8]
        res.delivery_id = record[9]

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
            self.strategy_type,
            self.asset_type,
            self.interval,
            self.lookback,
            self.strategy_config,
            self.strategy_hash,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
