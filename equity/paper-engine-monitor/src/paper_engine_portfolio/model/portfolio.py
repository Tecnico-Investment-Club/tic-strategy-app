"""Portfolio data model."""

from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import logging
from typing import List, Optional, Tuple

from paper_engine_portfolio._types import Key, Keys, Record
from paper_engine_portfolio.model.base import State

logger = logging.getLogger(__name__)


class Portfolio(State):
    """Portfolio state."""

    portfolio_id: int  # entity key
    portfolio_ts: datetime  # entity key
    long_notional: Optional[Decimal]
    short_notional: Optional[Decimal]
    notional: Optional[Decimal]
    long_wgt: Optional[Decimal]
    short_wgt: Optional[Decimal]
    long_rtn: Optional[Decimal]
    long_cum_rtn: Optional[Decimal]
    short_rtn: Optional[Decimal]
    short_cum_rtn: Optional[Decimal]
    rtn: Optional[Decimal]
    cum_rtn: Optional[Decimal]

    @property
    def hash(self) -> str:
        """Object sha256 hash value."""
        res = (
            f"{self.portfolio_id}, "
            f"{self.portfolio_ts}, "
            f"{self.long_notional}, "
            f"{self.short_notional}, "
            f"{self.notional}, "
            f"{self.long_wgt}, "
            f"{self.short_wgt}, "
            f"{self.long_rtn}, "
            f"{self.long_cum_rtn}, "
            f"{self.short_rtn}, "
            f"{self.short_cum_rtn}, "
            f"{self.rtn}, "
            f"{self.cum_rtn}"
        )

        return sha256(res.encode("utf-8")).hexdigest()

    @property
    def key(self) -> Key:
        """Object key."""
        return (self.portfolio_id, self.portfolio_ts)

    @classmethod
    def from_source(cls, record: Record) -> "Portfolio":
        """Creates object from source record."""
        res = cls()
        res.event_id = None
        res.delivery_id = None
        res.portfolio_id = record[0]  # entity key
        res.portfolio_ts = record[1]  # entity key
        res.long_notional = record[2] if record[2] else None
        res.short_notional = record[3] if record[3] else None
        res.notional = record[4] if record[4] else None
        res.long_wgt = record[5] if record[5] else None
        res.short_wgt = record[6] if record[6] else None
        res.long_rtn = record[7] if record[7] else None
        res.long_cum_rtn = record[8] if record[8] else None
        res.short_rtn = record[9] if record[9] else None
        res.short_cum_rtn = record[10] if record[10] else None
        res.rtn = record[11] if record[11] else None
        res.cum_rtn = record[12] if record[12] else None

        return res

    @classmethod
    def from_target(cls, record: Tuple) -> "Portfolio":
        """Creates object from target record."""
        res = cls()
        res.portfolio_id = record[0]
        res.portfolio_ts = record[1]
        res.long_notional = record[2]
        res.short_notional = record[3]
        res.notional = record[4]
        res.long_wgt = record[5]
        res.short_wgt = record[6]
        res.long_rtn = record[7]
        res.long_cum_rtn = record[8]
        res.short_rtn = record[9]
        res.short_cum_rtn = record[10]
        res.rtn = record[11]
        res.cum_rtn = record[12]
        _ = record[13]  # hash
        res.event_id = record[14]
        res.delivery_id = record[15]

        return res

    @classmethod
    def removal_instance(cls, event_id: int, delivery_id: int, key: Key) -> "Portfolio":
        """Creates an empty object instance (for removal event logs)."""
        res = cls()
        res.event_id = event_id
        res.delivery_id = delivery_id
        (res.portfolio_id, res.portfolio_ts) = key

        return res

    @staticmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""
        return [(r[0], r[1]) for r in records]

    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""
        return (
            self.portfolio_id,
            self.portfolio_ts,
            self.long_notional,
            self.short_notional,
            self.notional,
            self.long_wgt,
            self.short_wgt,
            self.long_rtn,
            self.long_cum_rtn,
            self.short_rtn,
            self.short_cum_rtn,
            self.rtn,
            self.cum_rtn,
            self.hash,
            self.event_id,
            self.delivery_id,
        )
