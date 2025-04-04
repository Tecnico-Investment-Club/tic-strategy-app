"""SOURCE Orders data model."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Optional

from paper_engine_portfolio._types import Key, Record
from paper_engine_portfolio.model.source_model.base import SourceState

logger = logging.getLogger(__name__)


class Orders(SourceState):
    """Orders state."""

    event_type: str  # entity key
    portfolio_id: int  # entity key
    side: int
    asset_id_type: str
    asset_id: str
    order_ts: datetime
    target_wgt: Optional[Decimal] = None
    real_wgt: Optional[Decimal] = None
    quantity: Optional[Decimal] = None
    notional: Optional[Decimal] = None

    @classmethod
    def from_source(cls, record: Record) -> "Orders":
        """Creates object from source_model record."""
        res = cls()
        res.event_type = record[0]
        res.portfolio_id = record[1]
        res.side = record[2] if record[2] else None
        res.asset_id_type = record[3] if record[3] else None
        res.asset_id = record[4] if record[4] else None
        res.order_ts = record[5] if record[5] else None
        res.target_wgt = record[6] if record[6] else None
        res.real_wgt = record[7] if record[7] else None
        res.quantity = record[8] if record[8] else None
        res.notional = record[9] if record[9] else None

        return res

    @property
    def key(self) -> Key:
        """Get object key."""
        return (self.portfolio_id, self.asset_id, self.order_ts)

    def __repr__(self) -> str:
        return (
            f"({self.event_type}, "
            f"{self.portfolio_id}, "
            f"{self.side}, "
            f"{self.asset_id_type}, "
            f"{self.asset_id}, "
            f"{self.order_ts}, "
            f"{self.target_wgt}, "
            f"{self.real_wgt}, "
            f"{self.quantity}, "
            f"{self.notional})"
        )
