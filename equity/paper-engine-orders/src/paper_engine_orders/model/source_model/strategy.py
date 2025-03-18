"""SOURCE Strategy data model."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Optional

from paper_engine_orders._types import Key, Record
from paper_engine_orders.model.source_model.base import SourceState

logger = logging.getLogger(__name__)


class Strategy(SourceState):
    """Strategy state."""

    name: str = "STRATEGY LATEST"

    event_type: str
    strategy_id: int  # entity key
    asset_id_type: str
    asset_id: str
    datadate: datetime
    decision_ts: Optional[datetime] = None
    factor: Optional[Decimal] = None
    decision: Optional[int] = None

    @classmethod
    def from_source(cls, record: Record) -> "Strategy":
        """Creates object from source_model record."""
        res = cls()
        res.event_type = record[0]
        res.strategy_id = record[1]
        res.asset_id_type = record[2]
        if res.asset_id_type == "TICKER":
            res.asset_id = record[3].partition(".")[0]
        else:
            res.asset_id = record[3] if record[3] else None
        res.datadate = record[4] if record[4] else None
        res.decision_ts = record[5] if record[5] else None
        res.factor = record[6] if record[6] else None
        res.decision = record[7] if record[7] else None

        return res

    @property
    def key(self) -> Key:
        """Get object key."""
        return (self.strategy_id, self.asset_id)

    def __repr__(self) -> str:
        return (
            f"({self.event_type}, "
            f"{self.strategy_id}, "
            f"{self.asset_id_type}, "
            f"{self.asset_id}, "
            f"{self.datadate}, "
            f"{self.decision_ts}, "
            f"{self.factor}, "
            f"{self.decision})"
        )
