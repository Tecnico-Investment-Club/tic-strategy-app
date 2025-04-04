"""Entity data model."""

from enum import Enum


class Entity(str, Enum):
    """Type of Entity."""

    PORTFOLIO = "portfolio"
    PORTFOLIO_LATEST = "portfolio_latest"
    PORTFOLIO_CONTROL = "portfolio_control"
    POSITION = "position"
    POSITION_LATEST = "position_latest"

    def __repr__(self) -> str:
        return str(self.value)
