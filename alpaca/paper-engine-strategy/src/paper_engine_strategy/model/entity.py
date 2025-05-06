"""Entity data model."""

from enum import Enum


class Entity(str, Enum):
    """Type of Entity."""

    STRATEGY = "portfolio_optimization"
    STRATEGY_CONFIG = "strategy_config"
    STRATEGY_CONTROL = "strategy_control"
    STRATEGY_LATEST = "strategy_latest"

    def __repr__(self) -> str:
        return str(self.value)
