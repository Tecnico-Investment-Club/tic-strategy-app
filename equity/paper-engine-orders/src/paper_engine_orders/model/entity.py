"""Entity data model."""

from enum import Enum


class Entity(str, Enum):
    """Type of Entity."""

    ORDERS = "orders"
    ORDERS_LATEST = "orders_latest"
    ORDERS_CONTROL = "orders_control"
    ORDERS_CONFIG = "orders_config"

    def __repr__(self) -> str:
        return str(self.value)
