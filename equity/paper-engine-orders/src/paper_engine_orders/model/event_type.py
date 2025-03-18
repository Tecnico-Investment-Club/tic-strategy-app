"""Event Type data model."""

from enum import Enum


class EventType(str, Enum):
    """Type of log event."""

    CREATE = "CREATE"
    AMEND = "AMEND"
    REMOVE = "REMOVE"
    POINTLESS_AMEND = "POINTLESS_AMEND"
    RECREATE = "RECREATE"

    def __repr__(self) -> str:
        return str(self.value)
