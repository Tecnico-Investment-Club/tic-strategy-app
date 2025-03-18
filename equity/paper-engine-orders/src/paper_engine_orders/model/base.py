"""Base data model."""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from paper_engine_orders._types import Key, Keys, Message, Record
from paper_engine_orders.model.event_type import EventType


class State(ABC):
    """Base state."""

    event_id: Optional[int] = None
    delivery_id: Optional[int] = None

    @property
    @abstractmethod
    def hash(self) -> str:
        """Object sha256 hash value."""

    @property
    @abstractmethod
    def key(self) -> Key:
        """Object key."""

    @classmethod
    @abstractmethod
    def from_source(cls, record: Record) -> "State":
        """Creates object from source record."""

    @classmethod
    @abstractmethod
    def from_target(cls, record: Tuple) -> "State":
        """Creates object from target record."""

    @classmethod
    @abstractmethod
    def removal_instance(cls, event_id: int, delivery_id: int, key: Key) -> "State":
        """Creates an empty object instance (for removal event logs)."""

    @staticmethod
    @abstractmethod
    def list_ids_from_source(records: List[Record]) -> Keys:
        """Creates a list with all entity keys from source file."""

    @abstractmethod
    def as_tuple(self) -> Tuple:
        """Returns object values as a tuple."""


class EventLog(ABC):
    """Base event log."""

    event_type: EventType
    curr: State
    prev: Optional[State] = None

    @property
    @abstractmethod
    def mask(self) -> str:
        """Event log record change mask."""

    def as_record(self) -> Tuple:
        """Returns object values as target record."""
        curr = self.curr.as_tuple()
        prev = (None,) * len(curr) if self.prev is None else self.prev.as_tuple()

        return (self.event_type.value,) + curr + prev + (self.mask,)

    @classmethod
    def from_states(
        cls, event_type: EventType, curr: State, prev: Optional[State]
    ) -> "EventLog":
        """Creates an event log object instance."""
        res = cls()
        res.event_type = event_type
        res.curr = curr
        res.prev = prev

        return res

    @property
    @abstractmethod
    def topic(self) -> str:
        """Returns object topic to publish."""

    @property
    @abstractmethod
    def message(self) -> Message:
        """Returns object message to publish."""
