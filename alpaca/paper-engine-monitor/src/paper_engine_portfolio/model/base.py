"""Base data model."""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from paper_engine_portfolio._types import Key, Keys, Record


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
