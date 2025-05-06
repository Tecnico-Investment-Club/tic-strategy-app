"""SOURCE data model."""

from abc import ABC, abstractmethod

from paper_engine_strategy._types import Key, Record


class SourceState(ABC):
    """Base state."""

    name: str

    @classmethod
    @abstractmethod
    def from_source(cls, record: Record) -> "SourceState":
        """Creates object from source_model record."""

    @property
    @abstractmethod
    def key(self) -> Key:
        """Get object key."""
