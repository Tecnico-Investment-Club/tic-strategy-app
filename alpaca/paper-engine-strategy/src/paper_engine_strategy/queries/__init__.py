"""Queries implementation."""

from .strategy import Queries as StrategyQueries
from .strategy_config import Queries as StrategyConfigQueries
from .strategy_control import Queries as StrategyControlQueries
from .strategy_latest import Queries as StrategyLatestQueries

__all__ = [
    "StrategyQueries",
    "StrategyControlQueries",
    "StrategyConfigQueries",
    "StrategyLatestQueries",
]
