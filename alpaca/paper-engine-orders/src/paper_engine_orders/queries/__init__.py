"""Queries implementation."""

from .orders import Queries as OrdersQueries
from .orders_config import Queries as OrdersConfigQueries
from .orders_control import Queries as OrdersControlQueries
from .orders_latest import Queries as OrdersLatestQueries

__all__ = [
    "OrdersQueries",
    "OrdersLatestQueries",
    "OrdersConfigQueries",
    "OrdersControlQueries",
]
