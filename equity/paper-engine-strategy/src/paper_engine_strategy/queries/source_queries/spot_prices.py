"""SOURCE Spot Klines queries."""

from paper_engine_strategy.queries.source_queries.base import BaseSourceQueries


class Queries(BaseSourceQueries):
    """SOURCE Spot Klines queries."""
    LOAD_LATEST = (
        "SELECT MAX(open_time) "
        "FROM {schema}.spot_{interval}; "
    )

    LOAD_RECORDS = (
        "SELECT id, "
        "       symbol, "
        "       open_time, "
        "       open_price, "
        "       high_price, "
        "       low_price, "
        "       close_price "
        "FROM {schema}.spot_{interval} "
        "WHERE datadate > %s; "
    )
