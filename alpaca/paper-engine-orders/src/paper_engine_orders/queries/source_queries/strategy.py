"""Strategy Latest queries."""

from paper_engine_orders.queries.source_queries.base import BaseSourceQueries


class Queries(BaseSourceQueries):
    """SOURCE strategy queries."""

    LOAD_LATEST_DELIVERY_METADATA = (
        "SELECT strategy_id, MAX(delivery_id), MAX(datadate) "
        "FROM strategy_latest "
        "GROUP BY strategy_id;"
    )

    LOAD_LATEST = (
        "SELECT strategy_id, "
        "       asset_id_type,"
        "       asset_id,"
        "       datadate,"
        "       decision_ts,"
        "       weight,"
        "       decision "
        "FROM paper_engine.strategy_latest "
        "WHERE strategy_id = %s;"
    )
