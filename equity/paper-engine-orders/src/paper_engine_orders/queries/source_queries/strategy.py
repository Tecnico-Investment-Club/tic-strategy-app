"""Strategy Latest queries."""

from paper_engine_orders.queries.source_queries.base import BaseSourceQueries


class Queries(BaseSourceQueries):
    """SOURCE strategy queries."""

    LOAD_STRATEGY_LATEST = (
        "SELECT strategy_id, MAX(delivery_id), MAX(datadate) "
        "FROM paper_engine.strategy_latest "
        "GROUP BY strategy_id;"
    )

    LOAD_LATEST_EVENTS = (
        "SELECT event_type, "
        "       curr_strategy_id,"
        "       curr_asset_id_type,"
        "       curr_asset_id,"
        "       curr_datadate,"
        "       curr_decision_ts,"
        "       curr_factor,"
        "       curr_decision "
        "FROM paper_engine.strategy_latest_event_log "
        "WHERE curr_strategy_id = %s "
        "AND delivery_id = %s;"
    )
