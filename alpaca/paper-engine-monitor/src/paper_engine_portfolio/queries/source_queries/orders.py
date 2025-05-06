"""Orders Latest queries."""

from paper_engine_portfolio.queries.source_queries.base import BaseSourceQueries


class Queries(BaseSourceQueries):
    """SOURCE orders latest queries."""

    LOAD_INITIAL_PORTFOLIO_VALUES = (
        "SELECT COALESCE(SUM(notional) FILTER (WHERE side = '1'), 0) - COALESCE(SUM(notional) FILTER (WHERE side = '-1'), 0), "
        "       COALESCE(SUM(notional) FILTER (WHERE side = '1'), 0), "
        "       COALESCE(SUM(notional) FILTER (WHERE side = '-1'), 0) "
        "FROM orders "
        "WHERE portfolio_id = %(portfolio_id)s "
        "AND order_ts = ( "
        "    SELECT min(order_ts) "
        "    FROM orders "
        "    WHERE portfolio_id = %(portfolio_id)s"
        "    )"
    )

    LOAD_LATEST_DELIVERY_METADATA = (
        "SELECT portfolio_id, MAX(delivery_id) "
        "FROM orders_latest "
        "GROUP BY portfolio_id;"
    )

    LOAD_LATEST_EVENTS = (
        "SELECT event_type, "
        "       curr_portfolio_id,"
        "       curr_side,"
        "       curr_asset_id_type,"
        "       curr_asset_id,"
        "       curr_order_ts,"
        "       curr_target_wgt,"
        "       curr_real_wgt,"
        "       curr_quantity,"
        "       curr_notional "
        "FROM orders_latest_event_log "
        "WHERE portfolio_id = %s "
        "AND delivery_id = %s;"
    )

    LOAD_PORTFOLIO_ID = (
        "SELECT portfolio_id " "FROM orders_config " "WHERE account_id = %s;"
    )
