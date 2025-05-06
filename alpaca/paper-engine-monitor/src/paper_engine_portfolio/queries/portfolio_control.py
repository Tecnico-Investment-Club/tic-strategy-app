"""Portfolio Control queries."""

from paper_engine_portfolio.queries.base import BaseQueries


class Queries(BaseQueries):
    """Portfolio Control queries."""

    LOAD_STATE = "SELECT * FROM portfolio_control WHERE (portfolio_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO portfolio_control ("
        "    portfolio_id, "
        "    last_monitor_ts, "
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (portfolio_id) DO "
        "UPDATE SET "
        "    portfolio_id=EXCLUDED.portfolio_id,"
        "    last_monitor_ts=EXCLUDED.last_monitor_ts,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM portfolio_control WHERE portfolio_id IN (VALUES %s);"

