"""Orders Control queries."""

from paper_engine_orders.queries.base import BaseQueries


class Queries(BaseQueries):
    """Orders Control queries."""

    LOAD_STATE = "SELECT * FROM orders_control WHERE (portfolio_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO orders_control ("
        "    portfolio_id, "
        "    last_read_delivery_id, "
        "    last_decision_datadate, "
        "    last_rebal_ts,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (portfolio_id) DO "
        "UPDATE SET "
        "    portfolio_id=EXCLUDED.portfolio_id,"
        "    last_read_delivery_id=EXCLUDED.last_read_delivery_id,"
        "    last_decision_datadate=EXCLUDED.last_decision_datadate,"
        "    last_rebal_ts=EXCLUDED.last_rebal_ts,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM orders_control WHERE portfolio_id IN (VALUES %s);"
