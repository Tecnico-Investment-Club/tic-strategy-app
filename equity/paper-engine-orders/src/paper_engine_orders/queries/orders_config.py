"""Orders Config queries."""

from paper_engine_orders.queries.base import BaseQueries


class Queries(BaseQueries):
    """Orders Config queries."""

    LOAD_STATE = "SELECT * FROM orders_config WHERE (portfolio_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO orders_config ("
        "    portfolio_id, "
        "    strategy_id, "
        "    portfolio_type,"
        "    rebal_freq,"
        "    adjust,"
        "    wgt_method,"
        "    portfolio_hash,"
        "    account_id,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (portfolio_id) DO "
        "UPDATE SET "
        "    portfolio_id=EXCLUDED.portfolio_id,"
        "    strategy_id=EXCLUDED.strategy_id,"
        "    portfolio_type=EXCLUDED.portfolio_type,"
        "    rebal_freq=EXCLUDED.rebal_freq,"
        "    adjust=EXCLUDED.adjust,"
        "    wgt_method=EXCLUDED.wgt_method,"
        "    portfolio_hash=EXCLUDED.portfolio_hash,"
        "    account_id=EXCLUDED.account_id,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM orders_config WHERE portfolio_id IN (VALUES %s);"
