"""Strategy Config queries."""

from paper_engine_strategy.queries.base import BaseQueries


class Queries(BaseQueries):
    """Strategy Config queries."""

    LOAD_STATE = "SELECT * FROM strategy_config WHERE (strategy_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO strategy_config ("
        "    strategy_id, "
        "    strategy_name, "
        "    strategy_type, "
        "    alpha, "
        "    factor, "
        "    signal_lifetime, "
        "    top_threshold,"
        "    top_threshold_type,"
        "    bottom_threshold,"
        "    bottom_threshold_type,"
        "    strategy_hash,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (strategy_id) DO "
        "UPDATE SET "
        "    strategy_id=EXCLUDED.strategy_id,"
        "    strategy_name=EXCLUDED.strategy_name,"
        "    strategy_type=EXCLUDED.strategy_type,"
        "    alpha=EXCLUDED.alpha,"
        "    factor=EXCLUDED.factor,"
        "    signal_lifetime=EXCLUDED.signal_lifetime,"
        "    top_threshold=EXCLUDED.top_threshold,"
        "    top_threshold_type=EXCLUDED.top_threshold_type,"
        "    bottom_threshold=EXCLUDED.bottom_threshold,"
        "    bottom_threshold_type=EXCLUDED.bottom_threshold_type,"
        "    strategy_hash=EXCLUDED.strategy_hash,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM strategy_config WHERE strategy_id IN (VALUES %s);"
