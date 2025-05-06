"""Strategy Control queries."""

from paper_engine_strategy.queries.base import BaseQueries


class Queries(BaseQueries):
    """Strategy Control queries."""

    LOAD_STATE = "SELECT * FROM paper_engine.strategy_control WHERE (strategy_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO paper_engine.strategy_control ("
        "    strategy_id, "
        "    last_decision_ts, "
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (strategy_id) DO "
        "UPDATE SET "
        "    strategy_id=EXCLUDED.strategy_id,"
        "    last_decision_ts=EXCLUDED.last_decision_ts,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM paper_engine.strategy_control WHERE (strategy_id) IN (VALUES %s);"
