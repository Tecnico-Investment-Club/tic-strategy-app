"""Strategy Control queries."""

from paper_engine_strategy.queries.base import BaseQueries


class Queries(BaseQueries):
    """Strategy Control queries."""

    LOAD_STATE = "SELECT * FROM strategy_control WHERE (strategy_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO strategy_control ("
        "    strategy_id, "
        "    last_decision_date, "
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (strategy_id) DO "
        "UPDATE SET "
        "    strategy_id=EXCLUDED.strategy_id,"
        "    last_decision_date=EXCLUDED.last_decision_date,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM strategy_control WHERE (strategy_id) IN (VALUES %s);"
