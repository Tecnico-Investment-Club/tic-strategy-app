"""Strategy Config queries."""

from paper_engine_strategy.queries.base import BaseQueries


class Queries(BaseQueries):
    """Strategy Config queries."""

    LOAD_STATE = "SELECT * FROM paper_engine.strategy_config WHERE (strategy_id) IN (VALUES %s);"
    UPSERT = (
        "INSERT INTO paper_engine.strategy_config ("
        "    strategy_id, "
        "    strategy_type, "
        "    asset_type, "
        "    interval, "
        "    lookback, "
        "    strategy_config, "
        "    strategy_hash,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (strategy_id) DO "
        "UPDATE SET "
        "    strategy_id=EXCLUDED.strategy_id,"
        "    strategy_type=EXCLUDED.strategy_type,"
        "    asset_type=EXCLUDED.asset_type,"
        "    interval=EXCLUDED.interval,"
        "    lookback=EXCLUDED.lookback,"
        "    strategy_config=EXCLUDED.strategy_config,"
        "    strategy_hash=EXCLUDED.strategy_hash,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM paper_engine.strategy_config WHERE strategy_id IN (VALUES %s);"
