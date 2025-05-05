"""Strategy Latest queries."""

from paper_engine_strategy.queries.base import BaseQueries


class Queries(BaseQueries):
    """Strategy Latest queries."""

    LOAD_STATE = "SELECT * FROM paper_engine.strategy_latest  WHERE (strategy_id, asset_id_type, asset_id) IN (VALUES %s);"  # noqa: B950
    LOAD_FULL_STATE = "SELECT * FROM paper_engine.strategy_latest;"  # noqa: B950
    UPSERT = (
        "INSERT INTO paper_engine.strategy_latest ("
        "    strategy_id, "
        "    asset_id_type, "
        "    asset_id,"
        "    datadate,"
        "    decision_ts,"
        "    weight,"
        "    decision,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (strategy_id, asset_id_type, asset_id) DO "
        "UPDATE SET "
        "    strategy_id=EXCLUDED.strategy_id,"
        "    asset_id_type=EXCLUDED.asset_id_type,"
        "    asset_id=EXCLUDED.asset_id,"
        "    datadate=EXCLUDED.datadate,"
        "    decision_ts=EXCLUDED.decision_ts,"
        "    weight=EXCLUDED.weight,"
        "    decision=EXCLUDED.decision,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM paper_engine.strategy_latest WHERE (strategy_id, asset_id_type, asset_id) IN (VALUES %s);"  # noqa: B950
