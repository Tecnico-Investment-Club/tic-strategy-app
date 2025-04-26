"""Strategy queries."""

from paper_engine_strategy.queries.base import BaseQueries


class Queries(BaseQueries):
    """Strategy queries."""

    LOAD_STATE = "SELECT * FROM portfolio_optimization WHERE (strategy_id, asset_id_type, asset_id, datadate) IN (VALUES %s);"  # noqa: B950
    UPSERT = (
        "INSERT INTO portfolio_optimization ("
        "    strategy_id, "
        "    asset_id_type, "
        "    asset_id,"
        "    datadate,"
        "    decision_ts,"
        "    factor,"
        "    decision,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (strategy_id, asset_id_type, asset_id, datadate) DO "
        "UPDATE SET "
        "    strategy_id=EXCLUDED.strategy_id,"
        "    asset_id_type=EXCLUDED.asset_id_type,"
        "    asset_id=EXCLUDED.asset_id,"
        "    datadate=EXCLUDED.datadate,"
        "    decision_ts=EXCLUDED.decision_ts,"
        "    factor=EXCLUDED.factor,"
        "    decision=EXCLUDED.decision,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM portfolio_optimization WHERE (strategy_id, asset_id_type, asset_id, datadate) IN (VALUES %s);"  # noqa: B950
