"""Position queries."""

from paper_engine_monitor.queries.base import BaseQueries


class Queries(BaseQueries):
    """Position queries."""

    LOAD_STATE = "SELECT * FROM position WHERE (portfolio_id, asset_id, position_ts) IN (VALUES %s);"  # noqa: B950
    UPSERT = (
        "INSERT INTO position ("
        "    portfolio_id, "
        "    side, "
        "    asset_id_type, "
        "    asset_id,"
        "    position_ts,"
        "    wgt,"
        "    quantity,"
        "    notional,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (portfolio_id, asset_id, position_ts) DO "
        "UPDATE SET "
        "    portfolio_id=EXCLUDED.portfolio_id,"
        "    side=EXCLUDED.side,"
        "    asset_id_type=EXCLUDED.asset_id_type,"
        "    asset_id=EXCLUDED.asset_id,"
        "    position_ts=EXCLUDED.position_ts,"
        "    wgt=EXCLUDED.wgt,"
        "    quantity=EXCLUDED.quantity,"
        "    notional=EXCLUDED.notional,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM position WHERE (portfolio_id, asset_id, position_ts) IN (VALUES %s);"  # noqa: B950
