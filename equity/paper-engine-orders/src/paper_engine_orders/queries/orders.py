"""Orders queries."""

from paper_engine_orders.queries.base import BaseQueries


class Queries(BaseQueries):
    """Orders queries."""

    LOAD_STATE = (
        "SELECT * FROM orders WHERE (portfolio_id, asset_id, order_ts) IN (VALUES %s);"
    )
    UPSERT = (
        "INSERT INTO orders ("
        "    portfolio_id, "
        "    side, "
        "    asset_id_type, "
        "    asset_id,"
        "    order_ts,"
        "    target_wgt,"
        "    real_wgt,"
        "    quantity,"
        "    notional,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (portfolio_id, asset_id, order_ts) DO "
        "UPDATE SET "
        "    portfolio_id=EXCLUDED.portfolio_id,"
        "    side=EXCLUDED.side,"
        "    asset_id_type=EXCLUDED.asset_id_type,"
        "    asset_id=EXCLUDED.asset_id,"
        "    order_ts=EXCLUDED.order_ts,"
        "    target_wgt=EXCLUDED.target_wgt,"
        "    real_wgt=EXCLUDED.real_wgt,"
        "    quantity=EXCLUDED.quantity,"
        "    notional=EXCLUDED.notional,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = (
        "DELETE FROM orders WHERE (portfolio_id, asset_id, order_ts) IN (VALUES %s);"
    )
    APPEND_LOG = (
        "INSERT INTO orders_event_log ("
        "    event_type,"
        "    curr_portfolio_id, "
        "    curr_side, "
        "    curr_asset_id_type, "
        "    curr_asset_id, "
        "    curr_order_ts, "
        "    curr_target_wgt, "
        "    curr_real_wgt, "
        "    curr_quantity, "
        "    curr_notional, "
        "    curr_hash, event_id, delivery_id,"
        "    prev_portfolio_id, "
        "    prev_side, "
        "    prev_asset_id_type, "
        "    prev_asset_id, "
        "    prev_order_ts, "
        "    prev_target_wgt, "
        "    prev_real_wgt, "
        "    prev_quantity, "
        "    prev_notional, "
        "    prev_hash, prev_event_id, prev_delivery_id,"
        "    mask"
        ") VALUES %s;"
    )
