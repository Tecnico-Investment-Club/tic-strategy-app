"""Portfolio Latest queries."""

from paper_engine_monitor.queries.base import BaseQueries


class Queries(BaseQueries):
    """Portfolio Latest queries."""

    LOAD_STATE = "SELECT * FROM portfolio_latest WHERE (portfolio_id) IN (VALUES %s);"

    LOAD_FULL_STATE = (
        "SELECT * FROM portfolio_latest;"
    )

    UPSERT = (
        "INSERT INTO portfolio_latest ("
        "    portfolio_id, "
        "    portfolio_ts, "
        "    long_notional,"
        "    short_notional,"
        "    notional,"
        "    long_wgt,"
        "    short_wgt,"
        "    long_rtn,"
        "    long_cum_rtn,"
        "    short_rtn,"
        "    short_cum_rtn,"
        "    rtn,"
        "    cum_rtn,"
        "    hash, event_id, delivery_id"
        ") VALUES %s "
        "ON CONFLICT (portfolio_id) DO "
        "UPDATE SET "
        "    portfolio_id=EXCLUDED.portfolio_id,"
        "    portfolio_ts=EXCLUDED.portfolio_ts,"
        "    long_notional=EXCLUDED.long_notional,"
        "    short_notional=EXCLUDED.short_notional,"
        "    notional=EXCLUDED.notional,"
        "    long_wgt=EXCLUDED.long_wgt,"
        "    short_wgt=EXCLUDED.short_wgt,"
        "    long_rtn=EXCLUDED.long_rtn,"
        "    long_cum_rtn=EXCLUDED.long_cum_rtn,"
        "    short_rtn=EXCLUDED.short_rtn,"
        "    short_cum_rtn=EXCLUDED.short_cum_rtn,"
        "    rtn=EXCLUDED.rtn,"
        "    cum_rtn=EXCLUDED.cum_rtn,"
        "    hash=EXCLUDED.hash,"
        "    event_id=EXCLUDED.event_id,"
        "    delivery_id=EXCLUDED.delivery_id;"
    )
    DELETE = "DELETE FROM portfolio_latest WHERE (portfolio_id) IN (VALUES %s);"
