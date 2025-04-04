CREATE TABLE portfolio_latest
(
    portfolio_id                            BIGINT,
    portfolio_ts                            TIMESTAMP,
    long_notional                           DECIMAL(24,4),
    short_notional                          DECIMAL(24,4),
    notional                                DECIMAL(24,4),
    long_wgt                                DECIMAL(10,4),
    short_wgt                               DECIMAL(10,4),
    long_rtn                                DECIMAL(24,4),
    long_cum_rtn                            DECIMAL(24,4),
    short_rtn                               DECIMAL(24,4),
    short_cum_rtn                           DECIMAL(24,4),
    rtn                                     DECIMAL(24,4),
    cum_rtn                                 DECIMAL(24,4),

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id)
);
