CREATE TABLE portfolio_control
(
    portfolio_id                            BIGINT,
    last_monitor_ts                         TIMESTAMP,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id)
);
