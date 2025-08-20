CREATE TABLE IF NOT EXISTS orders_config
(
    portfolio_id                            BIGINT,
    strategy_id                             BIGINT,
    portfolio_name                          VARCHAR NOT NULL UNIQUE,
    account_id                              VARCHAR,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id)
);
