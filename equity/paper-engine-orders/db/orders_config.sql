CREATE TABLE orders_config
(
    portfolio_id                            BIGINT,
    strategy_id                             BIGINT,
    portfolio_type                          VARCHAR(50),
    rebal_freq                              VARCHAR(50),
    adjust                                  BOOLEAN,
    wgt_method                              VARCHAR(50),
    portfolio_hash                          VARCHAR,
    account_id                              VARCHAR,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id),
    UNIQUE (portfolio_hash)
);
