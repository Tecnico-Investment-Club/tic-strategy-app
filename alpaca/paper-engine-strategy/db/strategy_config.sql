CREATE TABLE strategy_config
(
    strategy_id                             BIGINT,
    strategy_type                           VARCHAR,
    asset_type                              VARCHAR,
    interval                                VARCHAR,
    lookback                                INTEGER,
    strategy_config                         JSONB,
    strategy_hash                           VARCHAR,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(strategy_id),
    UNIQUE (strategy_hash)
);
