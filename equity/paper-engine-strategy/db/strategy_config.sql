CREATE TABLE strategy_config
(
    strategy_id                             BIGINT,
    strategy_type                           VARCHAR,
    strategy_config                         JSONB,
    strategy_hash                           VARCHAR,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(strategy_id),
    UNIQUE (strategy_hash)
);
