CREATE TABLE strategy_config
(
    strategy_id                             BIGINT,
    strategy_name                           VARCHAR(50),
    strategy_type                           VARCHAR(50),
    alpha                                   VARCHAR(50),
    factor                                  JSONB,
    signal_lifetime                         VARCHAR(20),
    top_threshold                           INTEGER,
    top_threshold_type                      VARCHAR(20),
    bottom_threshold                        INTEGER,
    bottom_threshold_type                   VARCHAR(20),
    strategy_hash                           VARCHAR,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(strategy_id),
    UNIQUE (strategy_hash)
);
