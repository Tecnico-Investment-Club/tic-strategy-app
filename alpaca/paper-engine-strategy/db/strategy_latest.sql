CREATE TABLE IF NOT EXISTS paper_engine.strategy_latest
(
    strategy_id                             BIGINT,
    asset_id_type                           VARCHAR(20),
    asset_id                                VARCHAR(20),
    datadate                                TIMESTAMP,
    decision_ts                             TIMESTAMP,
    weight                                  DECIMAL(24,4),
    decision                                INTEGER,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(strategy_id, asset_id_type, asset_id)
);
