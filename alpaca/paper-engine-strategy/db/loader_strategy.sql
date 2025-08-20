CREATE TABLE IF NOT EXISTS paper_engine.loader_strategy (
    delivery_id                             BIGINT,
    delivery_ts                             TIMESTAMP NOT NULL,
    runtime                                 INTERVAL,

    PRIMARY KEY(delivery_id)
);
CREATE INDEX idx_loader_strategy_delivery_ts ON loader_strategy(delivery_ts);
