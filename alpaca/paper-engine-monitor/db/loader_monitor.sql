CREATE TABLE IF NOT EXISTS paper_engine.loader_monitor (
    delivery_id                             BIGINT,
    delivery_ts                             TIMESTAMP NOT NULL,
    runtime                                 INTERVAL,

    PRIMARY KEY(delivery_id)
);
CREATE INDEX IF NOT EXISTS idx_loader_monitor_delivery_ts ON loader_monitor(delivery_ts);
