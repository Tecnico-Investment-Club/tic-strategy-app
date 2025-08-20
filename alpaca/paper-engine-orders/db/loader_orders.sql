CREATE TABLE IF NOT EXISTS loader_orders (
    delivery_id                             BIGINT,
    delivery_ts                             TIMESTAMP NOT NULL,
    runtime                                 INTERVAL,

    PRIMARY KEY(delivery_id)
);
CREATE INDEX idx_loader_orders_delivery_ts ON loader_orders(delivery_ts);
