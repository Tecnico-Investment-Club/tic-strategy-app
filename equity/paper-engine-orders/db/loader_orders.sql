CREATE TABLE loader_orders (
    delivery_id                             BIGINT,
    delivery_ts                             TIMESTAMP NOT NULL,
    runtime                                 INTERVAL,

    PRIMARY KEY(delivery_id)
);
CREATE INDEX idx_loader_orders_row_creation ON loader_orders(row_creation);
