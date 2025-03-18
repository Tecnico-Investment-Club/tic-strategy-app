CREATE TABLE loader_orders (
    delivery_id                             BIGINT,
    last_read_delivery                      BIGINT NOT NULL,
    row_creation                            TIMESTAMP NOT NULL,
    summary                                 JSONB,
    runtime                                 INTERVAL,

    PRIMARY KEY(delivery_id)
);
CREATE INDEX idx_loader_orders_row_creation ON loader_orders(row_creation);
