CREATE TABLE loader_monitor (
    delivery_id                             BIGINT,
    delivery_ts                             TIMESTAMP NOT NULL,
    runtime                                 INTERVAL,

    PRIMARY KEY(delivery_id)
);
CREATE INDEX idx_loader_portfolio_row_creation ON loader_portfolio(row_creation);
