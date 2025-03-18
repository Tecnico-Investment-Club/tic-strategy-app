CREATE TABLE orders_latest
(
    portfolio_id                            BIGINT,
    side                                    VARCHAR(20),
    asset_id_type                           VARCHAR(20),
    asset_id                                VARCHAR(20),
    order_ts                                TIMESTAMP,
    target_wgt                              DECIMAL(10,4),
    real_wgt                                DECIMAL(10,4),
    quantity                                DECIMAL(10,4),
    notional                                DECIMAL(24,4),

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE REFERENCES orders_latest_event_log(event_id),
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id, asset_id)
);
