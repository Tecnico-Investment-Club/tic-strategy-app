CREATE TABLE position
(
    portfolio_id                            BIGINT,
    side                                    VARCHAR(20),
    asset_id_type                           VARCHAR(20),
    asset_id                                VARCHAR(20),
    position_ts                             TIMESTAMP,
    wgt                                     DECIMAL(10,4),
    quantity                                DECIMAL(24,4),
    notional                                DECIMAL(24,4),

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id, asset_id, position_ts)
);
