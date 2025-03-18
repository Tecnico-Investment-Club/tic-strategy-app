CREATE TABLE orders_control
(
    portfolio_id                            BIGINT,
    last_read_delivery_id                   BIGINT,
    last_decision_datadate                  TIMESTAMP,
    last_rebal_ts                           TIMESTAMP,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE REFERENCES orders_control_event_log(event_id),
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(portfolio_id)
);
