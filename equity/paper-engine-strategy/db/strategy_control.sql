CREATE TABLE strategy_control
(
    strategy_id                             BIGINT,
    last_decision_date                      TIMESTAMP,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(strategy_id)
);
