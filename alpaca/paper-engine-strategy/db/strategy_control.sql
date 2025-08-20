CREATE TABLE IF NOT EXISTS strategy_control
(
    strategy_id                             BIGINT,
    last_decision_ts                        TIMESTAMP,

    hash                                    VARCHAR NOT NULL,

    event_id                                BIGINT NOT NULL UNIQUE,
    delivery_id                             BIGINT NOT NULL,

    PRIMARY KEY(strategy_id)
);
