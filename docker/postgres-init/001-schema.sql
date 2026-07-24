CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS backend_identity (
    id SMALLINT PRIMARY KEY,
    marker TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_metrics (
    observed_at TIMESTAMPTZ NOT NULL,
    sensor_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
    'sensor_metrics',
    by_range('observed_at'),
    if_not_exists => TRUE
);
