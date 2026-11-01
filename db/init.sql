
-- TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ─────────────────────────────────────────────────────────────────────────────
-- Raw events hypertable
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sensor_events (
    time         TIMESTAMPTZ      NOT NULL,
    sensor_id    TEXT             NOT NULL,
    sensor_type  TEXT             NOT NULL,
    location     TEXT             NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    unit         TEXT             NOT NULL,
    quality      SMALLINT         NOT NULL DEFAULT 100
);

SELECT create_hypertable('sensor_events', 'time', if_not_exists => TRUE);

-- (sensor_id, time DESC) powers both the per-sensor history queries and the
-- "latest reading per sensor" lookups via TimescaleDB SkipScan — no separate
-- sensor_latest table required.
CREATE INDEX IF NOT EXISTS idx_sensor_events_sensor_id ON sensor_events (sensor_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_events_type      ON sensor_events (sensor_type, time DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- Continuous aggregate: 1-minute buckets
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_stats_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time)  AS bucket,
    sensor_id,
    sensor_type,
    location,
    unit,
    AVG(value)                     AS avg_value,
    MIN(value)                     AS min_value,
    MAX(value)                     AS max_value,
    STDDEV(value)                  AS stddev_value,
    COUNT(*)                       AS event_count
FROM sensor_events
GROUP BY bucket, sensor_id, sensor_type, location, unit
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_stats_1min',
    start_offset      => INTERVAL '10 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Continuous aggregate: 1-hour buckets (hierarchical, built on 1-min CA)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_stats_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket)  AS bucket,
    sensor_id,
    sensor_type,
    location,
    unit,
    AVG(avg_value)                 AS avg_value,
    MIN(min_value)                 AS min_value,
    MAX(max_value)                 AS max_value,
    SUM(event_count)               AS event_count
FROM sensor_stats_1min
GROUP BY time_bucket('1 hour', bucket), sensor_id, sensor_type, location, unit
WITH NO DATA;

-- Window (start - end) must span at least two 1-hour buckets, so start_offset
-- is 3 hours against a 1-hour end_offset (a 2-hour, two-bucket window).
SELECT add_continuous_aggregate_policy('sensor_stats_1hour',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Latest reading per sensor — computed on read, not maintained as a table.
-- SELECT DISTINCT ON (sensor_id) over the hypertable uses the
-- (sensor_id, time DESC) index + TimescaleDB SkipScan, so there is no hot
-- write path and no table to keep in sync.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW sensor_latest AS
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    sensor_type,
    location,
    time AS last_seen,
    value,
    unit,
    quality
FROM sensor_events
ORDER BY sensor_id, time DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- Compression (chunks older than 7 days)
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE sensor_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id,sensor_type',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('sensor_events',
    compress_after => INTERVAL '7 days',
    if_not_exists  => TRUE
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Retention (drop raw data older than 90 days)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT add_retention_policy('sensor_events',
    drop_after    => INTERVAL '90 days',
    if_not_exists => TRUE
);
