-- =============================================================================
-- schema.sql — Star Schema + operational tables for the NYC Yellow Taxi ETL.
--
-- Idempotent: safe to run many times (setup.sh applies it on every run).
--   * CREATE ... IF NOT EXISTS for every table and index
--   * seed rows use INSERT ... ON CONFLICT
--   * everything runs in one transaction: all or nothing
-- Note: IF NOT EXISTS does not change a table that already exists. During
-- development, use `bash setup.sh --reset-db` to rebuild after editing a table.
--
-- Layout
--   Dimensions : dim_date, dim_time, dim_payment_type, dim_vendor, dim_location
--   Fact       : fact_taxi_trips   (grain: one accepted, de-duplicated trip record)
--   Summaries  : agg_trips_daily_hourly, agg_monthly_fare_stats (derived, rebuildable)
--   Staging    : stg_trips (UNLOGGED, emptied after each publish)
--   Operations : etl_source_files, etl_pipeline_runs, etl_rejected_summary, etl_quality_results
-- =============================================================================

BEGIN;

-- Hide "already exists, skipping" notices on re-runs; warnings and errors still show.
SET LOCAL client_min_messages = warning;

-- -----------------------------------------------------------------------------
-- Operational tables (lineage and monitoring)
-- -----------------------------------------------------------------------------

-- One row per source file ever registered. Its small id is stored on every fact
-- row instead of the file name, which saves space.
CREATE TABLE IF NOT EXISTS etl_source_files (
    source_file_id   SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name        VARCHAR(100) NOT NULL UNIQUE,
    source_month     CHAR(7)      NOT NULL CHECK (source_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    file_size_bytes  BIGINT,
    row_count        BIGINT,
    first_loaded_at  TIMESTAMPTZ,
    last_loaded_at   TIMESTAMPTZ,
    last_run_id      BIGINT       -- run that last published this file (no FK: avoids a circular reference)
);

-- One row per monthly pipeline run, including failed runs.
-- source_file_id is NULL when the run failed before the file could be registered
-- (for example, the file was missing).
CREATE TABLE IF NOT EXISTS etl_pipeline_runs (
    run_id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    prefect_flow_run_id UUID,
    source_month        CHAR(7)      NOT NULL,
    source_file_id      SMALLINT     REFERENCES etl_source_files (source_file_id),
    status              VARCHAR(10)  NOT NULL DEFAULT 'RUNNING'
                                     CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    ended_at            TIMESTAMPTZ,
    duration_seconds    NUMERIC(10, 2),
    rows_read           BIGINT,      -- rows in the source file
    rows_staged         BIGINT,      -- rows that passed rules R1-R12
    rows_deduplicated   BIGINT,      -- extra copies removed by rule D1
    rows_loaded         BIGINT,      -- rows inserted into fact_taxi_trips (= accepted)
    rows_rejected       BIGINT,      -- R1-R12 rejects + rows_deduplicated
    error_step          VARCHAR(50),
    error_message       TEXT
);
CREATE INDEX IF NOT EXISTS ix_etl_pipeline_runs_month ON etl_pipeline_runs (source_month, started_at DESC);

-- Reject counts per run and reason (e.g. distance_not_positive).
CREATE TABLE IF NOT EXISTS etl_rejected_summary (
    run_id     BIGINT      NOT NULL REFERENCES etl_pipeline_runs (run_id) ON DELETE CASCADE,
    reason     VARCHAR(40) NOT NULL,
    row_count  BIGINT      NOT NULL CHECK (row_count >= 0),
    PRIMARY KEY (run_id, reason)
);

-- Results of data quality checks QC1-QC9 per run.
CREATE TABLE IF NOT EXISTS etl_quality_results (
    run_id          BIGINT      NOT NULL REFERENCES etl_pipeline_runs (run_id) ON DELETE CASCADE,
    check_name      VARCHAR(10) NOT NULL,             -- QC1 ... QC9
    severity        VARCHAR(10) NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING')),
    passed          BOOLEAN     NOT NULL,
    observed_value  NUMERIC,
    threshold_value NUMERIC,
    details         TEXT,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);

-- -----------------------------------------------------------------------------
-- Dimension tables
-- -----------------------------------------------------------------------------

-- Calendar: one row per day. Smart key YYYYMMDD (e.g. 20230615).
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER     PRIMARY KEY,
    full_date     DATE        NOT NULL UNIQUE,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month         SMALLINT    NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name    VARCHAR(10) NOT NULL,
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),   -- ISO: 1 = Monday
    day_name      VARCHAR(10) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL
);

-- Covers 2023 (selected dataset) to 2026 (optional demo month) = 1,461 days.
INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                      day_of_month, day_of_week, day_name, is_weekend)
SELECT to_char(d, 'YYYYMMDD')::INTEGER,
       d::DATE,
       EXTRACT(YEAR    FROM d)::SMALLINT,
       EXTRACT(QUARTER FROM d)::SMALLINT,
       EXTRACT(MONTH   FROM d)::SMALLINT,
       to_char(d, 'FMMonth'),
       EXTRACT(DAY     FROM d)::SMALLINT,
       EXTRACT(ISODOW  FROM d)::SMALLINT,
       to_char(d, 'FMDay'),
       EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series(DATE '2023-01-01', DATE '2026-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- Hour of day: 24 rows. Day-part boundaries are a business-friendly choice (assumption A9).
CREATE TABLE IF NOT EXISTS dim_time (
    hour_key    SMALLINT    PRIMARY KEY CHECK (hour_key BETWEEN 0 AND 23),
    hour_label  CHAR(5)     NOT NULL,          -- '17:00'
    day_part    VARCHAR(20) NOT NULL
);

INSERT INTO dim_time (hour_key, hour_label, day_part)
SELECT h,
       lpad(h::TEXT, 2, '0') || ':00',
       CASE WHEN h BETWEEN 0  AND 5  THEN 'Night'
            WHEN h BETWEEN 6  AND 11 THEN 'Morning'
            WHEN h BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN h BETWEEN 17 AND 20 THEN 'Evening'
            ELSE                          'Late evening'
       END
FROM generate_series(0, 23) AS g(h)
ON CONFLICT (hour_key) DO UPDATE
    SET hour_label = EXCLUDED.hour_label,
        day_part   = EXCLUDED.day_part;

-- Payment types from the TLC data dictionary (March 18, 2025).
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key   SMALLINT    PRIMARY KEY,
    payment_type_name  VARCHAR(30) NOT NULL,
    is_app_booked      BOOLEAN     NOT NULL
);

INSERT INTO dim_payment_type (payment_type_key, payment_type_name, is_app_booked) VALUES
    (0, 'Flex Fare trip', TRUE),
    (1, 'Credit card',    FALSE),
    (2, 'Cash',           FALSE),
    (3, 'No charge',      FALSE),
    (4, 'Dispute',        FALSE),
    (5, 'Unknown',        FALSE),
    (6, 'Voided trip',    FALSE)
ON CONFLICT (payment_type_key) DO UPDATE
    SET payment_type_name = EXCLUDED.payment_type_name,
        is_app_booked     = EXCLUDED.is_app_booked;

-- Technology providers (vendors) from the TLC data dictionary (assumption A15:
-- dictionary names are used for all years).
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key   SMALLINT     PRIMARY KEY,
    vendor_name  VARCHAR(60)  NOT NULL
);

INSERT INTO dim_vendor (vendor_key, vendor_name) VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'Curb Mobility, LLC'),
    (6, 'Myle Technologies Inc'),
    (7, 'Helix')
ON CONFLICT (vendor_key) DO UPDATE
    SET vendor_name = EXCLUDED.vendor_name;

-- Taxi zones. Rows are loaded by the pipeline from reference/taxi_zone_lookup.csv
-- (265 zones). Used twice by the fact table: pickup and drop-off (role-playing dimension).
CREATE TABLE IF NOT EXISTS dim_location (
    location_key  SMALLINT     PRIMARY KEY,
    borough       VARCHAR(50)  NOT NULL,
    zone          VARCHAR(100) NOT NULL,
    service_zone  VARCHAR(50)  NOT NULL
);

-- -----------------------------------------------------------------------------
-- Fact table
-- Columns are ordered by storage size (8-byte, 4, 2, 1, variable) to avoid
-- padding bytes in every row. Grain: one accepted, de-duplicated trip record.
--
-- Partitioned by source file (one partition per monthly file, created by the
-- pipeline). Reloading a month TRUNCATEs only its partition inside the publish
-- transaction: no dead rows, no table growth, no VACUUM needed. (Measured in
-- Phase 4: delete-and-reinsert left 35% free space in an unpartitioned table.)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    pickup_datetime        TIMESTAMP     NOT NULL,   -- local NYC time as published (assumption A17)
    source_row_number      INTEGER       NOT NULL,   -- row position in the source file
    pickup_date_key        INTEGER       NOT NULL REFERENCES dim_date (date_key),
    source_file_id         SMALLINT      NOT NULL REFERENCES etl_source_files (source_file_id),
    pickup_hour_key        SMALLINT      NOT NULL REFERENCES dim_time (hour_key),
    pu_location_key        SMALLINT      NOT NULL REFERENCES dim_location (location_key),
    do_location_key        SMALLINT      NOT NULL REFERENCES dim_location (location_key),
    payment_type_key       SMALLINT      NOT NULL REFERENCES dim_payment_type (payment_type_key),
    vendor_key             SMALLINT      NOT NULL REFERENCES dim_vendor (vendor_key),
    rate_code_id           SMALLINT      NOT NULL,   -- NULL in the source becomes 99 (Null/unknown)
    passenger_count        SMALLINT,
    store_and_fwd_flag     BOOLEAN,
    trip_duration_min      NUMERIC(8, 2),            -- NULL when drop-off time = pickup time
    trip_distance          NUMERIC(9, 2)  NOT NULL,
    fare_amount            NUMERIC(10, 2) NOT NULL,
    extra                  NUMERIC(10, 2) NOT NULL,
    mta_tax                NUMERIC(10, 2) NOT NULL,
    tip_amount             NUMERIC(10, 2) NOT NULL,
    tolls_amount           NUMERIC(10, 2) NOT NULL,
    improvement_surcharge  NUMERIC(10, 2) NOT NULL,
    congestion_surcharge   NUMERIC(10, 2),           -- NULL for Flex Fare rows
    airport_fee            NUMERIC(10, 2),           -- NULL for Flex Fare rows
    total_amount           NUMERIC(10, 2) NOT NULL,
    cbd_congestion_fee     NUMERIC(10, 2),           -- 2025+ only; NULL in 2023
    request_source         VARCHAR(10),              -- 2026 only, undocumented; NULL in 2023

    CONSTRAINT pk_fact_taxi_trips PRIMARY KEY (source_file_id, source_row_number),
    CONSTRAINT ck_fact_rate_code     CHECK (rate_code_id IN (1, 2, 3, 4, 5, 6, 99)),
    CONSTRAINT ck_fact_passengers    CHECK (passenger_count IS NULL OR passenger_count >= 0),
    CONSTRAINT ck_fact_duration      CHECK (trip_duration_min IS NULL OR trip_duration_min BETWEEN 0 AND 1440),
    CONSTRAINT ck_fact_distance      CHECK (trip_distance > 0 AND trip_distance <= 100),
    CONSTRAINT ck_fact_fare          CHECK (fare_amount > 0 AND fare_amount <= 1000),
    CONSTRAINT ck_fact_total         CHECK (total_amount > 0)
) PARTITION BY LIST (source_file_id);

-- BRIN index: stores only min/max per block of rows. Tiny, and effective because
-- rows are inserted in date order. Created automatically on every partition.
CREATE INDEX IF NOT EXISTS brin_fact_pickup_date ON fact_taxi_trips USING brin (pickup_date_key);

-- -----------------------------------------------------------------------------
-- Summary tables (derived from fact_taxi_trips; rebuilt per month in the same
-- transaction that publishes the month)
-- -----------------------------------------------------------------------------

-- One row per day x hour x payment type (~61k rows per year). Feeds all dashboard KPIs.
CREATE TABLE IF NOT EXISTS agg_trips_daily_hourly (
    date_key                 INTEGER        NOT NULL REFERENCES dim_date (date_key),
    hour_key                 SMALLINT       NOT NULL REFERENCES dim_time (hour_key),
    payment_type_key         SMALLINT       NOT NULL REFERENCES dim_payment_type (payment_type_key),
    trip_count               BIGINT         NOT NULL,
    sum_fare                 NUMERIC(14, 2) NOT NULL,
    sum_distance             NUMERIC(14, 2) NOT NULL,
    sum_total                NUMERIC(14, 2) NOT NULL,
    sum_tip                  NUMERIC(14, 2) NOT NULL,
    sum_fare_per_mile_ratio  NUMERIC(18, 4) NOT NULL,   -- lets the dashboard compute the average of ratios
    PRIMARY KEY (date_key, hour_key, payment_type_key)
);

-- Median fare per mile per month. payment_type_key NULL = all payment types.
-- Medians cannot be combined across months, so the dashboard shows them per month.
CREATE TABLE IF NOT EXISTS agg_monthly_fare_stats (
    source_month                 CHAR(7)        NOT NULL,
    payment_type_key             SMALLINT       REFERENCES dim_payment_type (payment_type_key),
    trip_count                   BIGINT         NOT NULL,
    median_fare_per_mile         NUMERIC(10, 4) NOT NULL,
    CONSTRAINT uq_agg_monthly_fare_stats UNIQUE NULLS NOT DISTINCT (source_month, payment_type_key)
);

-- -----------------------------------------------------------------------------
-- Staging table: UNLOGGED (not written to the change log = faster, and the data
-- is temporary). No constraints, so COPY is as fast as possible; rules are applied
-- in Python before COPY, and duplicates are removed in SQL before publish.
-- Holds dropoff_datetime because rule D1 needs it (the fact table does not store it).
-- -----------------------------------------------------------------------------
CREATE UNLOGGED TABLE IF NOT EXISTS stg_trips (
    source_file_id         SMALLINT,
    source_row_number      INTEGER,
    pickup_datetime        TIMESTAMP,
    dropoff_datetime       TIMESTAMP,
    pickup_date_key        INTEGER,
    pickup_hour_key        SMALLINT,
    pu_location_key        SMALLINT,
    do_location_key        SMALLINT,
    payment_type_key       SMALLINT,
    vendor_key             SMALLINT,
    rate_code_id           SMALLINT,
    passenger_count        SMALLINT,
    store_and_fwd_flag     BOOLEAN,
    trip_duration_min      NUMERIC(8, 2),
    trip_distance          NUMERIC(9, 2),
    fare_amount            NUMERIC(10, 2),
    extra                  NUMERIC(10, 2),
    mta_tax                NUMERIC(10, 2),
    tip_amount             NUMERIC(10, 2),
    tolls_amount           NUMERIC(10, 2),
    improvement_surcharge  NUMERIC(10, 2),
    congestion_surcharge   NUMERIC(10, 2),
    airport_fee            NUMERIC(10, 2),
    total_amount           NUMERIC(10, 2),
    cbd_congestion_fee     NUMERIC(10, 2),
    request_source         VARCHAR(10)
);

-- -----------------------------------------------------------------------------
-- Documentation stored in the database (shown by \d+ in psql)
-- -----------------------------------------------------------------------------
COMMENT ON TABLE fact_taxi_trips IS 'Grain: one accepted, de-duplicated yellow taxi trip record. PK = source file + row number. One partition per source file.';
COMMENT ON TABLE dim_location IS 'TLC taxi zones (role-playing: pickup and drop-off).';
COMMENT ON TABLE agg_trips_daily_hourly IS 'Derived summary for the dashboard; rebuilt per month from fact_taxi_trips.';
COMMENT ON TABLE agg_monthly_fare_stats IS 'Median fare per mile per month; payment_type_key NULL = all payment types.';
COMMENT ON TABLE stg_trips IS 'UNLOGGED staging for one month; emptied after each publish.';
COMMENT ON COLUMN fact_taxi_trips.total_amount IS 'As reported. Vendor 1 usually excludes the $2.50 congestion surcharge (plan Q15).';

COMMIT;
