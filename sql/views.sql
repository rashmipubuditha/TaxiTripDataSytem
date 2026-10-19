-- =============================================================================
-- views.sql — metric views for the dashboard and for ad-hoc SQL.
--
-- All views read the small summary tables (agg_*), never the 37 M-row fact table,
-- so they answer in milliseconds. sql/metrics.sql computes the same metrics from
-- fact_taxi_trips to prove the two agree.
-- Re-runnable: views are dropped and re-created in one transaction (setup.sh applies
-- this file on every run, after schema.sql).
-- =============================================================================
BEGIN;
SET LOCAL client_min_messages = warning;

DROP VIEW IF EXISTS vw_revenue_by_payment_type, vw_trips_by_hour, vw_fare_per_mile_monthly,
                    vw_fare_per_mile, vw_trips_daily_hourly CASCADE;

-- Base view: the summary table with readable dimension attributes.
-- The dashboard filters this view by year / month / payment type and aggregates.
CREATE VIEW vw_trips_daily_hourly AS
SELECT d.date_key, d.full_date, d.year, d.quarter, d.month, d.month_name,
       d.day_of_week, d.day_name, d.is_weekend,
       t.hour_key, t.hour_label, t.day_part,
       p.payment_type_key, p.payment_type_name, p.is_app_booked,
       a.trip_count, a.sum_fare, a.sum_distance, a.sum_total, a.sum_tip, a.sum_fare_per_mile_ratio
FROM agg_trips_daily_hourly a
JOIN dim_date d          ON d.date_key = a.date_key
JOIN dim_time t          ON t.hour_key = a.hour_key
JOIN dim_payment_type p  ON p.payment_type_key = a.payment_type_key;

-- Metric 1: average fare amount per mile (all loaded data).
--   weighted          = total fare / total miles   (headline: "what one mile earns")
--   avg_of_ratios     = mean of each trip's fare / distance (inflated by very short trips)
CREATE VIEW vw_fare_per_mile AS
SELECT sum(trip_count)                                              AS trips,
       round(sum(sum_fare) / nullif(sum(sum_distance), 0), 4)      AS fare_per_mile_weighted,
       round(sum(sum_fare_per_mile_ratio) / nullif(sum(trip_count), 0), 4) AS fare_per_mile_avg_of_ratios
FROM agg_trips_daily_hourly;

-- Metric 1, per month, with the median (typical trip) from agg_monthly_fare_stats.
CREATE VIEW vw_fare_per_mile_monthly AS
SELECT v.year, v.month, v.month_name,
       sum(v.trip_count)                                                  AS trips,
       round(sum(v.sum_fare) / nullif(sum(v.sum_distance), 0), 4)        AS fare_per_mile_weighted,
       round(sum(v.sum_fare_per_mile_ratio) / nullif(sum(v.trip_count), 0), 4) AS fare_per_mile_avg_of_ratios,
       round(max(m.median_fare_per_mile), 4)                             AS fare_per_mile_median
FROM vw_trips_daily_hourly v
LEFT JOIN agg_monthly_fare_stats m
       ON m.source_month = to_char(make_date(v.year, v.month, 1), 'YYYY-MM') AND m.payment_type_key IS NULL
GROUP BY v.year, v.month, v.month_name;

-- Metric 2: peak hours — trips per pickup hour, ranked (rank 1 = busiest).
CREATE VIEW vw_trips_by_hour AS
SELECT hour_key, hour_label, day_part,
       sum(trip_count)                                                     AS trip_count,
       round(100.0 * sum(trip_count) / sum(sum(trip_count)) OVER (), 2)   AS share_pct,
       rank() OVER (ORDER BY sum(trip_count) DESC)                         AS busy_rank
FROM vw_trips_daily_hourly
GROUP BY hour_key, hour_label, day_part;

-- Metric 3: total revenue by payment type (revenue = total_amount; fare and tips shown too).
CREATE VIEW vw_revenue_by_payment_type AS
SELECT payment_type_key, payment_type_name,
       sum(trip_count)                                                     AS trip_count,
       sum(sum_total)                                                      AS total_revenue,
       sum(sum_fare)                                                       AS fare_revenue,
       sum(sum_tip)                                                        AS tip_revenue,
       round(100.0 * sum(sum_total) / sum(sum(sum_total)) OVER (), 2)     AS revenue_share_pct
FROM vw_trips_daily_hourly
GROUP BY payment_type_key, payment_type_name;

COMMENT ON VIEW vw_fare_per_mile IS 'Metric 1: fare per mile (weighted = headline; avg_of_ratios for comparison).';
COMMENT ON VIEW vw_trips_by_hour IS 'Metric 2: trips per pickup hour; busy_rank 1 = peak hour.';
COMMENT ON VIEW vw_revenue_by_payment_type IS 'Metric 3: revenue (total_amount) by payment type.';

COMMIT;
