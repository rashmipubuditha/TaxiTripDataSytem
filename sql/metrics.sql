-- =============================================================================
-- metrics.sql — the three business metrics computed directly from fact_taxi_trips.
--
-- Run:  docker compose exec -T postgres psql -U taxi -d taxi_dw < sql/metrics.sql
--
-- Sections 1-4 scan the full fact table (~37 M rows) and take ~10-20 s each; the
-- dashboard reads the summary views instead (milliseconds). Section 4 checks that the
-- fact table and the summary views agree exactly. Section 5 (exact median) is slow.
-- =============================================================================
\timing on
\pset footer off

\echo
\echo '=== Metric 1: Average fare amount per mile ==='
\echo 'weighted = SUM(fare)/SUM(miles) (headline) | avg_of_ratios = AVG(fare/miles); median: section 5'
SELECT count(*)                                                        AS trips,
       round(sum(fare_amount) / nullif(sum(trip_distance), 0), 2)      AS fare_per_mile_weighted,
       round(avg(fare_amount / trip_distance), 2)                       AS fare_per_mile_avg_of_ratios
FROM fact_taxi_trips;

\echo
\echo '=== Metric 2: Peak hours for taxi rides (pickup hour, busiest first) ==='
SELECT t.hour_label, t.day_part,
       count(*)                                                         AS trip_count,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)              AS share_pct
FROM fact_taxi_trips f
JOIN dim_time t ON t.hour_key = f.pickup_hour_key
GROUP BY t.hour_key, t.hour_label, t.day_part
ORDER BY trip_count DESC;

\echo
\echo '=== Metric 3: Total revenue by payment type (revenue = total_amount) ==='
SELECT p.payment_type_name,
       count(*)                                                         AS trip_count,
       sum(f.total_amount)                                              AS total_revenue,
       sum(f.fare_amount)                                               AS fare_revenue,
       sum(f.tip_amount)                                                AS tip_revenue,
       round(100.0 * sum(f.total_amount) / sum(sum(f.total_amount)) OVER (), 2) AS revenue_share_pct
FROM fact_taxi_trips f
JOIN dim_payment_type p ON p.payment_type_key = f.payment_type_key
GROUP BY p.payment_type_key, p.payment_type_name
ORDER BY total_revenue DESC;

\echo
\echo '=== 4. Reconciliation: fact table vs summary views (every row must say t) ==='
WITH fact AS (
    SELECT count(*) AS trips, sum(total_amount) AS revenue,
           round(sum(fare_amount) / sum(trip_distance), 4) AS fpm
    FROM fact_taxi_trips
), summary AS (
    SELECT (SELECT sum(trip_count) FROM vw_trips_by_hour)               AS trips,
           (SELECT sum(total_revenue) FROM vw_revenue_by_payment_type)  AS revenue,
           (SELECT fare_per_mile_weighted FROM vw_fare_per_mile)        AS fpm
)
SELECT f.trips = s.trips     AS trips_match,
       f.revenue = s.revenue AS revenue_match,
       f.fpm = s.fpm         AS fare_per_mile_match
FROM fact f, summary s;

\echo
\echo '=== 5. (slow, ~2 min) Exact full-year median fare per mile — sorts all 37 M ratios ==='
\echo 'Per-month medians are precomputed in agg_monthly_fare_stats (that is what the dashboard shows).'
\echo 'Press Ctrl+C to skip.'
SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY fare_amount / trip_distance)::numeric, 2)
       AS fare_per_mile_median_full_year
FROM fact_taxi_trips;
