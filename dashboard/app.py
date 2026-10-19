"""NYC Yellow Taxi — single-page Streamlit dashboard.

Run from the project root:   streamlit run dashboard/app.py

Every number is queried from PostgreSQL at run time (nothing is hard-coded). Charts read
the metric views over the summary tables (sql/views.sql), so the page stays fast with
37 M trips. Filters are passed as SQL parameters (never pasted into SQL text).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # allow 'import pipeline'
from pipeline.config import get_settings  # noqa: E402

# --- Chart style (validated palette; see dataviz reference) -----------------------------
BLUE, ORANGE = "#2a78d6", "#eb6834"          # categorical slots 1 and 2
MUTED = "#c3c2b7"                             # de-emphasised marks
SEQUENTIAL_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
GRID = "rgba(137,135,129,0.25)"


def style(fig: go.Figure, height: int = 320, legend: bool = False) -> go.Figure:
    """Thin, recessive chrome: hairline solid grid, no chart border, room for axis labels."""
    fig.update_layout(height=height, margin=dict(l=8, r=8, t=8, b=8), showlegend=legend,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                      hoverlabel=dict(font_size=13), bargap=0.25,
                      font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif"))
    fig.update_xaxes(showgrid=False, linecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, gridwidth=1, zeroline=False)
    return fig


# --- Database access ------------------------------------------------------------------------

@st.cache_resource
def get_engine():
    s = get_settings()
    url = URL.create("postgresql+psycopg", username=s.postgres_user, password=s.postgres_password,
                     host=s.postgres_host, port=s.postgres_port, database=s.postgres_db)
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=600, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a parameterised query (cached 10 min). params is a tuple of (name, value) pairs;
    tuple values become lists, which psycopg sends as PostgreSQL arrays (for = ANY(...))."""
    bound = {k: list(v) if isinstance(v, tuple) else v for k, v in params}
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=bound)


st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")

try:
    periods = query("SELECT DISTINCT year, month, month_name FROM vw_trips_daily_hourly ORDER BY year, month")
    payment_types = query("SELECT payment_type_key, payment_type_name FROM dim_payment_type ORDER BY 1")
except Exception as exc:  # DB down, wrong password, schema not applied...
    st.error(f"Cannot read from the database. Is PostgreSQL running (`bash setup.sh`) and loaded "
             f"(`python -m pipeline.flows`)?\n\nDetails: {exc}")
    st.stop()

if periods.empty:
    st.warning("No data loaded yet. Run `python -m pipeline.flows` first.")
    st.stop()

# --- Filters (one place, scoping every chart) --------------------------------------------
with st.sidebar:
    st.header("Filters")
    years = sorted(periods["year"].unique())
    months_per_year = periods.groupby("year")["month"].count()
    default_year = max(years, key=lambda y: (months_per_year[y], y))  # most months loaded, then latest
    year = st.selectbox("Year", years, index=years.index(default_year))
    year_periods = periods[periods["year"] == year]
    quarter_options = sorted({(m - 1) // 3 + 1 for m in year_periods["month"]})
    quarters = st.multiselect("Quarter", quarter_options, default=quarter_options,
                              format_func=lambda q: f"Q{q}")
    month_choices = year_periods[year_periods["month"].map(lambda m: (m - 1) // 3 + 1).isin(quarters)]
    months = st.multiselect("Months", month_choices["month"].tolist(), default=month_choices["month"].tolist(),
                            format_func=dict(zip(year_periods["month"], year_periods["month_name"], strict=True)).get)
    type_names = dict(zip(payment_types["payment_type_key"], payment_types["payment_type_name"], strict=True))
    present_types = query("SELECT DISTINCT payment_type_key FROM agg_trips_daily_hourly ORDER BY 1")
    present_types = present_types["payment_type_key"].tolist()
    ptypes = st.multiselect("Payment types", present_types, default=present_types, format_func=type_names.get)
    st.caption("All charts and KPIs use these filters. Data refreshes every 10 minutes.")

if not months or not ptypes:
    st.info("Select at least one month and one payment type.")
    st.stop()

FILTER = "year = :year AND month = ANY(:months) AND payment_type_key = ANY(:ptypes)"
params = (("year", int(year)), ("months", tuple(int(m) for m in months)),
          ("ptypes", tuple(int(p) for p in ptypes)))
month_keys = tuple(f"{year}-{m:02d}" for m in months)

# --- Header ---------------------------------------------------------------------------------
last_run = query("SELECT max(ended_at) AS t FROM etl_pipeline_runs WHERE status = 'SUCCESS'")["t"].iloc[0]
last_run_text = (pd.Timestamp(last_run).to_pydatetime().astimezone().strftime("%Y-%m-%d %H:%M")  # local time
                 if pd.notna(last_run) else "never")
st.title(f"NYC Yellow Taxi — {year}")
st.caption(f"Source: NYC TLC trip records · {len(months)} month(s) selected · "
           f"last successful pipeline run: {last_run_text}")

# --- KPIs -----------------------------------------------------------------------------------
totals = query(f"""SELECT sum(trip_count) trips, sum(sum_total) revenue, sum(sum_fare) fare,
                         sum(sum_distance) miles, sum(sum_fare_per_mile_ratio) ratio_sum
                  FROM vw_trips_daily_hourly WHERE {FILTER}""", params).iloc[0]
by_hour = query(f"""SELECT hour_key, hour_label, day_part, sum(trip_count) AS trips
                    FROM vw_trips_daily_hourly WHERE {FILTER}
                    GROUP BY hour_key, hour_label, day_part ORDER BY hour_key""", params)
quality = query("""SELECT sum(rows_rejected) AS rejected, sum(rows_read) AS read
                   FROM (SELECT DISTINCT ON (source_month) rows_rejected, rows_read
                         FROM etl_pipeline_runs WHERE status = 'SUCCESS' AND source_month = ANY(:mk)
                         ORDER BY source_month, run_id DESC) latest""", (("mk", month_keys),)).iloc[0]

trips = int(totals["trips"] or 0)
fare_per_mile = float(totals["fare"]) / float(totals["miles"]) if totals["miles"] else float("nan")
avg_of_ratios = float(totals["ratio_sum"]) / trips if trips else float("nan")
peak = by_hour.sort_values("trips", ascending=False).iloc[0]
reject_rate = 100 * float(quality["rejected"]) / float(quality["read"]) if quality["read"] else float("nan")

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total trips", f"{trips:,}")
k2.metric("Total revenue", f"${float(totals['revenue']) / 1e6:,.2f} M",
          help="Sum of total_amount (excludes cash tips).")
k3.metric("Fare per mile", f"${fare_per_mile:,.2f}",
          help="Weighted: total fare ÷ total miles. See the fare-per-mile section for other definitions.")
k4.metric("Peak hour", f"{peak['hour_label']}", help=f"{int(peak['trips']):,} trips start in this hour.")
k5.metric("Rows rejected by cleaning", f"{reject_rate:.2f}%",
          help="From the latest successful pipeline run of each selected month (all payment types).")

# --- Metric 2: peak hours ----------------------------------------------------------------
st.subheader("When are taxis busiest?")
top3 = set(by_hour.nlargest(3, "trips")["hour_key"])
c1, c2 = st.columns([3, 2])
with c1:
    fig = go.Figure(go.Bar(
        x=by_hour["hour_label"], y=by_hour["trips"],
        marker=dict(color=[BLUE if h in top3 else MUTED for h in by_hour["hour_key"]], cornerradius=4),
        customdata=by_hour["day_part"],
        hovertemplate="<b>%{x}</b> (%{customdata})<br>%{y:,} trips<extra></extra>"))
    # Direct labels on the three peak bars only (annotations keep their size; bar text would shrink).
    for _, row in by_hour[by_hour["hour_key"].isin(top3)].iterrows():
        fig.add_annotation(x=row["hour_label"], y=row["trips"], text=f"{row['trips'] / 1e6:.1f}M",
                           showarrow=False, yshift=12, font=dict(size=12))
    fig.update_xaxes(tickangle=0, tickmode="array", tickvals=by_hour["hour_label"].tolist()[::3])
    st.plotly_chart(style(fig).update_yaxes(title="Trips started"), width="stretch")
    st.caption("Trips by pickup hour. The three busiest hours are highlighted.")
with c2:
    heat = query(f"""SELECT day_of_week, min(day_name) AS day_name, hour_key, sum(trip_count) AS trips
                     FROM vw_trips_daily_hourly WHERE {FILTER}
                     GROUP BY day_of_week, hour_key""", params)
    grid = heat.pivot(index="day_of_week", columns="hour_key", values="trips").sort_index()
    day_labels = heat.drop_duplicates("day_of_week").set_index("day_of_week")["day_name"].reindex(grid.index)
    fig = go.Figure(go.Heatmap(
        z=grid.values, x=[f"{h:02d}" for h in grid.columns], y=day_labels.tolist(),
        colorscale=[[i / (len(SEQUENTIAL_BLUE) - 1), c] for i, c in enumerate(SEQUENTIAL_BLUE)],
        xgap=2, ygap=2, colorbar=dict(title="Trips", thickness=10),
        hovertemplate="%{y} %{x}:00<br>%{z:,} trips<extra></extra>"))
    st.plotly_chart(style(fig).update_yaxes(autorange="reversed", showgrid=False), width="stretch")
    st.caption("Trips by weekday and pickup hour (darker = more trips).")

# --- Metric 3: revenue by payment type -----------------------------------------------------
st.subheader("How is revenue collected?")
revenue = query(f"""SELECT payment_type_name, sum(trip_count) AS trips, sum(sum_total) AS revenue,
                           sum(sum_fare) AS fare, sum(sum_tip) AS tips
                    FROM vw_trips_daily_hourly WHERE {FILTER}
                    GROUP BY payment_type_name ORDER BY revenue""", params)
revenue["share_pct"] = 100 * revenue["revenue"] / revenue["revenue"].sum()
c1, c2 = st.columns([3, 2])
with c1:
    fig = go.Figure(go.Bar(
        x=revenue["revenue"], y=revenue["payment_type_name"], orientation="h",
        marker=dict(color=BLUE, cornerradius=4),
        text=[f"${v / 1e6:,.1f}M · {s:.1f}%" for v, s in zip(revenue["revenue"], revenue["share_pct"], strict=True)],
        textposition="outside", cliponaxis=False,
        hovertemplate="<b>%{y}</b><br>$%{x:,.2f}<extra></extra>"))
    fig.update_xaxes(title="Revenue (total_amount, $)", showgrid=True, gridcolor=GRID)
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(style(fig, height=280).update_layout(margin=dict(r=110)), width="stretch")
with c2:
    table = revenue.sort_values("revenue", ascending=False)[["payment_type_name", "trips", "revenue", "share_pct"]]
    st.dataframe(table, hide_index=True, width="stretch", column_config={
        "payment_type_name": st.column_config.TextColumn("Payment type"),
        "trips": st.column_config.NumberColumn("Trips", format="localized"),
        "revenue": st.column_config.NumberColumn("Revenue", format="dollar"),
        "share_pct": st.column_config.NumberColumn("Share", format="%.1f%%")})
    st.caption("Cash tips are not recorded, so cash revenue is understated. Vendor 1 usually leaves the "
               "\\$2.50 congestion surcharge out of total_amount (about −1.9% overall).")

# --- Metric 1: fare per mile ------------------------------------------------------------
st.subheader("How much does a mile cost?")
monthly = query(f"""SELECT month, min(month_name) AS month_name, sum(trip_count) AS trips,
                           sum(sum_total) AS revenue, sum(sum_fare) / sum(sum_distance) AS weighted
                    FROM vw_trips_daily_hourly WHERE {FILTER}
                    GROUP BY month ORDER BY month""", params)
monthly["month_name"] = monthly["month_name"].str[:3]
# Medians cannot be combined: use the single selected payment type's median, else all types.
median_type = int(ptypes[0]) if len(ptypes) == 1 else None
medians = query("""SELECT CAST(substr(source_month, 6, 2) AS int) AS month, median_fare_per_mile AS median
                   FROM agg_monthly_fare_stats
                   WHERE source_month = ANY(:mk) AND payment_type_key IS NOT DISTINCT FROM :pt""",
                (("mk", month_keys), ("pt", median_type)))
monthly = monthly.merge(medians, on="month", how="left")
m1, m2, m3 = st.columns(3)
m1.metric("Weighted (headline)", f"${fare_per_mile:.2f} / mile", help="Total fare ÷ total miles.")
if monthly["median"].notna().any():
    lo, hi = monthly["median"].min(), monthly["median"].max()
    median_text = f"${lo:.2f} / mile" if len(monthly) == 1 else f"${lo:.2f}–{hi:.2f}"
else:
    median_text = "n/a"
m2.metric("Typical trip (median)" + ("" if len(monthly) == 1 else ", monthly range"), median_text,
          help="Median of each trip's fare ÷ distance, per month. Medians of different months cannot be "
               "combined into one number, so a multi-month selection shows the range of monthly medians.")
m3.metric("Average of trip ratios", f"${avg_of_ratios:.2f} / mile",
          help="Mean of each trip's fare ÷ distance. Very short trips with a minimum fare inflate it.")
fig = go.Figure()
fig.add_trace(go.Scatter(x=monthly["month_name"], y=monthly["weighted"], name="Weighted (total fare ÷ total miles)",
                         mode="lines+markers", line=dict(color=BLUE, width=2), marker=dict(size=8),
                         hovertemplate="%{x}: $%{y:.2f} per mile<extra>Weighted</extra>"))
fig.add_trace(go.Scatter(x=monthly["month_name"], y=monthly["median"], name="Median trip",
                         mode="lines+markers", line=dict(color=ORANGE, width=2), marker=dict(size=8),
                         hovertemplate="%{x}: $%{y:.2f} per mile<extra>Median trip</extra>"))
st.plotly_chart(style(fig, height=300, legend=True).update_yaxes(title="$ per mile", rangemode="tozero"),
                width="stretch")
note = (f"Median for **{type_names[median_type]}** only." if median_type is not None
        else "Median line covers all payment types (medians cannot be re-combined for a subset).")
st.caption("Why three numbers? The weighted value says what one mile earns overall. The average of "
           "per-trip ratios is pulled up by very short trips (a \\$3 minimum fare over 0.1 mile = \\$30/mile). "
           f"The median is the typical trip. {note}")

# --- Monthly trend (two charts, never a dual axis) ----------------------------------------
st.subheader("Monthly trend")
c1, c2 = st.columns(2)
with c1:
    fig = go.Figure(go.Bar(x=monthly["month_name"], y=monthly["trips"], marker=dict(color=BLUE, cornerradius=4),
                           hovertemplate="%{x}: %{y:,} trips<extra></extra>"))
    st.plotly_chart(style(fig, height=260).update_yaxes(title="Trips"), width="stretch")
with c2:
    fig = go.Figure(go.Bar(x=monthly["month_name"], y=monthly["revenue"], marker=dict(color=BLUE, cornerradius=4),
                           hovertemplate="%{x}: $%{y:,.0f}<extra></extra>"))
    st.plotly_chart(style(fig, height=260).update_yaxes(title="Revenue ($)"), width="stretch")
st.caption("Context for 2023: the airport fee rose from \\$1.25 to \\$1.75 in April; September–November had "
           "about 2.5× more zero-distance records, which the pipeline rejected (visible in Pipeline health).")

# --- Table view (every chart's numbers in one place) -------------------------------------
with st.expander("Data tables (all numbers behind the charts)"):
    st.markdown("**Trips by hour**")
    st.dataframe(by_hour.rename(columns={"hour_label": "Hour", "day_part": "Day part", "trips": "Trips"})
                 .drop(columns="hour_key"), hide_index=True, width="stretch")
    st.markdown("**Monthly**")
    st.dataframe(monthly.drop(columns="month").rename(columns={
        "month_name": "Month", "trips": "Trips", "revenue": "Revenue ($)",
        "weighted": "Fare per mile, weighted ($)", "median": "Fare per mile, median ($)"}),
        hide_index=True, width="stretch")

# --- Pipeline health ------------------------------------------------------------------------
with st.expander("Pipeline health"):
    runs = query("""SELECT run_id, source_month, status, started_at, round(duration_seconds) AS seconds,
                           rows_read, rows_loaded, rows_rejected, rows_deduplicated, error_step, error_message
                    FROM etl_pipeline_runs ORDER BY run_id DESC LIMIT 30""")
    failed = int((runs["status"] == "FAILED").sum())
    st.markdown(f"**Last {len(runs)} runs** — {failed} failed" + (" ⚠️" if failed else " ✅"))
    st.dataframe(runs, hide_index=True, width="stretch")
    rejects = query("""SELECT r.source_month AS month, s.reason, s.row_count
                       FROM etl_rejected_summary s JOIN etl_pipeline_runs r USING (run_id)
                       WHERE r.run_id IN (SELECT DISTINCT ON (source_month) run_id FROM etl_pipeline_runs
                                          WHERE status = 'SUCCESS' AND source_month = ANY(:mk)
                                          ORDER BY source_month, run_id DESC)""", (("mk", month_keys),))
    if not rejects.empty:
        st.markdown("**Rows rejected by reason** (latest successful run per selected month)")
        st.dataframe(rejects.pivot_table(index="reason", columns="month", values="row_count", aggfunc="sum",
                                         fill_value=0), width="stretch")
    checks = query("""SELECT r.source_month AS month, q.check_name, q.severity, q.passed, q.details
                      FROM etl_quality_results q JOIN etl_pipeline_runs r USING (run_id)
                      WHERE r.run_id IN (SELECT DISTINCT ON (source_month) run_id FROM etl_pipeline_runs
                                         WHERE status = 'SUCCESS' AND source_month = ANY(:mk)
                                         ORDER BY source_month, run_id DESC)""", (("mk", month_keys),))
    if not checks.empty:
        warnings = checks[~checks["passed"]].sort_values(["month", "check_name"])
        st.markdown("**Data quality checks** (latest successful run per selected month) — "
                    + (f"⚠️ {len(warnings)} warning(s)" if len(warnings) else "✅ all passed"))
        st.caption("QC1–QC6 are critical: a failure rolls the month back, so published months always pass them. "
                   "QC7–QC9 are warnings: the data is loaded but unusual enough to review.")
        # Status text, not colour alone: ✅ pass / ⚠️ warning / ❌ failed.
        status = checks.assign(status=[("✅" if p else ("⚠️" if s == "WARNING" else "❌"))
                                       for p, s in zip(checks["passed"], checks["severity"], strict=True)])
        st.dataframe(status.pivot(index="month", columns="check_name", values="status"), width="stretch")
        for _, w in warnings.iterrows():
            st.markdown(f"- ⚠️ **{w['month']} {w['check_name']}**: {w['details']}")

with st.expander("Notes and assumptions"):
    st.markdown("""
- **Data:** NYC TLC yellow taxi trips, 2023. About 2.9% of raw rows are rejected by the cleaning
  rules (zero or impossible distances, non-positive fares, pickups outside the file's month, fares above \\$1,000, …).
- **Revenue** = `total_amount` as reported (what the passenger was charged, excluding cash tips).
- **Fare per mile** headline = total fare ÷ total miles (weighted). Fare excludes taxes, tips and fees.
- **Peak hours** use the pickup time (local NYC time).
- **Flex Fare** (payment type 0) = app-booked trip with an upfront price (TLC data dictionary, 2025).
- Numbers come from summary tables that the pipeline rebuilds from the fact table on every load;
  `sql/metrics.sql` recomputes them from the 37 M-row fact table and checks they match.
""")
