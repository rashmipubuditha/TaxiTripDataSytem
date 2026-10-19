"""NYC Yellow Taxi — single-page Streamlit dashboard.

Run from the project root:   streamlit run dashboard/app.py

Every number is queried from PostgreSQL at run time (nothing is hard-coded). Charts read
the metric views over the summary tables (sql/views.sql), so the page stays fast with
37 M trips. Filters are passed as SQL parameters (never pasted into SQL text).
"""
from __future__ import annotations

import base64
import io
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))  # allow 'import pipeline'
from pipeline.config import get_settings  # noqa: E402

COVER_IMAGE = PROJECT_ROOT / "public" / "image.png"

# --- Design tokens ----------------------------------------------------------------------
# Text colours (all >= 5.3:1 contrast on white and on the page background).
INK, BODY, MUTED_TEXT = "#16191f", "#3d434d", "#5f6670"
# Chart colours: validated categorical slots 1-2 (dataviz palette), muted marks, sequential ramp.
BLUE, ORANGE = "#2a78d6", "#eb6834"
MUTED = "#c9ccd1"
SEQUENTIAL_BLUE = ["#e6effb", "#b7d3f6", "#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]
GRID = "rgba(22,25,31,0.08)"
FONT = "Inter, system-ui, -apple-system, 'Segoe UI', sans-serif"

CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stSidebar"],
button, input, textarea, select, .stMarkdown, .stCaption {{ font-family: {FONT}; }}
.stApp {{ background: #f5f6f8; color: {BODY}; }}
[data-testid="stHeader"] {{ background: transparent; }}
.block-container {{ padding-top: 1.2rem; padding-bottom: 3rem; max-width: 1420px; }}
h1, h2, h3, h4 {{ color: {INK}; font-family: {FONT}; letter-spacing: -0.01em; }}
[data-testid="stCaptionContainer"], .stCaption {{ color: {MUTED_TEXT}; }}

/* Sidebar */
[data-testid="stSidebar"] {{ background: #ffffff; border-right: 1px solid rgba(22,25,31,0.08); }}
.side-brand {{ font-weight: 800; font-size: 1.15rem; color: {INK}; margin: 0.2rem 0 0.1rem; }}
.side-sub {{ color: {MUTED_TEXT}; font-size: 0.82rem; margin-bottom: 0.8rem; }}
.side-nav a {{ display: block; color: {BODY}; text-decoration: none; padding: 0.28rem 0.55rem;
              border-radius: 6px; font-size: 0.9rem; }}
.side-nav a:hover {{ background: #f1f3f6; color: {INK}; }}

/* Hero cover */
.hero {{ position: relative; border-radius: 16px; overflow: hidden; min-height: 300px;
        background-size: cover; background-position: center 62%;
        display: flex; align-items: flex-end; margin-bottom: 1.4rem;
        box-shadow: 0 10px 30px rgba(16,24,40,0.18); }}
.hero-inner {{ padding: 2.2rem 2.4rem; max-width: 820px; }}
.hero-kicker {{ color: #f5b301; font-weight: 700; font-size: 0.78rem; letter-spacing: 0.14em;
               text-transform: uppercase; display: flex; align-items: center; gap: 0.6rem; }}
.hero-kicker::before {{ content: ""; width: 28px; height: 3px; background: #f5b301; border-radius: 2px; }}
.hero h1 {{ color: #ffffff; font-size: 2.6rem; font-weight: 800; line-height: 1.1; margin: 0.45rem 0 0.5rem;
           padding: 0; }}
.hero p {{ color: rgba(255,255,255,0.88); font-size: 1.02rem; line-height: 1.5; margin: 0 0 1rem; }}
.chips {{ display: flex; flex-wrap: wrap; gap: 0.5rem; }}
.chip {{ background: rgba(255,255,255,0.14); color: #ffffff; border: 1px solid rgba(255,255,255,0.22);
        padding: 0.28rem 0.7rem; border-radius: 999px; font-size: 0.8rem; font-weight: 500;
        backdrop-filter: blur(4px); }}

/* Section headings */
.section {{ margin: 2.2rem 0 0.8rem; }}
.section-num {{ color: #1f63b8; font-weight: 700; font-size: 0.78rem; letter-spacing: 0.12em;
               text-transform: uppercase; }}
.section h2 {{ font-size: 1.55rem; font-weight: 750; margin: 0.15rem 0 0.25rem; padding: 0; }}
.section p {{ color: {MUTED_TEXT}; margin: 0; font-size: 0.95rem; }}

/* Insight callout */
.insight {{ background: #ffffff; border: 1px solid rgba(22,25,31,0.08); border-left: 4px solid #1f63b8;
           border-radius: 10px; padding: 0.75rem 1rem; color: {BODY}; font-size: 0.95rem;
           margin: 0.2rem 0 0.9rem; }}
.insight b {{ color: {INK}; }}

/* KPI cards (st.metric with border) */
[data-testid="stMetric"] {{ background: #ffffff; border-radius: 12px; padding: 0.9rem 1rem 0.8rem;
                            box-shadow: 0 1px 2px rgba(16,24,40,0.04); }}
[data-testid="stMetricLabel"] p {{ color: {MUTED_TEXT}; font-weight: 600; font-size: 0.76rem;
                                   text-transform: uppercase; letter-spacing: 0.04em; }}
[data-testid="stMetricValue"] {{ color: {INK}; font-weight: 750; font-size: 1.5rem; }}
[data-testid="stMetricValue"] > div {{ overflow: visible; text-overflow: clip; white-space: nowrap; }}
[data-testid="stMetricDelta"] {{ background: transparent !important; padding-left: 0 !important; }}
[data-testid="stMetricDelta"] div, [data-testid="stMetricDelta"] p {{ color: {MUTED_TEXT} !important;
                                                                      font-size: 0.82rem; font-weight: 500;
                                                                      white-space: normal !important;
                                                                      overflow: visible !important; }}

/* Cards: st.container(border=True, key="card_...") gets the CSS class st-key-card_... */
[class*="st-key-card_"] {{ background: #ffffff; border-radius: 12px !important;
                          border: 1px solid rgba(22,25,31,0.08) !important;
                          box-shadow: 0 1px 2px rgba(16,24,40,0.04); padding: 1rem 1.1rem 0.6rem; }}
.card-title {{ color: {INK}; font-weight: 650; font-size: 1rem; margin: 0 0 0.1rem; }}
.card-sub {{ color: {MUTED_TEXT}; font-size: 0.84rem; margin: 0 0 0.4rem; }}

/* Tables: aligned digits */
[data-testid="stDataFrame"] {{ font-variant-numeric: tabular-nums; }}
.footer {{ color: {MUTED_TEXT}; font-size: 0.82rem; border-top: 1px solid rgba(22,25,31,0.1);
          margin-top: 2.5rem; padding-top: 1rem; }}
</style>
"""


def usd(value: float, decimals: int = 2) -> str:
    """Dollar amount for HTML/markdown text ('&#36;' avoids Streamlit reading $...$ as maths)."""
    return f"&#36;{value:,.{decimals}f}"


def usd_m(value: float) -> str:
    return f"&#36;{value / 1e6:,.1f}M"


def style(fig: go.Figure, height: int = 320, legend: bool = False) -> go.Figure:
    """Thin, recessive chrome: hairline solid grid, transparent background inside the card."""
    fig.update_layout(height=height, margin=dict(l=4, r=8, t=8, b=4), showlegend=legend,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(color=BODY)),
                      hoverlabel=dict(font_size=13, font_family=FONT), bargap=0.28,
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(family=FONT, color=MUTED_TEXT, size=12))
    fig.update_xaxes(showgrid=False, linecolor="rgba(22,25,31,0.18)", title_font=dict(color=BODY))
    fig.update_yaxes(gridcolor=GRID, gridwidth=1, zeroline=False, title_font=dict(color=BODY))
    return fig


def section(anchor: str, number: str, title: str, question: str) -> None:
    st.markdown(f'<div class="section" id="{anchor}"><div class="section-num">{number}</div>'
                f'<h2>{title}</h2><p>{question}</p></div>', unsafe_allow_html=True)


def insight(html: str) -> None:
    st.markdown(f'<div class="insight">{html}</div>', unsafe_allow_html=True)


def card_title(title: str, subtitle: str = "") -> None:
    st.markdown(f'<div class="card-title">{title}</div>'
                + (f'<div class="card-sub">{subtitle}</div>' if subtitle else ""), unsafe_allow_html=True)


def card(name: str):
    """A white card: bordered container with a key, so the CSS above can style it."""
    return st.container(border=True, key=f"card_{name}")


def kpi(column, label: str, value: str, note: str, help_text: str | None = None,
        spark: list | None = None, spark_type: str = "area") -> None:
    """KPI card: value, a neutral context line inside the card, optional sparkline."""
    note = note.replace("&#36;", "\\$")  # delta line is markdown: escape $ so it is not read as maths
    column.metric(label, value, delta=note or None, delta_color="off", delta_arrow="off", help=help_text,
                  border=True, chart_data=spark if spark and len(spark) > 1 else None, chart_type=spark_type)


# --- Data access ------------------------------------------------------------------------

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


@st.cache_resource
def cover_image_css() -> str:
    """The cover photo, resized and compressed in memory (the original file is not changed)."""
    if not COVER_IMAGE.is_file():
        return "linear-gradient(120deg, #1c2430, #2a3a52)"
    from PIL import Image

    with Image.open(COVER_IMAGE) as img:
        img = img.convert("RGB")
        img.thumbnail((1800, 1800))
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=80, optimize=True, progressive=True)
    data = base64.b64encode(buffer.getvalue()).decode()
    overlay = "linear-gradient(90deg, rgba(10,14,20,0.88) 0%, rgba(10,14,20,0.62) 45%, rgba(10,14,20,0.18) 100%)"
    return f"{overlay}, url(data:image/jpeg;base64,{data})"


# --- Page ---------------------------------------------------------------------------------
st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)

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

# --- Sidebar: brand, filters, navigation ----------------------------------------------------
with st.sidebar:
    st.markdown('<div class="side-brand">🚕 NYC Taxi Analytics</div>'
                '<div class="side-sub">Yellow taxi trip records · TLC</div>', unsafe_allow_html=True)
    st.markdown("**Filters**")
    years = sorted(periods["year"].unique())
    months_per_year = periods.groupby("year")["month"].count()
    default_year = max(years, key=lambda y: (months_per_year[y], y))  # most months loaded, then latest
    year = st.selectbox("Year", years, index=years.index(default_year))
    year_periods = periods[periods["year"] == year]
    quarter_options = sorted({(m - 1) // 3 + 1 for m in year_periods["month"]})
    quarters = st.multiselect("Quarter", quarter_options, default=quarter_options,
                              format_func=lambda q: f"Q{q}")
    month_choices = year_periods[year_periods["month"].map(lambda m: (m - 1) // 3 + 1).isin(quarters)]
    month_labels = dict(zip(year_periods["month"], year_periods["month_name"], strict=True))
    months = st.multiselect("Months", month_choices["month"].tolist(), default=month_choices["month"].tolist(),
                            format_func=month_labels.get)
    type_names = dict(zip(payment_types["payment_type_key"], payment_types["payment_type_name"], strict=True))
    present_types = query("SELECT DISTINCT payment_type_key FROM agg_trips_daily_hourly ORDER BY 1")
    present_types = present_types["payment_type_key"].tolist()
    ptypes = st.multiselect("Payment types", present_types, default=present_types, format_func=type_names.get)
    st.caption("Every chart and KPI on the page uses these filters. Data refreshes every 10 minutes.")
    st.markdown("**On this page**")
    st.markdown('<div class="side-nav">'
                '<a href="#overview">1 · Overview</a><a href="#demand">2 · Demand &amp; peak hours</a>'
                '<a href="#revenue">3 · Revenue by payment type</a><a href="#pricing">4 · Fare per mile</a>'
                '<a href="#trends">5 · Monthly trends</a><a href="#quality">6 · Data quality &amp; pipeline</a>'
                '</div>', unsafe_allow_html=True)

if not months or not ptypes:
    st.info("Select at least one month and one payment type.")
    st.stop()

FILTER = "year = :year AND month = ANY(:months) AND payment_type_key = ANY(:ptypes)"
params = (("year", int(year)), ("months", tuple(int(m) for m in months)),
          ("ptypes", tuple(int(p) for p in ptypes)))
month_keys = tuple(f"{year}-{m:02d}" for m in months)

# --- Queries (all on the small summary view) ---------------------------------------------
totals = query(f"""SELECT sum(trip_count) trips, sum(sum_total) revenue, sum(sum_fare) fare,
                         sum(sum_distance) miles, sum(sum_tip) tips, sum(sum_fare_per_mile_ratio) ratio_sum,
                         count(DISTINCT date_key) days
                  FROM vw_trips_daily_hourly WHERE {FILTER}""", params).iloc[0]
by_hour = query(f"""SELECT hour_key, hour_label, day_part, sum(trip_count) AS trips,
                           sum(sum_fare) / nullif(sum(sum_distance), 0) AS fare_per_mile
                    FROM vw_trips_daily_hourly WHERE {FILTER}
                    GROUP BY hour_key, hour_label, day_part ORDER BY hour_key""", params)
monthly = query(f"""SELECT month, min(month_name) AS month_name, sum(trip_count) AS trips,
                           sum(sum_total) AS revenue, sum(sum_fare) / sum(sum_distance) AS weighted,
                           sum(sum_distance) / sum(trip_count) AS avg_miles
                    FROM vw_trips_daily_hourly WHERE {FILTER}
                    GROUP BY month ORDER BY month""", params)
monthly["month_name"] = monthly["month_name"].str[:3]
last_run = query("SELECT max(ended_at) AS t FROM etl_pipeline_runs WHERE status = 'SUCCESS'")["t"].iloc[0]
last_run_text = (pd.Timestamp(last_run).to_pydatetime().astimezone().strftime("%d %b %Y, %H:%M")  # local time
                 if pd.notna(last_run) else "never")

trips = int(totals["trips"] or 0)
revenue_total = float(totals["revenue"] or 0)
miles = float(totals["miles"] or 0)
fare_total = float(totals["fare"] or 0)
days = int(totals["days"] or 0)
fare_per_mile = fare_total / miles if miles else float("nan")
avg_of_ratios = float(totals["ratio_sum"]) / trips if trips else float("nan")
peak = by_hour.sort_values("trips", ascending=False).iloc[0]
quiet = by_hour.sort_values("trips").iloc[0]
selected_month_names = [month_labels[m] for m in months]
period_text = (selected_month_names[0] if len(months) == 1
               else f"{selected_month_names[0][:3]}–{selected_month_names[-1][:3]}")

# --- Hero cover -----------------------------------------------------------------------------
st.markdown(f"""
<div class="hero" style="background-image: {cover_image_css()};">
  <div class="hero-inner">
    <div class="hero-kicker">NYC TLC · Yellow Taxi Trip Records</div>
    <h1>NYC Yellow Taxi Analytics — {year}</h1>
    <p>How much a mile costs, when New York's taxis are busiest, and how passengers pay —
       computed from {trips:,} cleaned trips in a PostgreSQL star schema.</p>
    <div class="chips">
      <span class="chip">📅 {period_text} {year} · {len(months)} month(s)</span>
      <span class="chip">🧾 {len(ptypes)} payment type(s)</span>
      <span class="chip">🔄 Last pipeline run: {last_run_text}</span>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

# --- 1. Overview ------------------------------------------------------------------------------
section("overview", "1 · Overview", "Key figures", "The headline numbers for the selected period.")
k1, k2, k3, k4, k5 = st.columns(5)
kpi(k1, "Total trips", f"{trips:,}", f"≈ {trips / days:,.0f} per day" if days else "",
    "Trips per month in the sparkline.", spark=monthly["trips"].tolist(), spark_type="bar")
kpi(k2, "Total revenue", f"${revenue_total / 1e6:,.2f} M",
    f"≈ {usd(revenue_total / trips)} per trip" if trips else "",
    "Sum of total_amount: everything the passenger was charged, excluding cash tips. Sparkline: per month.",
    spark=monthly["revenue"].round(0).tolist(), spark_type="bar")
kpi(k3, "Fare per mile", f"${fare_per_mile:,.2f}", "fare ÷ miles",
    "Weighted: total fare ÷ total miles. Sparkline: per month. See section 4 for other definitions.",
    spark=monthly["weighted"].round(3).tolist(), spark_type="line")
kpi(k4, "Peak hour", f"{peak['hour_label']}",
    f"{peak['trips'] / 1e6:.2f}M trips · {100 * peak['trips'] / trips:.1f}%" if trips else "",
    "The pickup hour with the most trips. Sparkline: trips by hour, 00:00–23:00.",
    spark=by_hour["trips"].tolist(), spark_type="area")
kpi(k5, "Average trip", f"{miles / trips:.2f} mi" if trips else "–",
    f"{usd(fare_total / trips)} avg fare" if trips else "",
    "Average distance per trip. Sparkline: per month.", spark=monthly["avg_miles"].round(3).tolist(),
    spark_type="line")

# --- 2. Demand -------------------------------------------------------------------------------
section("demand", "2 · Demand", "When are taxis busiest?", "Trips by pickup hour, weekday and weekend.")
evening = by_hour[by_hour["hour_key"].between(17, 19)]["trips"].sum()
insight(f"<b>{peak['hour_label']}</b> is the busiest hour with <b>{int(peak['trips']):,}</b> trips. "
        f"The evening rush (17:00–19:59) carries <b>{100 * evening / trips:.1f}%</b> of all trips, "
        f"while <b>{quiet['hour_label']}</b> is the quietest hour ({int(quiet['trips']):,} trips).")

top3 = set(by_hour.nlargest(3, "trips")["hour_key"])
c1, c2 = st.columns([3, 2])
with c1, card("01"):
    card_title("Trips by pickup hour", "The three busiest hours are highlighted; hover any bar for its value.")
    fig = go.Figure(go.Bar(
        x=by_hour["hour_label"], y=by_hour["trips"],
        marker=dict(color=[BLUE if h in top3 else MUTED for h in by_hour["hour_key"]], cornerradius=4),
        customdata=by_hour["day_part"],
        hovertemplate="<b>%{x}</b> (%{customdata})<br>%{y:,} trips<extra></extra>"))
    fig.add_annotation(x=peak["hour_label"], y=peak["trips"], text=f"Peak · {peak['trips'] / 1e6:.2f}M",
                       showarrow=False, yshift=13, font=dict(size=12, color=INK))
    fig.update_xaxes(tickangle=0, tickmode="array", tickvals=by_hour["hour_label"].tolist()[::3])
    st.plotly_chart(style(fig, height=330).update_yaxes(title="Trips started"), width="stretch")
with c2, card("02"):
    card_title("Weekday × hour", "Darker = more trips.")
    heat = query(f"""SELECT day_of_week, min(day_name) AS day_name, hour_key, sum(trip_count) AS trips
                     FROM vw_trips_daily_hourly WHERE {FILTER}
                     GROUP BY day_of_week, hour_key""", params)
    grid = heat.pivot(index="day_of_week", columns="hour_key", values="trips").sort_index()
    day_labels = heat.drop_duplicates("day_of_week").set_index("day_of_week")["day_name"].reindex(grid.index)
    fig = go.Figure(go.Heatmap(
        z=grid.values, x=[f"{h:02d}" for h in grid.columns], y=[d[:3] for d in day_labels],
        colorscale=[[i / (len(SEQUENTIAL_BLUE) - 1), c] for i, c in enumerate(SEQUENTIAL_BLUE)],
        xgap=2, ygap=2, colorbar=dict(title="Trips", thickness=10, outlinewidth=0),
        hovertemplate="%{y} %{x}:00<br>%{z:,} trips<extra></extra>"))
    fig.update_xaxes(tickmode="array", tickvals=[f"{h:02d}" for h in range(0, 24, 3)])
    st.plotly_chart(style(fig, height=330).update_yaxes(autorange="reversed", showgrid=False), width="stretch")

weekpart = query(f"""SELECT is_weekend, hour_key, hour_label, sum(trip_count) AS trips,
                            count(DISTINCT date_key) AS days
                     FROM vw_trips_daily_hourly WHERE {FILTER}
                     GROUP BY is_weekend, hour_key, hour_label ORDER BY hour_key""", params)
weekpart["per_day"] = weekpart["trips"] / weekpart["days"]
with card("03"):
    card_title("Weekday vs weekend: average trips per day, by hour",
               "Weekends shift demand later into the night; weekdays have a sharp evening rush.")
    fig = go.Figure()
    for is_weekend, name, color in ((False, "Weekday (Mon–Fri)", BLUE), (True, "Weekend (Sat–Sun)", ORANGE)):
        part = weekpart[weekpart["is_weekend"] == is_weekend]
        fig.add_trace(go.Scatter(x=part["hour_label"], y=part["per_day"], name=name, mode="lines+markers",
                                 line=dict(color=color, width=2), marker=dict(size=7),
                                 hovertemplate="%{x}: %{y:,.0f} trips per day<extra>" + name + "</extra>"))
    fig.update_xaxes(tickangle=0, tickmode="array", tickvals=by_hour["hour_label"].tolist()[::3])
    st.plotly_chart(style(fig, height=280, legend=True).update_yaxes(title="Avg trips per day", tickformat=","),
                    width="stretch")

# --- 3. Revenue ------------------------------------------------------------------------------
section("revenue", "3 · Revenue", "How is revenue collected?",
        "Total revenue (total_amount) by payment type, with trip counts and average trip value.")
revenue = query(f"""SELECT payment_type_name, sum(trip_count) AS trips, sum(sum_total) AS revenue,
                           sum(sum_fare) AS fare, sum(sum_tip) AS tips
                    FROM vw_trips_daily_hourly WHERE {FILTER}
                    GROUP BY payment_type_name ORDER BY revenue""", params)
revenue["share_pct"] = 100 * revenue["revenue"] / revenue["revenue"].sum()
revenue["trip_share_pct"] = 100 * revenue["trips"] / revenue["trips"].sum()
revenue["per_trip"] = revenue["revenue"] / revenue["trips"]
top_type = revenue.sort_values("revenue", ascending=False).iloc[0]
by_name = revenue.set_index("payment_type_name")
detail = ""
if {"Credit card", "Cash"} <= set(by_name.index):
    card_row, cash_row = by_name.loc["Credit card"], by_name.loc["Cash"]
    detail = (f" A card trip averages <b>{usd(card_row['per_trip'])}</b> against "
              f"<b>{usd(cash_row['per_trip'])}</b> for cash — partly because card tips are recorded "
              f"({usd(card_row['tips'] / card_row['trips'])} per card trip) while cash tips are not.")
insight(f"<b>{top_type['payment_type_name']}</b> brings in <b>{top_type['share_pct']:.1f}%</b> of revenue "
        f"({usd_m(top_type['revenue'])}).{detail}")

c1, c2 = st.columns(2)
with c1, card("04"):
    card_title("Revenue by payment type", "Sorted by revenue; labels show revenue and share.")
    fig = go.Figure(go.Bar(
        x=revenue["revenue"], y=revenue["payment_type_name"], orientation="h",
        marker=dict(color=BLUE, cornerradius=4),
        text=[f"${v / 1e6:,.1f}M · {s:.1f}%" for v, s in zip(revenue["revenue"], revenue["share_pct"], strict=True)],
        textposition="outside", cliponaxis=False, textfont=dict(color=BODY),
        hovertemplate="<b>%{y}</b><br>$%{x:,.2f}<extra></extra>"))
    fig.update_xaxes(title="Revenue ($)", showgrid=True, gridcolor=GRID)
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(style(fig, height=300).update_layout(margin=dict(r=120)), width="stretch")
with c2, card("05"):
    card_title("Detail by payment type")
    table = revenue.sort_values("revenue", ascending=False).assign(revenue_m=lambda d: d["revenue"] / 1e6)[
        ["payment_type_name", "trips", "trip_share_pct", "revenue_m", "share_pct", "per_trip"]]
    st.dataframe(table, hide_index=True, width="stretch", column_config={
        "payment_type_name": st.column_config.TextColumn("Payment type"),
        "trips": st.column_config.NumberColumn("Trips", format="localized"),
        "trip_share_pct": st.column_config.NumberColumn("% trips", format="%.1f%%"),
        "revenue_m": st.column_config.NumberColumn("Revenue", format="$%.1fM"),
        "share_pct": st.column_config.NumberColumn("% revenue", format="%.1f%%"),
        "per_trip": st.column_config.NumberColumn("Per trip", format="$%.2f")})
    st.caption("Cash tips are not recorded, so cash revenue is understated. Vendor 1 usually leaves the "
               "\\$2.50 congestion surcharge out of total_amount (about −1.9% overall).")

# --- 4. Fare per mile -------------------------------------------------------------------------
section("pricing", "4 · Pricing", "How much does a mile cost?",
        "Meter fare per mile — three ways to measure it, by month and by hour of day.")
# Medians cannot be combined: use the single selected payment type's median, else all types.
median_type = int(ptypes[0]) if len(ptypes) == 1 else None
medians = query("""SELECT CAST(substr(source_month, 6, 2) AS int) AS month, median_fare_per_mile AS median
                   FROM agg_monthly_fare_stats
                   WHERE source_month = ANY(:mk) AND payment_type_key IS NOT DISTINCT FROM :pt""",
                (("mk", month_keys), ("pt", median_type)))
monthly = monthly.merge(medians, on="month", how="left")
dearest_hour = by_hour.sort_values("fare_per_mile", ascending=False).iloc[0]
cheapest_hour = by_hour.sort_values("fare_per_mile").iloc[0]
insight(f"Across all miles driven, a mile earned <b>{usd(fare_per_mile)}</b> in meter fare. "
        f"The typical (median) trip pays more per mile because short trips carry a minimum fare. "
        f"By time of day, miles are dearest at <b>{dearest_hour['hour_label']}</b> "
        f"({usd(dearest_hour['fare_per_mile'])}/mile, slow traffic) and cheapest at "
        f"<b>{cheapest_hour['hour_label']}</b> ({usd(cheapest_hour['fare_per_mile'])}/mile).")

m1, m2, m3 = st.columns(3)
if monthly["median"].notna().any():
    lo, hi = monthly["median"].min(), monthly["median"].max()
    median_text = f"${lo:.2f} / mile" if len(monthly) == 1 else f"${lo:.2f}–{hi:.2f}"
else:
    median_text = "n/a"
kpi(m1, "Weighted (headline)", f"${fare_per_mile:.2f} / mile", "total fare ÷ total miles",
    "What one mile earns overall; long trips count more.")
kpi(m2, "Typical trip (median)" + ("" if len(monthly) == 1 else ", monthly range"), median_text,
    "median of each trip's fare ÷ distance",
    "Medians of different months cannot be combined into one number, so a multi-month selection "
    "shows the range of monthly medians.")
kpi(m3, "Average of trip ratios", f"${avg_of_ratios:.2f} / mile", "inflated by very short trips",
    "Mean of each trip's fare ÷ distance. A $3 minimum fare over 0.1 mile is $30/mile.")

c1, c2 = st.columns(2)
with c1, card("06"):
    card_title("Fare per mile by month", "Weighted vs typical (median) trip.")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=monthly["month_name"], y=monthly["weighted"], name="Weighted",
                             mode="lines+markers", line=dict(color=BLUE, width=2), marker=dict(size=8),
                             hovertemplate="%{x}: $%{y:.2f} per mile<extra>Weighted</extra>"))
    fig.add_trace(go.Scatter(x=monthly["month_name"], y=monthly["median"], name="Median trip",
                             mode="lines+markers", line=dict(color=ORANGE, width=2), marker=dict(size=8),
                             hovertemplate="%{x}: $%{y:.2f} per mile<extra>Median trip</extra>"))
    st.plotly_chart(style(fig, height=290, legend=True).update_yaxes(title="$ per mile", rangemode="tozero"),
                    width="stretch")
    st.caption(f"Median for {type_names[median_type]} only." if median_type is not None
               else "Median line covers all payment types (medians cannot be re-combined for a subset).")
with c2, card("07"):
    card_title("Fare per mile by pickup hour", "Weighted; slower traffic means more fare per mile.")
    fig = go.Figure(go.Scatter(x=by_hour["hour_label"], y=by_hour["fare_per_mile"], mode="lines+markers",
                               line=dict(color=BLUE, width=2), marker=dict(size=7), fill="tozeroy",
                               fillcolor="rgba(42,120,214,0.08)",
                               hovertemplate="%{x}: $%{y:.2f} per mile<extra></extra>"))
    fig.update_xaxes(tickangle=0, tickmode="array", tickvals=by_hour["hour_label"].tolist()[::3])
    st.plotly_chart(style(fig, height=290).update_yaxes(title="$ per mile", rangemode="tozero"), width="stretch")

# --- 5. Monthly trends (two charts, never a dual axis) ----------------------------------------
section("trends", "5 · Trends", "How did the year develop?", "Trips and revenue per month.")
if len(monthly) > 1:
    busiest_m, quietest_m = monthly.loc[monthly["trips"].idxmax()], monthly.loc[monthly["trips"].idxmin()]
    insight(f"<b>{busiest_m['month_name']}</b> was the busiest month ({int(busiest_m['trips']):,} trips, "
            f"{usd_m(busiest_m['revenue'])}); <b>{quietest_m['month_name']}</b> the quietest "
            f"({int(quietest_m['trips']):,} trips). In 2023 the airport fee rose from {usd(1.25)} to "
            f"{usd(1.75)} in April, and September–November had about 2.5× more zero-distance records, "
            f"which the pipeline rejected (see section 6).")
c1, c2 = st.columns(2)
with c1, card("08"):
    card_title("Trips per month")
    fig = go.Figure(go.Bar(x=monthly["month_name"], y=monthly["trips"], marker=dict(color=BLUE, cornerradius=4),
                           hovertemplate="%{x}: %{y:,} trips<extra></extra>"))
    st.plotly_chart(style(fig, height=260).update_yaxes(title="Trips"), width="stretch")
with c2, card("09"):
    card_title("Revenue per month")
    fig = go.Figure(go.Bar(x=monthly["month_name"], y=monthly["revenue"], marker=dict(color=BLUE, cornerradius=4),
                           hovertemplate="%{x}: $%{y:,.0f}<extra></extra>"))
    st.plotly_chart(style(fig, height=260).update_yaxes(title="Revenue ($)"), width="stretch")

with st.expander("Data tables — every number behind the charts"):
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Trips and fare per mile by hour**")
        st.dataframe(by_hour.drop(columns="hour_key"), hide_index=True, width="stretch", column_config={
            "hour_label": "Hour", "day_part": "Day part",
            "trips": st.column_config.NumberColumn("Trips", format="localized"),
            "fare_per_mile": st.column_config.NumberColumn("Fare per mile", format="dollar")})
    with t2:
        st.markdown("**Monthly**")
        st.dataframe(monthly.drop(columns="month"), hide_index=True, width="stretch", column_config={
            "month_name": "Month",
            "trips": st.column_config.NumberColumn("Trips", format="localized"),
            "revenue": st.column_config.NumberColumn("Revenue", format="dollar"),
            "weighted": st.column_config.NumberColumn("Fare/mile (weighted)", format="dollar"),
            "median": st.column_config.NumberColumn("Fare/mile (median)", format="dollar")})

# --- 6. Data quality & pipeline -------------------------------------------------------------
section("quality", "6 · Data quality", "Can these numbers be trusted?",
        "What the pipeline loaded, what it rejected and why, and the result of every quality check.")
latest_runs = """SELECT DISTINCT ON (source_month) run_id FROM etl_pipeline_runs
                 WHERE status = 'SUCCESS' AND source_month = ANY(:mk) ORDER BY source_month, run_id DESC"""
quality = query(f"""SELECT coalesce(sum(rows_read), 0) AS read, coalesce(sum(rows_loaded), 0) AS loaded,
                           coalesce(sum(rows_rejected), 0) AS rejected,
                           coalesce(sum(rows_deduplicated), 0) AS dedup
                    FROM etl_pipeline_runs WHERE run_id IN ({latest_runs})""", (("mk", month_keys),)).iloc[0]
checks = query(f"""SELECT r.source_month AS month, q.check_name, q.severity, q.passed, q.details
                   FROM etl_quality_results q JOIN etl_pipeline_runs r USING (run_id)
                   WHERE r.run_id IN ({latest_runs})""", (("mk", month_keys),))
warnings = checks[~checks["passed"]].sort_values(["month", "check_name"]) if not checks.empty else checks
reject_rate = 100 * float(quality["rejected"]) / float(quality["read"]) if quality["read"] else float("nan")

q1, q2, q3, q4 = st.columns(4)
kpi(q1, "Rows read", f"{int(quality['read']):,}", "raw records in the source files")
kpi(q2, "Rows loaded", f"{int(quality['loaded']):,}", "accepted into the star schema")
kpi(q3, "Rejected rows", f"{reject_rate:.2f}%",
    f"{int(quality['rejected']):,} rows · {int(quality['dedup']):,} duplicates",
    "Latest successful run of each selected month (all payment types).")
kpi(q4, "Quality warnings", f"{len(warnings)}", "critical checks: all passed" if len(checks) else "no checks yet",
    "QC1–QC6 are critical (a failure rolls the month back); QC7–QC9 are warnings.")

c1, c2 = st.columns([5, 6])
rejects = query(f"""SELECT s.reason, sum(s.row_count) AS row_count
                    FROM etl_rejected_summary s WHERE s.run_id IN ({latest_runs})
                    GROUP BY s.reason ORDER BY row_count""", (("mk", month_keys),))
rejects["row_count"] = rejects["row_count"].astype("int64")
with c1, card("10"):
    card_title("Rejected rows by reason", "Each rejected row is counted once, under the first rule it fails.")
    if not rejects.empty:
        labels = rejects["reason"].str.replace("_", " ").str.capitalize()
        fig = go.Figure(go.Bar(x=rejects["row_count"], y=labels, orientation="h",
                               marker=dict(color=BLUE, cornerradius=4),
                               text=[f"{v:,}" for v in rejects["row_count"]], textposition="outside",
                               cliponaxis=False, textfont=dict(color=BODY),
                               hovertemplate="<b>%{y}</b><br>%{x:,} rows<extra></extra>"))
        fig.update_xaxes(showgrid=True, gridcolor=GRID, title="Rows")
        fig.update_yaxes(showgrid=False)
        st.plotly_chart(style(fig, height=300).update_layout(margin=dict(r=70)), width="stretch")
with c2, card("11"):
    card_title("Quality checks per month", "✅ passed · ⚠️ warning · ❌ failed")
    if not checks.empty:
        status = checks.assign(status=[("✅" if p else ("⚠️" if s == "WARNING" else "❌"))
                                       for p, s in zip(checks["passed"], checks["severity"], strict=True)])
        grid = status.pivot(index="month", columns="check_name", values="status")
        st.dataframe(grid, width="stretch", height=min(460, 38 + 35 * len(grid)),
                     column_config={"month": st.column_config.TextColumn("Month", width="small"),
                                    **{c: st.column_config.TextColumn(c, width=36) for c in grid.columns}})
for _, w in warnings.iterrows():
    st.markdown(f"⚠️ **{w['month']} · {w['check_name']}** — {w['details']}")

with st.expander("Pipeline run history (last 30 runs)"):
    runs = query("""SELECT run_id, source_month, status, started_at, round(duration_seconds) AS seconds,
                           rows_read, rows_loaded, rows_rejected, rows_deduplicated, error_step, error_message
                    FROM etl_pipeline_runs ORDER BY run_id DESC LIMIT 30""")
    failed = int((runs["status"] == "FAILED").sum())
    st.markdown(f"**{len(runs)} runs shown — {failed} failed**" + (" ⚠️" if failed else " ✅"))
    st.dataframe(runs, hide_index=True, width="stretch")

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

st.markdown('<div class="footer">Source: NYC Taxi &amp; Limousine Commission trip records · '
            'Pipeline: Python + Prefect → PostgreSQL star schema · Dashboard: Streamlit + Plotly · '
            'Cover photo: <code>public/image.png</code></div>', unsafe_allow_html=True)
