"""Streamlit dashboard: Overview (all data), One day (the six reports), Live (auto-refreshing).

Reports read the views; Overview and Live read fact_event, which covers every event type.
Aggregation stays in SQL — a wrong number means a wrong view, not a wrong chart.

Usage:
    poetry run streamlit run apps/dashboard/src/app.py
"""
import altair as alt
import pandas as pd
import streamlit as st

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient

st.set_page_config(page_title="Event stream analytics", page_icon="📊", layout="wide")


def run(sql: str, params: tuple | None = None) -> pd.DataFrame:
    with PostgresClient(settings) as pg:
        rows, columns = pg.fetch_all(sql, params)
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=60)
def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """60s TTL matches the job's trigger. Live tab bypasses this on purpose."""
    return run(sql, params)


def ranking(df: pd.DataFrame, label: str, value: str = "view_count", height: int = 320) -> None:
    """Horizontal bar sorted by value. Altair because st.bar_chart sorts alphabetically."""
    if df.empty:
        st.info("No data.")
        return
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{value}:Q", title=None),
            y=alt.Y(f"{label}:N", sort="-x", title=None),
            tooltip=list(df.columns),
        )
        .properties(width="container", height=height)
    )
    st.altair_chart(chart)


def hourly(df: pd.DataFrame, value: str = "view_count") -> None:
    """Full 0-23 day so quiet hours read as zero. Expect few bars: one report_date only
    spans 30-90 minutes of event time."""
    if df.empty:
        st.info("No data.")
        return
    full_day = (
        df.set_index("hour")[value]
        .reindex(range(24), fill_value=0)
        .rename_axis("hour")
        .reset_index()
    )
    chart = (
        alt.Chart(full_day)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="hour (UTC)"),
            y=alt.Y(f"{value}:Q", title=None),
            tooltip=["hour", value],
        )
        .properties(width="container", height=260)
    )
    st.altair_chart(chart)


def stacked_hourly(df: pd.DataFrame, color: str, value: str = "events") -> None:
    """Hour-of-day stacked bar. Only for low-cardinality splits — stacking 12 browsers is
    unreadable. x pinned to 0-23 so missing hours show as gaps."""
    if df.empty:
        st.info("No data.")
        return
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="hour (UTC)", scale=alt.Scale(domain=list(range(24)))),
            y=alt.Y(f"{value}:Q", title=None, stack=True),
            color=alt.Color(f"{color}:N", title=None),
            tooltip=["hour", color, value],
        )
        .properties(width="container", height=300)
    )
    st.altair_chart(chart)


st.title("Event stream analytics")

if run("SELECT COUNT(*) AS n FROM fact_event").at[0, "n"] == 0:
    st.warning("fact_event is empty — start the ingestion bridge and the streaming job first.")
    st.stop()

tab_overview, tab_day, tab_live = st.tabs(["Overview", "One day", "Live"])

# ══════════════════════════════════════════════════════════════════════════
# Overview — everything, no date filter
# ══════════════════════════════════════════════════════════════════════════
with tab_overview:
    totals = query(
        """
        SELECT COUNT(*) AS events, COUNT(DISTINCT event_type) AS event_types,
               COUNT(DISTINCT device_id) AS devices, COUNT(DISTINCT report_date) AS days,
               MIN(report_date) AS first_day, MAX(report_date) AS last_day
        FROM fact_event
        """
    )
    row = totals.iloc[0]

    a, b, c, d = st.columns(4)
    a.metric("Events", f"{row.events:,}")
    b.metric("Event types", f"{row.event_types}")
    c.metric("Devices", f"{row.devices:,}")
    d.metric("Days covered", f"{row.days}", help=f"{row.first_day} → {row.last_day}")

    st.subheader("Events per day")
    ranking(
        query(
            "SELECT report_date::text AS day, COUNT(*) AS events "
            "FROM fact_event GROUP BY 1 ORDER BY 1"
        ),
        "day",
        "events",
        height=160,
    )

    st.subheader("Device mix through the day")
    st.caption(
        "All days combined — a single report_date only spans 30–90 minutes, so pooling them is "
        "what gives this chart more than one bar."
    )
    stacked_hourly(
        query(
            """
            SELECT f.hour, COALESCE(dv.device_category, 'unknown') AS device_category,
                   COUNT(*) AS events
            FROM fact_event f LEFT JOIN dim_device dv ON f.device_key = dv.device_key
            GROUP BY 1, 2 ORDER BY 1
            """
        ),
        "device_category",
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Event types")
        ranking(
            query(
                "SELECT event_type, COUNT(*) AS events FROM fact_event "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 15"
            ),
            "event_type",
            "events",
            height=420,
        )
    with right:
        st.subheader("Where visitors actually are")
        st.caption("From the IP, not the storefront's domain.")
        ranking(
            query(
                """
                SELECT dl.country_name AS country, COUNT(*) AS events
                FROM fact_event f JOIN dim_location dl ON f.location_key = dl.location_key
                WHERE dl.country_name IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 15
                """
            ),
            "country",
            "events",
            height=420,
        )

    st.subheader("Storefront locale vs real location")
    border = query(
        """
        SELECT CASE WHEN is_cross_border THEN 'Different country'
                    WHEN is_cross_border IS FALSE THEN 'Same country'
                    ELSE 'Unknown' END AS bucket,
               COUNT(*) AS events
        FROM v_product_view_enriched GROUP BY 1 ORDER BY 2 DESC
        """
    )
    ranking(border, "bucket", "events", height=140)

# ══════════════════════════════════════════════════════════════════════════
# One day — the six reports
# ══════════════════════════════════════════════════════════════════════════
with tab_day:
    dates = query("SELECT DISTINCT report_date FROM fact_event ORDER BY 1 DESC")

    controls, _ = st.columns([2, 3])
    with controls:
        report_date = st.selectbox("Report date", dates["report_date"], format_func=str)
        top_n = st.slider("Top N", min_value=5, max_value=50, value=10, step=5)
    st.caption(
        "The views expose report_date instead of hard-coding today, so any past day works here."
    )

    day_totals = query(
        """
        SELECT COUNT(*) AS events, COUNT(DISTINCT product_id) AS products,
               COUNT(DISTINCT device_id) AS visitors
        FROM v_product_view_enriched WHERE report_date = %s
        """,
        (report_date,),
    )
    a, b, c = st.columns(3)
    a.metric("Product views", f"{day_totals.at[0, 'events']:,}")
    b.metric("Distinct products", f"{day_totals.at[0, 'products']:,}")
    c.metric("Distinct devices", f"{day_totals.at[0, 'visitors']:,}")

    # What was viewed, and from which storefront
    left, right = st.columns(2)
    with left:
        st.subheader(f"Top {top_n} products")
        products = query(
            "SELECT product_id, product_name, view_count FROM v_top_products_today "
            "WHERE report_date = %s",
            (report_date,),
        )
        products["label"] = products["product_name"].fillna(products["product_id"])
        ranking(products.head(top_n), "label")
    with right:
        st.subheader("Views by storefront locale")
        countries = query(
            "SELECT country_domain, country_name, view_count FROM v_top_countries_today "
            "WHERE report_date = %s",
            (report_date,),
        )
        countries["label"] = countries["country_name"].fillna(countries["country_domain"])
        ranking(countries.head(top_n), "label")

    # Where the traffic came from, and which store served it
    left, right = st.columns(2)
    with left:
        st.subheader(f"Top {top_n} referrers")
        ranking(
            query(
                "SELECT referrer_url, view_count FROM v_top_referrers_today "
                "WHERE report_date = %s",
                (report_date,),
            ).head(top_n),
            "referrer_url",
        )
    with right:
        st.subheader("Stores by country")
        st.dataframe(
            query(
                "SELECT country_domain, country_name, store_id, view_count "
                "FROM v_stores_by_country WHERE report_date = %s",
                (report_date,),
            ),
            width="stretch",
            hide_index=True,
            height=320,
        )

    # When the views happened
    st.subheader("Views by hour")
    scope = st.radio(
        "Scope", ["All products", "One product"], horizontal=True, label_visibility="collapsed"
    )
    if scope == "All products":
        hourly(
            query(
                "SELECT hour, SUM(view_count)::bigint AS view_count FROM v_product_hourly "
                "WHERE report_date = %s GROUP BY 1 ORDER BY 1",
                (report_date,),
            )
        )
    elif products.empty:
        st.info("No products viewed on this date.")
    else:
        choices = products.head(top_n)
        picked = st.selectbox(
            "Product",
            choices["product_id"],
            format_func=lambda pid: choices.loc[choices["product_id"] == pid, "label"].iat[0],
        )
        hourly(
            query(
                "SELECT hour, view_count FROM v_product_hourly "
                "WHERE report_date = %s AND product_id = %s ORDER BY hour",
                (report_date, picked),
            )
        )

    # Two flat charts — a stacked browser-by-hour bar is unreadable.
    st.subheader("Devices")
    devices = query(
        "SELECT browser, os, device_category, SUM(view_count)::bigint AS view_count "
        "FROM v_device_hourly WHERE report_date = %s GROUP BY 1,2,3",
        (report_date,),
    )
    if devices.empty:
        st.info("No data for this date.")
    else:
        left, right = st.columns(2)
        with left:
            st.caption("By category")
            ranking(
                devices.groupby("device_category", as_index=False)["view_count"].sum(),
                "device_category",
                height=180,
            )
        with right:
            st.caption("Top browsers")
            ranking(
                devices.groupby("browser", as_index=False)["view_count"]
                .sum()
                .nlargest(10, "view_count"),
                "browser",
                height=300,
            )
        with st.expander("Browser × OS detail"):
            st.dataframe(
                devices.sort_values("view_count", ascending=False),
                width="stretch",
                hide_index=True,
            )

# ══════════════════════════════════════════════════════════════════════════
# Live — bypasses the cache so the stream is visibly moving
# ══════════════════════════════════════════════════════════════════════════
with tab_live:
    st.caption("Refreshes every 5 seconds. The streaming job commits a batch every 60s, so the "
               "counters step up rather than climb smoothly.")

    @st.fragment(run_every=5)
    def live_panel():
        pulse = run(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE ingested_at > NOW() - INTERVAL '1 minute') AS last_min,
                   COUNT(*) FILTER (WHERE ingested_at > NOW() - INTERVAL '5 minutes') AS last_5min,
                   MAX(ingested_at) AS newest
            FROM fact_event
            """
        ).iloc[0]

        a, b, c, d = st.columns(4)
        a.metric("Rows in fact_event", f"{pulse.total:,}")
        b.metric("Last minute", f"{pulse.last_min:,}")
        c.metric("Last 5 minutes", f"{pulse.last_5min:,}")
        d.metric(
            "Newest row",
            pulse.newest.strftime("%H:%M:%S") if pd.notna(pulse.newest) else "—",
            help="ingested_at of the most recent row, UTC",
        )

        st.subheader("Latest events")
        st.dataframe(
            run(
                """
                SELECT f.ingested_at, f.event_timestamp, f.event_type,
                       dp.product_name, ds.country_domain AS site,
                       dl.country_name AS ip_country, dv.browser, dv.device_category
                FROM fact_event f
                LEFT JOIN dim_product  dp ON f.product_key  = dp.product_key
                LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
                LEFT JOIN dim_location dl ON f.location_key = dl.location_key
                LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
                ORDER BY f.event_key DESC
                LIMIT 25
                """
            ),
            width="stretch",
            hide_index=True,
            height=460,
        )

    live_panel()
