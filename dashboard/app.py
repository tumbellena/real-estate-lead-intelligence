# Ops dashboard for the lead enrichment pipeline. A separate Streamlit app
# (its own Dockerfile/requirements.txt, no dependency on the app/ package)
# that reads straight from Postgres - the same database the api and worker
# containers write to - rather than calling either of their HTTP APIs. For
# a read-only monitoring view, querying the shared source of truth directly
# is simpler than adding another network hop through an API that already
# has its own job to do.
#
# Auto-refreshes every REFRESH_INTERVAL_SECONDS by sleeping at the end of
# the script and calling st.rerun() - the simplest form of Streamlit
# auto-refresh, at the cost of holding this session's server thread asleep
# between refreshes. Fine for a handful of people watching an internal
# dashboard; a busier multi-viewer dashboard would reach for a JS-timer
# based auto-refresh component instead so the server thread isn't parked.
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

REFRESH_INTERVAL_SECONDS = 5

DATABASE_URL = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")

# Same validated status palette used everywhere a state needs a color in
# this project - never color alone, every swatch below ships with a text
# label right next to it.
COLOR_GOOD = "#0ca30c"
COLOR_WARNING = "#fab219"
COLOR_CRITICAL = "#d03b3b"
COLOR_MUTED = "#898781"
COLOR_SEQUENTIAL = "#2a78d6"
COLOR_SEQUENTIAL_FILL = "rgba(42, 120, 214, 0.1)"  # Plotly's fillcolor wants rgba(), not 8-digit hex
COLOR_TEXT_SECONDARY = "#52514e"

CIRCUIT_STATE_STYLE = {
    "closed": (COLOR_GOOD, "CLOSED"),
    "half_open": (COLOR_WARNING, "HALF-OPEN"),
    "open": (COLOR_CRITICAL, "OPEN"),
}

st.set_page_config(page_title="Lead Enrichment Ops Dashboard", layout="wide")


def _utcnow() -> datetime:
    # Naive UTC, matching every timestamp column in the database (see the
    # naive/aware note in app/circuit_breaker.py in the main app) - a
    # timezone-aware value here would compare unequal to a naive one
    # even at the same instant.
    return datetime.now(timezone.utc).replace(tzinfo=None)


@st.cache_resource
def get_connection():
    """
    One connection, reused across reruns (st.cache_resource persists it
    across this session's auto-refresh cycles) rather than opening and
    closing a fresh one every 5 seconds. is_connection_healthy() below
    covers the case where it's gone stale (idle timeout, DB restart).
    """
    return psycopg2.connect(DATABASE_URL)


def is_connection_healthy(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except psycopg2.Error:
        return False


def fetch_headline_metrics(conn) -> tuple[int, float]:
    today_start = _utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*) FILTER (WHERE status IN ('enriched', 'failed')) AS processed_today,
                count(*) FILTER (WHERE status = 'failed') AS failed_today
            FROM leads
            WHERE updated_at >= %s
            """,
            (today_start,),
        )
        processed_today, failed_today = cur.fetchone()
    error_rate = (failed_today / processed_today * 100) if processed_today else 0.0
    return processed_today, error_rate


def fetch_circuit_states(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT service_name, state FROM circuit_breaker_state ORDER BY service_name")
        return dict(cur.fetchall())


def fetch_enriched_per_minute(conn) -> pd.DataFrame:
    window_start = _utcnow() - timedelta(hours=1)
    query = """
        SELECT date_trunc('minute', updated_at) AS minute, count(*) AS enriched
        FROM leads
        WHERE status = 'enriched' AND updated_at >= %s
        GROUP BY 1
    """
    df = pd.read_sql_query(query, conn, params=(window_start,))
    df["minute"] = pd.to_datetime(df["minute"])

    # Reindex over every minute in the window, not just the ones with
    # activity, so a quiet stretch draws as a flat line at zero instead of
    # a gap - a line chart with holes in it reads as missing data, not "zero."
    full_index = pd.date_range(
        start=window_start.replace(second=0, microsecond=0) + timedelta(minutes=1),
        end=_utcnow().replace(second=0, microsecond=0),
        freq="min",
    )
    df = df.set_index("minute").reindex(full_index, fill_value=0).rename_axis("minute").reset_index()
    return df


def fetch_dlq_items(conn) -> pd.DataFrame:
    query = """
        SELECT service_name, error_message, next_retry_at, attempts, created_at
        FROM dead_letter_queue
        ORDER BY created_at DESC
        LIMIT 10
    """
    return pd.read_sql_query(query, conn)


def fetch_dlq_size(conn) -> int:
    # Every row here represents a lead that was never successfully
    # enriched - a success deletes its row (see process_lead in the
    # worker) - so the table's size is the unresolved count, whether a row
    # is still being retried or has already been given up on.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dead_letter_queue")
        return cur.fetchone()[0]


def fetch_recent_events(conn) -> pd.DataFrame:
    query = """
        SELECT created_at, lead_id, event_type, service_name, message
        FROM enrichment_events
        ORDER BY created_at DESC
        LIMIT 20
    """
    df = pd.read_sql_query(query, conn)
    # psycopg2 hands back lead_id as a Python uuid.UUID; cast to str before
    # it reaches st.dataframe(), which serializes via pyarrow - object-dtype
    # columns holding non-primitive Python objects are a known rough edge there.
    df["lead_id"] = df["lead_id"].astype(str)
    return df


def fetch_automation_enabled(conn) -> bool:
    env_enabled = os.getenv("AUTOMATION_ENABLED", "true").strip().lower() == "true"
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM system_config WHERE key = 'automation_enabled'")
        row = cur.fetchone()
    persisted_value = (row[0] == "true") if row else True
    # Mirrors app/kill_switch.py's is_automation_enabled(): off if EITHER
    # the env var forces it off OR the runtime-toggled value says so.
    return env_enabled and persisted_value


def status_pill_html(label: str, color: str, text: str) -> str:
    return f"""
    <div style="display:flex; align-items:center; gap:10px; padding:14px 16px;
                border:1px solid rgba(11,11,11,0.10); border-radius:10px;
                background:#fcfcfb;">
      <div style="width:18px; height:18px; border-radius:50%; background:{color};
                  flex-shrink:0; box-shadow:0 0 0 3px {color}22;"></div>
      <div>
        <div style="font-weight:600; font-size:14px; color:#0b0b0b;">{label}</div>
        <div style="font-size:12px; color:{COLOR_TEXT_SECONDARY};">{text}</div>
      </div>
    </div>
    """


def banner_html(enabled: bool) -> str:
    if enabled:
        color, text = COLOR_GOOD, "Automation Enabled"
    else:
        color, text = COLOR_CRITICAL, "AUTOMATION HALTED"
    return f"""
    <div style="padding:14px 20px; border-radius:10px; background:{color}1a;
                border:1px solid {color}55; display:flex; align-items:center; gap:10px;">
      <div style="width:14px; height:14px; border-radius:50%; background:{color}; flex-shrink:0;"></div>
      <span style="font-weight:700; font-size:16px; color:{color};">{text}</span>
    </div>
    """


def render() -> None:
    st.title("Lead Enrichment Ops Dashboard")

    conn = get_connection()
    if not is_connection_healthy(conn):
        # The cached connection died (DB restart, idle timeout) - drop it
        # from the cache and get a fresh one instead of failing this cycle.
        get_connection.clear()
        conn = get_connection()

    processed_today, error_rate = fetch_headline_metrics(conn)
    circuit_states = fetch_circuit_states(conn)
    enriched_per_minute = fetch_enriched_per_minute(conn)
    dlq_items = fetch_dlq_items(conn)
    dlq_size = fetch_dlq_size(conn)
    recent_events = fetch_recent_events(conn)
    automation_enabled = fetch_automation_enabled(conn)

    # --- 1. Headline numbers ---------------------------------------------
    col1, col2 = st.columns(2)
    col1.metric("Leads Processed Today", processed_today)
    col2.metric("Current Error Rate", f"{error_rate:.1f}%")

    st.divider()

    # --- 2. Circuit breaker indicators ------------------------------------
    st.subheader("Circuit Breakers")
    services = ["property_api", "phone_api", "claude", "crm_webhook"]
    cols = st.columns(4)
    for col, service_name in zip(cols, services):
        state = circuit_states.get(service_name)
        color, text = CIRCUIT_STATE_STYLE.get(state, (COLOR_MUTED, "UNKNOWN"))
        with col:
            st.markdown(status_pill_html(service_name, color, text), unsafe_allow_html=True)

    st.divider()

    # --- 3. Leads enriched over the last hour -----------------------------
    st.subheader("Leads Enriched — Last Hour")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=enriched_per_minute["minute"],
            y=enriched_per_minute["enriched"],
            mode="lines",
            line=dict(color=COLOR_SEQUENTIAL, width=2, shape="linear"),
            fill="tozeroy",
            fillcolor=COLOR_SEQUENTIAL_FILL,
            hovertemplate="%{x|%H:%M}<br>%{y} enriched<extra></extra>",
        )
    )
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(title=None, gridcolor="#e1e0d9"),
        yaxis=dict(title="Leads enriched", gridcolor="#e1e0d9", rangemode="tozero"),
        plot_bgcolor="#fcfcfb",
        paper_bgcolor="#fcfcfb",
        showlegend=False,
    )
    st.plotly_chart(fig, width='stretch')

    st.divider()

    # --- 4. Dead letter queue -----------------------------------------------
    st.subheader(f"Dead Letter Queue ({dlq_size} unresolved)")
    if dlq_items.empty:
        st.caption("Nothing in the DLQ right now.")
    else:
        display_df = dlq_items.rename(
            columns={
                "service_name": "Service",
                "error_message": "Error",
                "next_retry_at": "Next Retry At",
                "attempts": "Attempts",
                "created_at": "First Failed At",
            }
        )
        st.dataframe(display_df, hide_index=True, width='stretch')

    st.divider()

    # --- 5. Live-updating log tail -------------------------------------------
    st.subheader("Recent Enrichment Events")
    if recent_events.empty:
        st.caption("No events yet.")
    else:
        display_events = recent_events.rename(
            columns={
                "created_at": "Time",
                "lead_id": "Lead ID",
                "event_type": "Event",
                "service_name": "Service",
                "message": "Message",
            }
        )
        st.dataframe(display_events, hide_index=True, width='stretch')

    st.divider()

    # --- 6. Kill switch status ------------------------------------------------
    st.markdown(banner_html(automation_enabled), unsafe_allow_html=True)

    st.caption(
        f"Last updated {_utcnow().strftime('%H:%M:%S')} UTC · "
        f"auto-refreshing every {REFRESH_INTERVAL_SECONDS}s"
    )


try:
    render()
except Exception as exc:
    st.error(f"Dashboard failed to load: {exc}")

time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()
