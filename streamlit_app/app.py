import streamlit as st
import anthropic
import duckdb
import pandas as pd
import plotly.express as px
import os
import re
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="CabStream",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.main .block-container {
    padding: 0 2rem 2rem 2rem;
    max-width: 1400px;
}

/* Top bar */
.topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 20px 0 30px 0;
    border-bottom: 1px solid #1C2235;
    margin-bottom: 32px;
}
.topbar-logo {
    font-size: 1.1rem;
    font-weight: 800;
    letter-spacing: -0.5px;
    color: #E8EAF0;
}
.topbar-logo span { color: #F7C948; }
.topbar-tag {
    font-size: 0.72rem;
    color: #4A5270;
    letter-spacing: 2px;
    text-transform: uppercase;
    font-weight: 600;
}

/* Stat strip */
.stat-strip {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 12px;
    margin-bottom: 36px;
}
.stat-cell {
    background: #0F1420;
    border: 1px solid #1C2235;
    border-radius: 8px;
    padding: 16px 20px;
}
.stat-cell.accent { border-color: #F7C948; }
.stat-val {
    font-size: 1.9rem;
    font-weight: 800;
    color: #F7C948;
    letter-spacing: -1px;
    line-height: 1;
    margin: 0;
}
.stat-val.dim { color: #E8EAF0; }
.stat-lbl {
    font-size: 0.72rem;
    color: #4A5270;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    font-weight: 600;
    margin-top: 6px;
}

/* Query input */
.query-section { margin-bottom: 28px; }
.query-label {
    font-size: 0.72rem;
    color: #4A5270;
    text-transform: uppercase;
    letter-spacing: 2px;
    font-weight: 600;
    margin-bottom: 10px;
}

/* Story cards */
.story-group { margin-bottom: 24px; }
.story-group-label {
    font-size: 0.68rem;
    color: #F7C948;
    text-transform: uppercase;
    letter-spacing: 2.5px;
    font-weight: 700;
    margin-bottom: 8px;
    padding-bottom: 6px;
    border-bottom: 1px solid #1C2235;
}

/* Result area */
.result-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
}
.result-count {
    font-size: 0.75rem;
    color: #4A5270;
    letter-spacing: 1px;
    font-weight: 600;
}
.result-count span {
    color: #F7C948;
    font-weight: 700;
}

/* SQL terminal */
.sql-terminal {
    background: #0A0D16;
    border: 1px solid #1C2235;
    border-radius: 8px;
    padding: 16px 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: #6B7FA8;
    line-height: 1.7;
    margin-top: 16px;
    position: relative;
}
.sql-terminal::before {
    content: 'SQL';
    position: absolute;
    top: -8px;
    left: 12px;
    background: #0A0D16;
    color: #F7C948;
    font-size: 0.6rem;
    letter-spacing: 2px;
    font-weight: 700;
    padding: 0 6px;
    font-family: 'Inter', sans-serif;
}

/* Insight box */
.insight-box {
    background: #0F1420;
    border-left: 3px solid #F7C948;
    border-radius: 0 8px 8px 0;
    padding: 14px 18px;
    font-size: 0.88rem;
    color: #A0ACCA;
    line-height: 1.6;
    margin-top: 16px;
}

/* Button overrides */
div[data-testid="stButton"] > button {
    background: #F7C948 !important;
    color: #080B14 !important;
    font-weight: 700 !important;
    border: none !important;
    border-radius: 6px !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.5px !important;
    padding: 8px 24px !important;
    font-family: 'Inter', sans-serif !important;
    transition: opacity 0.15s !important;
}
div[data-testid="stButton"] > button:hover {
    opacity: 0.85 !important;
}

/* Story button — secondary style */
div[data-testid="stButton"].story-btn > button {
    background: transparent !important;
    color: #6B7FA8 !important;
    border: 1px solid #1C2235 !important;
    font-weight: 400 !important;
    font-size: 0.82rem !important;
    text-align: left !important;
    padding: 10px 14px !important;
}
div[data-testid="stButton"].story-btn > button:hover {
    border-color: #F7C948 !important;
    color: #E8EAF0 !important;
    opacity: 1 !important;
}

/* Dataframe */
div[data-testid="stDataFrame"] {
    border: 1px solid #1C2235;
    border-radius: 8px;
    overflow: hidden;
}

/* Input */
div[data-testid="stTextInput"] input {
    background: #0F1420 !important;
    border: 1px solid #1C2235 !important;
    border-radius: 6px !important;
    color: #E8EAF0 !important;
    font-size: 0.9rem !important;
    padding: 12px 16px !important;
    font-family: 'Inter', sans-serif !important;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #F7C948 !important;
    box-shadow: 0 0 0 1px #F7C94820 !important;
}

/* Hide streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
ADLS_ACCOUNT = "cabstreamdata"
MODEL = "claude-sonnet-4-6"

GOLD_PATHS = {
    "fact_trips_yellow": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/fact_trips_yellow",
    "fact_trips_fhvhv": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/fact_trips_fhvhv",
    "dim_time": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/dim_time",
    "dim_zone": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/dim_zone",
}

MAX_LIMIT = 1000

SCHEMA_CONTEXT = """
You are a SQL expert for a NYC taxi analytics platform with 1.57 billion trips (2019-2025).

Tables:
- fact_trips_yellow (252M rows): Yellow taxi trips. Columns: date_key, pickup_location_id, dropoff_location_id, pickup_hour, year, month, trip_distance, fare_amount, tip_amount, total_amount, passenger_count, payment_type, is_weekend, is_rush_hour, weather_temp_c, weather_wind_kmh, weather_precip_mm
- fact_trips_fhvhv (1.295B rows): Uber/Lyft/Via/Juno trips. Columns: date_key, pickup_location_id, dropoff_location_id, pickup_hour, year, month, trip_distance, fare_amount, tip_amount, driver_pay, trip_time, company (Uber/Lyft/Via/Juno), is_weekend, is_rush_hour
- dim_time (2,557 rows): Date dimension. Columns: date_key, date, year, month, day, day_of_week, day_name, quarter, is_weekend, year_month
- dim_zone (265 rows): NYC zones. Columns: location_id, borough, zone, service_zone

Join: fact.pickup_location_id = dim_zone.location_id | fact.date_key = dim_time.date_key
Borough values: Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR
COVID period: year=2020, month=4

RULES:
- Output ONLY raw SQL on the first line, nothing else
- Always include LIMIT (max 1000)
- SELECT only
- Second line blank, then plain English explanation
"""

QUESTION_GROUPS = {
    "COVID COLLAPSE": [
        "Show yellow taxi trips month by month in 2020",
        "Compare April 2019 vs April 2020 by borough",
        "How long did recovery take after April 2020?",
    ],
    "UBER VS YELLOW": [
        "Compare Uber and yellow taxi by year 2019–2024",
        "What % of 2024 trips were rideshare vs yellow?",
        "Which company had most trips in 2023?",
    ],
    "WEATHER IMPACT": [
        "Do trips increase when it rains?",
        "Average fare on cold vs warm days in Manhattan",
        "How does wind speed affect tip amounts?",
    ],
    "BOROUGH PATTERNS": [
        "Which borough generates most yellow taxi revenue?",
        "Top 10 pickup zones in Manhattan",
        "Average trip distance from JFK vs LaGuardia",
    ],
}


# ── Backend ───────────────────────────────────────────────────────────────────
def validate_sql(sql):
    if not sql.strip().upper().startswith("SELECT"):
        return False, "Only SELECT statements allowed"
    for kw in {"INSERT","UPDATE","DELETE","DROP","CREATE","ALTER","TRUNCATE"}:
        if re.search(r'\b' + kw + r'\b', sql.upper()):
            return False, f"Blocked: {kw}"
    if "LIMIT" not in sql.upper():
        return False, "Missing LIMIT clause"
    m = re.search(r'\bLIMIT\s+(\d+)', sql.upper())
    if m and int(m.group(1)) > MAX_LIMIT:
        return False, f"LIMIT exceeds {MAX_LIMIT}"
    return True, ""


@st.cache_resource
def get_connection():
    conn = duckdb.connect()
    conn.execute("INSTALL azure; LOAD azure;")
    key = os.getenv("ADLS_STORAGE_KEY")
    conn.execute(f"""
        CREATE SECRET azure_secret (
            TYPE AZURE,
            CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=cabstreamdata;AccountKey={key};EndpointSuffix=core.windows.net'
        )
    """)
    for table, path in GOLD_PATHS.items():
        conn.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM delta_scan('{path}')")
    return conn


def generate_sql(question):
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    resp = client.messages.create(
        model=MODEL, max_tokens=1000,
        system=SCHEMA_CONTEXT,
        messages=[{"role": "user", "content": f"Question: {question}"}]
    )
    raw = re.sub(r'```sql\s*|```\s*', '', resp.content[0].text.strip(), flags=re.IGNORECASE).strip()
    lines = [l for l in raw.split("\n") if l.strip()]
    sql = lines[0].strip().rstrip(";") if lines else ""
    explanation = " ".join(lines[2:]) if len(lines) > 2 else ""
    return sql, explanation


# ── TOP BAR ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="topbar">
    <div class="topbar-logo">🚕 <span>CabStream</span></div>
    <div class="topbar-tag">NYC Taxi Intelligence · 1.57B Trips · 2019–2025</div>
</div>
""", unsafe_allow_html=True)

# ── STAT STRIP ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="stat-strip">
    <div class="stat-cell accent">
        <p class="stat-val">1.57B</p>
        <p class="stat-lbl">Total trips analyzed</p>
    </div>
    <div class="stat-cell">
        <p class="stat-val">−96.6%</p>
        <p class="stat-lbl">Demand drop · Apr 2020</p>
    </div>
    <div class="stat-cell">
        <p class="stat-val">86.9%</p>
        <p class="stat-lbl">Rideshare share · 2024</p>
    </div>
    <div class="stat-cell">
        <p class="stat-val dim">7 yrs</p>
        <p class="stat-lbl">2019 through 2025</p>
    </div>
    <div class="stat-cell">
        <p class="stat-val dim">265</p>
        <p class="stat-lbl">NYC taxi zones mapped</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── MAIN COLUMNS ──────────────────────────────────────────────────────────────
query_col, story_col = st.columns([3, 1], gap="large")

with query_col:
    st.markdown('<p class="query-label">Natural language query</p>', unsafe_allow_html=True)

    if "active_question" not in st.session_state:
        st.session_state.active_question = ""

    typed = st.text_input(
        "query",
        value=st.session_state.active_question,
        placeholder="Ask anything about NYC taxi data...",
        label_visibility="collapsed"
    )

    ask = st.button("Run query →")

    question = typed or st.session_state.active_question

    if ask and question:
        with st.spinner(""):
            try:
                sql, explanation = generate_sql(question)
                valid, err = validate_sql(sql)

                if not valid:
                    st.error(f"🛡️ Guardrail blocked: {err}")
                    st.markdown(f'<div class="sql-terminal">{sql}</div>', unsafe_allow_html=True)
                else:
                    conn = get_connection()
                    df = conn.execute(sql).df()

                    st.markdown(f"""
                    <div class="result-header">
                        <span class="result-count">RESULTS <span>{len(df):,} rows</span></span>
                    </div>
                    """, unsafe_allow_html=True)

                    st.dataframe(df, use_container_width=True, height=220)

                    # Chart
                    num = df.select_dtypes(include="number").columns.tolist()
                    cat = df.select_dtypes(exclude="number").columns.tolist()

                    if num and cat:
                        fig = px.bar(
                            df, x=cat[0], y=num[0],
                            template="plotly_dark",
                            color_discrete_sequence=["#F7C948"]
                        )
                        fig.update_layout(
                            paper_bgcolor="#080B14",
                            plot_bgcolor="#080B14",
                            font=dict(family="Inter", color="#6B7FA8", size=11),
                            title=dict(text=question, font=dict(color="#E8EAF0", size=13)),
                            xaxis=dict(gridcolor="#1C2235", tickfont=dict(color="#4A5270")),
                            yaxis=dict(gridcolor="#1C2235", tickfont=dict(color="#4A5270")),
                            margin=dict(l=0, r=0, t=40, b=0),
                            showlegend=False
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    elif len(num) >= 2:
                        fig = px.line(
                            df, y=num[:2],
                            template="plotly_dark",
                            color_discrete_sequence=["#F7C948", "#3B82F6"]
                        )
                        fig.update_layout(
                            paper_bgcolor="#080B14",
                            plot_bgcolor="#080B14",
                            font=dict(family="Inter", color="#6B7FA8"),
                            margin=dict(l=0, r=0, t=20, b=0)
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    st.markdown(f'<div class="sql-terminal">{sql}</div>', unsafe_allow_html=True)

                    if explanation:
                        st.markdown(f'<div class="insight-box">💡 {explanation}</div>', unsafe_allow_html=True)

            except Exception as e:
                st.error(str(e))

with story_col:
    for group, questions in QUESTION_GROUPS.items():
        st.markdown(f'<div class="story-group-label">{group}</div>', unsafe_allow_html=True)
        for q in questions:
            if st.button(q, key=q, use_container_width=True):
                st.session_state.active_question = q
                st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)