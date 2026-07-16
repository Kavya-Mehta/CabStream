import gradio as gr
import anthropic
import duckdb
import pandas as pd
import plotly.express as px
import os
import re
from dotenv import load_dotenv

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────
ADLS_ACCOUNT = "cabstreamdata"
MODEL = "claude-sonnet-4-6"
MAX_LIMIT = 1000

GOLD_PATHS = {
    "fact_trips_yellow": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/fact_trips_yellow",
    "fact_trips_fhvhv": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/fact_trips_fhvhv",
    "dim_time": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/dim_time",
    "dim_zone": f"abfss://delta@{ADLS_ACCOUNT}.dfs.core.windows.net/gold/dim_zone",
}

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

EXAMPLES = [
    "Show yellow taxi trips month by month in 2020",
    "Compare Uber and yellow taxi trip counts year by year 2019-2024",
    "Which borough had the most trips in April 2020?",
    "What percentage of 2024 NYC trips were rideshare vs yellow taxi?",
    "Top 10 pickup zones in Manhattan by trip count",
    "Average fare on rainy days vs dry days for yellow taxis",
    "Which hours have the highest demand on weekends?",
    "Compare Uber vs Lyft trip counts by year",
]

# ── Backend ───────────────────────────────────────────────────────────────────
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

# Initialize connection once
conn = get_connection()

def validate_sql(sql):
    if not sql.strip().upper().startswith("SELECT"):
        return False, "Only SELECT statements allowed"
    for kw in {"INSERT","UPDATE","DELETE","DROP","CREATE","ALTER","TRUNCATE"}:
        if re.search(r'\b' + kw + r'\b', sql.upper()):
            return False, f"Blocked keyword: {kw}"
    if "LIMIT" not in sql.upper():
        return False, "Missing LIMIT clause"
    m = re.search(r'\bLIMIT\s+(\d+)', sql.upper())
    if m and int(m.group(1)) > MAX_LIMIT:
        return False, f"LIMIT exceeds {MAX_LIMIT}"
    return True, ""

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

def query(question):
    if not question.strip():
        return None, "", "Please enter a question.", None

    try:
        sql, explanation = generate_sql(question)
        valid, err = validate_sql(sql)

        if not valid:
            return None, sql, f"🛡️ Guardrail blocked: {err}", None

        df = conn.execute(sql).df()

        # Build chart
        fig = None
        num = df.select_dtypes(include="number").columns.tolist()
        cat = df.select_dtypes(exclude="number").columns.tolist()

        if num and cat and len(df) > 1:
            fig = px.bar(
                df, x=cat[0], y=num[0],
                title=question,
                color_discrete_sequence=["#F7C948"],
                template="plotly_dark"
            )
            fig.update_layout(
                paper_bgcolor="#0F1117",
                plot_bgcolor="#0F1117",
                font=dict(color="#AAAAAA", size=11),
                title_font=dict(color="#FFFFFF", size=13),
                margin=dict(l=0, r=0, t=40, b=0),
                xaxis=dict(gridcolor="#1C2235"),
                yaxis=dict(gridcolor="#1C2235"),
            )

        result_text = f"✅ **{len(df):,} results** · {explanation}" if explanation else f"✅ **{len(df):,} results**"

        return df, f"```sql\n{sql}\n```", result_text, fig

    except Exception as e:
        return None, "", f"❌ Error: {str(e)}", None


# ── Gradio UI ─────────────────────────────────────────────────────────────────
css = """
#title { text-align: center; }
#title h1 { font-size: 2.5rem; font-weight: 900; color: #F7C948; }
#title p { color: #888; font-size: 1rem; }
.stat-row { display: flex; gap: 12px; margin: 16px 0; }
.stat { background: #0F1420; border: 1px solid #1C2235; border-radius: 8px; 
        padding: 14px 20px; flex: 1; text-align: center; }
.stat.accent { border-color: #F7C948; }
.stat-val { font-size: 1.8rem; font-weight: 800; color: #F7C948; }
.stat-lbl { font-size: 0.7rem; color: #555; text-transform: uppercase; 
            letter-spacing: 1.5px; margin-top: 4px; }
footer { visibility: hidden; }
"""

with gr.Blocks(
    theme=gr.themes.Base(
        primary_hue=gr.themes.colors.yellow,
        neutral_hue=gr.themes.colors.slate,
    ).set(
        body_background_fill="#080B14",
        body_text_color="#E8EAF0",
        block_background_fill="#0F1420",
        block_border_color="#1C2235",
        input_background_fill="#0F1420",
        button_primary_background_fill="#F7C948",
        button_primary_text_color="#080B14",
        button_primary_background_fill_hover="#E8B800",
    ),
    css=css,
    title="CabStream — NYC Taxi Intelligence"
) as demo:

    # Header
    gr.HTML("""
    <div id="title">
        <h1>🚕 CabStream</h1>
        <p>Ask anything about <strong>1.57 billion NYC taxi and rideshare trips</strong> · 2019–2025</p>
    </div>
    <div class="stat-row">
        <div class="stat accent">
            <div class="stat-val">1.57B</div>
            <div class="stat-lbl">Total trips</div>
        </div>
        <div class="stat">
            <div class="stat-val" style="color:#E8EAF0">−96.6%</div>
            <div class="stat-lbl">COVID collapse · Apr 2020</div>
        </div>
        <div class="stat">
            <div class="stat-val">86.9%</div>
            <div class="stat-lbl">Rideshare share · 2024</div>
        </div>
        <div class="stat">
            <div class="stat-val" style="color:#E8EAF0">7 yrs</div>
            <div class="stat-lbl">2019 through 2025</div>
        </div>
        <div class="stat">
            <div class="stat-val" style="color:#E8EAF0">265</div>
            <div class="stat-lbl">NYC zones mapped</div>
        </div>
    </div>
    """)

    # Input
    with gr.Row():
        question_box = gr.Textbox(
            placeholder="e.g. How did COVID affect yellow taxi demand month by month in 2020?",
            label="Natural language query",
            scale=5
        )
        run_btn = gr.Button("Run →", variant="primary", scale=1)

    # Examples
    gr.Examples(
        examples=EXAMPLES,
        inputs=question_box,
        label="Example questions — click to run"
    )

    # Outputs
    status = gr.Markdown()
    
    with gr.Row():
        with gr.Column(scale=3):
            result_table = gr.Dataframe(label="Results", wrap=True)
            result_chart = gr.Plot(label="Chart")
        with gr.Column(scale=2):
            sql_output = gr.Markdown(label="Generated SQL")

    # Wire up
    run_btn.click(
        fn=query,
        inputs=question_box,
        outputs=[result_table, sql_output, status, result_chart]
    )
    question_box.submit(
        fn=query,
        inputs=question_box,
        outputs=[result_table, sql_output, status, result_chart]
    )

demo.launch()