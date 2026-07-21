from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv
import duckdb
import os
import anthropic
import re
import pandas as pd

load_dotenv(Path(__file__).parent.parent.parent / ".env")

from app.guardrails import validate_sql, enforce_limit, sanitize_sql

db = None
MODEL = "claude-sonnet-4-6"


def find_html():
    candidates = [
        Path(__file__).parent.parent / "static" / "index.html",
        Path(__file__).parent.parent / "index.html",
        Path(__file__).parent.parent.parent / "streamlit_app" / "index.html",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


INTENT_MAP = {
    "covid_monthly": {
        "table": "answer_covid_monthly",
        "keywords": ["covid", "2020", "monthly", "month by month", "pandemic", "lockdown", "decline", "collapse"],
        "description": "Monthly yellow taxi trips in 2020 showing COVID impact"
    },
    "borough_recovery": {
        "table": "answer_borough_recovery",
        "keywords": ["recovery", "fastest recovery", "bounce back", "q3 2019", "q3 2020", "borough recover", "recover after covid"],
        "description": "Borough-level COVID recovery comparing Q3 2019 vs Q3 2020 vs Q3 2021"
    },
    "rideshare_vs_yellow": {
        "table": "answer_rideshare_vs_yellow",
        "keywords": ["rideshare", "uber vs yellow", "yellow vs uber", "market share", "dominance", "percentage of trips", "rideshare vs yellow", "uber and yellow"],
        "description": "Year-by-year comparison of rideshare vs yellow taxi trips and market share"
    },
    "top_zones": {
        "table": "answer_top_zones",
        "keywords": ["top zones", "pickup zones", "most popular", "busiest zone", "popular pickup", "top 10", "top pickup"],
        "description": "Top 20 NYC taxi pickup zones by trip count"
    },
    "weather_impact": {
        "table": "answer_weather_impact",
        "keywords": ["rain", "weather", "precipitation", "rainy", "wet", "storm", "sunny", "dry", "umbrella", "temperature", "cold", "warm"],
        "description": "Impact of weather conditions on trip counts, fares, and tips"
    },
    "rush_hour": {
        "table": "answer_rush_hour",
        "keywords": ["rush hour", "rush", "peak hour", "peak time", "busy hour", "weekend", "weekday", "non-rush"],
        "description": "Rush hour vs non-rush hour patterns for trips, fares, and tips"
    },
    "payment_patterns": {
        "table": "answer_payment_patterns",
        "keywords": ["payment", "credit card", "cash", "tip percentage", "payment type", "how people pay", "pay by", "prefer", "driver prefer"],
        "description": "Payment type breakdown with tip percentages and average fares"
    },
    "borough_revenue": {
        "table": "answer_borough_revenue",
        "keywords": ["borough revenue", "revenue by borough", "which borough", "most revenue", "borough generate", "earn most", "revenue"],
        "description": "Total revenue and trips by NYC borough across all years"
    },
    "congestion_pricing": {
        "table": "answer_congestion_pricing",
        "keywords": ["congestion", "congestion pricing", "2025", "january 2025", "toll", "manhattan tax", "congestion charge"],
        "description": "Impact of NYC congestion pricing on Manhattan taxi trips and fares in Jan 2025 vs Jan 2024"
    },
    "yearly_tips": {
        "table": "answer_yearly_tips",
        "keywords": ["tip", "tipping", "year over year", "yearly", "tip amount", "tip changed", "tip trend", "biggest jump", "tip behavior"],
        "description": "Year over year average tip amounts and tip percentages for yellow taxi"
    },
}

FALLBACK_SCHEMA = """
You are a SQL expert for a NYC taxi analytics platform.

AVAILABLE TABLES:
- monthly_trips: year, month, taxi_type, trips, avg_fare, avg_tip, avg_distance, total_revenue
- borough_summary: year, borough, trips, avg_fare, avg_tip, total_revenue, avg_distance, taxi_type
- zone_summary: zone, borough, service_zone, trips, avg_fare, avg_tip, avg_distance, total_revenue
- hourly_summary: pickup_hour, is_weekend(BOOL), is_rush_hour(BOOL), trips, avg_fare, avg_tip, avg_tip_pct, avg_distance
- weather_summary: borough, temp_bucket, rain_bucket, trips, avg_fare, avg_tip, avg_distance
- quarterly_recovery: year, quarter, borough, trips, avg_fare, total_revenue
- yearly_summary: year, taxi_type, trips, avg_fare, avg_tip, avg_distance, total_revenue
- payment_summary: year, payment_type, payment_label, trips, avg_fare, avg_tip, avg_tip_pct, avg_distance

RULES:
- ALWAYS include LIMIT (max 100)
- SELECT only — no INSERT, UPDATE, DELETE, DROP
- No CTEs, no subqueries, no self-joins
- Use WHERE year IN (x,y) GROUP BY year for year comparisons
- Output ONLY raw SQL on first line, blank line, then plain English explanation
"""

KNOWN_TABLES = [
    "monthly_trips", "borough_summary", "zone_summary", "hourly_summary",
    "weather_summary", "quarterly_recovery", "yearly_summary", "payment_summary",
    "answer_covid_monthly", "answer_borough_recovery", "answer_rideshare_vs_yellow",
    "answer_top_zones", "answer_weather_impact", "answer_rush_hour",
    "answer_payment_patterns", "answer_borough_revenue", "answer_congestion_pricing",
    "answer_yearly_tips",
]

NYC_TAXI_FACTS = """
Key facts about NYC taxi data (2019-2025):
- April 2020: 96.6% COVID collapse in yellow taxi demand (6.2M trips in Jan 2020 → 210K in Apr 2020)
- 2024: 86.9% of NYC trips are rideshare (Uber 65%, Lyft 22%, Yellow Taxi 13%)
- January 2025: NYC congestion pricing started — $9 toll for entering Manhattan below 60th St
- Yellow taxi trips declined every year since 2019
- Uber dominates rideshare with ~65% market share in 2024
- Total dataset: 1.57B trips, 252M yellow taxi + 1.295B rideshare (2019-2025)
- Weather: 99.8% of yellow taxi trips have matched weather data
- Top pickup zone: Midtown Manhattan consistently leads
- Yellow taxi average fare grew from $13 (2019) to $20 (2024)
- Uber average fare grew from $17 (2019) to $26 (2024)
- Credit card trips tip 22.79% on average vs 0% for cash trips
- Manhattan generates most yellow taxi revenue
- Queens and Bronx recovered faster than Manhattan after COVID
"""


def classify_intent(question: str) -> str | None:
    q = question.lower()
    scores = {}
    for intent, config in INTENT_MAP.items():
        score = sum(1 for kw in config["keywords"] if kw in q)
        if score > 0:
            scores[intent] = score
    return max(scores, key=scores.get) if scores else None


def explain_answer(question: str, table_name: str, results: list) -> str:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    intent_desc = next(
        (v["description"] for v in INTENT_MAP.values() if v["table"] == table_name),
        "NYC taxi data"
    )
    response = client.messages.create(
        model=MODEL,
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": f"""Question: {question}
Data source: {intent_desc}
Results: {str(results[:10])}

Write a 2-3 sentence plain-English answer using specific numbers. No markdown."""
        }]
    )
    return response.content[0].text.strip()


def generate_fallback_sql(question: str) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model=MODEL,
        max_tokens=500,
        system=FALLBACK_SCHEMA,
        messages=[{"role": "user", "content": f"Question: {question}"}]
    )
    raw = re.sub(r'```sql\s*|```\s*', '', response.content[0].text.strip(), flags=re.IGNORECASE)
    lines = [l for l in raw.split("\n") if l.strip()]
    sql = lines[0].strip().rstrip(";") if lines else ""
    explanation = " ".join(lines[2:]) if len(lines) > 2 else ""
    sql = sanitize_sql(sql)
    sql = enforce_limit(sql)
    is_valid, error = validate_sql(sql)
    return {"sql": sql, "explanation": explanation, "valid": is_valid, "error": error}


def knowledge_answer(question: str) -> str:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model=MODEL,
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": f"""You are an NYC taxi data expert. The user asked: "{question}"

{NYC_TAXI_FACTS}

Answer the question using these facts. Give the closest relevant insight if exact data isn't available.
2-3 sentences max. Be specific with numbers. No markdown."""
        }]
    )
    return response.content[0].text.strip()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    print("Loading tables from CSV...")
    db = duckdb.connect()

    data_dir = Path(__file__).parent.parent / "data"

    for table_name in KNOWN_TABLES:
        try:
            csv_path = data_dir / f"{table_name}.csv"
            df = pd.read_csv(csv_path)
            db.register(table_name, df)
            count = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  {table_name}: {count} rows")
        except Exception as e:
            print(f"  SKIP {table_name}: {e}")

    print("All tables loaded. Ready.")
    yield
    db.close()


app = FastAPI(
    title="CabStream Text-to-SQL Agent",
    version="3.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class QuestionRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    question: str
    sql: str
    results: list
    explanation: str
    row_count: int
    valid: bool
    error: str
    data_source: str


@app.get("/")
def serve_frontend():
    html_path = find_html()
    if html_path:
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>CabStream API running</h1>")


@app.get("/health")
def health():
    tables = db.execute("SHOW TABLES").fetchdf()
    return {
        "status": "healthy",
        "tables_loaded": len(tables),
        "tables": {
            row["name"]: db.execute(f"SELECT COUNT(*) FROM {row['name']}").fetchone()[0]
            for _, row in tables.iterrows()
        }
    }


@app.post("/query", response_model=QueryResponse)
def query(request: QuestionRequest):
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    intent = classify_intent(question)
    if intent:
        table = INTENT_MAP[intent]["table"]
        try:
            df = db.execute(f"SELECT * FROM {table} LIMIT 100").df()
            results = df.to_dict(orient="records")
            if results:
                explanation = explain_answer(question, table, results)
                return QueryResponse(
                    question=question,
                    sql=f"SELECT * FROM {table} LIMIT 100",
                    results=results,
                    explanation=explanation,
                    row_count=len(results),
                    valid=True, error="",
                    data_source="pre-computed"
                )
        except Exception:
            pass

    result = generate_fallback_sql(question)
    if result["valid"]:
        try:
            df = db.execute(result["sql"]).df()
            results = df.to_dict(orient="records")
            if results:
                explanation = explain_answer(question, "fallback", results)
                return QueryResponse(
                    question=question,
                    sql=result["sql"],
                    results=results,
                    explanation=explanation,
                    row_count=len(results),
                    valid=True, error="",
                    data_source="sql"
                )
        except Exception:
            pass

    explanation = knowledge_answer(question)
    return QueryResponse(
        question=question,
        sql="", results=[],
        explanation=explanation,
        row_count=0,
        valid=True,
        error="",
        data_source="knowledge"
    )


@app.get("/examples")
def examples():
    return {"examples": [
        "Show yellow taxi trips month by month in 2020",
        "Compare Uber vs yellow taxi by year",
        "Which borough generates most revenue?",
        "How does rain affect tips?",
        "Top 10 pickup zones in Manhattan",
        "Rush hour vs non-rush hour patterns",
        "Which rideshare company had most trips in 2023?",
        "Which borough recovered fastest after COVID?",
        "Credit card vs cash tip percentages",
        "What percentage of 2024 trips were rideshare?",
        "Did congestion pricing affect Manhattan taxi trips in 2025?",
        "How did average tip amounts change year over year?"
    ]}