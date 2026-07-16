from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv
import duckdb
import os

load_dotenv(Path(__file__).parent.parent.parent / ".env")

from app.agent import generate_sql_with_schema, explain_results
from app.guardrails import validate_sql

SUMMARY_SCHEMA = """
You are a SQL expert for a NYC taxi analytics platform with 1.57 billion trips (2019-2025).

AVAILABLE TABLES (pre-aggregated, fast):

TABLE: monthly_trips (266 rows)
Columns: year (INT), month (INT), taxi_type (STRING: 'Yellow Taxi','Uber','Lyft','Via','Juno'),
         trips (LONG), avg_fare (DOUBLE), avg_tip (DOUBLE), avg_distance (DOUBLE), total_revenue (DOUBLE)
Use for: year/month trends, COVID impact, rideshare vs yellow comparison over time

TABLE: borough_summary (56 rows)
Columns: year (INT), borough (STRING: 'Manhattan','Brooklyn','Queens','Bronx','Staten Island','EWR'),
         trips (LONG), avg_fare (DOUBLE), avg_tip (DOUBLE), total_revenue (DOUBLE), avg_distance (DOUBLE), taxi_type (STRING)
Use for: borough comparisons, geographic patterns, revenue by location

TABLE: zone_summary (262 rows)
Columns: zone (STRING), borough (STRING), service_zone (STRING),
         trips (LONG), avg_fare (DOUBLE), avg_tip (DOUBLE), avg_distance (DOUBLE), total_revenue (DOUBLE)
Use for: top pickup zones, airport questions, zone-level patterns

TABLE: hourly_summary (48 rows)
Columns: pickup_hour (INT 0-23), is_weekend (BOOLEAN), is_rush_hour (BOOLEAN),
         trips (LONG), avg_fare (DOUBLE), avg_tip (DOUBLE), avg_tip_pct (DOUBLE), avg_distance (DOUBLE)
Use for: time-of-day patterns, rush hour vs non-rush, weekend vs weekday

TABLE: weather_summary (20 rows)
Columns: temp_bucket (STRING: 'Freezing (<0C)','Cold (0-10C)','Mild (10-20C)','Warm (20-30C)','Hot (>30C)'),
         rain_bucket (STRING: 'No rain','Light rain','Moderate rain','Heavy rain'),
         trips (LONG), avg_fare (DOUBLE), avg_tip (DOUBLE), avg_distance (DOUBLE)
Use for: weather impact on demand, rain effect on fares/tips, temperature patterns

RULES:
- ALWAYS end every SQL query with LIMIT — no exceptions, even for aggregations returning 1-2 rows
- Default LIMIT 100, never exceed LIMIT 1000
- SELECT only — no INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE
- Output ONLY raw SQL on the first line, nothing else
- Second line blank, then plain English explanation
- COVID peak: year=2020, month=4
- taxi_type for yellow: 'Yellow Taxi'
- taxi_type for rideshare: 'Uber', 'Lyft', 'Via', 'Juno'
"""

db = None


def find_project_root():
    current = Path(__file__).parent
    while current != current.parent:
        if (current / ".git").exists() or (current / "README.md").exists():
            return current
        current = current.parent
    return Path(__file__).parent.parent.parent


PROJECT_ROOT = find_project_root()
HTML_PATH = PROJECT_ROOT / "streamlit_app" / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    print("Loading summary tables into memory...")
    db = duckdb.connect()
    db.execute("INSTALL azure; LOAD azure;")
    key = os.getenv("ADLS_STORAGE_KEY")
    db.execute(f"""
        CREATE SECRET azure_secret (
            TYPE AZURE,
            CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=cabstreamdata;AccountKey={key};EndpointSuffix=core.windows.net'
        )
    """)
    summary = "abfss://delta@cabstreamdata.dfs.core.windows.net/summary"
    db.execute(f"CREATE TABLE monthly_trips AS SELECT * FROM delta_scan('{summary}/monthly_trips')")
    db.execute(f"CREATE TABLE borough_summary AS SELECT * FROM delta_scan('{summary}/borough_summary')")
    db.execute(f"CREATE TABLE zone_summary AS SELECT * FROM delta_scan('{summary}/zone_summary')")
    db.execute(f"CREATE TABLE hourly_summary AS SELECT * FROM delta_scan('{summary}/hourly_summary')")
    db.execute(f"CREATE TABLE weather_summary AS SELECT * FROM delta_scan('{summary}/weather_summary')")
    print("All summary tables loaded. Ready.")
    print(f"  monthly_trips: {db.execute('SELECT COUNT(*) FROM monthly_trips').fetchone()[0]} rows")
    print(f"  borough_summary: {db.execute('SELECT COUNT(*) FROM borough_summary').fetchone()[0]} rows")
    print(f"  zone_summary: {db.execute('SELECT COUNT(*) FROM zone_summary').fetchone()[0]} rows")
    print(f"  hourly_summary: {db.execute('SELECT COUNT(*) FROM hourly_summary').fetchone()[0]} rows")
    print(f"  weather_summary: {db.execute('SELECT COUNT(*) FROM weather_summary').fetchone()[0]} rows")
    yield
    db.close()


app = FastAPI(
    title="CabStream Text-to-SQL Agent",
    description="Ask plain-English questions about 1.57B NYC taxi trips",
    version="2.0.0",
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
    return FileResponse(str(HTML_PATH), media_type="text/html")


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "tables": {
            "monthly_trips": db.execute("SELECT COUNT(*) FROM monthly_trips").fetchone()[0],
            "borough_summary": db.execute("SELECT COUNT(*) FROM borough_summary").fetchone()[0],
            "zone_summary": db.execute("SELECT COUNT(*) FROM zone_summary").fetchone()[0],
            "hourly_summary": db.execute("SELECT COUNT(*) FROM hourly_summary").fetchone()[0],
            "weather_summary": db.execute("SELECT COUNT(*) FROM weather_summary").fetchone()[0],
        }
    }


@app.post("/query", response_model=QueryResponse)
def query(request: QuestionRequest):
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    result = generate_sql_with_schema(question, SUMMARY_SCHEMA)

    if not result["valid"]:
        return QueryResponse(
            question=question, sql=result["sql"], results=[],
            explanation=result["explanation"], row_count=0,
            valid=False, error=result["error"], data_source="none"
        )

    try:
        df = db.execute(result["sql"]).df()
        results = df.to_dict(orient="records")
        explanation = explain_results(question, result["sql"], results)
        return QueryResponse(
            question=question, sql=result["sql"], results=results,
            explanation=explanation, row_count=len(results),
            valid=True, error="", data_source="summary"
        )
    except Exception as e:
        return QueryResponse(
            question=question, sql=result["sql"], results=[],
            explanation="", row_count=0, valid=False,
            error=f"SQL execution error: {str(e)}", data_source="summary"
        )


@app.get("/examples")
def examples():
    return {"examples": [
        "Show yellow taxi trips month by month in 2020",
        "Compare Uber vs yellow taxi trip counts by year",
        "Which borough generates the most revenue?",
        "How does rain affect average tip amounts?",
        "Top 10 pickup zones in Manhattan by trip count",
        "Compare rush hour vs non-rush hour tip percentages",
        "Which rideshare company had the most trips in 2023?",
        "How did COVID affect demand month by month in 2020?"
    ]}