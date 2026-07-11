# main.py
# FastAPI backend for Text-to-SQL agent

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import duckdb
import os
from app.agent import generate_sql, explain_results
from app.guardrails import validate_sql

app = FastAPI(
    title="CabStream Text-to-SQL Agent",
    description="Ask plain-English questions about 1.57B NYC taxi trips",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Gold layer paths — local parquet exported from Azure
GOLD_PATH = os.getenv("GOLD_PATH", "data/gold")


def get_db():
    conn = duckdb.connect()
    # Register Gold tables as views
    conn.execute(f"""
        CREATE OR REPLACE VIEW fact_trips_yellow AS
        SELECT * FROM read_parquet('{GOLD_PATH}/fact_trips_yellow/**/*.parquet')
    """)
    conn.execute(f"""
        CREATE OR REPLACE VIEW fact_trips_fhvhv AS
        SELECT * FROM read_parquet('{GOLD_PATH}/fact_trips_fhvhv/**/*.parquet')
    """)
    conn.execute(f"""
        CREATE OR REPLACE VIEW dim_time AS
        SELECT * FROM read_parquet('{GOLD_PATH}/dim_time/**/*.parquet')
    """)
    conn.execute(f"""
        CREATE OR REPLACE VIEW dim_zone AS
        SELECT * FROM read_parquet('{GOLD_PATH}/dim_zone/**/*.parquet')
    """)
    return conn


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


@app.get("/")
def root():
    return {
        "name": "CabStream Text-to-SQL Agent",
        "status": "running",
        "tables": ["fact_trips_yellow", "fact_trips_fhvhv", "dim_time", "dim_zone"],
        "total_rows": "1,548,145,603"
    }


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/query", response_model=QueryResponse)
def query(request: QuestionRequest):
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    # Generate SQL
    result = generate_sql(question)

    if not result["valid"]:
        return QueryResponse(
            question=question,
            sql=result["sql"],
            results=[],
            explanation=result["explanation"],
            row_count=0,
            valid=False,
            error=result["error"]
        )

    # Execute SQL
    try:
        conn = get_db()
        df = conn.execute(result["sql"]).df()
        results = df.to_dict(orient="records")
        row_count = len(results)

        # Get plain-English explanation of results
        explanation = explain_results(question, result["sql"], results)

        return QueryResponse(
            question=question,
            sql=result["sql"],
            results=results,
            explanation=explanation,
            row_count=row_count,
            valid=True,
            error=""
        )

    except Exception as e:
        return QueryResponse(
            question=question,
            sql=result["sql"],
            results=[],
            explanation="",
            row_count=0,
            valid=False,
            error=f"SQL execution error: {str(e)}"
        )


@app.get("/examples")
def examples():
    return {
        "examples": [
            "Which borough had the most trips in April 2020?",
            "How did COVID affect yellow taxi demand month by month in 2020?",
            "What is the average fare for trips from JFK airport?",
            "Compare Uber vs Lyft trip counts by year",
            "Which hours have the highest demand on weekends?",
            "What percentage of trips in 2024 were rideshare vs yellow taxi?",
            "How does rain affect average tip amounts?",
            "What are the top 10 most popular pickup zones in Manhattan?"
        ]
    }