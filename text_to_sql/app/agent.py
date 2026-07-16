import re
import anthropic
from app.schema import SCHEMA_CONTEXT, SCHEMA_SUMMARY
from app.guardrails import validate_sql, enforce_limit, sanitize_sql

client = anthropic.Anthropic()
MODEL = "claude-sonnet-4-6"


def generate_sql(question: str) -> dict:
    system_prompt = f"""You are a SQL expert for a NYC taxi analytics platform.
Given a natural language question, generate a single valid SQL SELECT statement.

{SCHEMA_CONTEXT}

STRICT OUTPUT FORMAT:
1. First line: The SQL query only (no explanation, no markdown, no backticks)
2. Second line: blank
3. Third line onwards: Plain English explanation

ALWAYS include LIMIT even on COUNT(*) or aggregation queries (e.g. LIMIT 100).
Never use markdown. Never use code blocks. Output raw SQL on the first line only."""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": f"Question: {question}"}],
            system=system_prompt
        )

        raw_output = response.content[0].text.strip()
        raw_output = re.sub(r'```sql\s*|```\s*', '', raw_output, flags=re.IGNORECASE)
        lines = [l for l in raw_output.split("\n") if l.strip()]
        sql = lines[0].strip().rstrip(";") if lines else ""
        explanation = " ".join(lines[2:]) if len(lines) > 2 else ""

        sql = sanitize_sql(sql)
        sql = enforce_limit(sql)
        is_valid, error = validate_sql(sql)

        if not is_valid:
            return {"sql": sql, "explanation": explanation, "valid": False, "error": error}

        return {"sql": sql, "explanation": explanation, "valid": True, "error": ""}

    except Exception as e:
        return {"sql": "", "explanation": "", "valid": False, "error": f"Agent error: {str(e)}"}


def generate_sql_with_schema(question: str, schema: str) -> dict:
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": f"Question: {question}"}],
            system=schema
        )

        raw_output = response.content[0].text.strip()
        raw_output = re.sub(r'```sql\s*|```\s*', '', raw_output, flags=re.IGNORECASE)
        lines = [l for l in raw_output.split("\n") if l.strip()]
        sql = lines[0].strip().rstrip(";") if lines else ""
        explanation = " ".join(lines[2:]) if len(lines) > 2 else ""

        sql = sanitize_sql(sql)
        sql = enforce_limit(sql)
        is_valid, error = validate_sql(sql)

        if not is_valid:
            return {"sql": sql, "explanation": explanation, "valid": False, "error": error}

        return {"sql": sql, "explanation": explanation, "valid": True, "error": ""}

    except Exception as e:
        return {"sql": "", "explanation": "", "valid": False, "error": f"Agent error: {str(e)}"}


def explain_results(question: str, sql: str, results: list) -> str:
    results_str = str(results[:10])
    response = client.messages.create(
        model=MODEL,
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": f"""Question: {question}
SQL used: {sql}
Results (first 10 rows): {results_str}

Write a 2-3 sentence plain-English summary of what these results show.
Be specific with numbers. No markdown."""
        }]
    )
    return response.content[0].text.strip()