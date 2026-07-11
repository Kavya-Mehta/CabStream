# agent.py
# Claude-powered Text-to-SQL agent
# Injects schema context, generates SQL, applies guardrails

import anthropic
from app.schema import SCHEMA_CONTEXT, SCHEMA_SUMMARY
from app.guardrails import validate_sql, enforce_limit, sanitize_sql

client = anthropic.Anthropic()
MODEL = "claude-sonnet-4-6"


def generate_sql(question: str) -> dict:
    """
    Takes a natural language question, returns SQL + explanation.
    Returns dict with keys: sql, explanation, valid, error
    """

    system_prompt = f"""You are a SQL expert for a NYC taxi analytics platform.
Given a natural language question, generate a single valid SQL SELECT statement.

{SCHEMA_CONTEXT}

STRICT OUTPUT FORMAT:
1. First line: The SQL query only (no explanation, no markdown, no backticks)
2. Second line: blank
3. Third line onwards: Plain English explanation of what the query does and what the results mean

ALWAYS include LIMIT even on COUNT(*) or aggregation queries (e.g. LIMIT 100).
Never use markdown. Never use code blocks. Output raw SQL on the first line only."""

    user_prompt = f"Question: {question}"

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt
        )

        raw_output = response.content[0].text.strip()
        lines = raw_output.split("\n")

        # First non-empty line is SQL
        sql_line = ""
        explanation_lines = []
        found_sql = False

        for i, line in enumerate(lines):
            if not found_sql and line.strip():
                sql_line = line.strip()
                found_sql = True
            elif found_sql and line.strip():
                explanation_lines.append(line.strip())

        sql = sanitize_sql(sql_line)
        explanation = " ".join(explanation_lines)

        # Validate
        is_valid, error = validate_sql(sql)
        if not is_valid:
            return {
                "sql": sql,
                "explanation": explanation,
                "valid": False,
                "error": error
            }

        # Enforce limit
        sql = enforce_limit(sql)

        return {
            "sql": sql,
            "explanation": explanation,
            "valid": True,
            "error": ""
        }

    except Exception as e:
        return {
            "sql": "",
            "explanation": "",
            "valid": False,
            "error": f"Agent error: {str(e)}"
        }


def explain_results(question: str, sql: str, results: list) -> str:
    """
    Takes the question, SQL, and query results,
    returns a plain-English summary of findings.
    """
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