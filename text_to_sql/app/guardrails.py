# guardrails.py
# SQL guardrails for Text-to-SQL agent
# Enforces SELECT-only, schema validation, and LIMIT enforcement
# These run BEFORE any SQL touches the database

import re
from typing import Tuple

# Allowed tables — exactly what exists in Gold layer
ALLOWED_TABLES = {
    "fact_trips_yellow",
    "fact_trips_fhvhv",
    "dim_time",
    "dim_zone",
}

# Dangerous SQL keywords — block anything that modifies data
FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "REPLACE", "MERGE", "UPSERT", "GRANT", "REVOKE",
    "EXEC", "EXECUTE", "CALL", "PRAGMA", "ATTACH", "DETACH",
}

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Validates SQL against all guardrails.
    Returns (is_valid, error_message).
    If valid, error_message is empty string.
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query"

    sql_clean = sql.strip()

    # Rule 1: Must start with SELECT
    if not sql_clean.upper().startswith("SELECT"):
        return False, "Only SELECT statements are allowed"

    # Rule 2: No forbidden keywords anywhere in the query
    sql_upper = sql_clean.upper()
    for keyword in FORBIDDEN_KEYWORDS:
        # Use word boundary to avoid false positives
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"Forbidden keyword detected: {keyword}"

    # Rule 3: Only allowed tables referenced
    # Extract table names from FROM and JOIN clauses
    table_pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    referenced_tables = set(re.findall(table_pattern, sql_upper))
    unknown_tables = {t.lower() for t in referenced_tables} - ALLOWED_TABLES
    if unknown_tables:
        return False, f"Unknown tables referenced: {unknown_tables}"

    # Rule 4: Must have LIMIT clause
    if "LIMIT" not in sql_upper:
        return False, "Query must include a LIMIT clause"

    # Rule 5: LIMIT must not exceed MAX_LIMIT
    limit_match = re.search(r'\bLIMIT\s+(\d+)', sql_upper)
    if limit_match:
        limit_val = int(limit_match.group(1))
        if limit_val > MAX_LIMIT:
            return False, f"LIMIT {limit_val} exceeds maximum allowed ({MAX_LIMIT})"

    # Rule 6: No semicolons mid-query (prevents query chaining)
    if sql_clean.rstrip(";").count(";") > 0:
        return False, "Multiple statements not allowed"

    return True, ""


def enforce_limit(sql: str) -> str:
    """
    Adds DEFAULT_LIMIT if no LIMIT clause present.
    Called after validate_sql passes (so LIMIT is guaranteed after this).
    """
    if "LIMIT" not in sql.upper():
        sql = sql.rstrip(";").strip()
        sql = f"{sql} LIMIT {DEFAULT_LIMIT}"
    return sql


def sanitize_sql(sql: str) -> str:
    """
    Cleans up SQL returned by Claude:
    - Strips markdown code blocks
    - Removes leading/trailing whitespace
    - Strips trailing semicolons
    """
    # Remove markdown code fences
    sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'```\s*', '', sql)
    sql = sql.strip().rstrip(";")
    return sql