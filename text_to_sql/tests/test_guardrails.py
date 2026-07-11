# test_guardrails.py
# Unit tests for SQL guardrails
# Run with: pytest text_to_sql/tests/test_guardrails.py -v

import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.guardrails import validate_sql, enforce_limit, sanitize_sql


class TestValidateSQL:

    # ── SELECT-only enforcement ───────────────────────────────────────────
    def test_valid_select(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is True
        assert error == ""

    def test_rejects_insert(self):
        sql = "INSERT INTO fact_trips_yellow VALUES (1, 2, 3)"
        valid, error = validate_sql(sql)
        assert valid is False
        assert error != ""

    def test_rejects_update(self):
        sql = "UPDATE fact_trips_yellow SET fare_amount = 0"
        valid, error = validate_sql(sql)
        assert valid is False
        assert error != ""

    def test_rejects_delete(self):
        sql = "DELETE FROM fact_trips_yellow WHERE year = 2019"
        valid, error = validate_sql(sql)
        assert valid is False
        assert error != ""

    def test_rejects_drop(self):
        sql = "DROP TABLE fact_trips_yellow"
        valid, error = validate_sql(sql)
        assert valid is False
        assert error != ""

    def test_rejects_create(self):
        sql = "CREATE TABLE hack AS SELECT * FROM fact_trips_yellow"
        valid, error = validate_sql(sql)
        assert valid is False
        assert error != ""

    # ── Table validation ──────────────────────────────────────────────────
    def test_valid_table_fact_yellow(self):
        sql = "SELECT year, COUNT(*) FROM fact_trips_yellow GROUP BY year LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is True

    def test_valid_table_fact_fhvhv(self):
        sql = "SELECT company, COUNT(*) FROM fact_trips_fhvhv GROUP BY company LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is True

    def test_valid_table_dim_time(self):
        sql = "SELECT * FROM dim_time LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is True

    def test_valid_table_dim_zone(self):
        sql = "SELECT * FROM dim_zone LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is True

    def test_rejects_unknown_table(self):
        sql = "SELECT * FROM secret_table LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is False
        assert "Unknown tables" in error

    def test_rejects_system_table(self):
        sql = "SELECT * FROM information_schema.tables LIMIT 10"
        valid, error = validate_sql(sql)
        assert valid is False

    # ── LIMIT enforcement ─────────────────────────────────────────────────
    def test_rejects_missing_limit(self):
        sql = "SELECT * FROM fact_trips_yellow"
        valid, error = validate_sql(sql)
        assert valid is False
        assert "LIMIT" in error

    def test_rejects_limit_too_high(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 9999"
        valid, error = validate_sql(sql)
        assert valid is False
        assert "exceeds maximum" in error

    def test_accepts_limit_at_max(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 1000"
        valid, error = validate_sql(sql)
        assert valid is True

    def test_accepts_limit_below_max(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 50"
        valid, error = validate_sql(sql)
        assert valid is True

    # ── Multi-statement prevention ────────────────────────────────────────
    def test_rejects_multiple_statements(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 10; DROP TABLE fact_trips_yellow"
        valid, error = validate_sql(sql)
        assert valid is False

    # ── JOIN queries ──────────────────────────────────────────────────────
    def test_valid_join_query(self):
        sql = """
            SELECT z.borough, COUNT(*) as trips
            FROM fact_trips_yellow f
            JOIN dim_zone z ON f.pickup_location_id = z.location_id
            GROUP BY z.borough
            LIMIT 10
        """
        valid, error = validate_sql(sql)
        assert valid is True

    def test_valid_complex_query(self):
        sql = """
            SELECT year, month, COUNT(*) as trips
            FROM fact_trips_yellow
            WHERE year = 2020
            GROUP BY year, month
            ORDER BY month
            LIMIT 12
        """
        valid, error = validate_sql(sql)
        assert valid is True


class TestEnforceLimit:

    def test_adds_limit_when_missing(self):
        sql = "SELECT * FROM fact_trips_yellow"
        result = enforce_limit(sql)
        assert "LIMIT 100" in result

    def test_does_not_duplicate_limit(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 50"
        result = enforce_limit(sql)
        assert result.count("LIMIT") == 1

    def test_preserves_existing_limit(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 25"
        result = enforce_limit(sql)
        assert "LIMIT 25" in result


class TestSanitizeSQL:

    def test_strips_markdown_code_block(self):
        sql = "```sql\nSELECT * FROM fact_trips_yellow LIMIT 10\n```"
        result = sanitize_sql(sql)
        assert "```" not in result
        assert "SELECT" in result

    def test_strips_trailing_semicolon(self):
        sql = "SELECT * FROM fact_trips_yellow LIMIT 10;"
        result = sanitize_sql(sql)
        assert not result.endswith(";")

    def test_strips_whitespace(self):
        sql = "  SELECT * FROM fact_trips_yellow LIMIT 10  "
        result = sanitize_sql(sql)
        assert result == result.strip()

    def test_strips_generic_code_block(self):
        sql = "```\nSELECT * FROM fact_trips_yellow LIMIT 10\n```"
        result = sanitize_sql(sql)
        assert "```" not in result