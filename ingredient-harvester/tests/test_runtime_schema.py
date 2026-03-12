from __future__ import annotations

from app.runtime_schema import _candidate_row_column_defs


def test_candidate_row_column_defs_use_postgres_boolean_false_default() -> None:
    defs = _candidate_row_column_defs("postgresql")
    assert defs["ingest_allowed"] == "BOOLEAN DEFAULT FALSE"


def test_candidate_row_column_defs_keep_sqlite_integer_default() -> None:
    defs = _candidate_row_column_defs("sqlite")
    assert defs["ingest_allowed"] == "INTEGER DEFAULT 0"
