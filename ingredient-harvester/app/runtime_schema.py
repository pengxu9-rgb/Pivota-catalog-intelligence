from __future__ import annotations

from sqlalchemy import inspect, text


def _candidate_row_column_defs(dialect_name: str) -> dict[str, str]:
    real = "DOUBLE PRECISION" if dialect_name == "postgresql" else "REAL"
    boolean = "BOOLEAN" if dialect_name == "postgresql" else "INTEGER"
    return {
        "candidate_id": "VARCHAR(255)",
        "sku_key": "VARCHAR(255)",
        "external_seed_id": "VARCHAR(64)",
        "external_product_id": "VARCHAR(255)",
        "cleaned_text": "TEXT",
        "parse_status": "VARCHAR(24)",
        "parse_confidence": real,
        "inci_list": "TEXT",
        "inci_list_json": "TEXT",
        "unrecognized_tokens_json": "TEXT",
        "normalization_notes_json": "TEXT",
        "needs_review_json": "TEXT",
        "review_status": "VARCHAR(24) DEFAULT 'UNREVIEWED'",
        "reviewed_by": "VARCHAR(128)",
        "reviewed_at": "TIMESTAMP",
        "audit_status": "VARCHAR(24) DEFAULT 'UNAUDITED'",
        "audit_score": real,
        "source_match_status": "VARCHAR(24)",
        "ingredient_signal_type": "VARCHAR(32)",
        "ingest_allowed": f"{boolean} DEFAULT 0",
    }


def ensure_runtime_schema(engine) -> None:
    dialect_name = engine.dialect.name
    inspector = inspect(engine)

    existing_columns = {column["name"] for column in inspector.get_columns("candidate_rows")}
    column_defs = _candidate_row_column_defs(dialect_name)

    with engine.begin() as conn:
        for column_name, ddl in column_defs.items():
            if column_name in existing_columns:
                continue
            conn.execute(text(f"ALTER TABLE candidate_rows ADD COLUMN {column_name} {ddl}"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS candidate_row_audit_findings (
                  id INTEGER PRIMARY KEY,
                  audit_run_id VARCHAR(48),
                  row_id VARCHAR(36),
                  import_id VARCHAR(36),
                  anomaly_type VARCHAR(64),
                  severity VARCHAR(16),
                  evidence_json TEXT,
                  recommended_action TEXT,
                  auto_fixable BOOLEAN DEFAULT 0,
                  created_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS candidate_row_corrections (
                  id INTEGER PRIMARY KEY,
                  audit_run_id VARCHAR(48),
                  row_id VARCHAR(36),
                  correction_type VARCHAR(64),
                  status VARCHAR(24),
                  auto_applied BOOLEAN DEFAULT 0,
                  actor VARCHAR(128),
                  before_payload_json TEXT,
                  after_payload_json TEXT,
                  error TEXT,
                  created_at TIMESTAMP
                )
                """
            )
        )

        index_statements = [
            "CREATE INDEX IF NOT EXISTS candidate_rows_candidate_id_idx ON candidate_rows (candidate_id)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_sku_key_idx ON candidate_rows (sku_key)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_external_seed_id_idx ON candidate_rows (external_seed_id)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_external_product_id_idx ON candidate_rows (external_product_id)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_parse_status_idx ON candidate_rows (parse_status)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_review_status_idx ON candidate_rows (review_status)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_audit_status_idx ON candidate_rows (audit_status)",
            "CREATE INDEX IF NOT EXISTS candidate_rows_ingest_allowed_idx ON candidate_rows (ingest_allowed)",
            "CREATE INDEX IF NOT EXISTS candidate_row_audit_findings_row_idx ON candidate_row_audit_findings (row_id, created_at)",
            "CREATE INDEX IF NOT EXISTS candidate_row_audit_findings_run_idx ON candidate_row_audit_findings (audit_run_id, created_at)",
            "CREATE INDEX IF NOT EXISTS candidate_row_corrections_row_idx ON candidate_row_corrections (row_id, created_at)",
            "CREATE INDEX IF NOT EXISTS candidate_row_corrections_run_idx ON candidate_row_corrections (audit_run_id, created_at)",
        ]
        for statement in index_statements:
            conn.execute(text(statement))
