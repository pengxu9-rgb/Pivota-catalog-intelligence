from __future__ import annotations

import argparse
import sys
from typing import Optional

from kb_pgvector import ensure_sslmode_require, mask_db_url
from kb_pgvector_ingest import DEFAULT_DB_URL_ENV, DEFAULT_DIM, DEFAULT_SCHEMA, DEFAULT_TABLE, _connect, init_schema, resolve_db_url


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Create or upgrade the dedicated pci_kb pgvector schema.")
    ap.add_argument("--db-url", default="", help=f"Database URL. Default: env {DEFAULT_DB_URL_ENV}, then DATABASE_URL.")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--dim", type=int, default=DEFAULT_DIM)
    ap.add_argument("--no-index", action="store_true", help="Skip creating vector index.")
    args = ap.parse_args(argv)

    db_url = resolve_db_url(args.db_url)
    if not db_url:
        print(f"Missing --db-url or env {DEFAULT_DB_URL_ENV}/DATABASE_URL", file=sys.stderr)
        return 2

    conn = _connect(db_url)
    try:
        init_schema(
            conn,
            schema=str(args.schema),
            table=str(args.table),
            dim=int(args.dim),
            create_index=not bool(args.no_index),
        )
    finally:
        conn.close()

    print(
        (
            f"INIT_OK schema={args.schema} table={args.table} dim={int(args.dim)} "
            f"db={mask_db_url(ensure_sslmode_require(db_url))}"
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
