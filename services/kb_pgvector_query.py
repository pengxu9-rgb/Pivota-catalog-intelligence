from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

from kb_pgvector import ensure_sslmode_require, hash_embedding, mask_db_url, vector_literal


try:
    import psycopg  # type: ignore[import-not-found]

    _DB_DRIVER = "psycopg"
except Exception:  # noqa: BLE001
    psycopg = None  # type: ignore[assignment]
    _DB_DRIVER = "psycopg2"
    import psycopg2  # type: ignore[import-not-found]


def _connect(db_url: str):
    url = ensure_sslmode_require(db_url)
    if _DB_DRIVER == "psycopg" and psycopg is not None:
        return psycopg.connect(url)
    return psycopg2.connect(url)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Query pgvector KB using the same hash embedding used for ingest.")
    ap.add_argument("--db-url", default="", help="Database URL. Default: env DATABASE_URL.")
    ap.add_argument("--schema", default="pci_kb")
    ap.add_argument("--table", default="sku_ingredients")
    ap.add_argument("--market", default="", help="Optional market filter, e.g. US/EU/CN.")
    ap.add_argument("--only-parse-status", default="OK")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--text", required=True, help="Query text (brand/product/ingredients).")
    args = ap.parse_args(argv)

    db_url = (args.db_url or "").strip() or (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        print("Missing --db-url or env DATABASE_URL", file=sys.stderr)
        return 2

    qualified = f"{args.schema}.{args.table}"
    qvec = vector_literal(hash_embedding(args.text, dim=384))

    where = []
    params = []
    if args.market:
        where.append("market = %s")
        params.append(args.market)
    if args.only_parse_status:
        where.append("parse_status = %s")
        params.append(args.only_parse_status)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
SELECT
  sku_key,
  market,
  brand,
  product_name,
  parse_status,
  review_status,
  (embedding <=> %s::vector) AS distance
FROM {qualified}
{where_sql}
ORDER BY embedding <=> %s::vector
LIMIT {int(args.limit)};
"""

    conn = _connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [qvec, *params, qvec])
            rows = cur.fetchall()
    finally:
        conn.close()

    print(f"db={mask_db_url(ensure_sslmode_require(db_url))}", file=sys.stderr)
    for r in rows:
        sku_key, market, brand, product_name, parse_status, review_status, distance = r
        print(f"{distance:.4f}\t{sku_key}\t{market}\t{brand}\t{product_name}\t{parse_status}\t{review_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

