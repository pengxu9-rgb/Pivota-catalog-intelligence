from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import pandas as pd

from kb_pgvector import ensure_sslmode_require, hash_embedding, mask_db_url, vector_literal
from ingredient_parser import clean_noise as parser_clean_noise
from ingredient_parser import _preprocess as parser_preprocess


try:
    import psycopg  # type: ignore[import-not-found]

    _DB_DRIVER = "psycopg"
except Exception:  # noqa: BLE001
    psycopg = None  # type: ignore[assignment]
    _DB_DRIVER = "psycopg2"
    import psycopg2  # type: ignore[import-not-found]
    import psycopg2.extras  # type: ignore[import-not-found]


DEFAULT_SCHEMA = "pci_kb"
DEFAULT_TABLE = "sku_ingredients"
DEFAULT_DIM = 384


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:  # noqa: BLE001
        pass
    return str(value).strip()


def _pick_first_nonempty(row: pd.Series, cols: list[str]) -> str:
    for c in cols:
        if c in row.index:
            v = _coerce_text(row.get(c))
            if v:
                return v
    return ""


def clean_raw_ingredient_text(raw: str) -> str:
    pre = parser_preprocess(raw or "")
    cleaned, _notes = parser_clean_noise(pre)
    return cleaned


def build_kb_text(*, brand: str, product_name: str, market: str, category: str, inci_list: str, raw_clean: str) -> str:
    parts: list[str] = []
    if brand:
        parts.append(f"brand:{brand}")
    if product_name:
        parts.append(f"product:{product_name}")
    if market:
        parts.append(f"market:{market}")
    if category:
        parts.append(f"category:{category}")
    if inci_list:
        parts.append(f"inci:{inci_list}")
    elif raw_clean:
        parts.append(f"ingredients:{raw_clean}")
    return " | ".join(parts)


def _json_or_none(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:  # noqa: BLE001
        pass
    if isinstance(value, (dict, list)):
        return value
    s = str(value).strip()
    if not s:
        return None
    # Some CSV exports wrap JSON arrays as strings.
    try:
        return json.loads(s)
    except Exception:  # noqa: BLE001
        return None


@dataclass(frozen=True)
class KbRow:
    sku_key: str
    market: str
    brand: str
    product_name: str
    category: str
    source_ref: str
    source_type: str
    harvest_status: str
    harvest_confidence: Optional[float]
    parse_status: str
    parse_confidence: Optional[float]
    review_status: str
    raw_ingredient_text_clean: str
    inci_list: str
    inci_list_json: Any
    embedding_literal: str


def build_rows(df: pd.DataFrame, *, dim: int, only_parse_status: str = "") -> list[KbRow]:
    out: list[KbRow] = []
    for _idx, row in df.iterrows():
        sku_key = _pick_first_nonempty(row, ["candidate_id", "sku_key", "row_id", "id"])
        if not sku_key:
            continue

        market = _pick_first_nonempty(row, ["market"])
        category = _pick_first_nonempty(row, ["category"])

        brand = _pick_first_nonempty(row, ["brand_en", "brand_original", "brand", "brand_zh"])
        product_name = _pick_first_nonempty(
            row,
            ["product_name_en", "product_name_original", "product_name", "product", "product_name_zh"],
        )

        source_ref = _pick_first_nonempty(row, ["source_ref", "source_url", "url"])
        source_type = _pick_first_nonempty(row, ["source_type"])
        harvest_status = _pick_first_nonempty(row, ["harvest_status", "status"])
        review_status = _pick_first_nonempty(row, ["review_status"])

        parse_status = _pick_first_nonempty(row, ["parse_status"])
        if only_parse_status and parse_status and parse_status != only_parse_status:
            continue

        try:
            harvest_conf = float(row.get("harvest_confidence")) if "harvest_confidence" in row.index else None
        except Exception:  # noqa: BLE001
            harvest_conf = None
        try:
            parse_conf = float(row.get("parse_confidence")) if "parse_confidence" in row.index else None
        except Exception:  # noqa: BLE001
            parse_conf = None

        raw = _pick_first_nonempty(row, ["raw_ingredient_text"])
        raw_clean = clean_raw_ingredient_text(raw) if raw else ""

        inci_list = _pick_first_nonempty(row, ["inci_list"])
        inci_json = _json_or_none(row.get("inci_list_json")) if "inci_list_json" in row.index else None

        if not (raw_clean or inci_list):
            # Skip rows without any usable ingredient signal.
            continue

        kb_text = build_kb_text(
            brand=brand,
            product_name=product_name,
            market=market,
            category=category,
            inci_list=inci_list,
            raw_clean=raw_clean,
        )
        emb = hash_embedding(kb_text, dim=dim)
        emb_lit = vector_literal(emb)

        out.append(
            KbRow(
                sku_key=sku_key,
                market=market,
                brand=brand,
                product_name=product_name,
                category=category,
                source_ref=source_ref,
                source_type=source_type,
                harvest_status=harvest_status,
                harvest_confidence=harvest_conf,
                parse_status=parse_status,
                parse_confidence=parse_conf,
                review_status=review_status,
                raw_ingredient_text_clean=raw_clean,
                inci_list=inci_list,
                inci_list_json=inci_json,
                embedding_literal=emb_lit,
            )
        )
    return out


def _connect(db_url: str):
    url = ensure_sslmode_require(db_url)
    if _DB_DRIVER == "psycopg" and psycopg is not None:
        return psycopg.connect(url)
    return psycopg2.connect(url)


def _exec_many(conn, sql: str, rows: list[tuple[Any, ...]], *, template: str, page_size: int) -> None:
    if _DB_DRIVER == "psycopg" and psycopg is not None:
        with conn.cursor() as cur:
            # psycopg3 has executemany but no execute_values; fall back to executemany.
            cur.executemany(sql.replace("VALUES %s", f"VALUES {template}"), rows)
        conn.commit()
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, template=template, page_size=page_size)
    conn.commit()


def init_schema(conn, *, schema: str, table: str, dim: int, create_index: bool) -> None:
    qualified = f"{schema}.{table}"

    ddl = [
        "CREATE EXTENSION IF NOT EXISTS vector;",
        f"CREATE SCHEMA IF NOT EXISTS {schema};",
        f"""
CREATE TABLE IF NOT EXISTS {qualified} (
  sku_key TEXT PRIMARY KEY,
  market TEXT,
  brand TEXT,
  product_name TEXT,
  category TEXT,
  source_ref TEXT,
  source_type TEXT,
  harvest_status TEXT,
  harvest_confidence DOUBLE PRECISION,
  parse_status TEXT,
  parse_confidence DOUBLE PRECISION,
  review_status TEXT,
  raw_ingredient_text_clean TEXT,
  inci_list TEXT,
  inci_list_json JSONB,
  embedding vector({int(dim)}),
  updated_at TIMESTAMPTZ DEFAULT now()
);
""",
        f"CREATE INDEX IF NOT EXISTS {table}_market_idx ON {qualified} (market);",
        f"CREATE INDEX IF NOT EXISTS {table}_parse_status_idx ON {qualified} (parse_status);",
        f"CREATE INDEX IF NOT EXISTS {table}_review_status_idx ON {qualified} (review_status);",
    ]

    if _DB_DRIVER == "psycopg" and psycopg is not None:
        with conn.cursor() as cur:
            for s in ddl:
                cur.execute(s)
        conn.commit()
    else:
        with conn.cursor() as cur:
            for s in ddl:
                cur.execute(s)
        conn.commit()

    if not create_index:
        return

    # Best-effort: HNSW if supported, else IVFFLAT. Either may fail depending on pgvector version.
    index_sqls = [
        f"CREATE INDEX IF NOT EXISTS {table}_embedding_hnsw ON {qualified} USING hnsw (embedding vector_cosine_ops);",
        f"CREATE INDEX IF NOT EXISTS {table}_embedding_ivfflat ON {qualified} USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);",
    ]

    for s in index_sqls:
        try:
            if _DB_DRIVER == "psycopg" and psycopg is not None:
                with conn.cursor() as cur:
                    cur.execute(s)
                conn.commit()
            else:
                with conn.cursor() as cur:
                    cur.execute(s)
                conn.commit()
            break
        except Exception:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass


def upsert_rows(
    conn,
    rows: list[KbRow],
    *,
    schema: str,
    table: str,
    batch_size: int,
) -> None:
    qualified = f"{schema}.{table}"
    sql = f"""
INSERT INTO {qualified} (
  sku_key,
  market,
  brand,
  product_name,
  category,
  source_ref,
  source_type,
  harvest_status,
  harvest_confidence,
  parse_status,
  parse_confidence,
  review_status,
  raw_ingredient_text_clean,
  inci_list,
  inci_list_json,
  embedding,
  updated_at
)
VALUES %s
ON CONFLICT (sku_key) DO UPDATE SET
  market = EXCLUDED.market,
  brand = EXCLUDED.brand,
  product_name = EXCLUDED.product_name,
  category = EXCLUDED.category,
  source_ref = EXCLUDED.source_ref,
  source_type = EXCLUDED.source_type,
  harvest_status = EXCLUDED.harvest_status,
  harvest_confidence = EXCLUDED.harvest_confidence,
  parse_status = EXCLUDED.parse_status,
  parse_confidence = EXCLUDED.parse_confidence,
  review_status = EXCLUDED.review_status,
  raw_ingredient_text_clean = EXCLUDED.raw_ingredient_text_clean,
  inci_list = EXCLUDED.inci_list,
  inci_list_json = EXCLUDED.inci_list_json,
  embedding = EXCLUDED.embedding,
  updated_at = now();
"""
    if _DB_DRIVER == "psycopg" and psycopg is not None:
        values: list[tuple[Any, ...]] = []
        for r in rows:
            values.append(
                (
                    r.sku_key,
                    r.market,
                    r.brand,
                    r.product_name,
                    r.category,
                    r.source_ref,
                    r.source_type,
                    r.harvest_status,
                    r.harvest_confidence,
                    r.parse_status,
                    r.parse_confidence,
                    r.review_status,
                    r.raw_ingredient_text_clean,
                    r.inci_list,
                    json.dumps(r.inci_list_json, ensure_ascii=False) if r.inci_list_json is not None else None,
                    r.embedding_literal,
                )
            )
        # psycopg3: inline template with ::vector casts
        template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::vector, now())"
        _exec_many(conn, sql, values, template=template, page_size=batch_size)
        return

    values2: list[tuple[Any, ...]] = []
    for r in rows:
        values2.append(
            (
                r.sku_key,
                r.market,
                r.brand,
                r.product_name,
                r.category,
                r.source_ref,
                r.source_type,
                r.harvest_status,
                r.harvest_confidence,
                r.parse_status,
                r.parse_confidence,
                r.review_status,
                r.raw_ingredient_text_clean,
                r.inci_list,
                psycopg2.extras.Json(r.inci_list_json) if r.inci_list_json is not None else None,
                r.embedding_literal,
            )
        )

    template2 = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector, now())"
    _exec_many(conn, sql, values2, template=template2, page_size=batch_size)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Ingest SKU ingredient data into Postgres + pgvector (hash embedding).")
    ap.add_argument("--csv", required=True, help="Input CSV path (harvested+parsed).")
    ap.add_argument("--db-url", default="", help="Database URL. Default: env DATABASE_URL.")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--dim", type=int, default=DEFAULT_DIM)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--only-parse-status", default="", help="If set, only ingest rows with this parse_status (e.g. OK).")
    ap.add_argument("--no-index", action="store_true", help="Skip creating vector index.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to DB; just print counts.")
    args = ap.parse_args(argv)

    db_url = (args.db_url or "").strip() or (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        print("Missing --db-url or env DATABASE_URL", file=sys.stderr)
        return 2

    csv_path = args.csv
    df = pd.read_csv(csv_path)

    rows = build_rows(df, dim=int(args.dim), only_parse_status=str(args.only_parse_status or "").strip())
    print(f"rows_total={len(df)} rows_ingest={len(rows)} dim={int(args.dim)} db={mask_db_url(ensure_sslmode_require(db_url))}", file=sys.stderr)

    if args.dry_run:
        return 0

    conn = _connect(db_url)
    try:
        init_schema(conn, schema=str(args.schema), table=str(args.table), dim=int(args.dim), create_index=not bool(args.no_index))
        upsert_rows(conn, rows, schema=str(args.schema), table=str(args.table), batch_size=int(args.batch_size))
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    print("OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
