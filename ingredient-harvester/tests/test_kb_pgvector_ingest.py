from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


SERVICES_DIR = Path(__file__).resolve().parents[2] / "services"
if str(SERVICES_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICES_DIR))

from kb_pgvector_ingest import build_rows  # noqa: E402


def test_build_rows_applies_review_and_audit_gates() -> None:
    df = pd.DataFrame(
        [
            {
                "candidate_id": "sku-pass",
                "market": "US",
                "brand": "Brand A",
                "product_name": "Product A",
                "source_ref": "https://example.com/a",
                "source_type": "official",
                "status": "OK",
                "parse_status": "OK",
                "review_status": "APPROVED",
                "audit_status": "PASS",
                "ingest_allowed": True,
                "raw_ingredient_text": "Water, Glycerin",
                "inci_list": "Aqua; Glycerin",
                "inci_list_json": '[{"order":1,"standard_name":"Aqua"}]',
            },
            {
                "candidate_id": "sku-review",
                "market": "US",
                "brand": "Brand B",
                "product_name": "Product B",
                "source_ref": "https://example.com/b",
                "source_type": "official",
                "status": "OK",
                "parse_status": "OK",
                "review_status": "UNREVIEWED",
                "audit_status": "REVIEW",
                "ingest_allowed": False,
                "raw_ingredient_text": "Water, Glycerin",
                "inci_list": "Aqua; Glycerin",
            },
        ]
    )

    rows = build_rows(
        df,
        dim=16,
        only_parse_status="OK",
        only_review_status="APPROVED",
        only_audit_status="PASS",
        require_ingest_allowed=True,
    )

    assert len(rows) == 1
    assert rows[0].sku_key == "sku-pass"
    assert rows[0].review_status == "APPROVED"
    assert rows[0].audit_status == "PASS"
    assert rows[0].ingest_allowed is True
