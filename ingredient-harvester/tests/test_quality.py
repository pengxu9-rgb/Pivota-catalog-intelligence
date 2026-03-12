from __future__ import annotations

from app.quality import build_audit_findings, compute_audit_status


def test_build_audit_findings_flags_blockers_and_reviews() -> None:
    findings = build_audit_findings(
        row_id="row_1",
        import_id="imp_1",
        brand="PATYKA",
        product_name="Detox Cleanser",
        source_ref="",
        source_type="official",
        raw_ingredient_text="Experience the ultimate luxury with Detox Cleanser.",
        cleaned_text="Experience the ultimate luxury with Detox Cleanser.",
        parse_status="NEEDS_REVIEW",
        parse_confidence=0.41,
        inci_list="",
        normalization_notes=["Removed UI phrase(s)"],
        source_match_status="mismatch",
        source_match_evidence={"source_ref": "https://example.com/contact-us"},
        ingredient_signal_type="noisy_text",
        duplicate_conflict={
            "sku_key": "sku_1",
            "current_row_id": "row_1",
            "conflicting_row_id": "row_2",
        },
    )

    anomaly_types = {finding["anomaly_type"] for finding in findings}
    assert "missing_source_ref" in anomaly_types
    assert "source_product_mismatch" in anomaly_types
    assert "parser_needs_review" in anomaly_types
    assert "marketing_copy_instead_of_ingredients" in anomaly_types
    assert "truncated_or_noisy_text" in anomaly_types
    assert "duplicate_conflicting_candidate" in anomaly_types


def test_compute_audit_status_prioritizes_blockers() -> None:
    fail_status, fail_score = compute_audit_status(
        [
            {"severity": "blocker", "anomaly_type": "source_product_mismatch"},
            {"severity": "review", "anomaly_type": "truncated_or_noisy_text"},
        ]
    )
    review_status, review_score = compute_audit_status(
        [
            {"severity": "review", "anomaly_type": "low_parse_confidence"},
        ]
    )
    pass_status, pass_score = compute_audit_status([])

    assert fail_status == "FAIL"
    assert review_status == "REVIEW"
    assert pass_status == "PASS"
    assert fail_score < review_score < pass_score
