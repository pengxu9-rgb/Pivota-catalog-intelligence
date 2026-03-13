from __future__ import annotations

from app.quality import build_audit_findings, compute_audit_status, compute_source_match_status


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


def test_build_audit_findings_flags_non_official_sources_for_review_even_when_tokens_match() -> None:
    findings = build_audit_findings(
        row_id="row_2",
        import_id="imp_1",
        brand="Olehenriksen",
        product_name="Barrier Booster Orange Ferment Vitamin C Essence",
        source_ref="https://www.cosdna.com/eng/barrier-booster-orange-ferment-vitamin-c-essence.html",
        source_type="ThirdParty",
        raw_ingredient_text="Ingredients: Water, Glycerin, Tocopherol",
        cleaned_text="Water, Glycerin, Tocopherol",
        parse_status="OK",
        parse_confidence=0.94,
        inci_list="Aqua; Glycerin; Tocopherol",
        normalization_notes=[],
        source_match_status="match",
        source_match_evidence={"source_ref": "https://www.cosdna.com/eng/barrier-booster-orange-ferment-vitamin-c-essence.html"},
        ingredient_signal_type="labeled_ingredients",
        duplicate_conflict=None,
    )

    anomaly_types = {finding["anomaly_type"]: finding for finding in findings}
    assert anomaly_types["source_host_not_allowlisted"] == {
        "row_id": "row_2",
        "import_id": "imp_1",
        "anomaly_type": "source_host_not_allowlisted",
        "severity": "review",
        "evidence": {
            "source_ref": "https://www.cosdna.com/eng/barrier-booster-orange-ferment-vitamin-c-essence.html",
            "source_host": "www.cosdna.com",
            "allowed_hosts": ["olehenriksen.com"],
            "source_type": "thirdparty",
        },
        "recommended_action": "Use a source hosted on the brand's official domain before approval, or keep this row in review if only third-party evidence is available.",
        "auto_fixable": False,
    }
    assert anomaly_types["non_official_source_requires_review"] == {
        "row_id": "row_2",
        "import_id": "imp_1",
        "anomaly_type": "non_official_source_requires_review",
        "severity": "review",
        "evidence": {
            "source_ref": "https://www.cosdna.com/eng/barrier-booster-orange-ferment-vitamin-c-essence.html",
            "source_type": "thirdparty",
            "source_match_status": "match",
        },
        "recommended_action": "Review non-official ingredient sources before approval, or replace them with an official product source.",
        "auto_fixable": False,
    }


def test_build_audit_findings_reclassifies_retailer_hosts_even_if_source_type_is_stale_official() -> None:
    findings = build_audit_findings(
        row_id="row_3",
        import_id="imp_1",
        brand="Olehenriksen",
        product_name="Dewtopia 20% Acid Night Treatment",
        source_ref="https://www.strawberrynet.com/en/ole-henriksen-transform-dewtopia-20-acid-night-treatment-30ml-1oz/270695",
        source_type="Official",
        raw_ingredient_text="Ingredients: Water, Glycolic Acid",
        cleaned_text="Water, Glycolic Acid",
        parse_status="OK",
        parse_confidence=0.98,
        inci_list="Aqua; Glycolic Acid",
        normalization_notes=[],
        source_match_status="match",
        source_match_evidence={"source_ref": "https://www.strawberrynet.com/en/ole-henriksen-transform-dewtopia-20-acid-night-treatment-30ml-1oz/270695"},
        ingredient_signal_type="labeled_ingredients",
        duplicate_conflict=None,
    )

    anomaly_types = {finding["anomaly_type"]: finding for finding in findings}
    assert anomaly_types["source_host_not_allowlisted"] == {
        "row_id": "row_3",
        "import_id": "imp_1",
        "anomaly_type": "source_host_not_allowlisted",
        "severity": "review",
        "evidence": {
            "source_ref": "https://www.strawberrynet.com/en/ole-henriksen-transform-dewtopia-20-acid-night-treatment-30ml-1oz/270695",
            "source_host": "www.strawberrynet.com",
            "allowed_hosts": ["olehenriksen.com"],
            "source_type": "retailer",
        },
        "recommended_action": "Use a source hosted on the brand's official domain before approval, or keep this row in review if only third-party evidence is available.",
        "auto_fixable": False,
    }
    assert anomaly_types["non_official_source_requires_review"] == {
        "row_id": "row_3",
        "import_id": "imp_1",
        "anomaly_type": "non_official_source_requires_review",
        "severity": "review",
        "evidence": {
            "source_ref": "https://www.strawberrynet.com/en/ole-henriksen-transform-dewtopia-20-acid-night-treatment-30ml-1oz/270695",
            "source_type": "retailer",
            "source_match_status": "match",
        },
        "recommended_action": "Review non-official ingredient sources before approval, or replace them with an official product source.",
        "auto_fixable": False,
    }


def test_build_audit_findings_blocks_official_rows_on_non_allowlisted_hosts() -> None:
    findings = build_audit_findings(
        row_id="row_4",
        import_id="imp_1",
        brand="Dermalogica",
        product_name="smart response serum",
        source_ref="https://www.skinsafeproducts.com/dermalogica-smart-response-serum-1-0-fl-oz",
        source_type="Official",
        raw_ingredient_text="Ingredients: Water, Glycerin, Tocopherol",
        cleaned_text="Water, Glycerin, Tocopherol",
        parse_status="OK",
        parse_confidence=0.99,
        inci_list="Aqua; Glycerin; Tocopherol",
        normalization_notes=[],
        source_match_status="match",
        source_match_evidence={"source_ref": "https://www.skinsafeproducts.com/dermalogica-smart-response-serum-1-0-fl-oz"},
        ingredient_signal_type="labeled_ingredients",
        duplicate_conflict=None,
    )

    assert findings == [
        {
            "row_id": "row_4",
            "import_id": "imp_1",
            "anomaly_type": "source_host_not_allowlisted",
            "severity": "blocker",
            "evidence": {
                "source_ref": "https://www.skinsafeproducts.com/dermalogica-smart-response-serum-1-0-fl-oz",
                "source_host": "www.skinsafeproducts.com",
                "allowed_hosts": ["dermalogica.com"],
                "source_type": "official",
            },
            "recommended_action": "Use a source hosted on the brand's official domain before approval, or keep this row in review if only third-party evidence is available.",
            "auto_fixable": False,
        }
    ]


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


def test_benign_normalization_notes_do_not_force_review() -> None:
    findings = build_audit_findings(
        row_id="row_3",
        import_id="imp_1",
        brand="Olehenriksen",
        product_name="Dewtopia 20% Acid Night Treatment",
        source_ref="https://olehenriksen.com/products/dewtopia-20-acid-night-treatment",
        source_type="Official",
        raw_ingredient_text="Aqua/Water/Eau, Glycerin",
        cleaned_text="Aqua/Water/Eau, Glycerin",
        parse_status="OK",
        parse_confidence=1.0,
        inci_list="Aqua; Glycerin",
        normalization_notes=["Mapped 'Aqua/Water/Eau' -> 'Aqua'"],
        source_match_status="match",
        source_match_evidence={"source_ref": "https://olehenriksen.com/products/dewtopia-20-acid-night-treatment"},
        ingredient_signal_type="labeled_ingredients",
        duplicate_conflict=None,
    )

    assert findings == []


def test_compute_source_match_status_normalizes_zero_o_confusions_for_official_urls() -> None:
    status, evidence = compute_source_match_status(
        "Pixi Beauty",
        "H2O SkinTint - Porcelain",
        "https://www.pixibeauty.com/products/h20-skin-tint",
        "Official",
    )

    assert status == "match"
    assert "h2o" in evidence["product_overlap"]
