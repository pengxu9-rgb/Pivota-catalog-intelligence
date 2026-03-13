from __future__ import annotations

import json
import re
from typing import Any, Optional
from urllib.parse import urlparse


STOP_TOKENS = {
    "with",
    "from",
    "your",
    "this",
    "that",
    "serum",
    "cream",
    "lotion",
    "cleanser",
    "treatment",
    "solution",
    "set",
    "kit",
    "gift",
    "card",
    "size",
    "travel",
}

MARKETING_COPY_RE = re.compile(
    r"("
    r"experience\s+the\s+ultimate\s+luxury"
    r"|how\s+to\s+use"
    r"|for\s+best\s+results"
    r"|clinically\s+proven"
    r"|discover"
    r"|daily\s+use"
    r"|apply\s+(?:daily|morning|evening)"
    r"|white\s+cast"
    r"|hydrates?"
    r"|protects?"
    r")",
    re.IGNORECASE,
)

REVIEW_WORTHY_NORMALIZATION_RE = re.compile(
    r"(removed|strip(?:ped)?|truncat|ui|artifact|noise|cleanup|boilerplate|marketing|label\s+prefix)",
    re.IGNORECASE,
)

RETAILER_HOST_MARKERS = (
    "sephora.",
    "ulta.",
    "amazon.",
    "lookfantastic.",
    "cultbeauty.",
    "douglas.",
    "strawberrynet.",
)

THIRD_PARTY_HOST_MARKERS = (
    "wikipedia.org",
    "incidecoder.com",
    "cosdna.com",
    "skincarisma.com",
)

OFFICIAL_HOST_ALLOWLIST = {
    "olehenriksen": ("olehenriksen.com",),
    "dermalogica": ("dermalogica.com",),
    "pixi": ("pixibeauty.com",),
    "pixi beauty": ("pixibeauty.com",),
}


def normalize_nonempty_string(value: Any) -> str:
    return str(value or "").strip()


def normalize_json_string(value: Any) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        raw = value.strip()
        return raw or "[]"
    return json.dumps(value, ensure_ascii=False)


def normalize_url_like(value: Any) -> str:
    raw = normalize_nonempty_string(value)
    if not raw:
        return ""
    if re.match(r"^[a-z][a-z0-9+.-]*://", raw, re.IGNORECASE):
        return raw
    if raw.startswith("www.") or "." in raw.split("/", 1)[0]:
        return f"https://{raw}"
    return ""


def _effective_source_type(source_type: str | None, source_ref: str) -> str:
    normalized = normalize_nonempty_string(source_type).lower()
    source_url = normalize_url_like(source_ref).lower()
    if any(host in source_url for host in THIRD_PARTY_HOST_MARKERS):
        return "thirdparty"
    if any(host in source_url for host in RETAILER_HOST_MARKERS):
        return "retailer"
    return normalized


def _is_review_worthy_normalization_note(note: str) -> bool:
    normalized = normalize_nonempty_string(note)
    if not normalized:
        return False
    return bool(REVIEW_WORTHY_NORMALIZATION_RE.search(normalized))


def _normalized_host(source_ref: str) -> str:
    normalized_source_ref = normalize_url_like(source_ref)
    if not normalized_source_ref:
        return ""
    try:
        return urlparse(normalized_source_ref).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


def _official_hosts_for_brand(brand: str) -> tuple[str, ...]:
    normalized_brand = normalize_nonempty_string(brand).lower()
    if not normalized_brand:
        return ()

    matches: list[str] = []
    for brand_key, hosts in OFFICIAL_HOST_ALLOWLIST.items():
        if brand_key in normalized_brand:
            matches.extend(hosts)
    return tuple(dict.fromkeys(matches))


def _host_matches_allowlist(host: str, allowed_hosts: tuple[str, ...]) -> bool:
    normalized_host = normalize_nonempty_string(host).lower()
    if not normalized_host:
        return False
    for allowed_host in allowed_hosts:
        normalized_allowed = normalize_nonempty_string(allowed_host).lower()
        if normalized_host == normalized_allowed or normalized_host.endswith(f".{normalized_allowed}"):
            return True
    return False


def _tokenize(text: str) -> set[str]:
    base_tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", (text or "").lower())
        if (len(token) >= 4 or (len(token) >= 3 and any(ch.isdigit() for ch in token))) and token not in STOP_TOKENS
    }
    tokens = set(base_tokens)
    for token in list(base_tokens):
        if "0" in token:
            tokens.add(token.replace("0", "o"))
    return tokens


def compute_source_match_status(brand: str, product_name: str, source_ref: str, source_type: str | None) -> tuple[str, dict[str, Any]]:
    normalized_source_ref = normalize_url_like(source_ref)
    if not normalized_source_ref:
        return "missing", {"source_ref": ""}

    try:
        parsed = urlparse(normalized_source_ref)
        url_text = f"{parsed.netloc} {parsed.path}"
    except Exception:  # noqa: BLE001
        return "unknown", {"source_ref": normalized_source_ref}

    url_tokens = _tokenize(url_text)
    brand_tokens = _tokenize(brand)
    product_tokens = _tokenize(product_name)
    brand_overlap = sorted(url_tokens & brand_tokens)
    product_overlap = sorted(url_tokens & product_tokens)

    if brand_overlap or product_overlap:
        return "match", {
            "source_ref": normalized_source_ref,
            "brand_overlap": brand_overlap,
            "product_overlap": product_overlap,
        }

    normalized_source_type = _effective_source_type(source_type, source_ref)
    if normalized_source_type in {"official", "retailer"} and product_tokens:
        return "mismatch", {
            "source_ref": normalized_source_ref,
            "brand_overlap": brand_overlap,
            "product_overlap": product_overlap,
        }

    return "unknown", {
        "source_ref": normalized_source_ref,
        "brand_overlap": brand_overlap,
        "product_overlap": product_overlap,
    }


def compute_ingredient_signal_type(raw_ingredient_text: str, cleaned_text: str, parse_status: str, inci_list: str) -> str:
    raw = normalize_nonempty_string(raw_ingredient_text)
    cleaned = normalize_nonempty_string(cleaned_text)
    inci = normalize_nonempty_string(inci_list)
    parse = normalize_nonempty_string(parse_status).upper()

    if not raw and not cleaned:
        return "none"
    if "ingredient" in raw.lower() or "inci" in raw.lower():
        return "labeled_ingredients"
    if parse == "OK" and inci:
        return "structured_list"
    if MARKETING_COPY_RE.search(raw):
        return "noisy_text"
    return "ambiguous_text"


def build_audit_findings(
    *,
    row_id: str,
    import_id: str,
    brand: str,
    product_name: str,
    source_ref: str,
    source_type: str | None,
    raw_ingredient_text: str,
    cleaned_text: str,
    parse_status: str,
    parse_confidence: float | None,
    inci_list: str,
    normalization_notes: list[Any],
    source_match_status: str,
    source_match_evidence: dict[str, Any],
    ingredient_signal_type: str,
    duplicate_conflict: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    normalized_source_type = _effective_source_type(source_type, source_ref)
    source_host = _normalized_host(source_ref)
    official_hosts = _official_hosts_for_brand(brand)

    def add(
        anomaly_type: str,
        severity: str,
        evidence: dict[str, Any],
        recommended_action: str,
        auto_fixable: bool,
    ) -> None:
        findings.append(
            {
                "row_id": row_id,
                "import_id": import_id,
                "anomaly_type": anomaly_type,
                "severity": severity,
                "evidence": evidence,
                "recommended_action": recommended_action,
                "auto_fixable": auto_fixable,
            }
        )

    if not normalize_nonempty_string(source_ref):
        add(
            "missing_source_ref",
            "blocker",
            {"source_ref": ""},
            "Provide a verifiable source URL before this row can advance to KB ingest.",
            False,
        )

    if source_match_status == "mismatch":
        add(
            "source_product_mismatch",
            "blocker",
            source_match_evidence,
            "Review the source URL and update it to the matching product page or approved source document.",
            False,
        )

    if official_hosts and source_host and not _host_matches_allowlist(source_host, official_hosts):
        severity = "blocker" if normalized_source_type == "official" else "review"
        add(
            "source_host_not_allowlisted",
            severity,
            {
                **source_match_evidence,
                "source_ref": normalize_url_like(source_ref),
                "source_host": source_host,
                "allowed_hosts": list(official_hosts),
                "source_type": normalized_source_type or normalize_nonempty_string(source_type).lower(),
            },
            "Use a source hosted on the brand's official domain before approval, or keep this row in review if only third-party evidence is available.",
            False,
        )

    if normalized_source_type in {"thirdparty", "retailer"}:
        add(
            "non_official_source_requires_review",
            "review",
            {
                **source_match_evidence,
                "source_type": normalized_source_type,
                "source_match_status": source_match_status,
            },
            "Review non-official ingredient sources before approval, or replace them with an official product source.",
            False,
        )

    if not normalize_nonempty_string(raw_ingredient_text):
        add(
            "empty_raw_ingredient_text",
            "blocker",
            {"raw_ingredient_text": ""},
            "Harvest or manually provide the raw ingredient text before parsing.",
            False,
        )

    normalized_parse_status = normalize_nonempty_string(parse_status).upper()
    if normalized_parse_status == "NEEDS_SOURCE":
        add(
            "parser_needs_source",
            "blocker",
            {"parse_status": normalized_parse_status, "raw_excerpt": normalize_nonempty_string(raw_ingredient_text)[:240]},
            "Capture a clearer ingredient source or replace the raw text with a verified ingredient section.",
            False,
        )
    elif normalized_parse_status == "NEEDS_REVIEW":
        add(
            "parser_needs_review",
            "blocker",
            {
                "parse_status": normalized_parse_status,
                "parse_confidence": parse_confidence,
                "raw_excerpt": normalize_nonempty_string(raw_ingredient_text)[:240],
            },
            "Resolve ambiguous or low-confidence ingredient tokens before ingestion.",
            False,
        )

    if ingredient_signal_type == "noisy_text":
        add(
            "marketing_copy_instead_of_ingredients",
            "review",
            {"raw_excerpt": normalize_nonempty_string(raw_ingredient_text)[:240]},
            "Replace marketing or usage copy with the actual ingredient section.",
            False,
        )

    if normalized_parse_status == "OK" and (parse_confidence or 0.0) < 0.8:
        add(
            "low_parse_confidence",
            "review",
            {"parse_confidence": parse_confidence, "inci_list": normalize_nonempty_string(inci_list)},
            "Review the parsed INCI list before approval because confidence is below the safe threshold.",
            False,
        )

    normalized_notes = [normalize_nonempty_string(note) for note in normalization_notes or [] if normalize_nonempty_string(note)]
    review_notes = [note for note in normalized_notes if _is_review_worthy_normalization_note(note)]
    if review_notes:
        add(
            "truncated_or_noisy_text",
            "review",
            {"normalization_notes": review_notes[:12], "cleaned_text": normalize_nonempty_string(cleaned_text)[:240]},
            "Check the cleaned ingredient text for truncation or UI noise before approval.",
            True,
        )

    if duplicate_conflict:
        add(
            "duplicate_conflicting_candidate",
            "blocker",
            duplicate_conflict,
            "Resolve conflicting duplicate candidates so each sku_key has one approved ingredient payload.",
            False,
        )

    return findings


def compute_audit_status(findings: list[dict[str, Any]]) -> tuple[str, float]:
    blocker_count = len([finding for finding in findings if finding.get("severity") == "blocker"])
    review_count = len([finding for finding in findings if finding.get("severity") == "review"])
    if blocker_count > 0:
        status = "FAIL"
    elif review_count > 0:
        status = "REVIEW"
    else:
        status = "PASS"

    score = max(0.0, min(1.0, 1.0 - (0.35 * blocker_count) - (0.08 * review_count)))
    return status, score
