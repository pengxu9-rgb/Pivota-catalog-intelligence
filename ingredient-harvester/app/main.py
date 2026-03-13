from __future__ import annotations

import io
import json
import os
import threading
import time
import uuid
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from redis import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential_jitter

from app.config import settings
from app.db import db_session, engine, utcnow
from app.jobs import harvest_row
from app.models import Base, CandidateRow, HarvestTask, ImportBatch, TaskRow, CandidateRowAuditFinding, CandidateRowCorrection
from app.parser_runtime import build_parser_snapshot, parser_error, parser_ready
from app.quality import (
    build_audit_findings,
    compute_audit_status,
    compute_ingredient_signal_type,
    compute_source_match_status,
    normalize_nonempty_string,
    normalize_url_like,
)
from app.runtime_schema import ensure_runtime_schema
from app.queue import enqueue
from app.schema import (
    CandidateRowView,
    CorrectionUpdateRequest,
    CreateTaskRequest,
    CreateTaskResponse,
    ImportAuditRequest,
    ImportAuditResponse,
    ImportResponse,
    ListRowsResponse,
    ParserReparseBatchRequest,
    ParserReparseBatchResponse,
    ParserReparseBatchResponseItem,
    ParserReparseRequest,
    ParserReparseResponse,
    ReviewUpdateRequest,
    TaskProgress,
    UpdateRowRequest,
    UpdateRowResponse,
)


_DB_READY = False
_DB_ERROR: Optional[str] = None
_DB_INIT_ATTEMPTS = int((os.getenv("HARVESTER_DB_INIT_ATTEMPTS") or "8").strip() or "8")
_DB_RETRY_INTERVAL_S = float((os.getenv("HARVESTER_DB_RETRY_INTERVAL_S") or "10").strip() or "10")
_DB_INIT_LOCK = threading.Lock()
_DB_LAST_TRY_AT = 0.0


def _loads_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return default
    raw = value.strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return default

def _require_parser() -> None:
    if parser_ready():
        return
    raise HTTPException(status_code=503, detail=parser_error() or "Parser not available.")


def _to_parser_response(snapshot: dict[str, Any]) -> ParserReparseResponse:
    return ParserReparseResponse(
        cleaned_text=str(snapshot.get("cleaned_text") or ""),
        parse_status=str(snapshot.get("parse_status") or "NEEDS_REVIEW"),
        inci_list=str(snapshot.get("inci_list") or ""),
        inci_list_json=_loads_json(snapshot.get("inci_list_json"), []),
        unrecognized_tokens=_loads_json(snapshot.get("unrecognized_tokens"), []),
        normalization_notes=_loads_json(snapshot.get("normalization_notes"), []),
        parse_confidence=float(snapshot.get("parse_confidence") or 0.0),
        needs_review=_loads_json(snapshot.get("needs_review"), []),
    )


def _request_id(request: Request) -> str:
    existing = (
        request.headers.get("x-harvester-request-id")
        or request.headers.get("x-request-id")
        or request.headers.get("x-railway-request-id")
    )
    if existing:
        return existing
    return uuid.uuid4().hex[:12]


def _set_db_error(message: str) -> None:
    global _DB_READY  # noqa: PLW0603
    global _DB_ERROR  # noqa: PLW0603
    _DB_READY = False
    _DB_ERROR = (message or "DB error")[:500]


@retry(
    stop=stop_after_attempt(max(1, _DB_INIT_ATTEMPTS)),
    wait=wait_exponential_jitter(initial=1, max=10),
)
def _init_db() -> None:
    Base.metadata.create_all(bind=engine)
    ensure_runtime_schema(engine)


def _maybe_init_db() -> None:
    global _DB_READY  # noqa: PLW0603
    global _DB_ERROR  # noqa: PLW0603
    global _DB_LAST_TRY_AT  # noqa: PLW0603

    if _DB_READY:
        return
    now = time.time()
    if now - _DB_LAST_TRY_AT < max(0.0, _DB_RETRY_INTERVAL_S):
        return
    if not _DB_INIT_LOCK.acquire(blocking=False):
        return
    try:
        _DB_LAST_TRY_AT = now
        Base.metadata.create_all(bind=engine)
        ensure_runtime_schema(engine)
        _DB_READY = True
        _DB_ERROR = None
    except Exception as exc:  # noqa: BLE001
        _DB_READY = False
        _DB_ERROR = f"DB init failed: {exc!s}"[:500]
    finally:
        _DB_INIT_LOCK.release()


def _require_db() -> None:
    if _DB_READY:
        return
    _maybe_init_db()
    if _DB_READY:
        return
    msg = _DB_ERROR or "Database not ready."
    raise HTTPException(status_code=503, detail=msg)

app = FastAPI(title="Ingredient Source Harvester", version="1.0.0")

@app.middleware("http")
async def _request_id_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    rid = _request_id(request)
    request.state.request_id = rid  # type: ignore[attr-defined]
    response = await call_next(request)
    response.headers["x-harvester-request-id"] = rid
    return response


@app.exception_handler(OperationalError)
async def _operational_error_handler(request: Request, exc: OperationalError):  # type: ignore[override]
    rid = getattr(request.state, "request_id", "unknown")
    # eslint-disable-next-line no-console
    print(f"[harvester][{rid}] db operational error: {exc!s}")
    _set_db_error(f"DB operational error: {exc!s}")
    return JSONResponse(status_code=503, content={"detail": _DB_ERROR, "request_id": rid})


@app.exception_handler(DBAPIError)
async def _dbapi_error_handler(request: Request, exc: DBAPIError):  # type: ignore[override]
    rid = getattr(request.state, "request_id", "unknown")
    # eslint-disable-next-line no-console
    print(f"[harvester][{rid}] db api error: {exc!s}")
    _set_db_error(f"DB error: {exc!s}")
    return JSONResponse(status_code=503, content={"detail": _DB_ERROR, "request_id": rid})


@app.exception_handler(RedisError)
async def _redis_error_handler(request: Request, exc: RedisError):  # type: ignore[override]
    rid = getattr(request.state, "request_id", "unknown")
    # eslint-disable-next-line no-console
    print(f"[harvester][{rid}] redis error: {exc!s}")
    return JSONResponse(status_code=503, content={"detail": f"Redis error: {exc!s}"[:500], "request_id": rid})


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):  # type: ignore[override]
    rid = getattr(request.state, "request_id", "unknown")
    # eslint-disable-next-line no-console
    print(f"[harvester][{rid}] unhandled error: {type(exc).__name__}: {exc!s}")
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal Server Error",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
            "request_id": rid,
        },
    )

origins = settings.cors_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if origins == ["*"] else origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["x-harvester-request-id"],
)


def _row_view(row: CandidateRow) -> CandidateRowView:
    return CandidateRowView(
        row_id=row.row_id,
        row_index=row.row_index,
        brand=row.brand,
        product_name=row.product_name,
        market=row.market,
        candidate_id=row.candidate_id,
        sku_key=row.sku_key,
        external_seed_id=row.external_seed_id,
        external_product_id=row.external_product_id,
        status=row.status,  # type: ignore[arg-type]
        confidence=row.confidence,
        source_type=row.source_type,
        source_ref=row.source_ref,
        raw_ingredient_text=row.raw_ingredient_text,
        cleaned_text=row.cleaned_text,
        parse_status=row.parse_status,  # type: ignore[arg-type]
        parse_confidence=row.parse_confidence,
        inci_list=row.inci_list,
        inci_list_json=_loads_json(row.inci_list_json, []),
        unrecognized_tokens=_loads_json(row.unrecognized_tokens_json, []),
        normalization_notes=_loads_json(row.normalization_notes_json, []),
        needs_review=_loads_json(row.needs_review_json, []),
        review_status=(row.review_status or "UNREVIEWED"),  # type: ignore[arg-type]
        reviewed_by=row.reviewed_by,
        reviewed_at=row.reviewed_at,
        audit_status=(row.audit_status or "UNAUDITED"),  # type: ignore[arg-type]
        audit_score=row.audit_score,
        source_match_status=row.source_match_status,  # type: ignore[arg-type]
        ingredient_signal_type=row.ingredient_signal_type,  # type: ignore[arg-type]
        ingest_allowed=bool(row.ingest_allowed),
        updated_at=row.updated_at,
        error=row.error,
    )


def _json_text(value: Any) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        raw = value.strip()
        return raw or "[]"
    return json.dumps(value, ensure_ascii=False)


def _parser_snapshot_or_none(raw_ingredient_text: Any) -> dict[str, Any] | None:
    raw = normalize_nonempty_string(raw_ingredient_text)
    if not raw or not parser_ready():
        return None
    return build_parser_snapshot(raw)


def _apply_parser_snapshot_to_row(row: CandidateRow, snapshot: dict[str, Any] | None) -> None:
    if not snapshot:
        row.cleaned_text = None
        row.parse_status = None
        row.parse_confidence = None
        row.inci_list = None
        row.inci_list_json = None
        row.unrecognized_tokens_json = None
        row.normalization_notes_json = None
        row.needs_review_json = None
        return

    row.cleaned_text = normalize_nonempty_string(snapshot.get("cleaned_text")) or None
    row.parse_status = normalize_nonempty_string(snapshot.get("parse_status")) or None
    row.parse_confidence = float(snapshot.get("parse_confidence") or 0.0)
    row.inci_list = normalize_nonempty_string(snapshot.get("inci_list")) or None
    row.inci_list_json = _json_text(snapshot.get("inci_list_json"))
    row.unrecognized_tokens_json = _json_text(snapshot.get("unrecognized_tokens"))
    row.normalization_notes_json = _json_text(snapshot.get("normalization_notes"))
    row.needs_review_json = _json_text(snapshot.get("needs_review"))


def _normalized_inci_key(value: Any) -> str:
    parts = [normalize_nonempty_string(part).lower() for part in str(value or "").split(";")]
    normalized = [part for part in parts if part]
    return ";".join(normalized)


def _same_source_path_with_variant_upgrade(current_source_ref: str, other_source_ref: str) -> bool:
    current_url = normalize_url_like(current_source_ref)
    other_url = normalize_url_like(other_source_ref)
    if not current_url or not other_url:
        return False

    try:
        current_parsed = urlparse(current_url)
        other_parsed = urlparse(other_url)
    except Exception:  # noqa: BLE001
        return False

    if current_parsed.netloc.lower() != other_parsed.netloc.lower():
        return False
    if current_parsed.path.rstrip("/").lower() != other_parsed.path.rstrip("/").lower():
        return False

    current_variant = parse_qs(current_parsed.query).get("variant", [])
    other_variant = parse_qs(other_parsed.query).get("variant", [])
    return bool(current_variant) and not other_variant


def _duplicate_conflict(db, row: CandidateRow) -> dict[str, Any] | None:
    sku_key = normalize_nonempty_string(row.sku_key or row.candidate_id)
    if not sku_key:
        return None

    matches = db.scalars(
        select(CandidateRow).where(
            CandidateRow.sku_key == sku_key,
            CandidateRow.row_id != row.row_id,
        )
    ).all()
    current_inci = _normalized_inci_key(row.inci_list)
    if not current_inci:
        return None

    for other in matches:
        other_inci = _normalized_inci_key(other.inci_list)
        if not other_inci or other_inci == current_inci:
            continue
        same_import = normalize_nonempty_string(other.import_id) == normalize_nonempty_string(row.import_id)
        other_review_status = normalize_nonempty_string(other.review_status).upper()
        # Historical rows should only block a new candidate if they were already approved.
        # Otherwise stale third-party or superseded rows can permanently poison official refreshes.
        if not same_import and other_review_status != "APPROVED":
            continue
        current_source_type = normalize_nonempty_string(row.source_type).lower()
        other_source_type = normalize_nonempty_string(other.source_type).lower()
        if (
            not same_import
            and other_review_status == "APPROVED"
            and current_source_type == "official"
            and other_source_type == "official"
            and _same_source_path_with_variant_upgrade(row.source_ref or "", other.source_ref or "")
        ):
            continue
        return {
            "sku_key": sku_key,
            "current_row_id": row.row_id,
            "conflicting_row_id": other.row_id,
            "conflicting_import_id": other.import_id,
            "conflicting_review_status": other.review_status,
            "current_inci_list": row.inci_list,
            "conflicting_inci_list": other.inci_list,
        }
    return None


def _persist_audit_findings(db, audit_run_id: str, row: CandidateRow, findings: list[dict[str, Any]]) -> None:
    for finding in findings:
        db.add(
            CandidateRowAuditFinding(
                audit_run_id=audit_run_id,
                row_id=row.row_id,
                import_id=row.import_id,
                anomaly_type=normalize_nonempty_string(finding.get("anomaly_type")),
                severity=normalize_nonempty_string(finding.get("severity")),
                evidence_json=json.dumps(finding.get("evidence") or {}, ensure_ascii=False),
                recommended_action=normalize_nonempty_string(finding.get("recommended_action")),
                auto_fixable=bool(finding.get("auto_fixable")),
                created_at=utcnow(),
            )
        )


def _record_correction(
    db,
    *,
    row: CandidateRow,
    audit_run_id: str | None,
    correction_type: str,
    status: str,
    auto_applied: bool,
    actor: str | None,
    before_payload: dict[str, Any] | None,
    after_payload: dict[str, Any] | None,
    error: str | None = None,
) -> None:
    db.add(
        CandidateRowCorrection(
            audit_run_id=audit_run_id,
            row_id=row.row_id,
            correction_type=correction_type,
            status=status,
            auto_applied=auto_applied,
            actor=actor,
            before_payload_json=json.dumps(before_payload or {}, ensure_ascii=False),
            after_payload_json=json.dumps(after_payload or {}, ensure_ascii=False),
            error=error,
            created_at=utcnow(),
        )
    )


def _row_payload(row: CandidateRow) -> dict[str, Any]:
    return {
        "row_id": row.row_id,
        "brand": row.brand,
        "product_name": row.product_name,
        "market": row.market,
        "candidate_id": row.candidate_id,
        "sku_key": row.sku_key,
        "external_seed_id": row.external_seed_id,
        "external_product_id": row.external_product_id,
        "raw_ingredient_text": row.raw_ingredient_text,
        "cleaned_text": row.cleaned_text,
        "source_ref": row.source_ref,
        "source_type": row.source_type,
        "status": row.status,
        "confidence": row.confidence,
        "parse_status": row.parse_status,
        "parse_confidence": row.parse_confidence,
        "inci_list": row.inci_list,
        "review_status": row.review_status,
        "audit_status": row.audit_status,
        "audit_score": row.audit_score,
        "ingest_allowed": bool(row.ingest_allowed),
    }


def _apply_current_audit(db, row: CandidateRow, audit_run_id: str | None = None) -> list[dict[str, Any]]:
    source_match_status, source_match_evidence = compute_source_match_status(
        row.brand,
        row.product_name,
        row.source_ref or "",
        row.source_type,
    )
    ingredient_signal_type = compute_ingredient_signal_type(
        row.raw_ingredient_text or "",
        row.cleaned_text or "",
        row.parse_status or "",
        row.inci_list or "",
    )
    findings = build_audit_findings(
        row_id=row.row_id,
        import_id=row.import_id,
        brand=row.brand,
        product_name=row.product_name,
        source_ref=row.source_ref or "",
        source_type=row.source_type,
        raw_ingredient_text=row.raw_ingredient_text or "",
        cleaned_text=row.cleaned_text or "",
        parse_status=row.parse_status or "",
        parse_confidence=row.parse_confidence,
        inci_list=row.inci_list or "",
        normalization_notes=_loads_json(row.normalization_notes_json, []),
        source_match_status=source_match_status,
        source_match_evidence=source_match_evidence,
        ingredient_signal_type=ingredient_signal_type,
        duplicate_conflict=_duplicate_conflict(db, row),
    )
    audit_status, audit_score = compute_audit_status(findings)
    row.source_match_status = source_match_status
    row.ingredient_signal_type = ingredient_signal_type
    row.audit_status = audit_status
    row.audit_score = audit_score
    row.ingest_allowed = bool(
        (row.parse_status or "").upper() == "OK"
        and row.review_status == "APPROVED"
        and audit_status == "PASS"
    )
    row.updated_at = utcnow()
    if audit_run_id:
        _persist_audit_findings(db, audit_run_id, row, findings)
    return findings


def _apply_conservative_corrections(db, row: CandidateRow, audit_run_id: str | None) -> bool:
    changed = False
    current_source_ref = normalize_nonempty_string(row.source_ref)
    normalized_source_ref = normalize_url_like(current_source_ref)
    if normalized_source_ref and normalized_source_ref != current_source_ref:
        before = _row_payload(row)
        row.source_ref = normalized_source_ref
        _record_correction(
            db,
            row=row,
            audit_run_id=audit_run_id,
            correction_type="normalize_source_ref",
            status="applied",
            auto_applied=True,
            actor="system:auto",
            before_payload=before,
            after_payload=_row_payload(row),
        )
        changed = True

    snapshot = _parser_snapshot_or_none(row.raw_ingredient_text)
    if snapshot:
        before = _row_payload(row)
        _apply_parser_snapshot_to_row(row, snapshot)
        _record_correction(
            db,
            row=row,
            audit_run_id=audit_run_id,
            correction_type="reparse_current_text",
            status="applied",
            auto_applied=True,
            actor="system:auto",
            before_payload=before,
            after_payload=_row_payload(row),
        )
        changed = True

    return changed


@app.get("/health")
def health() -> dict[str, Any]:
    redis_ready: Optional[bool] = None
    redis_error: Optional[str] = None
    if settings.redis_url:
        try:
            r = Redis.from_url(settings.redis_url, socket_connect_timeout=1, socket_timeout=1)
            r.ping()
            redis_ready = True
        except Exception as exc:  # noqa: BLE001
            redis_ready = False
            redis_error = str(exc)[:300]
    return {
        "ok": True,
        "queue_mode": settings.queue_mode,
        "has_redis": bool(settings.redis_url),
        "redis_ready": redis_ready,
        "redis_error": redis_error,
        "db_ready": _DB_READY,
        "db_url_scheme": (settings.db_url.split(":", 1)[0] if settings.db_url else None),
        "db_error": _DB_ERROR,
        "parser_ready": parser_ready(),
        "parser_error": parser_error(),
    }


@app.on_event("startup")
def _startup() -> None:
    global _DB_READY  # noqa: PLW0603
    global _DB_ERROR  # noqa: PLW0603
    try:
        _init_db()
        _DB_READY = True
        _DB_ERROR = None
    except RetryError as exc:
        _DB_READY = False
        _DB_ERROR = f"DB init failed after retries: {exc.last_attempt.exception()!s}"[:500]
    except Exception as exc:  # noqa: BLE001
        _DB_READY = False
        _DB_ERROR = f"DB init failed: {exc!s}"[:500]


def _find_column(df: pd.DataFrame, *names: str) -> str:
    normalized = {str(column).strip().lower(): str(column) for column in df.columns}
    for name in names:
        found = normalized.get(name.strip().lower())
        if found:
            return found
    return ""


def _clean_existing(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:  # noqa: BLE001
        pass
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null", "n/a", "na"}:
        return ""
    return text


def _safe_float(value: Any) -> float | None:
    text = _clean_existing(value)
    if not text:
        return None
    try:
        return float(text)
    except Exception:  # noqa: BLE001
        return None


def _new_audit_run_id(stage: str) -> str:
    normalized_stage = "".join(ch if ch.isalnum() else "_" for ch in normalize_nonempty_string(stage).lower()).strip("_")
    return f"iar_{(normalized_stage or 'audit')[:16]}_{uuid.uuid4().hex[:12]}"


def _summary_from_findings(rows: list[list[dict[str, Any]]]) -> dict[str, int]:
    blocker_count = 0
    review_count = 0
    info_count = 0
    flagged_rows = 0
    findings_total = 0
    for findings in rows:
        if findings:
            flagged_rows += 1
        findings_total += len(findings)
        for finding in findings:
            severity = normalize_nonempty_string(finding.get("severity")).lower()
            if severity == "blocker":
                blocker_count += 1
            elif severity == "review":
                review_count += 1
            else:
                info_count += 1
    return {
        "flagged_rows": flagged_rows,
        "findings_total": findings_total,
        "blocker_count": blocker_count,
        "review_count": review_count,
        "info_count": info_count,
    }


def _export_row_payload(row: CandidateRow, *, reviewed_mode: bool) -> dict[str, Any]:
    base = {
        "row_id": row.row_id,
        "row_index": row.row_index,
        "brand": row.brand,
        "product_name": row.product_name,
        "market": row.market,
        "status": row.status,
        "confidence": row.confidence,
        "source_type": row.source_type,
        "source_ref": row.source_ref,
        "raw_ingredient_text": row.raw_ingredient_text,
        "error": row.error,
        "updated_at": row.updated_at.isoformat(),
    }
    if not reviewed_mode:
        return base
    return {
        **base,
        "candidate_id": row.candidate_id,
        "sku_key": row.sku_key,
        "external_seed_id": row.external_seed_id,
        "external_product_id": row.external_product_id,
        "cleaned_text": row.cleaned_text,
        "parse_status": row.parse_status,
        "parse_confidence": row.parse_confidence,
        "inci_list": row.inci_list,
        "inci_list_json": _json_text(_loads_json(row.inci_list_json, [])),
        "unrecognized_tokens": _json_text(_loads_json(row.unrecognized_tokens_json, [])),
        "normalization_notes": _json_text(_loads_json(row.normalization_notes_json, [])),
        "needs_review": _json_text(_loads_json(row.needs_review_json, [])),
        "review_status": row.review_status or "UNREVIEWED",
        "reviewed_by": row.reviewed_by,
        "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else "",
        "audit_status": row.audit_status or "UNAUDITED",
        "audit_score": row.audit_score,
        "source_match_status": row.source_match_status,
        "ingredient_signal_type": row.ingredient_signal_type,
        "ingest_allowed": bool(row.ingest_allowed),
    }


def _reset_audit_state(row: CandidateRow) -> None:
    row.review_status = "UNREVIEWED"
    row.reviewed_by = None
    row.reviewed_at = None
    row.audit_status = "UNAUDITED"
    row.audit_score = None
    row.source_match_status = None
    row.ingredient_signal_type = None
    row.ingest_allowed = False


def _maybe_refresh_parser_snapshot(row: CandidateRow) -> None:
    snapshot = _parser_snapshot_or_none(row.raw_ingredient_text)
    _apply_parser_snapshot_to_row(row, snapshot)


@app.post("/v1/imports", response_model=ImportResponse)
async def create_import(file: UploadFile = File(...)) -> ImportResponse:
    _require_db()
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")
    raw = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"CSV parse failed: {exc}") from exc

    brand_col = _find_column(df, "brand", "brand_en", "brand_zh", "brand_original")
    product_col = _find_column(df, "product_name", "product", "product_name_en", "product_name_zh", "product_name_original", "product title")
    market_col = _find_column(df, "market", "country", "region")
    raw_ing_col = _find_column(df, "raw_ingredient_text", "ingredients", "ingredient_text")
    candidate_id_col = _find_column(df, "candidate_id")
    sku_key_col = _find_column(df, "sku_key", "sku")
    external_seed_id_col = _find_column(df, "external_seed_id", "seed_id")
    external_product_id_col = _find_column(df, "external_product_id")
    source_ref_col = _find_column(df, "source_ref", "source_url", "url")
    source_type_col = _find_column(df, "source_type")
    status_col = _find_column(df, "status", "harvest_status")
    confidence_col = _find_column(df, "confidence", "harvest_confidence")

    if not brand_col or not product_col or not market_col:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain columns: brand, product_name, market (case-insensitive).",
        )

    with db_session() as db:
        batch = ImportBatch(filename=file.filename)
        db.add(batch)
        db.commit()
        db.refresh(batch)

        total = 0
        for idx, r in df.iterrows():
            brand = _clean_existing(r.get(brand_col))
            product_name = _clean_existing(r.get(product_col))
            market = _clean_existing(r.get(market_col))
            if not (brand and product_name and market):
                continue
            existing = _clean_existing(r.get(raw_ing_col)) if raw_ing_col else ""
            imported_status = _clean_existing(r.get(status_col)) if status_col else ""
            candidate_id = _clean_existing(r.get(candidate_id_col)) if candidate_id_col else ""
            sku_key = _clean_existing(r.get(sku_key_col)) if sku_key_col else ""
            row = CandidateRow(
                import_id=batch.import_id,
                row_index=int(idx),
                brand=brand,
                product_name=product_name,
                market=market,
                candidate_id=candidate_id or None,
                sku_key=(sku_key or candidate_id) or None,
                external_seed_id=_clean_existing(r.get(external_seed_id_col)) or None if external_seed_id_col else None,
                external_product_id=_clean_existing(r.get(external_product_id_col)) or None if external_product_id_col else None,
                raw_ingredient_text=existing or None,
                source_ref=(normalize_url_like(r.get(source_ref_col)) or _clean_existing(r.get(source_ref_col))) or None if source_ref_col else None,
                source_type=_clean_existing(r.get(source_type_col)) or None if source_type_col else None,
                status=imported_status or ("SKIPPED" if existing else "EMPTY"),
                confidence=_safe_float(r.get(confidence_col)) if confidence_col else (1.0 if existing else None),
                updated_at=utcnow(),
            )
            if existing:
                _maybe_refresh_parser_snapshot(row)
                _reset_audit_state(row)
            db.add(row)
            total += 1
        db.commit()

        return ImportResponse(import_id=batch.import_id, filename=batch.filename, created_at=batch.created_at, total_rows=total)


@app.post("/v1/parser/re-parse", response_model=ParserReparseResponse)
def parser_reparse(req: ParserReparseRequest) -> ParserReparseResponse:
    _require_parser()
    return _to_parser_response(build_parser_snapshot(req.raw_ingredient_text))


@app.post("/v1/parser/re-parse-batch", response_model=ParserReparseBatchResponse)
def parser_reparse_batch(req: ParserReparseBatchRequest) -> ParserReparseBatchResponse:
    _require_parser()
    out: list[ParserReparseBatchResponseItem] = []
    for item in req.items or []:
        out.append(
            ParserReparseBatchResponseItem(
                row_id=item.row_id,
                result=_to_parser_response(build_parser_snapshot(item.raw_ingredient_text)),
            )
        )
    return ParserReparseBatchResponse(items=out)


@app.get("/v1/imports/{import_id}/rows", response_model=ListRowsResponse)
def list_rows(
    import_id: str,
    status: Optional[str] = Query(default=None),
    parse_status: Optional[str] = Query(default=None),
    review_status: Optional[str] = Query(default=None),
    audit_status: Optional[str] = Query(default=None),
    ingest_allowed: Optional[bool] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> ListRowsResponse:
    _require_db()
    with db_session() as db:
        stmt = select(CandidateRow).where(CandidateRow.import_id == import_id)
        if status:
            stmt = stmt.where(CandidateRow.status == status)
        if parse_status:
            stmt = stmt.where(CandidateRow.parse_status == parse_status)
        if review_status:
            stmt = stmt.where(CandidateRow.review_status == review_status)
        if audit_status:
            stmt = stmt.where(CandidateRow.audit_status == audit_status)
        if ingest_allowed is not None:
            stmt = stmt.where(CandidateRow.ingest_allowed == bool(ingest_allowed))
        if q:
            like = f"%{q.strip()}%"
            stmt = stmt.where(
                CandidateRow.brand.ilike(like)  # type: ignore[attr-defined]
                | CandidateRow.product_name.ilike(like)  # type: ignore[attr-defined]
                | CandidateRow.candidate_id.ilike(like)  # type: ignore[attr-defined]
                | CandidateRow.sku_key.ilike(like)  # type: ignore[attr-defined]
            )
        total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        items = db.scalars(stmt.order_by(CandidateRow.row_index.asc()).limit(limit).offset(offset)).all()
        return ListRowsResponse(import_id=import_id, total=int(total), items=[_row_view(r) for r in items])


@app.post("/v1/tasks", response_model=CreateTaskResponse)
def create_task(req: CreateTaskRequest) -> CreateTaskResponse:
    _require_db()
    with db_session() as db:
        batch = db.scalar(select(ImportBatch).where(ImportBatch.import_id == req.import_id))
        if not batch:
            raise HTTPException(status_code=404, detail="Import not found.")

        if req.row_ids:
            rows = db.scalars(
                select(CandidateRow).where(CandidateRow.import_id == req.import_id, CandidateRow.row_id.in_(req.row_ids))
            ).all()
        else:
            rows = db.scalars(select(CandidateRow).where(CandidateRow.import_id == req.import_id)).all()

        if not rows:
            raise HTTPException(status_code=400, detail="No rows selected.")

        task = HarvestTask(import_id=req.import_id, status="RUNNING", force=1 if req.force else 0, started_at=utcnow())
        db.add(task)
        db.commit()
        db.refresh(task)

        queued = 0
        for row in rows:
            tr = TaskRow(task_id=task.task_id, row_id=row.row_id, status="QUEUED")
            db.add(tr)
        db.commit()

        for row in rows:
            job_kwargs = {"task_id": task.task_id, "row_id": row.row_id, "force": bool(req.force)}
            ref = enqueue("app.jobs.harvest_row", kwargs=job_kwargs)
            if ref is None:
                harvest_row(**job_kwargs)
            queued += 1

        return CreateTaskResponse(task_id=task.task_id, import_id=req.import_id, status="RUNNING", queued=queued)


@app.get("/v1/tasks/{task_id}", response_model=TaskProgress)
def get_task(task_id: str) -> TaskProgress:
    _require_db()
    with db_session() as db:
        task = db.scalar(select(HarvestTask).where(HarvestTask.task_id == task_id))
        if not task:
            raise HTTPException(status_code=404, detail="Task not found.")

        counts = dict(
            db.execute(
                select(TaskRow.status, func.count()).where(TaskRow.task_id == task_id).group_by(TaskRow.status)
            ).all()
        )

        done = sum(counts.get(s, 0) for s in ["OK", "PENDING", "NEEDS_SOURCE", "SKIPPED", "ERROR"])
        total = sum(counts.values())

        status = task.status
        if status == "RUNNING" and total > 0 and done == total:
            task.status = "COMPLETED"
            task.finished_at = utcnow()
            db.add(task)
            db.commit()
            status = "COMPLETED"

        return TaskProgress(
            task_id=task.task_id,
            import_id=task.import_id,
            status=status,  # type: ignore[arg-type]
            force=bool(task.force),
            created_at=task.created_at,
            started_at=task.started_at,
            finished_at=task.finished_at,
            counts={k: int(v) for k, v in counts.items()},
        )


@app.patch("/v1/rows/{row_id}", response_model=UpdateRowResponse)
def update_row(row_id: str, req: UpdateRowRequest) -> UpdateRowResponse:
    _require_db()
    with db_session() as db:
        row = db.scalar(select(CandidateRow).where(CandidateRow.row_id == row_id))
        if not row:
            raise HTTPException(status_code=404, detail="Row not found.")

        payload = req.model_dump(exclude_unset=True)
        parser_related_change = False
        review_related_change = False

        if "status" in payload:
            row.status = req.status  # type: ignore[assignment]
        if "raw_ingredient_text" in payload:
            row.raw_ingredient_text = normalize_nonempty_string(req.raw_ingredient_text) or None
            _maybe_refresh_parser_snapshot(row)
            _reset_audit_state(row)
            parser_related_change = True
        if "cleaned_text" in payload:
            row.cleaned_text = normalize_nonempty_string(req.cleaned_text) or None
            parser_related_change = True
        if "source_ref" in payload:
            row.source_ref = normalize_url_like(req.source_ref) or normalize_nonempty_string(req.source_ref) or None
            parser_related_change = True
        if "source_type" in payload:
            row.source_type = normalize_nonempty_string(req.source_type) or None
            parser_related_change = True
        if "confidence" in payload:
            row.confidence = req.confidence
        if "parse_status" in payload:
            row.parse_status = normalize_nonempty_string(req.parse_status) or None
            parser_related_change = True
        if "parse_confidence" in payload:
            row.parse_confidence = req.parse_confidence
            parser_related_change = True
        if "inci_list" in payload:
            row.inci_list = normalize_nonempty_string(req.inci_list) or None
            parser_related_change = True
        if "inci_list_json" in payload:
            row.inci_list_json = _json_text(req.inci_list_json)
            parser_related_change = True
        if "unrecognized_tokens" in payload:
            row.unrecognized_tokens_json = _json_text(req.unrecognized_tokens)
            parser_related_change = True
        if "normalization_notes" in payload:
            row.normalization_notes_json = _json_text(req.normalization_notes)
            parser_related_change = True
        if "needs_review" in payload:
            row.needs_review_json = _json_text(req.needs_review)
            parser_related_change = True
        if "review_status" in payload:
            row.review_status = req.review_status or "UNREVIEWED"
            row.reviewed_at = utcnow()
            review_related_change = True
        if "reviewed_by" in payload:
            row.reviewed_by = normalize_nonempty_string(req.reviewed_by) or None
            review_related_change = True
        if "error" in payload:
            row.error = normalize_nonempty_string(req.error) or None

        if parser_related_change and not review_related_change and "review_status" not in payload:
            row.review_status = "UNREVIEWED"
            row.reviewed_by = None
            row.reviewed_at = None
        if parser_related_change or review_related_change:
            _apply_current_audit(db, row)

        row.updated_at = utcnow()
        db.add(row)
        db.commit()
        db.refresh(row)
        return UpdateRowResponse(row=_row_view(row))


@app.post("/v1/imports/{import_id}/audit", response_model=ImportAuditResponse)
def audit_import(import_id: str, req: ImportAuditRequest) -> ImportAuditResponse:
    _require_db()
    audit_run_id = _new_audit_run_id(req.stage)
    with db_session() as db:
        stmt = select(CandidateRow).where(CandidateRow.import_id == import_id)
        if req.row_ids:
            stmt = stmt.where(CandidateRow.row_id.in_(req.row_ids))
        rows = db.scalars(stmt.order_by(CandidateRow.row_index.asc())).all()
        if not rows:
            raise HTTPException(status_code=404, detail="Import not found or has no rows.")

        all_findings: list[list[dict[str, Any]]] = []
        corrected_rows = 0
        for row in rows:
            if req.apply_corrections and _apply_conservative_corrections(db, row, audit_run_id):
                corrected_rows += 1
            findings = _apply_current_audit(db, row, audit_run_id)
            db.add(row)
            all_findings.append(findings)

        db.commit()

    summary = _summary_from_findings(all_findings)
    return ImportAuditResponse(
        import_id=import_id,
        audit_run_id=audit_run_id,
        scanned=len(all_findings),
        flagged_rows=summary["flagged_rows"],
        findings_total=summary["findings_total"],
        blocker_count=summary["blocker_count"],
        review_count=summary["review_count"],
        info_count=summary["info_count"],
        corrected_rows=corrected_rows,
    )


@app.patch("/v1/rows/{row_id}/review", response_model=UpdateRowResponse)
def review_row(row_id: str, req: ReviewUpdateRequest) -> UpdateRowResponse:
    _require_db()
    with db_session() as db:
        row = db.scalar(select(CandidateRow).where(CandidateRow.row_id == row_id))
        if not row:
            raise HTTPException(status_code=404, detail="Row not found.")

        before = _row_payload(row)
        payload = req.model_dump(exclude_unset=True)

        if "raw_ingredient_text" in payload:
            row.raw_ingredient_text = normalize_nonempty_string(req.raw_ingredient_text) or None
        if "source_ref" in payload:
            row.source_ref = normalize_url_like(req.source_ref) or normalize_nonempty_string(req.source_ref) or None
        if "source_type" in payload:
            row.source_type = normalize_nonempty_string(req.source_type) or None

        if "cleaned_text" in payload:
            row.cleaned_text = normalize_nonempty_string(req.cleaned_text) or None
        if "parse_status" in payload:
            row.parse_status = normalize_nonempty_string(req.parse_status) or None
        if "parse_confidence" in payload:
            row.parse_confidence = req.parse_confidence
        if "inci_list" in payload:
            row.inci_list = normalize_nonempty_string(req.inci_list) or None
        if "inci_list_json" in payload:
            row.inci_list_json = _json_text(req.inci_list_json)
        if "unrecognized_tokens" in payload:
            row.unrecognized_tokens_json = _json_text(req.unrecognized_tokens)
        if "normalization_notes" in payload:
            row.normalization_notes_json = _json_text(req.normalization_notes)
        if "needs_review" in payload:
            row.needs_review_json = _json_text(req.needs_review)

        if row.raw_ingredient_text and "parse_status" not in payload:
            _maybe_refresh_parser_snapshot(row)

        row.review_status = req.review_status
        row.reviewed_by = normalize_nonempty_string(req.reviewed_by) or None
        row.reviewed_at = utcnow()
        row.updated_at = utcnow()
        _apply_current_audit(db, row)
        _record_correction(
            db,
            row=row,
            audit_run_id=None,
            correction_type="manual_review_update",
            status="applied",
            auto_applied=False,
            actor=row.reviewed_by or "reviewer",
            before_payload=before,
            after_payload=_row_payload(row),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return UpdateRowResponse(row=_row_view(row))


@app.patch("/v1/rows/{row_id}/correction", response_model=UpdateRowResponse)
def correct_row(row_id: str, req: CorrectionUpdateRequest) -> UpdateRowResponse:
    _require_db()
    with db_session() as db:
        row = db.scalar(select(CandidateRow).where(CandidateRow.row_id == row_id))
        if not row:
            raise HTTPException(status_code=404, detail="Row not found.")

        before = _row_payload(row)
        payload = req.model_dump(exclude_unset=True)
        reassessment_required = False

        if "raw_ingredient_text" in payload:
            row.raw_ingredient_text = normalize_nonempty_string(req.raw_ingredient_text) or None
            reassessment_required = True
        if "source_ref" in payload:
            row.source_ref = normalize_url_like(req.source_ref) or normalize_nonempty_string(req.source_ref) or None
            reassessment_required = True
        if "source_type" in payload:
            row.source_type = normalize_nonempty_string(req.source_type) or None
            reassessment_required = True
        if "brand" in payload:
            row.brand = normalize_nonempty_string(req.brand) or row.brand
            reassessment_required = True
        if "product_name" in payload:
            row.product_name = normalize_nonempty_string(req.product_name) or row.product_name
            reassessment_required = True
        if "market" in payload:
            row.market = normalize_nonempty_string(req.market) or row.market
            reassessment_required = True

        if req.apply_parser:
            _maybe_refresh_parser_snapshot(row)
            reassessment_required = True

        if reassessment_required:
            row.review_status = "UNREVIEWED"
            row.reviewed_by = None
            row.reviewed_at = None

        _apply_current_audit(db, row)
        row.updated_at = utcnow()
        _record_correction(
            db,
            row=row,
            audit_run_id=None,
            correction_type=normalize_nonempty_string(req.correction_type) or "manual_correction",
            status="applied",
            auto_applied=False,
            actor=normalize_nonempty_string(req.actor) or "operator",
            before_payload=before,
            after_payload=_row_payload(row),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return UpdateRowResponse(row=_row_view(row))


@app.post("/v1/rows/{row_id}/rerun", response_model=CreateTaskResponse)
def rerun_single_row(row_id: str, force: bool = Query(default=False)) -> CreateTaskResponse:
    _require_db()
    with db_session() as db:
        row = db.scalar(select(CandidateRow).where(CandidateRow.row_id == row_id))
        if not row:
            raise HTTPException(status_code=404, detail="Row not found.")
        req = CreateTaskRequest(import_id=row.import_id, row_ids=[row_id], force=force)
    return create_task(req)


@app.get("/v1/exports/{import_id}")
def export_import(
    import_id: str,
    format: str = Query(default="csv"),
    mode: str = Query(default="default"),
):
    _require_db()
    fmt = (format or "csv").strip().lower()
    if fmt not in {"csv", "xlsx"}:
        raise HTTPException(status_code=400, detail="format must be csv or xlsx")
    export_mode = (mode or "default").strip().lower()
    if export_mode not in {"default", "reviewed"}:
        raise HTTPException(status_code=400, detail="mode must be default or reviewed")

    with db_session() as db:
        rows = db.scalars(select(CandidateRow).where(CandidateRow.import_id == import_id).order_by(CandidateRow.row_index.asc())).all()
        if not rows:
            raise HTTPException(status_code=404, detail="Import not found or has no rows.")

    df = pd.DataFrame([_export_row_payload(r, reviewed_mode=export_mode == "reviewed") for r in rows])

    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        data = buf.getvalue().encode("utf-8")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="ingredient_harvest_{import_id}_{export_mode}.csv"'},
        )

    xbuf = io.BytesIO()
    with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="harvest")
    xbuf.seek(0)
    return StreamingResponse(
        xbuf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="ingredient_harvest_{import_id}_{export_mode}.xlsx"'},
    )
