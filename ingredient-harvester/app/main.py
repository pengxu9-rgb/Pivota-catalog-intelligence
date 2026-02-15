from __future__ import annotations

import io
import json
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

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
from app.models import Base, CandidateRow, HarvestTask, ImportBatch, TaskRow
from app.queue import enqueue
from app.schema import (
    CandidateRowView,
    CreateTaskRequest,
    CreateTaskResponse,
    ImportResponse,
    ListRowsResponse,
    ParserReparseBatchRequest,
    ParserReparseBatchResponse,
    ParserReparseBatchResponseItem,
    ParserReparseRequest,
    ParserReparseResponse,
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


_PARSER_ENGINE: Any = None
_PARSER_ERROR: Optional[str] = None
try:
    # In monorepo mode, Railway "root directory" can be `ingredient-harvester/`,
    # but the full repo is still available in the container. Import the parser
    # engine from `../services/ingredient_parser.py`.
    import sys as _sys

    _services_dir = Path(__file__).resolve().parents[2] / "services"
    if _services_dir.exists():
        _sys.path.insert(0, str(_services_dir))
    from ingredient_parser import ParserEngine, clean_noise  # type: ignore[import-not-found]
    from ingredient_parser import _coerce_text as _parser_coerce_text  # type: ignore[import-not-found]
    from ingredient_parser import _preprocess as _parser_preprocess  # type: ignore[import-not-found]

    _PARSER_ENGINE = ParserEngine()
except Exception as exc:  # noqa: BLE001
    _PARSER_ENGINE = None
    _PARSER_ERROR = f"Parser unavailable: {type(exc).__name__}: {exc!s}"[:300]


def _require_parser() -> Any:
    if _PARSER_ENGINE is not None:
        return _PARSER_ENGINE
    raise HTTPException(status_code=503, detail=_PARSER_ERROR or "Parser not available.")


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


def _to_parser_response(parsed: dict[str, Any], *, cleaned_text: str) -> ParserReparseResponse:
    return ParserReparseResponse(
        cleaned_text=cleaned_text or "",
        parse_status=str(parsed.get("parse_status") or "NEEDS_REVIEW"),
        inci_list=str(parsed.get("inci_list") or ""),
        inci_list_json=_loads_json(parsed.get("inci_list_json"), []),
        unrecognized_tokens=_loads_json(parsed.get("unrecognized_tokens"), []),
        normalization_notes=_loads_json(parsed.get("normalization_notes"), []),
        parse_confidence=float(parsed.get("parse_confidence") or 0.0),
        needs_review=_loads_json(parsed.get("needs_review"), []),
    )


_COL_NAME_SANITIZER = re.compile(r"[^a-z0-9]+")


def _normalize_col_name(name: str) -> str:
    return _COL_NAME_SANITIZER.sub("_", str(name or "").strip().lower()).strip("_")


def _first_matching_col(df: pd.DataFrame, aliases: list[str]) -> str:
    normalized_to_actual: dict[str, str] = {}
    for c in df.columns:
        key = _normalize_col_name(str(c))
        if key and key not in normalized_to_actual:
            normalized_to_actual[key] = str(c)
    for alias in aliases:
        key = _normalize_col_name(alias)
        if key in normalized_to_actual:
            return normalized_to_actual[key]
    return ""


def _clean_cell(value: Any) -> str:
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


def _normalize_http_url(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    if text.startswith("//"):
        return f"https:{text}"
    parsed = urlparse(text)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return text
    if not parsed.scheme and parsed.netloc:
        return f"https://{text}"
    if not parsed.scheme and parsed.path and "." in parsed.path and " " not in parsed.path:
        return f"https://{parsed.path}"
    return ""


def _infer_market_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()

    locale_match = re.search(r"/([a-z]{2})[-_]([a-z]{2})(?:/|$)", path)
    if locale_match:
        region = locale_match.group(2).upper()
        return "UK" if region == "GB" else region

    if host.endswith(".cn") or "/zh-cn/" in path:
        return "CN"

    tld_map = {
        ".us": "US",
        ".ca": "CA",
        ".uk": "UK",
        ".jp": "JP",
        ".kr": "KR",
        ".fr": "FR",
        ".de": "DE",
        ".it": "IT",
        ".es": "ES",
        ".au": "AU",
    }
    for tld, market in tld_map.items():
        if host.endswith(tld):
            return market
    return "GLOBAL"


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
        status=row.status,  # type: ignore[arg-type]
        confidence=row.confidence,
        source_type=row.source_type,
        source_ref=row.source_ref,
        raw_ingredient_text=row.raw_ingredient_text,
        updated_at=row.updated_at,
        error=row.error,
)


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
        "parser_ready": bool(_PARSER_ENGINE),
        "parser_error": _PARSER_ERROR,
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

    brand_col = _first_matching_col(df, ["brand", "brand_en", "brand_zh", "vendor"])
    product_col = _first_matching_col(
        df,
        [
            "product_name",
            "product",
            "product_name_en",
            "product_name_zh",
            "product title",
            "product_title",
            "title",
            "name",
        ],
    )
    market_col = _first_matching_col(df, ["market", "country", "region", "market_code", "locale"])
    url_col = _first_matching_col(
        df,
        [
            "product_url",
            "product url",
            "product_link",
            "product link",
            "url",
            "link",
            "deep_link",
            "deep link",
            "source_ref",
            "source_url",
        ],
    )
    raw_ing_col = _first_matching_col(df, ["raw_ingredient_text", "ingredients", "ingredient_text", "inci_list"])

    if not brand_col or not product_col:
        raise HTTPException(
            status_code=400,
            detail=(
                "CSV must contain columns for brand and product name "
                "(e.g. brand/product_name or Brand/Product Title, case-insensitive)."
            ),
        )

    with db_session() as db:
        batch = ImportBatch(filename=file.filename)
        db.add(batch)
        db.commit()
        db.refresh(batch)

        total = 0
        for idx, r in df.iterrows():
            brand = _clean_cell(r.get(brand_col))
            product_name = _clean_cell(r.get(product_col))
            if not (brand and product_name):
                continue
            existing = _clean_cell(r.get(raw_ing_col)) if raw_ing_col else ""
            source_ref = _normalize_http_url(_clean_cell(r.get(url_col))) if url_col else ""
            market = _clean_cell(r.get(market_col)) if market_col else ""
            if not market:
                market = _infer_market_from_url(source_ref) if source_ref else "GLOBAL"
            row = CandidateRow(
                import_id=batch.import_id,
                row_index=int(idx),
                brand=brand,
                product_name=product_name,
                market=(market or "GLOBAL").upper()[:16],
                raw_ingredient_text=existing or None,
                source_ref=source_ref or None,
                status="SKIPPED" if existing else "EMPTY",
                confidence=1.0 if existing else None,
                updated_at=utcnow(),
            )
            db.add(row)
            total += 1
        db.commit()

        return ImportResponse(import_id=batch.import_id, filename=batch.filename, created_at=batch.created_at, total_rows=total)


@app.post("/v1/parser/re-parse", response_model=ParserReparseResponse)
def parser_reparse(req: ParserReparseRequest) -> ParserReparseResponse:
    engine = _require_parser()
    raw = _parser_coerce_text(req.raw_ingredient_text)
    pre = _parser_preprocess(raw)
    cleaned, _ = clean_noise(pre)
    parsed = engine.parse(raw)
    return _to_parser_response(parsed, cleaned_text=cleaned)


@app.post("/v1/parser/re-parse-batch", response_model=ParserReparseBatchResponse)
def parser_reparse_batch(req: ParserReparseBatchRequest) -> ParserReparseBatchResponse:
    engine = _require_parser()
    out: list[ParserReparseBatchResponseItem] = []
    for item in req.items or []:
        raw = _parser_coerce_text(item.raw_ingredient_text)
        pre = _parser_preprocess(raw)
        cleaned, _ = clean_noise(pre)
        parsed = engine.parse(raw)
        out.append(ParserReparseBatchResponseItem(row_id=item.row_id, result=_to_parser_response(parsed, cleaned_text=cleaned)))
    return ParserReparseBatchResponse(items=out)


@app.get("/v1/imports/{import_id}/rows", response_model=ListRowsResponse)
def list_rows(
    import_id: str,
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> ListRowsResponse:
    _require_db()
    with db_session() as db:
        stmt = select(CandidateRow).where(CandidateRow.import_id == import_id)
        if status:
            stmt = stmt.where(CandidateRow.status == status)
        if q:
            like = f"%{q.strip()}%"
            stmt = stmt.where(
                CandidateRow.brand.ilike(like)  # type: ignore[attr-defined]
                | CandidateRow.product_name.ilike(like)  # type: ignore[attr-defined]
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

        if req.status is not None:
            row.status = req.status  # type: ignore[assignment]
        if req.raw_ingredient_text is not None:
            row.raw_ingredient_text = req.raw_ingredient_text or None
        if req.source_ref is not None:
            row.source_ref = req.source_ref or None
        if req.source_type is not None:
            row.source_type = req.source_type or None
        if req.confidence is not None:
            row.confidence = req.confidence
        if req.error is not None:
            row.error = req.error or None

        row.updated_at = utcnow()
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
def export_import(import_id: str, format: str = Query(default="csv")):
    _require_db()
    fmt = (format or "csv").strip().lower()
    if fmt not in {"csv", "xlsx"}:
        raise HTTPException(status_code=400, detail="format must be csv or xlsx")

    with db_session() as db:
        rows = db.scalars(select(CandidateRow).where(CandidateRow.import_id == import_id).order_by(CandidateRow.row_index.asc())).all()
        if not rows:
            raise HTTPException(status_code=404, detail="Import not found or has no rows.")

    df = pd.DataFrame(
        [
            {
                "brand": r.brand,
                "product_name": r.product_name,
                "market": r.market,
                "status": r.status,
                "confidence": r.confidence,
                "source_type": r.source_type,
                "source_ref": r.source_ref,
                "raw_ingredient_text": r.raw_ingredient_text,
                "error": r.error,
                "updated_at": r.updated_at.isoformat(),
            }
            for r in rows
        ]
    )

    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        data = buf.getvalue().encode("utf-8")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="ingredient_harvest_{import_id}.csv"'},
        )

    xbuf = io.BytesIO()
    with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="harvest")
    xbuf.seek(0)
    return StreamingResponse(
        xbuf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="ingredient_harvest_{import_id}.xlsx"'},
    )
