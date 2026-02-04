from __future__ import annotations

import io
import os
import threading
import time
import uuid
from typing import Any, Optional

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

    def col(name: str) -> str:
        for c in df.columns:
            if str(c).strip().lower() == name:
                return str(c)
        return ""

    brand_col = col("brand") or col("brand_en") or col("brand_zh")
    product_col = col("product_name") or col("product") or col("product_name_en") or col("product_name_zh") or col("product title")
    market_col = col("market") or col("country") or col("region")
    raw_ing_col = col("raw_ingredient_text") or col("ingredients") or col("ingredient_text")

    if not brand_col or not product_col or not market_col:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain columns: brand, product_name, market (case-insensitive).",
        )

    def clean_existing(val: Any) -> str:
        if val is None:
            return ""
        try:
            if pd.isna(val):
                return ""
        except Exception:  # noqa: BLE001
            pass
        text = str(val).strip()
        if not text:
            return ""
        if text.lower() in {"nan", "none", "null", "n/a", "na"}:
            return ""
        return text

    with db_session() as db:
        batch = ImportBatch(filename=file.filename)
        db.add(batch)
        db.commit()
        db.refresh(batch)

        total = 0
        for idx, r in df.iterrows():
            brand = str(r.get(brand_col) or "").strip()
            product_name = str(r.get(product_col) or "").strip()
            market = str(r.get(market_col) or "").strip()
            if not (brand and product_name and market):
                continue
            existing = clean_existing(r.get(raw_ing_col)) if raw_ing_col else ""
            row = CandidateRow(
                import_id=batch.import_id,
                row_index=int(idx),
                brand=brand,
                product_name=product_name,
                market=market,
                raw_ingredient_text=existing or None,
                status="SKIPPED" if existing else "EMPTY",
                confidence=1.0 if existing else None,
                updated_at=utcnow(),
            )
            db.add(row)
            total += 1
        db.commit()

        return ImportResponse(import_id=batch.import_id, filename=batch.filename, created_at=batch.created_at, total_rows=total)


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
