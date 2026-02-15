from __future__ import annotations

from sqlalchemy import select

from app.db import db_session, utcnow
from app.harvester.source_harvester import SourceHarvester
from app.models import CandidateRow, TaskRow
from app.non_cosmetic import non_cosmetic_skip_reason


def _has_meaningful_ingredients(text: str | None) -> bool:
    if not text:
        return False
    t = str(text).strip()
    if not t:
        return False
    if t.lower() in {"nan", "none", "null", "n/a", "na"}:
        return False
    return True


def harvest_row(*, task_id: str, row_id: str, force: bool) -> None:
    harvester = SourceHarvester()
    with db_session() as db:
        tr = db.scalar(select(TaskRow).where(TaskRow.task_id == task_id, TaskRow.row_id == row_id))
        row = db.scalar(select(CandidateRow).where(CandidateRow.row_id == row_id))
        if not tr or not row:
            return

        tr.status = "RUNNING"
        tr.started_at = utcnow()
        tr.message = None
        db.add(tr)
        db.commit()

        skip_reason = non_cosmetic_skip_reason(brand=row.brand, product_name=row.product_name)
        if skip_reason:
            row.status = "SKIPPED"
            row.error = skip_reason
            row.updated_at = utcnow()

            tr.status = "SKIPPED"
            tr.finished_at = utcnow()
            tr.message = skip_reason
            db.add_all([row, tr])
            db.commit()
            return

        if not force and _has_meaningful_ingredients(row.raw_ingredient_text):
            row.status = "SKIPPED"
            row.updated_at = utcnow()
            tr.status = "SKIPPED"
            tr.finished_at = utcnow()
            tr.message = "already_has_raw_ingredient_text"
            db.add_all([row, tr])
            db.commit()
            return

        try:
            outcome = harvester.process(
                market=row.market,
                brand=row.brand,
                product_name=row.product_name,
                preferred_url=row.source_ref,
            )
            row.status = outcome.status
            row.confidence = outcome.confidence
            row.raw_ingredient_text = outcome.raw_ingredient_text
            row.source_ref = outcome.source_ref
            row.source_type = outcome.source_type
            row.error = None
            row.updated_at = utcnow()

            tr.status = outcome.status
            tr.message = outcome.debug.get("hint") if isinstance(outcome.debug, dict) else None
            tr.finished_at = utcnow()

            db.add_all([row, tr])
            db.commit()
        except Exception as exc:  # noqa: BLE001
            row.status = "ERROR"
            row.error = str(exc)[:500]
            row.updated_at = utcnow()

            tr.status = "ERROR"
            tr.message = str(exc)[:500]
            tr.finished_at = utcnow()
            db.add_all([row, tr])
            db.commit()
