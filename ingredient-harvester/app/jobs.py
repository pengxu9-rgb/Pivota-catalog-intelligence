from __future__ import annotations

import json

from sqlalchemy import select

from app.db import db_session, utcnow
from app.harvester.source_harvester import SourceHarvester
from app.models import CandidateRow, TaskRow
from app.parser_runtime import build_parser_snapshot, parser_ready


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
            outcome = harvester.process(market=row.market, brand=row.brand, product_name=row.product_name)
            row.status = outcome.status
            row.confidence = outcome.confidence
            row.raw_ingredient_text = outcome.raw_ingredient_text
            row.source_ref = outcome.source_ref
            row.source_type = outcome.source_type
            row.cleaned_text = None
            row.parse_status = None
            row.parse_confidence = None
            row.inci_list = None
            row.inci_list_json = None
            row.unrecognized_tokens_json = None
            row.normalization_notes_json = None
            row.needs_review_json = None
            row.review_status = "UNREVIEWED"
            row.reviewed_by = None
            row.reviewed_at = None
            row.audit_status = "UNAUDITED"
            row.audit_score = None
            row.source_match_status = None
            row.ingredient_signal_type = None
            row.ingest_allowed = False
            row.error = None
            row.updated_at = utcnow()

            if parser_ready() and _has_meaningful_ingredients(outcome.raw_ingredient_text):
                snapshot = build_parser_snapshot(outcome.raw_ingredient_text)
                row.cleaned_text = snapshot["cleaned_text"] or None
                row.parse_status = snapshot["parse_status"] or None
                row.parse_confidence = snapshot["parse_confidence"]
                row.inci_list = snapshot["inci_list"] or None
                row.inci_list_json = json.dumps(snapshot["inci_list_json"] or [], ensure_ascii=False)
                row.unrecognized_tokens_json = json.dumps(snapshot["unrecognized_tokens"] or [], ensure_ascii=False)
                row.normalization_notes_json = json.dumps(snapshot["normalization_notes"] or [], ensure_ascii=False)
                row.needs_review_json = json.dumps(snapshot["needs_review"] or [], ensure_ascii=False)

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
