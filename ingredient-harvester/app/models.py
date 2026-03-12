from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.db import utcnow


class Base(DeclarativeBase):
    pass


class ImportBatch(Base):
    __tablename__ = "imports"

    import_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    filename: Mapped[str] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    rows: Mapped[list["CandidateRow"]] = relationship(back_populates="batch", cascade="all, delete-orphan")
    tasks: Mapped[list["HarvestTask"]] = relationship(back_populates="batch", cascade="all, delete-orphan")


class CandidateRow(Base):
    __tablename__ = "candidate_rows"

    row_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    import_id: Mapped[str] = mapped_column(String(36), ForeignKey("imports.import_id"), index=True)
    row_index: Mapped[int] = mapped_column(Integer)

    brand: Mapped[str] = mapped_column(String(256), default="")
    product_name: Mapped[str] = mapped_column(String(512), default="")
    market: Mapped[str] = mapped_column(String(16), default="")
    candidate_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    sku_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    external_seed_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    external_product_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)

    raw_ingredient_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cleaned_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(24), default="EMPTY", index=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    parse_status: Mapped[Optional[str]] = mapped_column(String(24), nullable=True, index=True)
    parse_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    inci_list: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inci_list_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    unrecognized_tokens_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalization_notes_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    needs_review_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    review_status: Mapped[str] = mapped_column(String(24), default="UNREVIEWED", index=True)
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    audit_status: Mapped[str] = mapped_column(String(24), default="UNAUDITED", index=True)
    audit_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source_match_status: Mapped[Optional[str]] = mapped_column(String(24), nullable=True)
    ingredient_signal_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    ingest_allowed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(default=utcnow)

    batch: Mapped["ImportBatch"] = relationship(back_populates="rows")
    task_rows: Mapped[list["TaskRow"]] = relationship(back_populates="row", cascade="all, delete-orphan")
    audit_findings: Mapped[list["CandidateRowAuditFinding"]] = relationship(
        back_populates="row",
        cascade="all, delete-orphan",
    )
    corrections: Mapped[list["CandidateRowCorrection"]] = relationship(
        back_populates="row",
        cascade="all, delete-orphan",
    )


class HarvestTask(Base):
    __tablename__ = "harvest_tasks"

    task_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    import_id: Mapped[str] = mapped_column(String(36), ForeignKey("imports.import_id"), index=True)

    status: Mapped[str] = mapped_column(String(24), default="RUNNING", index=True)
    force: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)

    batch: Mapped["ImportBatch"] = relationship(back_populates="tasks")
    rows: Mapped[list["TaskRow"]] = relationship(back_populates="task", cascade="all, delete-orphan")


class TaskRow(Base):
    __tablename__ = "task_rows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("harvest_tasks.task_id"), index=True)
    row_id: Mapped[str] = mapped_column(String(36), ForeignKey("candidate_rows.row_id"), index=True)

    status: Mapped[str] = mapped_column(String(24), default="QUEUED", index=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)

    task: Mapped["HarvestTask"] = relationship(back_populates="rows")
    row: Mapped["CandidateRow"] = relationship(back_populates="task_rows")


class CandidateRowAuditFinding(Base):
    __tablename__ = "candidate_row_audit_findings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    audit_run_id: Mapped[str] = mapped_column(String(48), index=True)
    row_id: Mapped[str] = mapped_column(String(36), ForeignKey("candidate_rows.row_id"), index=True)
    import_id: Mapped[str] = mapped_column(String(36), index=True)
    anomaly_type: Mapped[str] = mapped_column(String(64), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recommended_action: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auto_fixable: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    row: Mapped["CandidateRow"] = relationship(back_populates="audit_findings")


class CandidateRowCorrection(Base):
    __tablename__ = "candidate_row_corrections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    audit_run_id: Mapped[Optional[str]] = mapped_column(String(48), nullable=True, index=True)
    row_id: Mapped[str] = mapped_column(String(36), ForeignKey("candidate_rows.row_id"), index=True)
    correction_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    auto_applied: Mapped[bool] = mapped_column(Boolean, default=False)
    actor: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    before_payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    row: Mapped["CandidateRow"] = relationship(back_populates="corrections")
