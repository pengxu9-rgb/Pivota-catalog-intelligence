from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


RowStatus = Literal["EMPTY", "OK", "PENDING", "NEEDS_SOURCE", "SKIPPED", "ERROR"]
TaskStatus = Literal["RUNNING", "COMPLETED", "FAILED", "CANCELED"]


class ImportResponse(BaseModel):
    import_id: str
    filename: str
    created_at: datetime
    total_rows: int


class CandidateRowView(BaseModel):
    row_id: str
    row_index: int
    brand: str
    product_name: str
    market: str
    status: RowStatus
    confidence: float | None = None
    source_type: str | None = None
    source_ref: str | None = None
    raw_ingredient_text: str | None = None
    updated_at: datetime
    error: str | None = None


class ListRowsResponse(BaseModel):
    import_id: str
    total: int
    items: list[CandidateRowView]


class CreateTaskRequest(BaseModel):
    import_id: str
    row_ids: list[str] | None = None
    force: bool = False


class CreateTaskResponse(BaseModel):
    task_id: str
    import_id: str
    status: TaskStatus
    queued: int


class TaskProgress(BaseModel):
    task_id: str
    import_id: str
    status: TaskStatus
    force: bool
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    counts: dict[str, int] = Field(default_factory=dict)


class UpdateRowRequest(BaseModel):
    status: RowStatus | None = None
    raw_ingredient_text: str | None = None
    source_ref: str | None = None
    source_type: str | None = None
    confidence: float | None = None
    error: str | None = None


class UpdateRowResponse(BaseModel):
    row: CandidateRowView

