from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


RowStatus = Literal["EMPTY", "OK", "PENDING", "NEEDS_SOURCE", "SKIPPED", "ERROR"]
TaskStatus = Literal["RUNNING", "COMPLETED", "FAILED", "CANCELED"]
ParseStatus = Literal["OK", "NEEDS_SOURCE", "NEEDS_REVIEW"]


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
    confidence: Optional[float] = None
    source_type: Optional[str] = None
    source_ref: Optional[str] = None
    raw_ingredient_text: Optional[str] = None
    updated_at: datetime
    error: Optional[str] = None


class ListRowsResponse(BaseModel):
    import_id: str
    total: int
    items: list[CandidateRowView]


class CreateTaskRequest(BaseModel):
    import_id: str
    row_ids: Optional[list[str]] = None
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
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    counts: dict[str, int] = Field(default_factory=dict)


class UpdateRowRequest(BaseModel):
    status: Optional[RowStatus] = None
    raw_ingredient_text: Optional[str] = None
    source_ref: Optional[str] = None
    source_type: Optional[str] = None
    confidence: Optional[float] = None
    error: Optional[str] = None


class UpdateRowResponse(BaseModel):
    row: CandidateRowView


class ParserReparseRequest(BaseModel):
    raw_ingredient_text: Optional[str] = None


class ParserReparseResponse(BaseModel):
    cleaned_text: str
    parse_status: ParseStatus
    inci_list: str
    inci_list_json: list[dict] = Field(default_factory=list)
    unrecognized_tokens: list[str] = Field(default_factory=list)
    normalization_notes: list[str] = Field(default_factory=list)
    parse_confidence: float
    needs_review: list[dict] = Field(default_factory=list)


class ParserReparseBatchItem(BaseModel):
    row_id: str
    raw_ingredient_text: Optional[str] = None


class ParserReparseBatchRequest(BaseModel):
    items: list[ParserReparseBatchItem] = Field(default_factory=list)


class ParserReparseBatchResponseItem(BaseModel):
    row_id: str
    result: ParserReparseResponse


class ParserReparseBatchResponse(BaseModel):
    items: list[ParserReparseBatchResponseItem] = Field(default_factory=list)
