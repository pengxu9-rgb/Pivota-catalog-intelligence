from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from redis import Redis
from rq import Queue

from app.config import settings


@dataclass(frozen=True)
class JobRef:
    id: str


def _redis() -> Optional[Redis]:
    if not settings.redis_url:
        return None
    return Redis.from_url(settings.redis_url)


def enqueue(function_path: str, *, kwargs: dict, job_id: Optional[str] = None) -> Optional[JobRef]:
    mode = (settings.queue_mode or "").lower()
    if mode == "inline":
        return None
    if mode != "rq":
        return None
    redis_conn = _redis()
    if not redis_conn:
        return None
    qname = os.getenv("HARVESTER_RQ_QUEUE", "ingredient-harvester")
    q = Queue(qname, connection=redis_conn)
    job = q.enqueue(function_path, kwargs=kwargs, job_id=job_id)
    return JobRef(id=str(job.id))
