from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [s.strip() for s in (raw or "").split(",") if s.strip()]


@dataclass(frozen=True)
class Settings:
    db_url: str = os.getenv("HARVESTER_DB_URL", "sqlite:///./harvester.sqlite3")
    cors_origins: list[str] = None  # type: ignore[assignment]
    redis_url: Optional[str] = os.getenv("REDIS_URL") or None
    queue_mode: str = os.getenv("HARVESTER_QUEUE_MODE", "").strip().lower() or ""

    serper_api_key: Optional[str] = os.getenv("SERPER_API_KEY") or None
    google_cse_api_key: Optional[str] = os.getenv("GOOGLE_CSE_API_KEY") or None
    google_cse_id: Optional[str] = os.getenv("GOOGLE_CSE_ID") or None

    request_timeout_s: float = float(os.getenv("HARVESTER_REQUEST_TIMEOUT_S", "20"))

    def __post_init__(self) -> None:
        object.__setattr__(self, "cors_origins", _csv_env("HARVESTER_API_CORS_ORIGINS", "*"))
        mode = self.queue_mode
        if not mode:
            mode = "rq" if self.redis_url else "inline"
            object.__setattr__(self, "queue_mode", mode)


settings = Settings()
