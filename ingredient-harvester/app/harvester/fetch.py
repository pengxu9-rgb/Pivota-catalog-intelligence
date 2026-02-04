from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from app.config import settings


USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
]


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    html: str
    content_type: Optional[str]


def _headers() -> dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=1, max=10))
def fetch_html(url: str) -> FetchResult:
    r = requests.get(url, headers=_headers(), timeout=settings.request_timeout_s, allow_redirects=True)
    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower() or None
    text = r.text if isinstance(r.text, str) else ""
    return FetchResult(url=str(r.url), status_code=int(r.status_code), html=text, content_type=ctype)
