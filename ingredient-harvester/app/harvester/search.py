from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import httpx

from app.config import settings


class SearchEngine(Protocol):
    def search(self, query: str, *, top_k: int = 3) -> list[str]: ...


@dataclass(frozen=True)
class SerperDevSearchEngine:
    api_key: str

    def search(self, query: str, *, top_k: int = 3) -> list[str]:
        if not self.api_key:
            return []
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        payload = {"q": query, "num": max(1, min(10, int(top_k)))}
        with httpx.Client(timeout=settings.request_timeout_s) as client:
            r = client.post("https://google.serper.dev/search", headers=headers, json=payload)
            r.raise_for_status()
            data = r.json() if r.content else {}
        out: list[str] = []
        for item in (data.get("organic") or [])[:top_k]:
            link = (item.get("link") or "").strip()
            if link:
                out.append(link)
        return out


@dataclass(frozen=True)
class SerpApiSearchEngine:
    api_key: str

    def search(self, query: str, *, top_k: int = 3) -> list[str]:
        if not self.api_key:
            return []
        params = {
            "engine": "google",
            "q": query,
            "api_key": self.api_key,
            "num": max(1, min(10, int(top_k))),
        }
        with httpx.Client(timeout=settings.request_timeout_s) as client:
            r = client.get("https://serpapi.com/search.json", params=params)
            r.raise_for_status()
            data = r.json() if r.content else {}
        out: list[str] = []
        for item in (data.get("organic_results") or [])[:top_k]:
            link = (item.get("link") or "").strip()
            if link:
                out.append(link)
        return out


@dataclass(frozen=True)
class GoogleCseSearchEngine:
    api_key: str
    cse_id: str

    def search(self, query: str, *, top_k: int = 3) -> list[str]:
        if not self.api_key or not self.cse_id:
            return []
        params = {"key": self.api_key, "cx": self.cse_id, "q": query, "num": max(1, min(10, int(top_k)))}
        with httpx.Client(timeout=settings.request_timeout_s) as client:
            r = client.get("https://www.googleapis.com/customsearch/v1", params=params)
            r.raise_for_status()
            data = r.json() if r.content else {}
        out: list[str] = []
        for item in (data.get("items") or [])[:top_k]:
            link = (item.get("link") or "").strip()
            if link:
                out.append(link)
        return out


def default_search_engine() -> SearchEngine:
    if settings.serper_api_key:
        return SerperDevSearchEngine(settings.serper_api_key)
    if settings.serpapi_api_key:
        return SerpApiSearchEngine(settings.serpapi_api_key)
    if settings.google_cse_api_key and settings.google_cse_id:
        return GoogleCseSearchEngine(settings.google_cse_api_key, settings.google_cse_id)

    # No network search configured; return empty so API can still be used for manual review.
    class _NoopSearch:
        def search(self, query: str, *, top_k: int = 3) -> list[str]:
            return []

    return _NoopSearch()
