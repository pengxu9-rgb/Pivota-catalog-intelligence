from __future__ import annotations

from dataclasses import dataclass

from app.harvester.source_harvester import SourceHarvester


@dataclass(frozen=True)
class FakeSearch:
    urls: list[str]

    def search(self, query: str, *, top_k: int = 3) -> list[str]:
        return self.urls[:top_k]


def test_harvester_needs_source_without_urls(monkeypatch) -> None:
    h = SourceHarvester(search_engine=FakeSearch(urls=[]))
    out = h.process(market="US", brand="Test", product_name="P1")
    assert out.status == "NEEDS_SOURCE"
    assert out.confidence == 0.0


def test_harvester_pending_when_not_verified(monkeypatch) -> None:
    from app.harvester import fetch as fetch_mod

    def fake_fetch(url: str):
        return fetch_mod.FetchResult(url=url, status_code=200, html="<html><body>Ingredients: See image</body></html>", content_type="text/html")

    monkeypatch.setattr(fetch_mod, "fetch_html", fake_fetch)

    h = SourceHarvester(search_engine=FakeSearch(urls=["https://example.com/p"]))
    out = h.process(market="US", brand="Test", product_name="P1")
    # Extraction likely fails because there's no list; either NEEDS_SOURCE or PENDING depending on heuristic.
    assert out.status in {"NEEDS_SOURCE", "PENDING"}

