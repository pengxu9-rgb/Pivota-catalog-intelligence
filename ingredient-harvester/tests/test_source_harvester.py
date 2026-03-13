from __future__ import annotations

from dataclasses import dataclass

from app.harvester.source_harvester import SourceHarvester, classify_source_type


@dataclass(frozen=True)
class FakeSearch:
    urls: list[str]

    def search(self, query: str, *, top_k: int = 3) -> list[str]:
        return self.urls[:top_k]


@dataclass(frozen=True)
class QueryAwareSearch:
    by_query: dict[str, list[str]]

    def search(self, query: str, *, top_k: int = 3) -> list[str]:
        for key, urls in self.by_query.items():
            if key in query:
                return urls[:top_k]
        return []


def test_harvester_needs_source_without_urls(monkeypatch) -> None:
    h = SourceHarvester(search_engine=FakeSearch(urls=[]))
    out = h.process(market="US", brand="Test", product_name="P1")
    assert out.status == "NEEDS_SOURCE"
    assert out.confidence == 0.0


def test_harvester_pending_when_not_verified(monkeypatch) -> None:
    from app.harvester import fetch as fetch_mod
    from app.harvester import source_harvester as sh_mod

    def fake_fetch(url: str):
        return fetch_mod.FetchResult(url=url, status_code=200, html="<html><body>Ingredients: See image</body></html>", content_type="text/html")

    monkeypatch.setattr(sh_mod, "fetch_html", fake_fetch)

    h = SourceHarvester(search_engine=FakeSearch(urls=["https://example.com/p"]))
    out = h.process(market="US", brand="Test", product_name="P1")
    # Extraction likely fails because there's no list; either NEEDS_SOURCE or PENDING depending on heuristic.
    assert out.status in {"NEEDS_SOURCE", "PENDING"}


def test_harvester_tries_multiple_urls_until_ok(monkeypatch) -> None:
    from app.harvester import fetch as fetch_mod
    from app.harvester import source_harvester as sh_mod

    html_bad = "<html><body><h2>Ingredients</h2><div>Ingredients: See image</div></body></html>"
    html_good = "<html><body><h2>Ingredients</h2><div>Water, Glycerin, Sodium Chloride, Fragrance.</div></body></html>"

    def fake_fetch(url: str):
        if "bad" in url:
            return fetch_mod.FetchResult(url=url, status_code=200, html=html_bad, content_type="text/html")
        return fetch_mod.FetchResult(url=url, status_code=200, html=html_good, content_type="text/html")

    monkeypatch.setattr(sh_mod, "fetch_html", fake_fetch)

    h = SourceHarvester(search_engine=FakeSearch(urls=["https://example.com/bad", "https://example.com/good"]))
    out = h.process(market="US", brand="Test", product_name="P1")
    assert out.status == "OK"
    assert out.source_ref is not None and out.source_ref.endswith("/good")


def test_harvester_prefers_supplied_official_url_before_search_results(monkeypatch) -> None:
    from app.harvester import fetch as fetch_mod
    from app.harvester import source_harvester as sh_mod

    html_official = "<html><body><h2>Ingredients</h2><div>Water, Glycerin, Sodium Chloride, Fragrance.</div></body></html>"
    html_third_party = "<html><body><h2>Ingredients</h2><div>Ingredients copied from forum post</div></body></html>"

    calls = []

    def fake_fetch(url: str):
        calls.append(url)
        if "official" in url:
            return fetch_mod.FetchResult(url=url, status_code=200, html=html_official, content_type="text/html")
        return fetch_mod.FetchResult(url=url, status_code=200, html=html_third_party, content_type="text/html")

    monkeypatch.setattr(sh_mod, "fetch_html", fake_fetch)

    h = SourceHarvester(search_engine=FakeSearch(urls=["https://incidecoder.com/products/example-third-party"]))
    out = h.process(
        market="US",
        brand="Test",
        product_name="P1",
        preferred_urls=["https://brand.example.com/official-product-page"],
    )
    assert out.status == "OK"
    assert out.source_ref == "https://brand.example.com/official-product-page"
    assert calls[0] == "https://brand.example.com/official-product-page"
    assert classify_source_type(out.source_ref) == "Official"


def test_harvester_searches_official_domains_before_generic_results(monkeypatch) -> None:
    from app.harvester import fetch as fetch_mod
    from app.harvester import source_harvester as sh_mod

    html_official = "<html><body><h2>Ingredients</h2><div>Water, Glycerin, Sodium Chloride, Fragrance.</div></body></html>"
    html_third_party = "<html><body><h2>Ingredients</h2><div>Ingredients copied from forum post</div></body></html>"
    calls = []

    def fake_fetch(url: str):
        calls.append(url)
        if "dermalogica.com" in url:
            return fetch_mod.FetchResult(url=url, status_code=200, html=html_official, content_type="text/html")
        return fetch_mod.FetchResult(url=url, status_code=200, html=html_third_party, content_type="text/html")

    monkeypatch.setattr(sh_mod, "fetch_html", fake_fetch)

    h = SourceHarvester(
        search_engine=QueryAwareSearch(
            by_query={
                "site:dermalogica.com": ["https://dermalogica.com/products/smart-response-serum"],
                "ingredients list INCI": ["https://incidecoder.com/products/dermalogica-smart-response-serum"],
            }
        )
    )
    out = h.process(market="US", brand="Dermalogica", product_name="smart response serum")
    assert out.status == "OK"
    assert out.source_ref == "https://dermalogica.com/products/smart-response-serum"
    assert calls[0] == "https://dermalogica.com/products/smart-response-serum"


def test_harvester_returns_preferred_pending_when_search_fails(monkeypatch) -> None:
    from app.harvester import fetch as fetch_mod
    from app.harvester import source_harvester as sh_mod

    html_pending = "<html><body><h2>Ingredients</h2><div>On-the-Glow BASE gives skin a soft-focus finish and uses ceramide NP, fruit oils, glycerin, squalane, tocopherol.</div></body></html>"

    def fake_fetch(url: str):
        return fetch_mod.FetchResult(url=url, status_code=200, html=html_pending, content_type="text/html")

    class FailingSearch:
        def search(self, query: str, *, top_k: int = 3) -> list[str]:
            raise RuntimeError("serper 400")

    monkeypatch.setattr(sh_mod, "fetch_html", fake_fetch)

    h = SourceHarvester(search_engine=FailingSearch())
    out = h.process(
        market="US",
        brand="Pixi Beauty",
        product_name="On-the-Glow BASE - Porcelain",
        preferred_urls=["https://pixibeauty.com/products/on-the-glow-base"],
    )
    assert out.status == "PENDING"
    assert out.source_ref == "https://pixibeauty.com/products/on-the-glow-base"


def test_classify_source_type_marks_retailers_and_third_party_domains() -> None:
    assert classify_source_type("https://www.strawberrynet.com/en/ole-henriksen/product") == "Retailer"
    assert classify_source_type("https://incidecoder.com/products/dermalogica-smart-response-serum") == "ThirdParty"
    assert classify_source_type("https://olehenriksen.com/products/dewtopia-20-acid-night-treatment") == "Official"
