from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from app.harvester.extract import extract_ingredients
from app.harvester.fetch import fetch_html
from app.harvester.search import SearchEngine, default_search_engine


def build_query(market: str, brand: str, product_name: str) -> str:
    m = (market or "").strip().upper()
    b = (brand or "").strip()
    p = (product_name or "").strip()
    if m in {"CN", "CHN", "CHINA"}:
        base = f"{b} {p}".strip()
        return f"{base} 全成分".strip()
    base = f"{b} {p}".strip()
    return f"{base} ingredients list INCI".strip()


OFFICIAL_HOST_ALLOWLIST = {
    "olehenriksen": ("olehenriksen.com",),
    "dermalogica": ("dermalogica.com",),
    "pixi": ("pixibeauty.com",),
    "pixi beauty": ("pixibeauty.com",),
}


def official_hosts_for_brand(brand: str) -> tuple[str, ...]:
    normalized_brand = (brand or "").strip().lower()
    if not normalized_brand:
        return ()
    matches: list[str] = []
    for key, hosts in OFFICIAL_HOST_ALLOWLIST.items():
        if key in normalized_brand:
            matches.extend(hosts)
    return tuple(dict.fromkeys(matches))


def classify_source_type(url: str) -> str:
    u = (url or "").lower()
    if any(
        host in u
        for host in [
            "sephora.",
            "ulta.",
            "amazon.",
            "lookfantastic.",
            "cultbeauty.",
            "douglas.",
            "strawberrynet.",
        ]
    ):
        return "Retailer"
    if any(host in u for host in ["wikipedia.org", "incidecoder.com", "cosdna.com", "skincarisma.com"]):
        return "ThirdParty"
    return "Official"


@dataclass(frozen=True)
class HarvestOutcome:
    status: str
    confidence: float
    raw_ingredient_text: Optional[str]
    source_ref: Optional[str]
    source_type: Optional[str]
    debug: dict[str, Any]


class SourceHarvester:
    def __init__(self, search_engine: Optional[SearchEngine] = None) -> None:
        self.search_engine = search_engine or default_search_engine()

    def process(self, *, market: str, brand: str, product_name: str, preferred_urls: Optional[list[str]] = None) -> HarvestOutcome:
        query = build_query(market, brand, product_name)
        official_hosts = official_hosts_for_brand(brand)
        preferred = [str(url or "").strip() for url in (preferred_urls or []) if str(url or "").strip()]
        debug: dict[str, Any] = {
            "query": query,
            "preferred_urls": preferred,
            "official_hosts": list(official_hosts),
            "urls": list(preferred),
            "attempts": [],
        }

        best_pending: dict[str, Any] | None = None
        best_rank: float = -1.0

        def attempt_urls(urls: list[str]) -> Optional[HarvestOutcome]:
            nonlocal best_pending
            nonlocal best_rank
            try:
                for url in urls[:3]:
                    fetched = fetch_html(url)
                    if fetched.status_code >= 400:
                        debug["attempts"].append({"url": url, "error": f"http_{fetched.status_code}"})
                        continue
                    extracted = extract_ingredients(fetched.html, market=market)
                    if not extracted:
                        debug["attempts"].append({"url": url, "error": "no_extract"})
                        continue

                    confidence = float(extracted.score)
                    verified = bool(extracted.verified_in_dom)
                    debug["attempts"].append(
                        {
                            "url": fetched.url,
                            "score": confidence,
                            "verified": verified,
                            "hint": extracted.debug_hint,
                        }
                    )
                    if verified and confidence >= 0.8:
                        return HarvestOutcome(
                            status="OK",
                            confidence=min(1.0, confidence),
                            raw_ingredient_text=extracted.text,
                            source_ref=fetched.url,
                            source_type=classify_source_type(fetched.url),
                            debug={**debug, "picked": fetched.url, "hint": extracted.debug_hint},
                        )

                    rank = confidence + (0.05 if verified else 0.0)
                    if rank > best_rank:
                        best_rank = rank
                        best_pending = {
                            "confidence": confidence,
                            "verified": verified,
                            "text": extracted.text,
                            "url": fetched.url,
                            "hint": extracted.debug_hint,
                        }
            except Exception as exc:  # noqa: BLE001
                debug["attempts"].append({"url": urls[0] if urls else "", "error": str(exc)[:200]})
            return None

        preferred_result = attempt_urls(preferred)
        if preferred_result is not None:
            return preferred_result

        official_search_urls: list[str] = []
        for host in official_hosts:
            try:
                official_search_urls.extend(self.search_engine.search(f"{query} site:{host}", top_k=3))
            except Exception as exc:  # noqa: BLE001
                debug["attempts"].append({"url": f"search:site:{host}", "error": str(exc)[:200]})

        search_urls: list[str] = []
        try:
            search_urls = self.search_engine.search(query, top_k=3)
        except Exception as exc:  # noqa: BLE001
            debug["attempts"].append({"url": "search:generic", "error": str(exc)[:200]})

        urls = list(dict.fromkeys(official_search_urls + search_urls))
        debug["urls"] = list(dict.fromkeys(preferred + urls))
        searched_result = attempt_urls(urls)
        if searched_result is not None:
            return searched_result

        if best_pending is not None:
            confidence = float(best_pending["confidence"])
            verified = bool(best_pending["verified"])
            url = str(best_pending["url"])
            hint = str(best_pending["hint"])
            return HarvestOutcome(
                status="PENDING",
                confidence=max(0.3, min(0.8, confidence)),
                raw_ingredient_text=str(best_pending["text"]),
                source_ref=url,
                source_type=classify_source_type(url),
                debug={**debug, "picked": url, "hint": hint, "verified": verified},
            )

        return HarvestOutcome(
            status="NEEDS_SOURCE",
            confidence=0.0,
            raw_ingredient_text=None,
            source_ref=None,
            source_type=None,
            debug=debug,
        )
