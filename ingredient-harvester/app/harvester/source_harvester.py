from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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


def classify_source_type(url: str) -> str:
    u = (url or "").lower()
    if any(host in u for host in ["sephora.", "ulta.", "amazon.", "lookfantastic.", "cultbeauty.", "douglas."]):
        return "Retailer"
    if any(host in u for host in ["wikipedia.org", "incidecoder.com", "cosdna.com", "skincarisma.com"]):
        return "ThirdParty"
    return "Official"


@dataclass(frozen=True)
class HarvestOutcome:
    status: str
    confidence: float
    raw_ingredient_text: str | None
    source_ref: str | None
    source_type: str | None
    debug: dict[str, Any]


class SourceHarvester:
    def __init__(self, search_engine: SearchEngine | None = None) -> None:
        self.search_engine = search_engine or default_search_engine()

    def process(self, *, market: str, brand: str, product_name: str) -> HarvestOutcome:
        query = build_query(market, brand, product_name)
        urls = self.search_engine.search(query, top_k=3)
        debug: dict[str, Any] = {"query": query, "urls": urls, "attempts": []}

        for url in urls[:3]:
            try:
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
                if verified and confidence >= 0.8:
                    return HarvestOutcome(
                        status="OK",
                        confidence=min(1.0, confidence),
                        raw_ingredient_text=extracted.text,
                        source_ref=fetched.url,
                        source_type=classify_source_type(fetched.url),
                        debug={**debug, "picked": fetched.url, "hint": extracted.debug_hint},
                    )

                # Extracted something but not fully trusted.
                return HarvestOutcome(
                    status="PENDING",
                    confidence=max(0.3, min(0.8, confidence)),
                    raw_ingredient_text=extracted.text,
                    source_ref=fetched.url,
                    source_type=classify_source_type(fetched.url),
                    debug={**debug, "picked": fetched.url, "hint": extracted.debug_hint, "verified": verified},
                )
            except Exception as exc:  # noqa: BLE001
                debug["attempts"].append({"url": url, "error": str(exc)[:200]})
                continue

        return HarvestOutcome(
            status="NEEDS_SOURCE",
            confidence=0.0,
            raw_ingredient_text=None,
            source_ref=None,
            source_type=None,
            debug=debug,
        )

