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
    raw_ingredient_text: Optional[str]
    source_ref: Optional[str]
    source_type: Optional[str]
    debug: dict[str, Any]


class SourceHarvester:
    def __init__(self, search_engine: Optional[SearchEngine] = None) -> None:
        self.search_engine = search_engine or default_search_engine()

    def process(self, *, market: str, brand: str, product_name: str) -> HarvestOutcome:
        query = build_query(market, brand, product_name)
        urls = self.search_engine.search(query, top_k=3)
        debug: dict[str, Any] = {"query": query, "urls": urls, "attempts": []}

        best_pending: dict[str, Any] | None = None
        best_rank: float = -1.0

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

                # Extracted something but not fully trusted; keep searching other URLs and pick best at end.
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
                debug["attempts"].append({"url": url, "error": str(exc)[:200]})
                continue

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
