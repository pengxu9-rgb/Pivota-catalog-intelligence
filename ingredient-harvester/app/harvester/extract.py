from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup


KEYWORDS_EN = ["ingredients", "inci"]
KEYWORDS_ZH = ["全成分", "成分", "配料", "配方"]

COMMON_TOKENS = [
    "water",
    "aqua",
    "glycerin",
    "alcohol",
    "fragrance",
    "parfum",
    "sodium",
    "citric",
    "acid",
    "ethanol",
    "乙醇",
    "甘油",
    "水",
]

UI_ARTIFACT_PATTERNS = [
    r"\bread more\b",
    r"\bshow more\b",
    r"\bview full list\b",
    r"\bview more\b",
    r"\bsee more\b",
    r"\bclick here\b",
    r"\bsee text\b",
    r"\bsee image\b",
    r"\bback to top\b",
    r"\bback\b",
]

SCRIPT_OR_JSON_PATTERNS = [
    r"self\.__next_f\.push",
    r"__NEXT_DATA__",
    r"\bwebpack\b",
    r"\bfunction\s*\(",
    r"\bvar\s+\w+",
    r"\bconst\s+\w+",
    r"\blet\s+\w+",
    r"=>",
    r"\bwindow\.",
    r"\bdocument\.",
    r"\"assetsUrl\"",
    r"\"buildId\"",
    r"\"pageProps\"",
]


@dataclass(frozen=True)
class ExtractedIngredients:
    text: str
    score: float
    verified_in_dom: bool
    debug_hint: str


def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def clean_noise(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    # Zero-width / BOM characters.
    t = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", t)

    # Strip URLs.
    t = re.sub(r"https?://\S+", " ", t, flags=re.IGNORECASE)

    # Remove hashtag-style marketing tags.
    t = re.sub(r"(^|\s)#[\w-]+", " ", t)

    # Common UI artifacts and truncation markers.
    for pat in UI_ARTIFACT_PATTERNS:
        t = re.sub(pat, " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\[\s*more\s*\]", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"(\.{3,}|…)", " ", t)

    # Remove trailing truncation.
    t = re.sub(r"\b(and|&)\s*$", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\betc\.?\s*$", " ", t, flags=re.IGNORECASE)

    return _normalize_space(t)


def _strip_label_prefix(s: str) -> str:
    t = (s or "").strip()
    t = re.sub(r"^(ingredients?|inci)\s*[:：]\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^(全成分|成分|配料|配方)\s*[:：]\s*", "", t)
    return t.strip()


def _slice_after_label(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    m = re.search(r"(ingredients?|inci)\s*[:：]\s*", t, flags=re.IGNORECASE)
    if m and m.start() > 0:
        return t[m.end() :].strip()
    m = re.search(r"(全成分|成分|配料|配方)\s*[:：]\s*", t)
    if m and m.start() > 0:
        return t[m.end() :].strip()
    return t


def _strip_non_content_tags(soup: BeautifulSoup) -> None:
    for tag in soup.find_all(["script", "style", "noscript", "svg"]):
        tag.decompose()


def _dom_text(soup: BeautifulSoup) -> str:
    return _normalize_space(soup.get_text(" ", strip=True))


def verify_token_in_dom(html: str, extracted_text: str) -> bool:
    if not html or not extracted_text:
        return False
    soup = BeautifulSoup(html, "lxml")
    _strip_non_content_tags(soup)
    dom = _dom_text(soup).lower()
    token = _normalize_space(_strip_label_prefix(clean_noise(extracted_text)))[:20].lower()
    if not token:
        return False
    return token in dom


def _looks_like_script_or_json(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if len(t) >= 120 and t.lstrip().startswith(("{", "[")):
        return True
    if re.search("|".join(SCRIPT_OR_JSON_PATTERNS), t, flags=re.IGNORECASE):
        return True
    if t.count("{") >= 3 and t.count("}") >= 3 and t.count(":") >= 4 and t.count('"') >= 4:
        return True
    return False


def _feature_score(text: str) -> float:
    t = (text or "").strip()
    if not t:
        return 0.0
    if len(t) < 10:
        return 0.1
    if len(t) > 3000:
        return 0.2
    if _looks_like_script_or_json(t):
        return 0.05
    score = 0.0
    if "," in t or ";" in t or "，" in t or "；" in t:
        score += 0.35
    lower = t.lower()
    hits = sum(1 for tok in COMMON_TOKENS if tok in lower)
    score += min(0.35, hits * 0.08)
    # Token-ish density.
    parts = re.split(r"[,;，；]\s*", t)
    token_count = len([p for p in parts if p.strip()])
    if token_count >= 5:
        score += 0.3
    elif token_count >= 4:
        score += 0.25
    return min(1.0, score)


def _looks_like_heading(text: str, keywords: list[str]) -> bool:
    t = _normalize_space(text)
    if not t:
        return False
    if len(t) > 120:
        return False
    lower = t.lower()
    for k in keywords:
        kk = k.lower()
        if kk == "ingredients":
            if re.search(r"\bingredients?\b", lower):
                return True
        elif kk == "inci":
            if re.search(r"\binci\b", lower):
                return True
        else:
            if kk in lower:
                return True
    return False


def _collect_candidates(soup: BeautifulSoup, keywords: list[str]) -> list[str]:
    candidates: list[str] = []

    # 1) Elements with ingredient-ish id/class.
    for el in soup.select("[id*='ingredient'],[class*='ingredient'],[data-testid*='ingredient']"):
        txt = _normalize_space(el.get_text(" ", strip=True))
        if txt and len(txt) > 10:
            candidates.append(txt)

    # 2) Headings or strong labels followed by blocks.
    for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b", "span", "p", "div"]):
        label = _normalize_space(el.get_text(" ", strip=True))
        if not _looks_like_heading(label, keywords):
            continue

        # include label's own text (in case content is inline)
        if len(label) > 10:
            candidates.append(label)

        # next siblings (common pattern)
        nxt = el
        for _ in range(4):
            nxt = nxt.find_next_sibling()
            if not nxt:
                break
            txt = _normalize_space(nxt.get_text(" ", strip=True))
            if txt and len(txt) > 10:
                candidates.append(txt)

        # parent section text (fallback)
        parent = el.parent
        if parent:
            txt = _normalize_space(parent.get_text(" ", strip=True))
            if txt and len(txt) > 20:
                candidates.append(txt)

    return candidates


def extract_ingredients(html: str, *, market: str) -> Optional[ExtractedIngredients]:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    _strip_non_content_tags(soup)
    market_upper = (market or "").strip().upper()
    keywords = KEYWORDS_ZH if market_upper in {"CN", "CHN", "CHINA"} else KEYWORDS_EN

    candidates = _collect_candidates(soup, keywords)
    cleaned: list[str] = []
    for c in candidates:
        t = _slice_after_label(c)
        t = _strip_label_prefix(t)
        t = clean_noise(t)
        t = _normalize_space(t)
        if not t or len(t) < 10:
            continue
        if _looks_like_script_or_json(t):
            continue
        # Skip very low-signal blocks to avoid saving page dumps / UI snippets.
        if _feature_score(t) < 0.3:
            continue
        cleaned.append(t)

    # Deduplicate (case/space-insensitive) to reduce repeats.
    uniq: dict[str, str] = {}
    for t in cleaned:
        key = re.sub(r"\s+", " ", t).strip().lower()
        if key and key not in uniq:
            uniq[key] = t
    cleaned = list(uniq.values())

    # Prefer shorter, ingredient-list-like blocks (comma-rich) over full page dumps.
    cleaned.sort(key=lambda s: (-_feature_score(s), len(s)))

    if not cleaned:
        return None

    best = cleaned[0]
    score = _feature_score(best)
    verified = verify_token_in_dom(html, best)
    debug = f"candidates={len(cleaned)} score={score:.2f} verified={verified}"
    return ExtractedIngredients(text=best, score=score, verified_in_dom=verified, debug_hint=debug)
