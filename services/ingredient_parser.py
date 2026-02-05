"""
Ingredient Parser (PCI)

Goal
----
Parse non-structured, multi-lingual (EN/ZH) `raw_ingredient_text` into a
standardized INCI list, preserving original order with zero hallucination:
we only normalize/map tokens that exist in the original text.

Input  : CSV containing `raw_ingredient_text`
Output : same CSV with appended columns:
  - parse_status (OK / NEEDS_SOURCE / NEEDS_REVIEW)
  - inci_list (semicolon-separated)
  - inci_list_json (JSON string with order/standard_name/original_text)
  - unrecognized_tokens (JSON string list)
  - normalization_notes (JSON string list)
  - parse_confidence (0.0..1.0)
  - needs_review (JSON string list)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


# NOTE: Base mapping dictionary (50+ common cosmetic ingredients).
# Keys are common synonyms (EN/ZH) and values are standardized INCI names.
COMMON_INGREDIENTS_DB: dict[str, str] = {
    # Water
    "water": "Aqua",
    "aqua": "Aqua",
    "eau": "Aqua",
    "purified water": "Aqua",
    "deionized water": "Aqua",
    "de-ionized water": "Aqua",
    "water/aqua/eau": "Aqua",
    "water aqua eau": "Aqua",
    "aqua/water/eau": "Aqua",
    "aqua water eau": "Aqua",
    "水": "Aqua",
    "纯水": "Aqua",
    "去离子水": "Aqua",
    "纯净水": "Aqua",
    # Alcohols / solvents
    "alcohol": "Alcohol",
    "ethanol": "Alcohol",
    "变性乙醇": "Alcohol Denat.",
    "alcohol denat.": "Alcohol Denat.",
    "alcohol denat": "Alcohol Denat.",
    "denatured alcohol": "Alcohol Denat.",
    "sd alcohol 40-b": "Alcohol Denat.",
    "乙醇": "Alcohol",
    "异丙醇": "Isopropyl Alcohol",
    "isopropyl alcohol": "Isopropyl Alcohol",
    "丙二醇": "Propylene Glycol",
    "propylene glycol": "Propylene Glycol",
    "丁二醇": "Butylene Glycol",
    "butylene glycol": "Butylene Glycol",
    "戊二醇": "Pentylene Glycol",
    "pentylene glycol": "Pentylene Glycol",
    "propanediol": "Propanediol",
    "1,3-propanediol": "Propanediol",
    "1,2-hexanediol": "1,2-Hexanediol",
    "1,2 hexanediol": "1,2-Hexanediol",
    "hexanediol": "1,2-Hexanediol",
    "dimethyl isosorbide": "Dimethyl Isosorbide",
    "乙氧基二甘醇": "Ethoxydiglycol",
    "ethoxydiglycol": "Ethoxydiglycol",
    # Humectants
    "甘油": "Glycerin",
    "glycerin": "Glycerin",
    "glycerol": "Glycerin",
    "betaine": "Betaine",
    "甜菜碱": "Betaine",
    "sodium pca": "Sodium PCA",
    "zinc pca": "Zinc PCA",
    "尿素": "Urea",
    "urea": "Urea",
    "泛醇": "Panthenol",
    "panthenol": "Panthenol",
    "allantoin": "Allantoin",
    "尿囊素": "Allantoin",
    # Actives
    "烟酰胺": "Niacinamide",
    "niacinamide": "Niacinamide",
    "salicylic acid": "Salicylic Acid",
    "水杨酸": "Salicylic Acid",
    "glycolic acid": "Glycolic Acid",
    "乙醇酸": "Glycolic Acid",
    "lactic acid": "Lactic Acid",
    "乳酸": "Lactic Acid",
    "citric acid": "Citric Acid",
    "柠檬酸": "Citric Acid",
    "ascorbic acid": "Ascorbic Acid",
    "抗坏血酸": "Ascorbic Acid",
    "tocopherol": "Tocopherol",
    "retinol": "Retinol",
    "视黄醇": "Retinol",
    "retinyl palmitate": "Retinyl Palmitate",
    "视黄醇棕榈酸酯": "Retinyl Palmitate",
    "azelaic acid": "Azelaic Acid",
    "壬二酸": "Azelaic Acid",
    "tranexamic acid": "Tranexamic Acid",
    "传明酸": "Tranexamic Acid",
    "alpha-arbutin": "Alpha-Arbutin",
    "alpha arbutin": "Alpha-Arbutin",
    "熊果苷": "Arbutin",
    "arbutin": "Arbutin",
    # Hyaluronic family
    "hyaluronic acid": "Hyaluronic Acid",
    "透明质酸": "Hyaluronic Acid",
    "玻尿酸": "Hyaluronic Acid",
    "sodium hyaluronate": "Sodium Hyaluronate",
    "透明质酸钠": "Sodium Hyaluronate",
    # Surfactants
    "sodium laureth sulfate": "Sodium Laureth Sulfate",
    "sodium lauryl sulfate": "Sodium Lauryl Sulfate",
    "椰油酰胺丙基甜菜碱": "Cocamidopropyl Betaine",
    "cocamidopropyl betaine": "Cocamidopropyl Betaine",
    "decyl glucoside": "Decyl Glucoside",
    "coco-glucoside": "Coco-Glucoside",
    "coco glucoside": "Coco-Glucoside",
    "sodium cocoyl isethionate": "Sodium Cocoyl Isethionate",
    "sodium cocoyl glutamate": "Sodium Cocoyl Glutamate",
    "disodium cocoyl glutamate": "Disodium Cocoyl Glutamate",
    "sodium lauroyl sarcosinate": "Sodium Lauroyl Sarcosinate",
    # Emollients / silicones
    "caprylic/capric triglyceride": "Caprylic/Capric Triglyceride",
    "caprylic capric triglyceride": "Caprylic/Capric Triglyceride",
    "角鲨烷": "Squalane",
    "squalane": "Squalane",
    "dimethicone": "Dimethicone",
    "聚二甲基硅氧烷": "Dimethicone",
    "isopropyl myristate": "Isopropyl Myristate",
    "isopropyl palmitate": "Isopropyl Palmitate",
    "cetearyl alcohol": "Cetearyl Alcohol",
    "cetyl alcohol": "Cetyl Alcohol",
    "stearyl alcohol": "Stearyl Alcohol",
    "glyceryl stearate": "Glyceryl Stearate",
    "peg-100 stearate": "PEG-100 Stearate",
    "polysorbate 20": "Polysorbate 20",
    "polysorbate 60": "Polysorbate 60",
    "lecithin": "Lecithin",
    # Thickeners / polymers / salts
    "carbomer": "Carbomer",
    "卡波姆": "Carbomer",
    "xanthan gum": "Xanthan Gum",
    "黄原胶": "Xanthan Gum",
    "hydroxyethylcellulose": "Hydroxyethylcellulose",
    "羟乙基纤维素": "Hydroxyethylcellulose",
    "sodium chloride": "Sodium Chloride",
    "氯化钠": "Sodium Chloride",
    "acrylates/c10-30 alkyl acrylate crosspolymer": "Acrylates/C10-30 Alkyl Acrylate Crosspolymer",
    "acrylates c10-30 alkyl acrylate crosspolymer": "Acrylates/C10-30 Alkyl Acrylate Crosspolymer",
    # Chelators
    "disodium edta": "Disodium EDTA",
    "乙二胺四乙酸二钠": "Disodium EDTA",
    # Preservatives
    "phenoxyethanol": "Phenoxyethanol",
    "苯氧乙醇": "Phenoxyethanol",
    "ethylhexylglycerin": "Ethylhexylglycerin",
    "辛甘醇": "Ethylhexylglycerin",
    "chlorphenesin": "Chlorphenesin",
    "对羟基苯甲酸甲酯": "Methylparaben",
    "methylparaben": "Methylparaben",
    "propylparaben": "Propylparaben",
    "对羟基苯甲酸丙酯": "Propylparaben",
    "potassium sorbate": "Potassium Sorbate",
    "sodium benzoate": "Sodium Benzoate",
    "dehydroacetic acid": "Dehydroacetic Acid",
    "benzyl alcohol": "Benzyl Alcohol",
    # Antioxidants
    "bht": "BHT",
    "butylated hydroxytoluene": "BHT",
    # Fragrance / allergens (commonly listed in EU)
    "fragrance": "Parfum",
    "parfum": "Parfum",
    "香精": "Parfum",
    "limonene": "Limonene",
    "linalool": "Linalool",
    "citral": "Citral",
    "geraniol": "Geraniol",
    "coumarin": "Coumarin",
    "hydroxycitronellal": "Hydroxycitronellal",
    "citronellol": "Citronellol",
    "eugenol": "Eugenol",
    "benzyl salicylate": "Benzyl Salicylate",
}


NEW_COLUMNS = [
    "parse_status",
    "inci_list",
    "inci_list_json",
    "unrecognized_tokens",
    "normalization_notes",
    "parse_confidence",
    "needs_review",
]
_NEW_COLUMNS_LOWER = {c.lower() for c in NEW_COLUMNS}


INVALID_EXACT = {"n/a", "na", "none", "null", "nan"}
INVALID_PHRASES_EN = [
    "see image",
    "see packaging",
    "see package",
    "see back",
    "refer to packaging",
    "not available",
]
INVALID_PHRASES_ZH = [
    "详见包装",
    "见包装",
    "见外包装",
    "见图片",
    "请见包装",
]


LABEL_PREFIX_RE = re.compile(
    r"^\s*(?:ingredients?|ingredient\s+list|ingredients?\s+list|inci|inci\s+list)\s*[:：]\s*",
    re.IGNORECASE,
)
LABEL_PREFIX_ZH_RE = re.compile(r"^\s*(?:全成分|成分|配料|配方)\s*[:：]\s*")

# Noise cleanup patterns (UI/marketing artifacts often included by crawlers).
_URL_RE = re.compile(r"https?://[^\s)]+", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"(^|[\s,;，；])#[A-Za-z0-9][A-Za-z0-9_-]{0,40}")
_BRACKET_MORE_LESS_RE = re.compile(r"\[\s*(?:more|less)\s*\]", re.IGNORECASE)
_ELLIPSIS_RE = re.compile(r"\.{3,}|…+")
_TRUNCATION_END_RE = re.compile(r"(?:\b(?:and|&)\b\s*(?:\.{3,}|…)?\s*$|\betc\.?\s*$)", re.IGNORECASE)

# Keep this list short and high-signal; avoid removing common ingredient words.
_UI_ARTIFACT_PHRASES_RE = re.compile(
    r"("
    r"read\s+more(?:\s+on\s+how\s+to\s+read\s+an\s+ingredient\s+list)?"
    r"|read\s+less"
    r"|show\s+more|show\s+less"
    r"|see\s+more|see\s+less"
    r"|view\s+full\s+list"
    r"|click\s+here"
    r"|see\s+text"
    r"|show\s+all\s+ingredients(?:\s+by\s+function)?"
    r"|ingredients?\s+by\s+function"
    r"|how\s+to\s+read\s+an\s+ingredient\s+list"
    r")",
    re.IGNORECASE,
)


def clean_noise(text: str) -> tuple[str, list[str]]:
    """
    Remove common crawler noise without inventing new ingredients.

    - marketing tags: #vegan, #alcohol-free ...
    - UI artifacts: Read more / [more] / Show more ...
    - URLs: http(s)://...
    - truncations: trailing "and..." / "etc."
    """
    s = (text or "").strip()
    if not s:
        return "", []

    notes: list[str] = []

    # Remove zero-width spaces that frequently appear in scraped HTML.
    if any(ch in s for ch in ["\u200b", "\u200c", "\u200d", "\ufeff"]):
        s = s.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "").replace("\ufeff", "")
        notes.append("Removed zero-width spaces")

    # Strip URLs.
    s2 = _URL_RE.sub(" ", s)
    if s2 != s:
        notes.append("Stripped URL(s)")
        s = s2

    # Remove marketing tags like "#vegan".
    s2 = _HASHTAG_RE.sub(r"\1", s)
    if s2 != s:
        notes.append("Removed marketing hashtag(s)")
        s = s2

    # Remove bracket artifacts like "[more]".
    s2 = _BRACKET_MORE_LESS_RE.sub(" ", s)
    if s2 != s:
        notes.append("Removed UI bracket token(s)")
        s = s2

    # Remove common UI phrases.
    s2 = _UI_ARTIFACT_PHRASES_RE.sub(" ", s)
    if s2 != s:
        notes.append("Removed UI phrase(s)")
        s = s2

    # Fix common truncation endings.
    s2 = _TRUNCATION_END_RE.sub("", s)
    if s2 != s:
        notes.append("Removed truncation ending")
        s = s2

    # Normalize ellipsis.
    s2 = _ELLIPSIS_RE.sub(" ", s)
    if s2 != s:
        notes.append("Removed ellipsis artifact(s)")
        s = s2

    # Re-run truncation fix in case ellipsis normalization revealed a trailing conjunction.
    s2 = _TRUNCATION_END_RE.sub("", s)
    if s2 != s:
        notes.append("Removed truncation ending")
        s = s2

    # Final whitespace/punctuation normalization.
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip().strip("。．. ;；,，")
    return s, notes


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:  # noqa: BLE001
        pass
    s = str(value).strip()
    if not s:
        return ""
    if s.strip().lower() in {"nan", "none", "null", "n/a", "na"}:
        return ""
    return s


def _contains_cjk(s: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", s or ""))


def _strip_angle_brackets(s: str) -> tuple[str, list[str]]:
    removed: list[str] = []

    def _repl(m: re.Match[str]) -> str:
        removed.append(m.group(0))
        return ""

    out = re.sub(r"<[^>]{1,80}>", _repl, s or "")
    return out.strip(), removed


def _preprocess(raw: str) -> str:
    s = (raw or "").strip()
    s = LABEL_PREFIX_RE.sub("", s)
    s = LABEL_PREFIX_ZH_RE.sub("", s)
    # Remove trailing punctuation noise.
    s = s.strip().strip("。．. ;；,，")
    # Normalize whitespace/newlines.
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _looks_invalid_blob(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    low = t.lower()
    if low in INVALID_EXACT:
        return True
    if any(p in low for p in INVALID_PHRASES_EN):
        return True
    if any(p in t for p in INVALID_PHRASES_ZH):
        return True
    return False


def _split_outside_parens(text: str, separators: set[str]) -> list[str]:
    tokens: list[str] = []
    buf: list[str] = []
    depth = 0

    openers = {"(", "（", "[", "【", "{"}
    closers = {")", "）", "]", "】", "}"}

    for ch in text:
        if ch in openers:
            depth += 1
            buf.append(ch)
            continue
        if ch in closers and depth > 0:
            depth -= 1
            buf.append(ch)
            continue

        if depth == 0 and ch in separators:
            token = "".join(buf).strip()
            if token:
                tokens.append(token)
            buf = []
            continue

        buf.append(ch)

    last = "".join(buf).strip()
    if last:
        tokens.append(last)
    return tokens


def _split_ingredients(clean_text: str) -> tuple[list[str], bool]:
    """
    Returns (tokens, separator_detection_failed)
    """
    separators = {",", "，", "、", ";", "；", "\n", "\r", "\t", "|", "•", "·", "●", "・"}
    t = (clean_text or "").strip()
    if not t:
        return [], False

    tokens = _split_outside_parens(t, separators)

    # Secondary split: " / " (only when used as a delimiter, not for Caprylic/Capric).
    if len(tokens) == 1 and re.search(r"\s/\s", tokens[0]):
        tokens = _split_outside_parens(tokens[0], {"/"})

    normalized: list[str] = []
    for tok in tokens:
        s = (tok or "").strip()
        s = s.strip(" \t\n\r-–—•·●・")
        s = s.strip().strip("。．. ;；,，")
        s = re.sub(r"\s+", " ", s).strip()
        if s:
            normalized.append(s)

    # Separator detection failure heuristic: a single blob that looks like many tokens.
    # (e.g. "Aqua Glycerin Niacinamide ..." without commas)
    if len(normalized) == 1:
        blob = normalized[0]
        wordish = re.findall(r"[A-Za-z0-9]+", blob)
        sep_failed = len(blob) >= 80 or len(wordish) >= 6
    else:
        sep_failed = False
    return normalized, sep_failed


def _normalize_lookup_key(text: str) -> str:
    s = (text or "").strip().lower()
    s = s.replace("’", "'").replace("‘", "'")
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"<[^>]{1,80}>", "", s)
    # Remove parenthetical content for matching (e.g., "Fragrance (parfum)").
    s = re.sub(r"\([^)]*\)", "", s)
    # Normalize punctuation / spacing for matching.
    s = s.replace("/", " ")
    s = re.sub(r"[\u00ae\u2122]", "", s)  # ® ™
    s = re.sub(r"[.:：]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _canonicalize_unknown_english(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return ""
    # If token already contains uppercase/digits/symbols, keep as-is (avoid PEG -> Peg).
    if re.search(r"[A-Z0-9]", t) or re.search(r"[-/]", t):
        return t[:1].upper() + t[1:]
    return " ".join(w[:1].upper() + w[1:] for w in t.split())


def _append_unique(items: list[Any], value: Any) -> None:
    if value in items:
        return
    items.append(value)


@dataclass
class ParsedIngredient:
    order: int
    standard_name: str
    original_text: str
    uncertain: bool = False
    needs_review: bool = False


class ParserEngine:
    def __init__(self, mapping: Optional[dict[str, str]] = None) -> None:
        raw_map = mapping or COMMON_INGREDIENTS_DB
        norm_map: dict[str, str] = {}
        for k, v in raw_map.items():
            nk = _normalize_lookup_key(k)
            if not nk:
                continue
            # Keep the first mapping if duplicates exist.
            norm_map.setdefault(nk, v)
        self._map = norm_map

    def parse(self, raw_ingredient_text: Any) -> dict[str, Any]:
        raw = _coerce_text(raw_ingredient_text)
        clean = _preprocess(raw)
        clean, noise_notes = clean_noise(clean)

        if _looks_invalid_blob(clean):
            return {
                "parse_status": "NEEDS_SOURCE",
                "inci_list": "",
                "inci_list_json": "[]",
                "unrecognized_tokens": "[]",
                "normalization_notes": "[]",
                "parse_confidence": 0.0,
                "needs_review": "[]",
            }

        tokens, sep_failed = _split_ingredients(clean)
        if not tokens:
            return {
                "parse_status": "NEEDS_SOURCE",
                "inci_list": "",
                "inci_list_json": "[]",
                "unrecognized_tokens": "[]",
                "normalization_notes": "[]",
                "parse_confidence": 0.0,
                "needs_review": "[]",
            }

        confidence = 1.0
        if sep_failed:
            confidence -= 0.5

        parsed: list[ParsedIngredient] = []
        unrecognized: list[str] = []
        notes: list[str] = []
        if noise_notes:
            notes.extend(noise_notes)
        review_items: list[dict[str, Any]] = []

        order = 1
        for tok in tokens:
            token_before = tok
            tok, removed = _strip_angle_brackets(tok)
            if removed:
                notes.append(f"Removed tag(s) {removed} from '{token_before}'")
            tok = tok.strip()
            if not tok:
                continue

            key = _normalize_lookup_key(tok)
            mapped = self._map.get(key)

            if mapped:
                standard = mapped
                # Only note when mapping changes meaningfully.
                if _normalize_lookup_key(standard) != key:
                    notes.append(f"Mapped '{tok}' -> '{standard}'")
                parsed.append(ParsedIngredient(order=order, standard_name=standard, original_text=tok))
                order += 1
                continue

            # Fallbacks
            if _contains_cjk(tok):
                standard = tok
                _append_unique(unrecognized, tok)
                review_items.append({"original_text": tok, "issue": "No INCI mapping found"})
                confidence -= 0.1
                parsed.append(
                    ParsedIngredient(
                        order=order, standard_name=standard, original_text=tok, uncertain=True, needs_review=True
                    )
                )
                order += 1
                continue

            standard = _canonicalize_unknown_english(tok)
            if standard:
                _append_unique(unrecognized, tok)
                parsed.append(ParsedIngredient(order=order, standard_name=standard, original_text=tok, uncertain=True))
                order += 1

        confidence = max(0.0, min(1.0, float(confidence)))
        parse_status = "NEEDS_REVIEW" if confidence < 0.6 else "OK"

        inci_list = "; ".join(p.standard_name for p in parsed if p.standard_name)

        inci_json = json.dumps(
            [
                {
                    "order": p.order,
                    "standard_name": p.standard_name,
                    "original_text": p.original_text,
                    "uncertain": p.uncertain,
                    "needs_review": p.needs_review,
                }
                for p in parsed
            ],
            ensure_ascii=False,
        )

        return {
            "parse_status": parse_status,
            "inci_list": inci_list,
            "inci_list_json": inci_json,
            "unrecognized_tokens": json.dumps(unrecognized, ensure_ascii=False),
            "normalization_notes": json.dumps(notes, ensure_ascii=False),
            "parse_confidence": confidence,
            "needs_review": json.dumps(review_items, ensure_ascii=False),
        }


def _detect_raw_column(df: pd.DataFrame) -> str:
    candidates = ["raw_ingredient_text", "ingredients", "ingredient_text", "raw_ingredients"]
    lower_to_col = {str(c).strip().lower(): str(c) for c in df.columns}
    for name in candidates:
        if name in lower_to_col:
            return lower_to_col[name]
    return "raw_ingredient_text"


def _drop_existing_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    existing = [c for c in df.columns if str(c).strip().lower() in _NEW_COLUMNS_LOWER]
    if not existing:
        return df
    return df.drop(columns=existing)


def _run_self_test() -> bool:
    engine = ParserEngine()
    cases: list[tuple[str, dict[str, Any]]] = [
        (
            "Ingredients: Aqua (Water), Glycerin, Niacinamide, Phenoxyethanol.",
            {"parse_status": "OK", "inci_list": "Aqua; Glycerin; Niacinamide; Phenoxyethanol"},
        ),
        (
            "全成分：水，甘油，烟酰胺",
            {"parse_status": "OK", "inci_list": "Aqua; Glycerin; Niacinamide"},
        ),
        (
            "See image",
            {"parse_status": "NEEDS_SOURCE", "parse_confidence": 0.0},
        ),
        (
            "Aqua/Water/Eau, Glycerin, Parfum (Fragrance)",
            {"parse_status": "OK", "inci_list": "Aqua; Glycerin; Parfum"},
        ),
        (
            "Aqua Glycerin Niacinamide Zinc PCA Phenoxyethanol",
            {"parse_status": "NEEDS_REVIEW", "parse_confidence": 0.5},
        ),
        (
            "#vegan Water, Glycerin, Niacinamide, [more] Phenoxyethanol. Read more",
            {"parse_status": "OK", "inci_list": "Aqua; Glycerin; Niacinamide; Phenoxyethanol"},
        ),
        (
            "Water, Glycerin, and...",
            {"parse_status": "OK", "inci_list": "Aqua; Glycerin"},
        ),
        (
            "Water, Glycerin, etc.",
            {"parse_status": "OK", "inci_list": "Aqua; Glycerin"},
        ),
    ]

    ok = True
    for raw, expected in cases:
        got = engine.parse(raw)
        for k, v in expected.items():
            if got.get(k) != v:
                ok = False
                print(f"[self-test] FAIL key={k} expected={v!r} got={got.get(k)!r} raw={raw!r}", file=sys.stderr)
    if ok:
        print("[self-test] OK", file=sys.stderr)
    return ok


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Parse raw_ingredient_text into normalized INCI lists.")
    parser.add_argument(
        "--input",
        default="product_candidates_master_v0_i18n.csv",
        help="Input CSV path (default: product_candidates_master_v0_i18n.csv)",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output CSV path. Default: <input>.parsed.csv",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Do not write output file (still prints demo rows).",
    )
    parser.add_argument(
        "--demo-rows",
        type=int,
        default=10,
        help="Print first N parsed rows to stdout as CSV (default: 10). Set 0 to disable.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in parser self tests and exit.",
    )
    args = parser.parse_args(argv)

    if args.self_test:
        return 0 if _run_self_test() else 1

    try:
        df = pd.read_csv(args.input, encoding="utf-8-sig")
    except Exception:  # noqa: BLE001
        df = pd.read_csv(args.input)
    df = _drop_existing_output_columns(df)
    raw_col = _detect_raw_column(df)
    if raw_col not in df.columns:
        # Keep behavior predictable even if the column is missing.
        df[raw_col] = ""

    engine = ParserEngine()
    parsed_records = [engine.parse(v) for v in df[raw_col].tolist()]
    parsed_df = pd.DataFrame(parsed_records, columns=NEW_COLUMNS)

    out = pd.concat([df.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)

    if not args.no_write:
        output_path = args.output.strip()
        if not output_path:
            if args.input.lower().endswith(".csv"):
                output_path = f"{args.input[:-4]}.parsed.csv"
            else:
                output_path = f"{args.input}.parsed.csv"
        out.to_csv(output_path, index=False)

    if args.demo_rows and args.demo_rows > 0:
        demo = out.head(int(args.demo_rows))
        demo.to_csv(sys.stdout, index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
