from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


_PARSER_ENGINE: Any = None
_PARSER_ERROR: str | None = None
_PARSER_COERCE_TEXT = None
_PARSER_PREPROCESS = None
_PARSER_CLEAN_NOISE = None

try:
    services_dir = Path(__file__).resolve().parents[2] / "services"
    if services_dir.exists():
        sys.path.insert(0, str(services_dir))
    from ingredient_parser import ParserEngine, clean_noise  # type: ignore[import-not-found]
    from ingredient_parser import _coerce_text as parser_coerce_text  # type: ignore[import-not-found]
    from ingredient_parser import _preprocess as parser_preprocess  # type: ignore[import-not-found]

    _PARSER_ENGINE = ParserEngine()
    _PARSER_COERCE_TEXT = parser_coerce_text
    _PARSER_PREPROCESS = parser_preprocess
    _PARSER_CLEAN_NOISE = clean_noise
except Exception as exc:  # noqa: BLE001
    _PARSER_ENGINE = None
    _PARSER_ERROR = f"Parser unavailable: {type(exc).__name__}: {exc!s}"[:300]


def parser_ready() -> bool:
    return _PARSER_ENGINE is not None


def parser_error() -> str | None:
    return _PARSER_ERROR


def require_parser() -> Any:
    if _PARSER_ENGINE is not None:
        return _PARSER_ENGINE
    raise RuntimeError(_PARSER_ERROR or "Parser not available.")


def build_parser_snapshot(raw_ingredient_text: Any) -> dict[str, Any]:
    engine = require_parser()
    raw = _PARSER_COERCE_TEXT(raw_ingredient_text) if _PARSER_COERCE_TEXT else str(raw_ingredient_text or "")
    pre = _PARSER_PREPROCESS(raw) if _PARSER_PREPROCESS else raw
    cleaned, _ = _PARSER_CLEAN_NOISE(pre) if _PARSER_CLEAN_NOISE else (raw, [])
    parsed = engine.parse(raw)
    return {
        "cleaned_text": cleaned or "",
        "parse_status": str(parsed.get("parse_status") or "NEEDS_REVIEW"),
        "inci_list": str(parsed.get("inci_list") or ""),
        "inci_list_json": _loads_json(parsed.get("inci_list_json"), []),
        "unrecognized_tokens": _loads_json(parsed.get("unrecognized_tokens"), []),
        "normalization_notes": _loads_json(parsed.get("normalization_notes"), []),
        "parse_confidence": float(parsed.get("parse_confidence") or 0.0),
        "needs_review": _loads_json(parsed.get("needs_review"), []),
    }


def _loads_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return default
