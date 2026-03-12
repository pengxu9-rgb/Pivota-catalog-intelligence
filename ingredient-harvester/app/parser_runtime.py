from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


_PARSER_ENGINE: Any = None
_PARSER_ERROR: str | None = None
_PARSER_COERCE_TEXT = None
_PARSER_PREPROCESS = None
_PARSER_CLEAN_NOISE = None


def _load_parser_module():
    candidate_paths = [
        Path(__file__).resolve().parents[2] / "services" / "ingredient_parser.py",
        Path(__file__).resolve().parent / "ingredient_parser_vendor.py",
    ]
    for parser_path in candidate_paths:
        if not parser_path.exists():
            continue
        spec = importlib.util.spec_from_file_location(f"_ingredient_parser_{parser_path.stem}", parser_path)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    raise ModuleNotFoundError("ingredient_parser")


try:
    parser_module = _load_parser_module()
    ParserEngine = parser_module.ParserEngine
    clean_noise = parser_module.clean_noise
    parser_coerce_text = parser_module._coerce_text
    parser_preprocess = parser_module._preprocess

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
