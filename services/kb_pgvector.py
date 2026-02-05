from __future__ import annotations

import hashlib
import math
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


_TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:[-_'][A-Za-z0-9]+)*|[\u4e00-\u9fff]+")


def tokenize(text: str) -> list[str]:
    s = (text or "").strip().lower()
    if not s:
        return []
    return [m.group(0) for m in _TOKEN_RE.finditer(s)]


def hash_embedding(text: str, *, dim: int = 384) -> list[float]:
    """
    Deterministic, dependency-free embedding via hashing trick.

    - Zero hallucination: only uses given text tokens.
    - Stable across runs: uses blake2b (NOT python's salted hash()).
    """
    if dim <= 0:
        raise ValueError("dim must be > 0")

    vec = [0.0] * dim
    tokens = tokenize(text)
    if not tokens:
        return vec

    for tok in tokens:
        # 8 bytes is enough for stable index/sign.
        h = hashlib.blake2b(tok.encode("utf-8"), digest_size=8).digest()
        n = int.from_bytes(h, "big", signed=False)
        idx = int(n % dim)
        sign = 1.0 if (n & 1) == 1 else -1.0
        vec[idx] += sign

    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def vector_literal(vec: list[float], *, max_decimals: int = 6) -> str:
    if not vec:
        return "[]"
    fmt = f"{{:.{int(max_decimals)}f}}"
    return "[" + ",".join(fmt.format(float(v)) for v in vec) + "]"


def ensure_sslmode_require(db_url: str) -> str:
    """
    Railway external Postgres often requires TLS. If sslmode is not set, default to require.
    """
    url = (db_url or "").strip()
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return url
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "sslmode" not in {k.lower() for k in q.keys()}:
        q["sslmode"] = "require"
        parsed = parsed._replace(query=urlencode(q))
        return urlunparse(parsed)
    return url


def mask_db_url(db_url: str) -> str:
    url = (db_url or "").strip()
    if not url:
        return ""
    try:
        p = urlparse(url)
        if p.username and p.password:
            netloc = f"{p.username}:***@{p.hostname or ''}"
            if p.port:
                netloc += f":{p.port}"
            return urlunparse(p._replace(netloc=netloc))
    except Exception:
        return "<redacted>"
    return url

