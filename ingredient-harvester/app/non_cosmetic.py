from __future__ import annotations

import re
from typing import Optional


_GIFT_CARD_PATTERNS = [
    re.compile(r"\bgift[\s_-]*card\b", flags=re.IGNORECASE),
    re.compile(r"\be-?gift\b", flags=re.IGNORECASE),
    re.compile(r"\bgeschenkkarte\b", flags=re.IGNORECASE),
    re.compile(r"礼品卡"),
]


def non_cosmetic_skip_reason(*, brand: str, product_name: str) -> Optional[str]:
    text = f"{brand or ''} {product_name or ''}".strip()
    if not text:
        return None
    for pat in _GIFT_CARD_PATTERNS:
        if pat.search(text):
            return "non_cosmetic_product_gift_card"
    return None

