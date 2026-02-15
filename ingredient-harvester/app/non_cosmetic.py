from __future__ import annotations

import re
from typing import Optional


_RULES: list[tuple[str, list[re.Pattern[str]]]] = [
    (
        "non_cosmetic_product_gift_card",
        [
            re.compile(r"\bgift[\s_-]*card\b", flags=re.IGNORECASE),
            re.compile(r"\be-?gift\b", flags=re.IGNORECASE),
            re.compile(r"\bgeschenkkarte\b", flags=re.IGNORECASE),
            re.compile(r"礼品卡"),
        ],
    ),
    (
        "non_cosmetic_product_voucher_or_coupon",
        [
            re.compile(r"\bvoucher\b", flags=re.IGNORECASE),
            re.compile(r"\bcoupon\b", flags=re.IGNORECASE),
            re.compile(r"\bgutschein\b", flags=re.IGNORECASE),
            re.compile(r"优惠券"),
        ],
    ),
    (
        "non_cosmetic_product_accessory",
        [
            re.compile(r"\baccessor(y|ies)\b", flags=re.IGNORECASE),
            re.compile(r"\bpouch\b", flags=re.IGNORECASE),
            re.compile(r"\bkeychain\b", flags=re.IGNORECASE),
        ],
    ),
]


def non_cosmetic_skip_reason(*, brand: str, product_name: str) -> Optional[str]:
    text = f"{brand or ''} {product_name or ''}".strip()
    if not text:
        return None
    for reason, patterns in _RULES:
        for pat in patterns:
            if pat.search(text):
                return reason
    return None
