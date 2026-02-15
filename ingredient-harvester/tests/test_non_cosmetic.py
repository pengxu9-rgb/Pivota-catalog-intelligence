from __future__ import annotations

from app.non_cosmetic import non_cosmetic_skip_reason


def test_detects_gift_card_english() -> None:
    reason = non_cosmetic_skip_reason(brand="the ordinary", product_name="Digital Gift Card")
    assert reason == "non_cosmetic_product_gift_card"


def test_detects_gift_card_german() -> None:
    reason = non_cosmetic_skip_reason(brand="the ordinary", product_name="Digitale Geschenkkarte")
    assert reason == "non_cosmetic_product_gift_card"


def test_returns_none_for_regular_cosmetic_product() -> None:
    reason = non_cosmetic_skip_reason(brand="the ordinary", product_name="Niacinamide 10% + Zinc 1%")
    assert reason is None

