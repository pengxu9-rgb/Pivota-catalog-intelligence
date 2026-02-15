from __future__ import annotations

import pandas as pd

from app.main import _first_matching_col, _infer_market_from_url, _normalize_http_url


def test_catalog_export_headers_are_detected() -> None:
    df = pd.DataFrame(columns=["Brand", "Product Title", "Product URL", "Deep Link"])
    assert _first_matching_col(df, ["brand"]) == "Brand"
    assert _first_matching_col(df, ["product_name", "product title"]) == "Product Title"
    assert _first_matching_col(df, ["product_url", "deep_link"]) == "Product URL"


def test_url_normalization_adds_https_scheme() -> None:
    assert _normalize_http_url("theordinary.com/p/some-product") == "https://theordinary.com/p/some-product"
    assert _normalize_http_url("https://theordinary.com/p/some-product") == "https://theordinary.com/p/some-product"


def test_market_inference_uses_locale_or_domain() -> None:
    assert _infer_market_from_url("https://theordinary.com/en-us/p/some-product") == "US"
    assert _infer_market_from_url("https://theordinary.cn/product/a") == "CN"
    assert _infer_market_from_url("https://theordinary.com/p/some-product") == "GLOBAL"
