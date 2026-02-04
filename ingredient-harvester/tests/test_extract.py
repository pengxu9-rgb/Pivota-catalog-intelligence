from __future__ import annotations

from app.harvester.extract import extract_ingredients, verify_token_in_dom


HTML_EN = """
<html>
  <body>
    <h2>Ingredients</h2>
    <div>Water, Glycerin, Sodium Chloride, Fragrance.</div>
  </body>
</html>
"""


def test_extract_ingredients_en_ok() -> None:
    extracted = extract_ingredients(HTML_EN, market="US")
    assert extracted is not None
    assert "Water" in extracted.text
    assert extracted.score >= 0.8
    assert extracted.verified_in_dom is True


def test_verify_token_in_dom_false_when_missing() -> None:
    assert verify_token_in_dom("<html><body>nope</body></html>", "Water, Glycerin") is False


HTML_ZH = """
<html>
  <body>
    <div class="product-details">
      <div class="tab-title">全成分</div>
      <p>水，甘油，乙醇，香精。</p>
    </div>
  </body>
</html>
"""


def test_extract_ingredients_cn_ok() -> None:
    extracted = extract_ingredients(HTML_ZH, market="CN")
    assert extracted is not None
    assert "水" in extracted.text
    assert extracted.score >= 0.8
    assert extracted.verified_in_dom is True

