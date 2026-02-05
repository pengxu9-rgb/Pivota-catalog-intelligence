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


HTML_NOISY = """
<html>
  <body>
    <h2>Ingredients</h2>
    <div>Ingredients: Water, Glycerin, Sodium Chloride, Fragrance. Read more #vegan https://example.com/more</div>
  </body>
</html>
"""


def test_extract_cleans_noise() -> None:
    extracted = extract_ingredients(HTML_NOISY, market="US")
    assert extracted is not None
    assert "Read more" not in extracted.text
    assert "#vegan" not in extracted.text
    assert "https://" not in extracted.text


HTML_SCRIPT_ONLY = """
<html>
  <body>
    <h2>Ingredients</h2>
    <div>
      <script>
        self.__next_f.push([1,"Ingredients: Water, Glycerin, Parfum"]);
      </script>
    </div>
  </body>
</html>
"""


def test_extract_ignores_script_payload() -> None:
    extracted = extract_ingredients(HTML_SCRIPT_ONLY, market="US")
    assert extracted is None


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
