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


HTML_DERMALOGICA_ACCORDION = """
<html>
  <body>
    <section class="product-description rte">
      <details>
        <summary>
          <div class="summary-wrapper">
            <span class="headline">ingredients</span>
          </div>
        </summary>
        japanese cornelia cherry helps soothe skin
        Water/Aqua/Eau, Glycerin, Niacinamide, Butylene Glycol, 1,2-Hexanediol, Sodium Acrylates Crosspolymer-2, Sea Salt.
        Dermalogica is dedicated to maintaining the accuracy of the ingredient lists on this website.
      </details>
    </section>
  </body>
</html>
"""


def test_extract_ingredients_from_official_accordion_copy() -> None:
    extracted = extract_ingredients(HTML_DERMALOGICA_ACCORDION, market="US")
    assert extracted is not None
    assert extracted.text.startswith("Water/Aqua/Eau, Glycerin, Niacinamide")
    assert "Dermalogica is dedicated" not in extracted.text
    assert extracted.verified_in_dom is True


HTML_PIXI_POPUP = """
<html>
  <body>
    <div class="product-copy">
      <h2>On-the-Glow BASE</h2>
      <p>Buildable tinted balm that blurs, brightens and evens out skin tone.</p>
    </div>
    <div class="ingredients-popup hidden">
      <div class="ingredients-popup__inner">
        <span>ingredients</span>
        <p>Porcelain<br/>Caprylic/Capric Triglyceride, Diisostearyl Malate, Synthetic Wax, Polyglyceryl-2 Triisostearate, Triethylhexanoin, Boron Nitride, Calcium Sodium Borosilicate, Kaolin, Tocopheryl Acetate.</p>
        <p>Fair<br/>Caprylic/Capric Triglyceride, Diisostearyl Malate, Synthetic Wax, Polyglyceryl-2 Triisostearate, Triethylhexanoin, Boron Nitride, Calcium Sodium Borosilicate, Kaolin, Tocopheryl Acetate, Iron Oxides (CI 77492).</p>
      </div>
    </div>
  </body>
</html>
"""


HTML_PIXI_BRONZE_POPUP = """
<html>
  <body>
    <div class="product-copy">
      <h2>On-the-Glow Bronze</h2>
      <p>Swipe on for a sunkissed and healthy-looking complexion all year.</p>
    </div>
    <div class="ingredients-popup hidden">
      <div class="ingredients-popup__inner">
        <span>ingredients</span>
        <p><strong>BeachGlow</strong><br/>Caprylic/Capric Triglyceride, Diisostearyl Malate, Polyglyceryl-2 Triisostearate, Pentaerythrityl Tetraisostearate, Synthetic Wax, Aloe Barbadensis Leaf Extract, Aqua/Water/Eau, Iron Oxides (CI 77491, CI 77499).</p>
        <p><strong>SoftGlow</strong><br/>Caprylic/Capric Triglyceride, Diisostearyl Malate, Polyglyceryl-2 Triisostearate, Pentaerythrityl Tetraisostearate, Synthetic Wax, Aloe Barbadensis Leaf Extract, Aqua/Water/Eau, Tin Oxide, Titanium Dioxide (CI 77891), Iron Oxides (CI 77491).</p>
      </div>
    </div>
  </body>
</html>
"""


def test_extract_prefers_variant_matched_pixi_popup_over_marketing_copy() -> None:
    extracted = extract_ingredients(HTML_PIXI_POPUP, market="US", product_name="On-the-Glow BASE - Porcelain")
    assert extracted is not None
    assert extracted.text.startswith("Caprylic/Capric Triglyceride")
    assert "Buildable tinted balm" not in extracted.text
    assert extracted.score >= 0.7


def test_extract_prefers_matching_shade_when_popup_contains_multiple_variants() -> None:
    extracted = extract_ingredients(HTML_PIXI_BRONZE_POPUP, market="US", product_name="On-the-Glow Bronze - SoftGlow")
    assert extracted is not None
    assert extracted.text.startswith("Caprylic/Capric Triglyceride")
    assert "sunkissed" not in extracted.text
    assert "popup_variant=SoftGlow" in extracted.debug_hint
    assert "Tin Oxide" in extracted.text
