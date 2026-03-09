import assert from "node:assert/strict";
import test from "node:test";

import {
  extractProductUrlsFromHtml,
  isLikelyProductUrl,
  isStaticAssetUrl,
} from "../src/services/extractors/puppeteer";

const BASE_URL = "https://theordinary.com";

test("isStaticAssetUrl marks static resources correctly", () => {
  assert.equal(isStaticAssetUrl("https://theordinary.com/product/detail.css", BASE_URL), true);
  assert.equal(
    isStaticAssetUrl("https://theordinary.com/products/The%20Ordinary/hero-image.png", BASE_URL),
    true,
  );
  assert.equal(
    isStaticAssetUrl("https://theordinary.com/de-de/niacinamide-10-zinc-1-serum-100436.html", BASE_URL),
    false,
  );
});

test("isLikelyProductUrl supports .html and /products/ PDP URLs", () => {
  assert.equal(
    isLikelyProductUrl("https://theordinary.com/de-de/niacinamide-10-zinc-1-serum-100436.html", BASE_URL),
    true,
  );
  assert.equal(isLikelyProductUrl("https://theordinary.com/products/squalane-face-cleanser", BASE_URL), true);
  assert.equal(isLikelyProductUrl("https://theordinary.com/the-geranium-rose-body-cream", BASE_URL), true);
  assert.equal(isLikelyProductUrl("https://us.caudalie.com/c/all-products.html", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://us.caudalie.com/online-booking/location", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://theordinary.com/", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://theordinary.com/en-us", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://cdn.example.com/de-de/foo-100436.html", BASE_URL), false);
});

test("extractProductUrlsFromHtml prioritizes anchor links and filters non-product/static links", () => {
  const html = `
    <a href="/de-de/niacinamide-10-zinc-1-serum-100436.html">Niacinamide</a>
    <a href="/products/squalane-face-cleanser">Squalane Cleanser</a>
    <a href="/product/detail.css">Stylesheet</a>
    <a href="javascript:void(0)">noop</a>
    <a href="mailto:test@example.com">email</a>
    <a href="https://external.example/de-de/external-product-100436.html">external</a>
  `;

  const urls = extractProductUrlsFromHtml(html, BASE_URL);

  assert.deepEqual(
    new Set(urls),
    new Set([
      "https://theordinary.com/de-de/niacinamide-10-zinc-1-serum-100436.html",
      "https://theordinary.com/products/squalane-face-cleanser",
    ]),
  );
});

test("extractProductUrlsFromHtml regex fallback still excludes static resources", () => {
  const html = `
    <script>
      const a = "/products/squalane-face-cleanser";
      const b = "/products/The Ordinary/hero.png";
      const c = "/product/mini-discovery-set";
    </script>
  `;

  const urls = extractProductUrlsFromHtml(html, BASE_URL);

  assert.deepEqual(
    new Set(urls),
    new Set([
      "https://theordinary.com/products/squalane-face-cleanser",
      "https://theordinary.com/product/mini-discovery-set",
    ]),
  );
});

test("extractProductUrlsFromHtml decodes HTML-entity encoded absolute hrefs", () => {
  const html = `
    <a href="https&#x3A;&#x2F;&#x2F;theordinary.com&#x2F;the-geranium-rose-body-cream">Encoded PDP</a>
  `;

  const urls = extractProductUrlsFromHtml(html, BASE_URL);

  assert.deepEqual(urls, ["https://theordinary.com/the-geranium-rose-body-cream"]);
});

test("extractProductUrlsFromHtml ignores external social links", () => {
  const html = `
    <a href="https://www.instagram.com/theordinary/">Instagram</a>
    <a href="https://www.facebook.com/theordinary/">Facebook</a>
    <a href="/the-geranium-rose-body-cream">Product</a>
  `;

  const urls = extractProductUrlsFromHtml(html, BASE_URL);

  assert.deepEqual(urls, ["https://theordinary.com/the-geranium-rose-body-cream"]);
});
