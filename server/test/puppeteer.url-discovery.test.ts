import assert from "node:assert/strict";
import test from "node:test";

import {
  extractProductUrlsFromHtml,
  filterCleanProductImageAssetUrls,
  isLikelyProductUrl,
  isStaticAssetUrl,
  resolveStructuredImageUrl,
  resolveStructuredImageUrls,
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
  assert.equal(isLikelyProductUrl("https://theordinary.com/products/", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://theordinary.com/the-geranium-rose-body-cream", BASE_URL), true);
  assert.equal(isLikelyProductUrl("https://us.caudalie.com/c/all-products.html", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://us.caudalie.com/online-booking/location", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://roundlab.co.kr/member/privacy.html", "https://roundlab.co.kr"), false);
  assert.equal(isLikelyProductUrl("https://roundlab.co.kr/member/agreement.html", "https://roundlab.co.kr"), false);
  assert.equal(isLikelyProductUrl("https://roundlab.co.kr/shopinfo/company.html", "https://roundlab.co.kr"), false);
  assert.equal(isLikelyProductUrl("https://roundlab.co.kr/product/1025-독도-토너-200ml/22/", "https://roundlab.co.kr"), true);
  assert.equal(
    isLikelyProductUrl("https://www.kao-kirei.com/ja/official/sofina-ip/products/", "https://www.kao-kirei.com"),
    false,
  );
  assert.equal(
    isLikelyProductUrl(
      "https://www.kao-kirei.com/ja/official/sofina-ip/products/treatmentmist/",
      "https://www.kao-kirei.com",
    ),
    true,
  );
  assert.equal(
    isLikelyProductUrl(
      "https://www.kao-kirei.com/ja/item/khg/bioresarasarauv/4901301413246/",
      "https://www.kao-kirei.com",
    ),
    true,
  );
  assert.equal(isLikelyProductUrl("https://www.kao-kirei.com/jp/ja/shopping-guide.html", "https://www.kao-kirei.com"), false);
  assert.equal(isLikelyProductUrl("https://www.kao-kirei.com/jp/ja/transaction-law.html", "https://www.kao-kirei.com"), false);
  assert.equal(
    isLikelyProductUrl("https://www.kao-kirei.com/jp/ja/terms-of-subscription.html", "https://www.kao-kirei.com"),
    false,
  );
  assert.equal(isLikelyProductUrl("https://theordinary.com/", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://theordinary.com/en-us", BASE_URL), false);
  assert.equal(isLikelyProductUrl("https://cdn.example.com/de-de/foo-100436.html", BASE_URL), false);
  assert.equal(
    isLikelyProductUrl(
      "https://www.rarebeauty.com/products/always-an-optimist-pore-diffusing-primer-mini",
      "https://rarebeauty.com",
    ),
    true,
  );
  assert.equal(
    isLikelyProductUrl(
      "https://rarebeauty.com/products/always-an-optimist-pore-diffusing-primer-mini",
      "https://www.rarebeauty.com",
    ),
    true,
  );
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

test("resolveStructuredImageUrl reads the first usable URL from ImageObject arrays", () => {
  const url = resolveStructuredImageUrl("https://www.guerlain.com", [
    {
      "@type": "ImageObject",
      url: "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dwf327011b/01-ProductsViewer/P062033/P062033_G062033_E01_hi-res.jpg?sw=655&sh=655&sfrm=png",
    },
    {
      "@type": "ImageObject",
      url: "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dw97b9f8d5/01-ProductsViewer/P062033/P062033_E02_hi-res.jpg?sw=655&sh=655&sfrm=jpg",
    },
  ]);

  assert.equal(
    url,
    "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dwf327011b/01-ProductsViewer/P062033/P062033_G062033_E01_hi-res.jpg?sw=655&sh=655&sfrm=png",
  );
});

test("resolveStructuredImageUrl skips placeholder and favicon candidates when falling back", () => {
  const url = resolveStructuredImageUrl("https://www.guerlain.com", [
    "https://www.guerlain.com/on/demandware.static/Sites-Guerlain_US-Site/-/default/dw84c3d99e/images/placeholder.svg",
    "https://www.guerlain.com/on/demandware.static/Sites-Guerlain_US-Site/-/default/dw9fbf37fc/images/favicons/favicon-144x144.png",
    "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dwf327011b/01-ProductsViewer/P062033/P062033_G062033_E01_hi-res.png?sw=900&sh=900",
  ]);

  assert.equal(
    url,
    "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dwf327011b/01-ProductsViewer/P062033/P062033_G062033_E01_hi-res.png?sw=900&sh=900",
  );
});

test("resolveStructuredImageUrls preserves gallery order and filters invalid assets", () => {
  const urls = resolveStructuredImageUrls("https://www.guerlain.com", [
    {
      "@type": "ImageObject",
      url: "https://www.guerlain.com/on/demandware.static/Sites-Guerlain_US-Site/-/default/dw84c3d99e/images/placeholder.svg",
    },
    {
      "@type": "ImageObject",
      url: "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dwf327011b/01-ProductsViewer/P062033/P062033_G062033_E01_hi-res.png?sw=900&sh=900",
    },
    {
      "@type": "ImageObject",
      url: "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dw97b9f8d5/01-ProductsViewer/P062033/P062033_E02_hi-res.jpg?sw=655&sh=655&sfrm=jpg",
    },
  ]);

  assert.deepEqual(urls, [
    "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dwf327011b/01-ProductsViewer/P062033/P062033_G062033_E01_hi-res.png?sw=900&sh=900",
    "https://www.guerlain.com/dw/image/v2/BDCZ_PRD/on/demandware.static/-/Sites-GSA_master_catalog/default/dw97b9f8d5/01-ProductsViewer/P062033/P062033_E02_hi-res.jpg?sw=655&sh=655&sfrm=jpg",
  ]);
});

test("resolveStructuredImageUrls recovers malformed Cafe24 absolute CDN URLs", () => {
  const urls = resolveStructuredImageUrls("https://jolse.com", [
    "/https://cafe24img.poxo.com/jolsejolse/web/product/big/example.png",
    "//cafe24img.poxo.com/jolsejolse/web/product/extra/example.jpg",
    "https://jolse.com/https://cafe24img.poxo.com/jolsejolse/web/product/extra/runtime-normalized.jpg",
  ]);

  assert.deepEqual(urls, [
    "https://cafe24img.poxo.com/jolsejolse/web/product/big/example.png",
    "https://cafe24img.poxo.com/jolsejolse/web/product/extra/example.jpg",
    "https://cafe24img.poxo.com/jolsejolse/web/product/extra/runtime-normalized.jpg",
  ]);
});

test("filterCleanProductImageAssetUrls blocks nested merchant/CDN image paths at publication", () => {
  assert.deepEqual(
    filterCleanProductImageAssetUrls("https://jolse.com", [
      "https://jolse.com/https://cafe24img.poxo.com/jolsejolse/web/product/extra/final-gate.jpg",
    ]),
    ["https://cafe24img.poxo.com/jolsejolse/web/product/extra/final-gate.jpg"],
  );
});
