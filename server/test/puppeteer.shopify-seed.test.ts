import assert from "node:assert/strict";
import test from "node:test";

import {
  buildProductFromPageSignals,
  PuppeteerExtractor,
  buildProductPdpFields,
  choosePreferredProductOverview,
  classifyExtractedProductKind,
  enrichDirectShopifyPdpResponse,
  extractBundleComponents,
  extractLikelyFullIngredientListText,
  extractShopifyEmbeddedProductPayloadPdpFields,
  extractShopifyProductJsonAttributeScriptsFromHtml,
  getMissingPdpFieldReasons,
  isNonProductRedirectForRequestedPdp,
  isShopifyDirectPdpFallbackUsable,
  mergeShopifyDirectPdpFallback,
  normalizeStructuredProseDetailSections,
  normalizeImageVisionFields,
  pickBestJsonLdObjectForPage,
  productHasMissingPdpFields,
} from "../src/services/extractors/puppeteer";
import { createDiagnostics, fetchJsonTracked } from "../src/services/extractors/shared";

type MockRoute = {
  status?: number;
  headers?: Record<string, string>;
  body?: string;
  responseUrl?: string;
};

function createMockResponse(route: MockRoute): Response {
  const response = new Response(route.body ?? "", {
    status: route.status ?? 200,
    headers: route.headers,
  });
  if (route.responseUrl) {
    Object.defineProperty(response, "url", {
      value: route.responseUrl,
      configurable: true,
    });
  }
  return response;
}

async function withMockFetch(routes: Record<string, MockRoute>, fn: () => Promise<void>): Promise<void> {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (input: RequestInfo | URL, _init?: RequestInit) => {
    void _init;
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const route = routes[url];
    if (!route) {
      throw new Error(`Unexpected fetch: ${url}`);
    }
    return createMockResponse(route);
  }) as typeof fetch;

  try {
    await fn();
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test("fetchJsonTracked retries transient Shopify JSON failures", async () => {
  const originalFetch = globalThis.fetch;
  const originalRetries = process.env.CATALOG_JSON_FETCH_RETRIES;
  const originalBaseMs = process.env.CATALOG_JSON_FETCH_RETRY_BASE_MS;
  const calls: string[] = [];
  process.env.CATALOG_JSON_FETCH_RETRIES = "1";
  process.env.CATALOG_JSON_FETCH_RETRY_BASE_MS = "50";

  globalThis.fetch = (async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push(url);
    if (calls.length === 1) {
      return createMockResponse({
        status: 503,
        headers: { "content-type": "text/plain" },
        body: "Service Unavailable",
      });
    }
    return createMockResponse({
      status: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ products: [{ id: 1, handle: "glow-serum" }] }),
    });
  }) as typeof fetch;

  try {
    const diagnostics = createDiagnostics("example.com", "https://example.com");
    const result = await fetchJsonTracked<{ products: Array<{ id: number; handle: string }> }>(
      "https://example.com/products.json?limit=250&page=1",
      {},
      diagnostics,
    );

    assert.equal(calls.length, 2);
    assert.equal(result.status, 200);
    assert.equal(result.data?.products[0]?.handle, "glow-serum");
    assert.deepEqual(
      diagnostics.http_trace.map((entry) => entry.status),
      [503, 200],
    );
  } finally {
    globalThis.fetch = originalFetch;
    if (originalRetries === undefined) delete process.env.CATALOG_JSON_FETCH_RETRIES;
    else process.env.CATALOG_JSON_FETCH_RETRIES = originalRetries;
    if (originalBaseMs === undefined) delete process.env.CATALOG_JSON_FETCH_RETRY_BASE_MS;
    else process.env.CATALOG_JSON_FETCH_RETRY_BASE_MS = originalBaseMs;
  }
});

test("PuppeteerExtractor passes market cookies to Shopify direct PDP requests", async () => {
  const extractor = new PuppeteerExtractor();
  const requests: Array<{ url: string; cookie: string; acceptLanguage: string }> = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const headers = new Headers(init?.headers);
    requests.push({
      url,
      cookie: headers.get("cookie") || "",
      acceptLanguage: headers.get("accept-language") || "",
    });

    if (url === "https://olehenriksen.com/products/henriksen-tote.js") {
      return createMockResponse({
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          id: 101,
          title: "HENRIKSEN Tote",
          handle: "henriksen-tote",
          body_html: "",
          variants: [
            {
              id: 1001,
              sku: "83555",
              title: "Default Title",
              option1: "Default Title",
              price: 100,
              available: true,
              inventory_quantity: 12,
            },
          ],
          options: [{ name: "Title" }],
          images: [{ src: "https://cdn.example.com/tote.jpg" }],
        }),
      });
    }

    if (url === "https://olehenriksen.com/products/henriksen-tote") {
      return createMockResponse({
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      });
    }

    throw new Error(`Unexpected fetch: ${url}`);
  }) as typeof fetch;

  try {
    const result = await extractor.extract({
      brand: "Ole Henriksen",
      domain: "https://olehenriksen.com/products/henriksen-tote",
      market: "US",
      limit: 1,
    });

    assert.equal(result.products.length, 1);
    assert.equal(result.products[0]?.variants[0]?.currency, "USD");
    assert.equal(result.products[0]?.variants[0]?.hidden_from_selector, true);
    assert.equal(result.products[0]?.variants[0]?.source_quality_status, "quarantined");
    const directRequest = requests.find((entry) => entry.url === "https://olehenriksen.com/products/henriksen-tote.js");
    assert.ok(directRequest);
    assert.match(directRequest!.cookie, /localization=US/i);
    assert.match(directRequest!.cookie, /cart_currency=USD/i);
    assert.match(directRequest!.acceptLanguage, /en-US/i);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("PuppeteerExtractor retries canonical Shopify handle for duplicate copy PDP URLs", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://anua.us/products/heartleaf-77-soothing-toner-copy.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://anua.us/products/heartleaf-77-soothing-toner.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          id: 301,
          title: "Heartleaf 77 Soothing Toner",
          handle: "heartleaf-77-soothing-toner",
          body_html:
            "<p>A calming toner.</p><p>How to Use: Apply after cleansing.</p><p>Ingredients: Water, Houttuynia Cordata Extract.</p>",
          variants: [
            {
              id: 3001,
              sku: "ANUA-H77-TONER",
              title: "Default Title",
              option1: "Default Title",
              price: "2300",
              available: true,
              inventory_quantity: 5,
            },
          ],
          options: [{ name: "Title" }],
          images: [{ src: "https://cdn.example.com/anua-toner.jpg" }],
        }),
      },
      "https://anua.us/products/heartleaf-77-soothing-toner": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
      "https://anua.us/products/heartleaf-77-soothing-toner-copy": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><body><h1>404</h1></body></html>",
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Anua",
        domain: "https://anua.us/products/heartleaf-77-soothing-toner-copy",
        product_title: "Heartleaf 77 Soothing Toner",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://anua.us/products/heartleaf-77-soothing-toner");
      assert.equal(result.products[0]?.variants[0]?.sku, "ANUA-H77-TONER");
      assert.equal(result.products[0]?.variants[0]?.hidden_from_selector, true);
      assert.ok(
        result.logs.some((entry) => /Recovered Shopify direct product via canonical handle/.test(entry.msg)),
        "expected duplicate-handle seed to recover through canonical handle retry",
      );
    },
  );
});

test("PuppeteerExtractor recovers direct Shopify PDP via browser when .js feed is gone but browser PDP still resolves", async () => {
  const browserRecoveredProduct = {
    title: "Lip Sleeping Mask Topper",
    url: "https://us.laneige.com/products/lip-sleeping-mask-topper",
    description_raw: "A glossy lip topper with a cushiony shine.",
    details_sections: [
      { heading: "How to Use", body: "Apply to lips.", source_kind: "page_how_to_use_section" },
      { heading: "Key Ingredients", body: "Shea Butter", source_kind: "details_summary" },
    ],
    ingredients_raw: "Polybutene, Shea Butter",
    active_ingredients_raw: "",
    how_to_use_raw: "Apply to lips.",
    faq_items: [],
    field_sources: {
      description_raw: ["shopify_description"],
      details_sections: ["page_how_to_use_section", "details_summary"],
      ingredients_raw: ["details_section_ingredients"],
      active_ingredients_raw: [],
      how_to_use_raw: ["page_how_to_use_section"],
      faq_items: [],
    },
    image_url: "https://cdn.example.com/topper-main.jpg",
    image_urls: ["https://cdn.example.com/topper-main.jpg"],
    variant_skus: ["LAN-TOPPER-1", "LAN-TOPPER-2"],
    variants: [
      {
        id: "47231057854516",
        sku: "LAN-TOPPER-1",
        title: "Pumpkin Pie",
        option_name: "Shade",
        option_value: "Pumpkin Pie",
        price: 24,
        currency: "USD",
        availability: "in_stock",
        image_url: "https://cdn.example.com/topper-pumpkin.jpg",
        image_urls: ["https://cdn.example.com/topper-pumpkin.jpg"],
      },
      {
        id: "47231057887284",
        sku: "LAN-TOPPER-2",
        title: "Hot Cocoa",
        option_name: "Shade",
        option_value: "Hot Cocoa",
        price: 24,
        currency: "USD",
        availability: "in_stock",
        image_url: "https://cdn.example.com/topper-cocoa.jpg",
        image_urls: ["https://cdn.example.com/topper-cocoa.jpg"],
      },
    ],
  } as any;

  const extractor = new PuppeteerExtractor((async (_task, _options) => ({
    mode: "managed",
    result: browserRecoveredProduct,
  })) as any);

  await withMockFetch(
    {
      "https://us.laneige.com/products/lip-sleeping-mask-topper.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://us.laneige.com/products/lip-sleeping-mask-topper": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        responseUrl: "https://us.laneige.com/collections/merch",
        body: "<html><body>merch redirect shell</body></html>",
      },
      "https://us.laneige.com/search/suggest.json?q=Lip%20Sleeping%20Mask%20Topper&resources[type]=product&resources[limit]=8": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ resources: { results: { products: [] } } }),
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "LANEIGE",
        domain: "https://us.laneige.com/products/lip-sleeping-mask-topper",
        product_title: "Lip Sleeping Mask Topper",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.title, "Lip Sleeping Mask Topper");
      assert.equal(result.products[0]?.details_sections.length, 2);
      assert.equal(result.products[0]?.variants.length, 2);
      assert.equal(result.diagnostics?.discovery_strategy, "managed_browser");
      assert.match(
        result.logs.map((entry) => entry.msg).join("\n"),
        /Recovered Shopify direct PDP via browser scrape/,
      );
    },
  );
});

test("PuppeteerExtractor turns single default Shopify size evidence into a Size option", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://beautyofjoseon.com/products/dynasty-cream-10ml.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          id: 201,
          title: "Dynasty Cream 10ml",
          handle: "dynasty-cream-10ml",
          body_html: "A moisturizer.",
          variants: [
            {
              id: 2001,
              sku: "BOJ-DYNASTY-10ML",
              title: "Default Title",
              option1: "Default Title",
              price: "1200",
              available: true,
              inventory_quantity: 5,
            },
          ],
          options: [{ name: "Title" }],
          images: [{ src: "https://cdn.example.com/dynasty-10ml.jpg" }],
        }),
      },
      "https://beautyofjoseon.com/products/dynasty-cream-10ml": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Beauty of Joseon",
        domain: "https://beautyofjoseon.com/products/dynasty-cream-10ml",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.variants.length, 1);
      assert.equal(result.products[0]?.variants[0]?.option_name, "Size");
      assert.equal(result.products[0]?.variants[0]?.option_value, "10ml");
      assert.equal(result.products[0]?.variants[0]?.sku, "BOJ-DYNASTY-10ML");
    },
  );
});

test("PuppeteerExtractor reads single default Shopify size evidence from product images", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://example.com/products/peptide-serum.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          id: 202,
          title: "Peptide Serum",
          handle: "peptide-serum",
          body_html: "A peptide serum.",
          variants: [
            {
              id: 2002,
              sku: "SERUM-30ML",
              title: "Default Title",
              option1: "Default Title",
              price: "2400",
              available: true,
              inventory_quantity: 5,
            },
          ],
          options: [{ name: "Title" }],
          images: [{ src: "https://cdn.example.com/products/peptide-serum-30ml.png" }],
        }),
      },
      "https://example.com/products/peptide-serum": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Example",
        domain: "https://example.com/products/peptide-serum",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.variants[0]?.option_name, "Size");
      assert.equal(result.products[0]?.variants[0]?.option_value, "30ml");
    },
  );
});

test("PuppeteerExtractor reads single default Shopify pack-count evidence from product images", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://example.com/products/catechin-calming-pad.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          id: 203,
          title: "Catechin Calming Pad",
          handle: "catechin-calming-pad",
          body_html: "A calming toner pad.",
          variants: [
            {
              id: 2003,
              sku: "PAD-110EA",
              title: "Catechin Calming Pad",
              option1: "Catechin Calming Pad",
              price: "2300",
              available: true,
              inventory_quantity: 5,
              featured_image: {
                alt: "Catechin Calming Pad 110 ea",
                src: "https://cdn.example.com/products/catechin-calming-pad-110-ea.png",
              },
            },
          ],
          options: [{ name: "Catechin Calming Pad" }],
          images: [{ src: "https://cdn.example.com/products/catechin-calming-pad-110-ea.png" }],
        }),
      },
      "https://example.com/products/catechin-calming-pad": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Example",
        domain: "https://example.com/products/catechin-calming-pad",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.variants.length, 1);
      assert.equal(result.products[0]?.variants[0]?.option_name, "Size");
      assert.equal(result.products[0]?.variants[0]?.option_value, "110 ea");
      assert.equal(result.products[0]?.variants[0]?.sku, "PAD-110EA");
    },
  );
});

test("buildProductFromPageSignals reads single default size evidence from product volume text", () => {
  const product = buildProductFromPageSignals({
    extracted: {
      title: "Bouncy & Firm Eye Sleeping Mask",
      canonical: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
      metaDescription: "",
      priceTexts: ["$32"],
      imageCandidates: ["https://cdn.example.com/bouncy-eye.jpg"],
      scripts: [],
      embeddedProductScripts: [],
      domVariants: [],
      productVolumeText: "(0.70 fl. oz./20 mL)",
      productDetailsText: "An overnight cooling and hydrating eye treatment.",
      detailsSections: [],
      faqItems: [],
      faqHtmlSnippets: [],
    },
    pageLooksLikeProduct: true,
    sourceUrl: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
    baseUrl: "https://us.laneige.com",
    verbose: false,
    log: () => {},
  });

  assert.ok(product);
  assert.equal(product?.variants.length, 1);
  assert.equal(product?.variants[0]?.option_name, "Size");
  assert.equal(product?.variants[0]?.option_value, "20ml");
});

test("buildProductFromPageSignals keeps exact-item variant gallery and surfaces section media separately", () => {
  const product = buildProductFromPageSignals({
    extracted: {
      title: "Ceramidin Skin Barrier Moisturizing Cream",
      canonical: "https://www.drjart.com/product/28258/111504/moisturizers/ceramidintm-skin-barrier-moisturizing-cream",
      metaDescription: "Barrier moisturizing cream.",
      priceTexts: ["$48"],
      imageCandidates: [
        "https://www.drjart.com/media/group-hero.jpg",
        "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_0.jpg",
        "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_1.jpg",
        "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_2.jpg",
        "https://www.drjart.com/media/dj_sku_H6MN01_1000x1000_0.jpg",
        "https://www.drjart.com/media/dj_sku_H7T901_1000x1000_0.jpg",
      ],
      scripts: [
        JSON.stringify({
          "@context": "https://schema.org",
          "@type": "ProductGroup",
          name: "Ceramidin Skin Barrier Moisturizing Cream",
          image: ["https://www.drjart.com/media/group-hero.jpg"],
          hasVariant: [
            {
              "@type": "Product",
              name: "Ceramidin Skin Barrier Moisturizing Cream 50ml",
              sku: "DJ-50ML",
              size: "50ml",
              url: "https://www.drjart.com/product/28258/111504/moisturizers/ceramidintm-skin-barrier-moisturizing-cream?size=50ml",
              image: "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_0.jpg",
              offers: {
                "@type": "Offer",
                price: "48.00",
                priceCurrency: "USD",
                availability: "https://schema.org/InStock",
              },
            },
            {
              "@type": "Product",
              name: "Ceramidin Skin Barrier Moisturizing Cream 15ml",
              sku: "DJ-15ML",
              size: "15ml",
              url: "https://www.drjart.com/product/28258/111504/moisturizers/ceramidintm-skin-barrier-moisturizing-cream?size=15ml",
              image: "https://www.drjart.com/media/dj_sku_H6MN01_1000x1000_0.jpg",
              offers: {
                "@type": "Offer",
                price: "22.00",
                priceCurrency: "USD",
                availability: "https://schema.org/InStock",
              },
            },
          ],
        }),
      ],
      embeddedProductScripts: [],
      domVariants: [],
      productVolumeText: "",
      productDetailsText: "Barrier-supporting moisturizer.",
      detailsSections: [
        {
          heading: "Overview",
          body: "Ceramide-rich cream that helps reinforce the skin barrier.",
          source_kind: "heading_sibling",
          media_urls: ["https://www.drjart.com/media/regimen-step-1.jpg"],
        },
        {
          heading: "How to Use",
          body: "Apply morning and night after serum.",
          source_kind: "accordion_button",
          media_urls: ["https://www.drjart.com/media/how-to-apply.jpg"],
        },
      ],
      faqItems: [],
      faqHtmlSnippets: [],
    },
    pageLooksLikeProduct: true,
    sourceUrl: "https://www.drjart.com/product/28258/111504/moisturizers/ceramidintm-skin-barrier-moisturizing-cream?size=50ml",
    baseUrl: "https://www.drjart.com",
    verbose: false,
    log: () => {},
  });

  assert.ok(product);
  assert.deepEqual(product?.image_urls, [
    "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_0.jpg",
    "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_1.jpg",
    "https://www.drjart.com/media/dj_sku_H6MM01_1000x1000_2.jpg",
  ]);
  assert.equal(product?.variants[0]?.option_value, "50ml");
  assert.deepEqual(product?.variants[0]?.image_urls, product?.image_urls);
  assert.equal(product?.variants.length, 2);
  assert.deepEqual(product?.variants[1]?.image_urls, [
    "https://www.drjart.com/media/dj_sku_H6MN01_1000x1000_0.jpg",
  ]);
  assert.ok(
    !product?.variants.some((variant) =>
      (variant.image_urls || []).includes("https://www.drjart.com/media/dj_sku_H7T901_1000x1000_0.jpg"),
    ),
  );
  assert.deepEqual(product?.content_image_urls, [
    "https://www.drjart.com/media/regimen-step-1.jpg",
    "https://www.drjart.com/media/how-to-apply.jpg",
  ]);
  assert.deepEqual(product?.details_sections?.[0]?.media_urls, [
    "https://www.drjart.com/media/regimen-step-1.jpg",
  ]);
  assert.deepEqual(product?.details_sections?.[1]?.media_urls, [
    "https://www.drjart.com/media/how-to-apply.jpg",
  ]);
});

test("isNonProductRedirectForRequestedPdp catches product URLs redirected to homepage", () => {
  assert.equal(
    isNonProductRedirectForRequestedPdp(
      "https://peaceoutskincare.com/products/peace-out-acne-dots",
      "https://peaceoutskincare.com/",
      "https://peaceoutskincare.com",
    ),
    true,
  );
  assert.equal(
    isNonProductRedirectForRequestedPdp(
      "https://peaceoutskincare.com/products/peace-out-acne-dots",
      "https://peaceoutskincare.com/products/peace-out-acne-dots?variant=123",
      "https://peaceoutskincare.com",
    ),
    false,
  );
  assert.equal(
    isNonProductRedirectForRequestedPdp(
      "https://peaceoutskincare.com/collections/shop-all",
      "https://peaceoutskincare.com/",
      "https://peaceoutskincare.com",
    ),
    false,
  );
});

test("isShopifyDirectPdpFallbackUsable rejects homepage browser fallback for direct PDP enrichment", () => {
  const primaryProduct = {
    title: "Acne Healing Dots",
    url: "https://peaceoutskincare.com/products/peace-out-acne-dots",
    image_url: "",
    image_urls: [],
    variant_skus: ["PO-ACNE-DOTS"],
    variants: [
      {
        id: "v1",
        sku: "PO-ACNE-DOTS",
        url: "https://peaceoutskincare.com/products/peace-out-acne-dots",
        option_name: "Title",
        option_value: "Default Title",
        price: "19.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "",
        image_urls: [],
        ad_copy: "",
      },
    ],
  };
  const homepageFallback = {
    title: "Peace Out Skincare",
    url: "https://peaceoutskincare.com/products/peace-out-acne-dots",
    image_url: "https://peaceoutskincare.com/cdn/shop/files/homepage-hero.jpg",
    image_urls: ["https://peaceoutskincare.com/cdn/shop/files/homepage-hero.jpg"],
    variant_skus: [],
    faq_items: [
      {
        question: "Why Your Face Towel Might Be Sabotaging Your Skin",
        answer: "And what to do about it.",
        source_kind: "faq_section" as const,
        source_url: "about:blank",
        source_title: "FAQ",
      },
    ],
    variants: [],
  };
  const matchingFallback = {
    ...homepageFallback,
    title: "Acne Healing Dots",
    variant_skus: ["PO-ACNE-DOTS"],
    variants: primaryProduct.variants,
  };

  assert.equal(isShopifyDirectPdpFallbackUsable(primaryProduct, homepageFallback), false);
  assert.equal(isShopifyDirectPdpFallbackUsable(primaryProduct, matchingFallback), true);
});

test("PuppeteerExtractor honors direct Shopify PDP seed URLs", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 101,
    title: "Banana Bright 15% Vitamin C Dark Spot Serum",
    handle: "banana-bright-vitamin-c-serum",
    description: "<p>Brightening serum</p>",
    variants: [
      {
        id: 1001,
        sku: "OH-VC-001",
        title: "Default Title",
        option1: "Default Title",
        price: 6800,
        available: true,
        inventory_quantity: 12,
      },
    ],
    options: [{ name: "Variant" }],
    images: [
      { src: "https://cdn.example.com/banana-1.jpg" },
      { src: "https://cdn.example.com/banana-2.jpg" },
    ],
  };

  await withMockFetch(
    {
      "https://olehenriksen.com/products/banana-bright-vitamin-c-serum.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://olehenriksen.com/products/banana-bright-vitamin-c-serum": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Ole Henriksen",
        domain: "https://olehenriksen.com/products/banana-bright-vitamin-c-serum",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://olehenriksen.com/products/banana-bright-vitamin-c-serum");
      assert.equal(result.products[0]?.description_raw, "Brightening serum");
      assert.ok(result.products[0]?.field_sources?.description_raw?.includes("shopify_description"));
      assert.deepEqual(result.products[0]?.variant_skus, ["OH-VC-001"]);
      assert.equal(result.variants[0]?.price, "68.00");
      assert.equal(result.variants[0]?.currency, "USD");
      assert.deepEqual(result.products[0]?.image_urls, [
        "https://cdn.example.com/banana-1.jpg",
        "https://cdn.example.com/banana-2.jpg",
      ]);
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
    },
  );
});

test("PuppeteerExtractor repairs stale Shopify direct PDP handles via product-title search", async () => {
  const extractor = new PuppeteerExtractor();
  const recoveredProduct = {
    id: 9070600847610,
    title: "Rosewater Balancing Mist",
    handle: "rosewater-balancing-mist-new-100ml",
    type: "Essence and Toners",
    tags: ["Face Care", "Hydrating", "Rose"],
    body_html:
      "<p>Create a sensory moment of self-care with rose extract.</p><p>How to Use: Mist over face as needed.</p><p>Ingredients: Water, Rosa Damascena Flower Water, Glycerin.</p>",
    variants: [
      {
        id: 1001,
        sku: "JQ-ROSE-MIST",
        title: "Default Title",
        option1: "Default Title",
        price: 4800,
        available: true,
        inventory_quantity: 12,
      },
    ],
    options: [{ name: "Title" }],
    images: [{ src: "https://cdn.example.com/rose-mist.jpg" }],
  };

  await withMockFetch(
    {
      "https://jurlique.com/products/rosewater-balancing-mist-1.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://jurlique.com/products/rosewater-balancing-mist-1": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><body><h1>404</h1></body></html>",
      },
      "https://jurlique.com/search/suggest.json?q=Rosewater%20Balancing%20Mist&resources[type]=product&resources[limit]=8": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          resources: {
            results: {
              products: [
                {
                  title: "Rosewater Balancing Mist",
                  handle: "rosewater-balancing-mist-new-100ml",
                  url: "/products/rosewater-balancing-mist-new-100ml",
                  available: true,
                },
              ],
            },
          },
        }),
      },
      "https://jurlique.com/products/rosewater-balancing-mist-new-100ml.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(recoveredProduct),
      },
      "https://jurlique.com/products/rosewater-balancing-mist-new-100ml": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Jurlique",
        domain: "https://jurlique.com/products/rosewater-balancing-mist-1",
        product_title: "Rosewater Balancing Mist",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://jurlique.com/products/rosewater-balancing-mist-new-100ml");
      assert.equal(result.products[0]?.variants[0]?.sku, "JQ-ROSE-MIST");
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
      assert.ok(
        result.logs.some((entry) => /Recovered stale Shopify PDP handle via title search/.test(entry.msg)),
        "expected stale direct PDP to recover through structured Shopify search",
      );
    },
  );
});

test("PuppeteerExtractor repairs Shopify direct PDPs that redirect to an incompatible locale product", async () => {
  const extractor = new PuppeteerExtractor();
  const recoveredProduct = {
    id: 9731399221582,
    title: "Repulpant Lèvres Hyaluronic",
    handle: "repulpant-levres-hyaluronic",
    type: "Lip Care",
    body_html:
      "<p>Hyaluronic lip care.</p><p>How to Use: Apply to lips morning and evening.</p><p>Ingredients: Ricinus Communis Seed Oil, Glycerin, Sodium Hyaluronate.</p>",
    variants: [
      {
        id: 49216125010254,
        sku: "P0034",
        title: "7ml",
        option1: "7ml",
        price: 3500,
        available: true,
        inventory_quantity: 5,
      },
    ],
    options: [{ name: "Format" }],
    images: [{ src: "https://cdn.example.com/patyka-lip.jpg" }],
  };

  await withMockFetch(
    {
      "https://patyka.com/products/hyaluronic-lip-plumper.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://patyka.com/en-us/products/hyaluronic-lip-plumper": {
        status: 200,
        responseUrl: "https://patyka.com/es-ad/products/rellenador-de-labios-hialuronico",
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body><script type="application/ld+json">{"@type":"Product","name":"Rellenador de Labios Hialurónico"}</script></body></html>',
      },
      "https://patyka.com/search/suggest.json?q=Repulpant%20L%C3%A8vres%20Hyaluronic&resources[type]=product&resources[limit]=8": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          resources: {
            results: {
              products: [
                {
                  title: "Repulpant Lèvres Hyaluronic",
                  handle: "repulpant-levres-hyaluronic",
                  url: "/products/repulpant-levres-hyaluronic",
                  available: true,
                },
              ],
            },
          },
        }),
      },
      "https://patyka.com/products/repulpant-levres-hyaluronic.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(recoveredProduct),
      },
      "https://patyka.com/products/repulpant-levres-hyaluronic": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Patyka",
        domain: "https://patyka.com/en-us/products/hyaluronic-lip-plumper",
        product_title: "Repulpant Lèvres Hyaluronic",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://patyka.com/products/repulpant-levres-hyaluronic");
      assert.equal(result.products[0]?.variants[0]?.sku, "P0034");
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
      assert.ok(
        result.logs.some((entry) => /Recovered stale Shopify PDP handle via title search/.test(entry.msg)),
        "expected locale-drifted direct PDP to recover through structured Shopify search",
      );
    },
  );
});

test("PuppeteerExtractor fails fast when a Shopify direct PDP is deleted", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://fentybeauty.com/products/gloss-bomb-swirl-fu-y-chocolit.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://fentybeauty.com/products/gloss-bomb-swirl-fu-y-chocolit": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><body><h1>404</h1></body></html>",
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Fenty Beauty",
        domain: "https://fentybeauty.com/products/gloss-bomb-swirl-fu-y-chocolit",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 0);
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
      assert.equal(result.diagnostics?.failure_category, "no_product_urls");
      assert.ok(
        result.logs.some((entry) => /Skipping generic rediscovery/.test(entry.msg)),
        "expected direct PDP failure to stop before generic discovery",
      );
    },
  );
});

test("PuppeteerExtractor fails fast when a Shopify direct PDP redirects to a collection", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://fentybeauty.com/products/melt-awf-jelly-oil-makeup-melting-cleanser.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://fentybeauty.com/products/melt-awf-jelly-oil-makeup-melting-cleanser": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        responseUrl: "https://fentybeauty.com/en-nl/collections/skincare-cleanser",
        body: `
          <html>
            <body>
              <h1>Cleanser</h1>
              <a href="/products/total-cleansr-remove-it-all-cleanser">Total Cleans'r</a>
            </body>
          </html>
        `,
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Fenty Beauty",
        domain: "https://fentybeauty.com/products/melt-awf-jelly-oil-makeup-melting-cleanser",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 0);
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
      assert.equal(result.diagnostics?.failure_category, "no_product_urls");
      assert.ok(
        result.logs.some((entry) => /seed status=non_product_redirect/.test(entry.msg)),
        "expected collection redirect to stop before generic discovery",
      );
    },
  );
});

test("PuppeteerExtractor fails fast when a Shopify direct PDP redirects to the locale homepage", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://fentybeauty.com/products/gloss-bomb-swirl-twisted-lip-luminizer-fu-y-chocolit.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ status: 404, message: "Not Found" }),
      },
      "https://fentybeauty.com/products/gloss-bomb-swirl-twisted-lip-luminizer-fu-y-chocolit": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        responseUrl: "https://fentybeauty.com/en-nl",
        body: `
          <html>
            <body>
              <h1>Fenty Beauty</h1>
              <a href="/products/gloss-bomb-universal-lip-luminizer-fuy">Gloss Bomb</a>
            </body>
          </html>
        `,
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Fenty Beauty",
        domain: "https://fentybeauty.com/products/gloss-bomb-swirl-twisted-lip-luminizer-fu-y-chocolit",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 0);
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
      assert.equal(result.diagnostics?.failure_category, "no_product_urls");
      assert.ok(
        result.logs.some((entry) => /seed status=non_product_redirect/.test(entry.msg)),
        "expected homepage redirect to stop before generic discovery",
      );
    },
  );
});

test("enrichDirectShopifyPdpResponse preserves direct feed response when browser enrichment throws", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const response = {
    brand: "Tom Ford Beauty",
    domain: "https://www.tomfordbeauty.com/product/fucking-fabulous-parfum?size=50_ml",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Fucking Fabulous Parfum",
        url: "https://www.tomfordbeauty.com/products/fucking-fabulous-parfum",
        image_url: "",
        image_urls: [],
        variant_skus: ["TF-FF-050"],
        variants: [
          {
            id: "5001",
            sku: "TF-FF-050",
            url: "https://www.tomfordbeauty.com/products/fucking-fabulous-parfum",
            option_name: "Size",
            option_value: "50 ml",
            price: "395.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "",
            image_urls: [],
            ad_copy: "",
          },
        ],
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 395, max: 395, avg: 395 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "www.tomfordbeauty.com",
      resolved_base_url: "https://www.tomfordbeauty.com",
      discovery_strategy: "shopify_json",
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const result = await enrichDirectShopifyPdpResponse({
    brand: "Tom Ford Beauty",
    baseUrl: "https://www.tomfordbeauty.com",
    seedUrl: "https://www.tomfordbeauty.com/product/fucking-fabulous-parfum?size=50_ml",
    response,
    diagnostics: response.diagnostics,
    log: (type, msg) => logs.push({ type, msg }),
    browserRunner: async () => {
      throw new Error("browser explode");
    },
  });

  assert.equal(result.platform, "Shopify (Direct PDP)");
  assert.equal(result.products.length, 1);
  assert.equal(result.products[0]?.title, "Fucking Fabulous Parfum");
  assert.match(logs.map((entry) => `${entry.type}:${entry.msg}`).join("\n"), /Browser enrichment failed for Shopify PDP/);
});

test("buildProductPdpFields keeps Shopify Product Type tags out of user-visible PDP details", () => {
  const fields = buildProductPdpFields({
    detailsSections: [
      {
        heading: "Product Type",
        body: "Primer",
        source_kind: "shopify_product_tags",
      },
    ],
  });

  assert.equal(fields.details_sections, undefined);
  assert.equal(fields.field_capture_status.details_sections, "missing");
  assert.deepEqual(
    getMissingPdpFieldReasons({
      title: "Reflect Glow Prep Primer",
      url: "https://tirtir.global/products/reflect-glow-prep-primer",
      image_url: "https://cdn.example.com/primer-1.png",
      image_urls: ["https://cdn.example.com/primer-1.png", "https://cdn.example.com/primer-2.png"],
      variant_skus: ["TIRTIR-PRIMER"],
      variants: [],
      details_sections: [
        {
          heading: "Product Type",
          body: "Primer",
          source_kind: "embedded_product_json_tags",
        },
      ],
    }),
    ["overview", "ingredients"],
  );
});

test("enrichDirectShopifyPdpResponse recovers image-only Shopify PDP content through vision", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const seedUrl = "https://tirtir.global/products/reflect-glow-prep-primer";
  const response = {
    brand: "TIRTIR",
    domain: "https://tirtir.global",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Reflect Glow Prep Primer",
        url: seedUrl,
        image_url: "https://cdn.example.com/tirtir-primer-1.png",
        image_urls: [
          "https://cdn.example.com/tirtir-primer-1.png",
          "https://cdn.example.com/tirtir-primer-2.png",
          "https://cdn.example.com/tirtir-primer-3.png",
        ],
        variant_skus: ["TIRTIR-PRIMER"],
        details_sections: [
          {
            heading: "Product Type",
            body: "Primer",
            source_kind: "embedded_product_json_tags",
          },
        ],
        variants: [
          {
            id: "1",
            sku: "TIRTIR-PRIMER",
            url: seedUrl,
            option_name: "Title",
            option_value: "Default Title",
            price: "20.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/tirtir-primer-1.png",
            image_urls: ["https://cdn.example.com/tirtir-primer-1.png"],
            ad_copy: "",
          },
        ],
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 20, max: 20, avg: 20 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "tirtir.global",
      resolved_base_url: "https://tirtir.global",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  await withMockFetch(
    {
      [seedUrl]: {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><body></body></html>",
      },
    },
    async () => {
      const result = await enrichDirectShopifyPdpResponse({
        brand: "TIRTIR",
        baseUrl: "https://tirtir.global",
        seedUrl,
        response,
        diagnostics: response.diagnostics,
        log: (type, msg) => logs.push({ type, msg }),
        browserRunner: async () => {
          throw new Error("perimeterx");
        },
        imageVisionClient: async ({ imageUrls, missingReasons }) => {
          assert.deepEqual(missingReasons, ["overview", "ingredients"]);
          assert.equal(imageUrls.length, 3);
          return {
            descriptionRaw:
              "A glow-priming base designed to prep skin with a radiant finish before makeup application.",
            detailsSections: [
              {
                heading: "Benefits",
                body: "Creates a luminous-looking base and helps makeup apply more smoothly.",
                source_kind: "product_image_vision",
              },
              {
                heading: "Product Type",
                body: "Primer",
                source_kind: "product_image_vision",
              },
            ],
            howToUseRaw: "Apply a thin layer evenly before makeup.",
            ingredientsRaw: "Water, Glycerin, Butylene Glycol, Niacinamide, Panthenol.",
            contentImageUrls: imageUrls.slice(0, 2),
          };
        },
      });
      assert.equal(result.products[0]?.description_raw, undefined);
      assert.equal(result.products[0]?.how_to_use_raw, undefined);
      assert.equal(result.products[0]?.ingredients_raw, undefined);
      assert.deepEqual(result.products[0]?.details_sections?.map((section) => section.heading), ["Product Type"]);
      assert.deepEqual(result.products[0]?.field_sources?.details_sections || [], []);
      assert.equal(result.products[0]?.field_quality_summary?.description_raw?.source_quality_status, "quarantined");
      assert.deepEqual(result.products[0]?.field_quality_summary?.details_sections?.reason_codes, [
        "quarantined_source_kind",
      ]);
      assert.equal(result.products[0]?.field_quality_summary?.ingredients_raw?.source_origin, "image_vision");
      assert.match(result.products[0]?.quarantined_pdp_fields?.description_raw || "", /glow-priming base/);
      assert.deepEqual(result.products[0]?.quarantined_pdp_fields?.details_sections?.map((section) => section.heading), [
        "Benefits",
      ]);
      assert.deepEqual(result.products[0]?.content_image_urls, [
        "https://cdn.example.com/tirtir-primer-1.png",
        "https://cdn.example.com/tirtir-primer-2.png",
      ]);
      assert.match(logs.map((entry) => entry.msg).join("\n"), /Recovered Shopify PDP content via product image vision/);
    },
  );
});

test("normalizeImageVisionFields strips ingredient markers but does not infer active ingredients from section headings alone", () => {
  const fields = normalizeImageVisionFields(
    {
      description_raw:
        "Light-catching glow with a crystal-clear, glass-skin prep. 95% skincare-infused red serum primer for crystal glow, plumping and grip.",
      ingredients_raw: "* Water, Glycerin, Butylene Glycol, Niacinamide, Propanediol, Adenosine.",
      details_sections: [
        {
          heading: "5% NIACINAMIDE",
          body: "Refines the look of tone and texture.",
        },
        {
          heading: "PEPTINOL",
          body: "Helps support firmer, plumper-looking skin.",
        },
        {
          heading: "Clinical Test Results",
          body: "Visible skin-plumping improvement after use.",
        },
      ],
    },
    {
      title: "Reflect Glow Prep Primer",
      url: "https://tirtir.global/products/reflect-glow-prep-primer",
      image_url: "https://cdn.example.com/tirtir-primer-1.png",
      image_urls: ["https://cdn.example.com/tirtir-primer-1.png"],
      variant_skus: ["TIRTIR-PRIMER"],
      variants: [],
    },
    ["https://cdn.example.com/tirtir-primer-1.png"],
  );

  assert.equal(fields?.ingredientsRaw, "Water, Glycerin, Butylene Glycol, Niacinamide, Propanediol, Adenosine.");
  assert.equal(fields?.activeIngredientsRaw, undefined);
});

test("enrichDirectShopifyPdpResponse recovers FAQ via Okendo without browser enrichment", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const response = {
    brand: "Pixi Beauty",
    domain: "https://pixibeauty.com/products/clarity-tonic",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Clarity Tonic",
        url: "https://pixibeauty.com/products/clarity-tonic",
        image_url: "https://cdn.example.com/clarity-tonic.jpg",
        image_urls: ["https://cdn.example.com/clarity-tonic.jpg"],
        variant_skus: ["PIXI-CLARITY-001"],
        variants: [
          {
            id: "9001",
            sku: "PIXI-CLARITY-001",
            url: "https://pixibeauty.com/products/clarity-tonic",
            option_name: "Variant",
            option_value: "Default",
            price: "18.00",
            currency: "USD",
            stock: "In Stock",
            description: "Clarity Tonic",
            image_url: "https://cdn.example.com/clarity-tonic.jpg",
            image_urls: ["https://cdn.example.com/clarity-tonic.jpg"],
            ad_copy: "",
          },
        ],
        description_raw: "A clarifying toner for breakout-prone skin.",
        details_sections: [
          {
            heading: "Benefits",
            body: "Helps visibly clarify and smooth.",
            source_kind: "shopify_body_html_labeled_sections",
          },
        ],
        ingredients_raw: "Water, Glycerin, Salicylic Acid",
        how_to_use_raw: "Use AM and PM after cleansing.",
        field_capture_status: {
          description_raw: "present" as const,
          details_sections: "present" as const,
          ingredients_raw: "present" as const,
          active_ingredients_raw: "missing" as const,
          how_to_use_raw: "present" as const,
          faq_items: "missing" as const,
        },
        field_sources: {
          description_raw: ["shopify_body_html"],
          details_sections: ["shopify_body_html_labeled_sections"],
          ingredients_raw: ["shopify_body_html_labeled_ingredients"],
          active_ingredients_raw: [],
          how_to_use_raw: ["shopify_body_html_labeled_how_to_use"],
          faq_items: [],
        },
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 18, max: 18, avg: 18 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "pixibeauty.com",
      resolved_base_url: "https://pixibeauty.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  await withMockFetch(
    {
      "https://pixibeauty.com/products/clarity-tonic": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: `
          <html>
            <body>
              <script data-oke-metafield-data type="application/json">
                ${JSON.stringify({
                  averageRating: "4.5",
                  reviewCount: 2,
                  questionCount: 1,
                })}
              </script>
              <script type="application/json" id="oke-reviews-settings">
                ${JSON.stringify({
                  subscriberId: "store-123",
                  widgetSettings: {
                    homepageCarousel: {
                      defaultSort: "date desc",
                    },
                  },
                })}
              </script>
              <div data-oke-star-rating data-oke-reviews-product-id="shopify-456"></div>
            </body>
          </html>
        `,
      },
      "https://api.okendo.io/v1/stores/store-123/products/shopify-456/questions?limit=1": {
        status: 200,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          questions: [
            {
              body: "What percentage of salicylic acid does this product contain?",
              status: "approved",
              answers: [
                {
                  body: "<p>We don't disclose the percentage.</p>",
                  status: "approved",
                  isPrivate: false,
                  isStoreAnswer: true,
                },
              ],
            },
          ],
        }),
      },
      "https://api.okendo.io/v1/stores/store-123/products/shopify-456/reviews?limit=2&orderBy=date%20desc": {
        status: 200,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          areReviewsGrouped: false,
          reviews: [
            {
              reviewId: "review-1",
              rating: 5,
              title: "Clarity staple",
              body: "Keeps pores clear without stripping my skin.",
              status: "approved",
              reviewer: {
                displayName: "Soo J.",
                isVerified: true,
              },
            },
            {
              reviewId: "review-2",
              rating: 4,
              title: "Gentle exfoliation",
              body: "Balanced enough for frequent use.",
              status: "approved",
              reviewer: {
                displayName: "Maya R.",
              },
            },
          ],
        }),
      },
    },
    async () => {
      const result = await enrichDirectShopifyPdpResponse({
        brand: "Pixi Beauty",
        baseUrl: "https://pixibeauty.com",
        seedUrl: "https://pixibeauty.com/products/clarity-tonic",
        response,
        diagnostics: response.diagnostics,
        log: (type, msg) => logs.push({ type, msg }),
        browserRunner: async () => {
          throw new Error("browser should not run");
        },
      });

      assert.equal(result.products[0]?.faq_items?.length, 1);
      assert.deepEqual(result.products[0]?.faq_items, [
        {
          question: "What percentage of salicylic acid does this product contain?",
          answer: "We don't disclose the percentage.",
          source_kind: "okendo_questions_api",
          source_url: "https://pixibeauty.com/products/clarity-tonic",
          source_title: "Product Questions",
        },
      ]);
      assert.deepEqual(result.products[0]?.field_sources?.faq_items, ["okendo_questions_api"]);
      assert.equal(result.products[0]?.field_capture_status?.faq_items, "present");
      assert.deepEqual(result.products[0]?.review_summary, {
        rating: 4.5,
        review_count: 2,
        scale: 5,
        aggregation_scope: "product",
        exact_item_review_count: 2,
        preview_items: [
          {
            review_id: "review-1",
            rating: 5,
            author_label: "Soo J.",
            title: "Clarity staple",
            text_snippet: "Keeps pores clear without stripping my skin.",
            source: "merchant_public",
            source_kind: "okendo_reviews_api",
            source_scope: "merchant_public",
            content_review_state: "approved",
            public_visible: true,
            verified_buyer: true,
          },
          {
            review_id: "review-2",
            rating: 4,
            author_label: "Maya R.",
            title: "Gentle exfoliation",
            text_snippet: "Balanced enough for frequent use.",
            source: "merchant_public",
            source_kind: "okendo_reviews_api",
            source_scope: "merchant_public",
            content_review_state: "approved",
            public_visible: true,
          },
        ],
      });
      assert.doesNotMatch(logs.map((entry) => entry.msg).join("\n"), /Attempting browser enrichment/);
      assert.match(logs.map((entry) => entry.msg).join("\n"), /merchant review previews via Okendo reviews/);
    },
  );
});

test("enrichDirectShopifyPdpResponse escalates thin direct Shopify PDPs when page HTML shows richer product content", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const seedUrl = "https://www.tomfordbeauty.com/products/tom-ford-research-eye-repair-concentrate";
  const fallbackProduct = {
    title: "TOM FORD RESEARCH Eye Repair Concentrate",
    url: seedUrl,
    description_raw:
      "A moisturizing multi-tasker that visibly refreshes and depuffs the eye area while helping tired-looking eyes appear more awake.",
    details_sections: [
      {
        heading: "How to Use",
        body: "Pat gently around the eye area morning and night.",
        source_kind: "accordion_button",
        media_urls: ["https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf-eye-how-to-step.jpg?v=1"],
      },
      {
        heading: "Clinical Results",
        body: "Helps reduce the look of puffiness and refreshes tired-looking eyes.",
        source_kind: "heading_sibling",
        media_urls: ["https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf-eye-clinical.jpg?v=1"],
      },
    ],
    faq_items: [
      {
        question: "Can it be used day and night?",
        answer: "Yes. Apply morning and evening around the eye contour.",
        source_kind: "faq_linear_text",
        source_url: seedUrl,
        source_title: "FAQ",
      },
    ],
    content_image_urls: [
      "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf-eye-how-to-step.jpg?v=1",
      "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf-eye-clinical.jpg?v=1",
    ],
    image_url: "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1",
    image_urls: [
      "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1",
      "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_1.png?v=1",
      "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_2.png?v=1",
    ],
    variant_skus: ["TAGL01"],
    variants: [
      {
        id: "52015786131669",
        sku: "TAGL01",
        url: seedUrl,
        option_name: "Size",
        option_value: "15.0 ml",
        price: "295.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1",
        image_urls: [
          "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1",
          "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_1.png?v=1",
          "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_2.png?v=1",
        ],
        ad_copy: "",
      },
    ],
    field_sources: {
      description_raw: ["shopify_body_html"],
      details_sections: ["accordion_button", "heading_sibling"],
      ingredients_raw: [],
      active_ingredients_raw: [],
      how_to_use_raw: ["accordion_button"],
      faq_items: ["faq_linear_text"],
    },
  } as any;

  const response = {
    brand: "Tom Ford Beauty",
    domain: "https://www.tomfordbeauty.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "TOM FORD RESEARCH Eye Repair Concentrate",
        url: seedUrl,
        image_url: "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1",
        image_urls: ["https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1"],
        variant_skus: ["TAGL01"],
        variants: [
          {
            id: "52015786131669",
            sku: "TAGL01",
            url: seedUrl,
            option_name: "Size",
            option_value: "15.0 ml",
            price: "295.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1",
            image_urls: ["https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1"],
            ad_copy: "",
          },
        ],
        description_raw:
          "A moisturizing multi-tasker that instantly and effectively works to refresh, renew and depuff the eye area.",
        details_sections: [],
        faq_items: [],
        field_capture_status: {
          description_raw: "present" as const,
          details_sections: "missing" as const,
          ingredients_raw: "missing" as const,
          active_ingredients_raw: "missing" as const,
          how_to_use_raw: "missing" as const,
          faq_items: "missing" as const,
        },
        field_sources: {
          description_raw: ["shopify_body_html"],
          details_sections: [],
          ingredients_raw: [],
          active_ingredients_raw: [],
          how_to_use_raw: [],
          faq_items: [],
        },
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 295, max: 295, avg: 295 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "www.tomfordbeauty.com",
      resolved_base_url: "https://www.tomfordbeauty.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  await withMockFetch(
    {
      [seedUrl]: {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: `
          <html>
            <body>
              <section>
                <h2>How to Use</h2>
                <p>Pat gently around the eye area morning and night.</p>
              </section>
              <section>
                <h2>FAQ</h2>
                <p>Can it be used day and night?</p>
              </section>
              <img src="https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_0.png?v=1" />
              <img src="https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_1.png?v=1" />
              <img src="https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf_sku_TAGL01_2000x2000_2.png?v=1" />
              <img src="https://cdn.shopify.com/s/files/1/0761/9690/5173/files/tf-eye-how-to-step.jpg?v=1" />
            </body>
          </html>
        `,
      },
    },
    async () => {
      const result = await enrichDirectShopifyPdpResponse({
        brand: "Tom Ford Beauty",
        baseUrl: "https://www.tomfordbeauty.com",
        seedUrl,
        response,
        diagnostics: response.diagnostics,
        log: (type, msg) => logs.push({ type, msg }),
        browserRunner: async () => ({
          mode: "managed",
          result: fallbackProduct,
        }),
      });

      assert.equal(result.products[0]?.details_sections?.length, 2);
      assert.equal(result.products[0]?.faq_items?.length, 1);
      assert.deepEqual(result.products[0]?.content_image_urls, fallbackProduct.content_image_urls);
      assert.equal(result.products[0]?.image_urls?.length, 3);
      assert.equal(result.products[0]?.variants[0]?.image_urls?.length, 3);
      assert.match(
        logs.map((entry) => entry.msg).join("\n"),
        /requires browser enrichment for .*pdp_structured_sections.*pdp_faq.*pdp_content_images.*pdp_gallery_depth/i,
      );
    },
  );
});

test("productHasMissingPdpFields uses product type before requiring browser enrichment", () => {
  const makeProduct = (
    overrides: Partial<Parameters<typeof productHasMissingPdpFields>[0]> = {},
  ): Parameters<typeof productHasMissingPdpFields>[0] => ({
    title: "Untitled",
    url: "https://kyliecosmetics.com/products/untitled",
    image_url: "https://cdn.example.com/product.jpg",
    image_urls: ["https://cdn.example.com/product.jpg"],
    variant_skus: ["SKU-1"],
    variants: [
      {
        id: "1",
        sku: "SKU-1",
        url: "https://kyliecosmetics.com/products/untitled",
        option_name: "Variant",
        option_value: "Default",
        price: "20.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/product.jpg",
        image_urls: ["https://cdn.example.com/product.jpg"],
        ad_copy: "",
      },
    ],
    description_raw:
      "A concise merchant PDP description with enough product context to support a normalized listing without browser enrichment.",
    ...overrides,
  });

  assert.equal(
    productHasMissingPdpFields(
      makeProduct({
        title: "Chrome Makeup Bag",
        url: "https://kyliecosmetics.com/products/chrome-makeup-bag",
      }),
    ),
    false,
  );
  assert.deepEqual(
    getMissingPdpFieldReasons(
      makeProduct({
        title: "12 Days of Kylie Advent Calendar",
        url: "https://kyliecosmetics.com/products/kylie-advent-calendar-2025",
        description_raw: "",
      }),
    ),
    ["overview"],
  );
  assert.equal(
    productHasMissingPdpFields(
      makeProduct({
        title: "12 Days of Kylie Advent Calendar",
        url: "https://kyliecosmetics.com/products/kylie-advent-calendar-2025",
        details_sections: [
          {
            heading: "Details",
            body: "A limited-edition seasonal set with multiple beauty surprises.",
            source_kind: "shopify_body_html_labeled_sections",
          },
        ],
        description_raw: "",
      }),
    ),
    false,
  );
  assert.equal(
    productHasMissingPdpFields(
      makeProduct({
        title: "Cosmic Kylie Jenner 2.0 Eau de Parfum Deluxe Sample 1.2 ml",
        url: "https://kyliecosmetics.com/products/cosmic-kylie-jenner-2-0-eau-de-parfum-sample",
      }),
    ),
    false,
  );
  assert.equal(
    productHasMissingPdpFields(
      makeProduct({
        title: "Power Plush Longwear Foundation",
        url: "https://kyliecosmetics.com/products/power-plush-longwear-foundation",
        description_raw:
          "A longwear liquid foundation with medium buildable coverage and a soft matte finish for everyday makeup routines.",
      }),
    ),
    true,
  );
  assert.equal(
    productHasMissingPdpFields(
      makeProduct({
        title: "Power Plush Longwear Foundation",
        url: "https://kyliecosmetics.com/products/power-plush-longwear-foundation",
        description_raw:
          "A longwear liquid foundation with medium buildable coverage and a soft matte finish for everyday makeup routines.",
        ingredients_raw: "Water, Dimethicone, Iron Oxides, Titanium Dioxide.",
      }),
    ),
    false,
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "12 Days of Kylie Advent Calendar",
        url: "https://kyliecosmetics.com/products/kylie-advent-calendar-2025",
      }),
    ),
    "bundle",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Power Plush Longwear Foundation",
        url: "https://kyliecosmetics.com/products/power-plush-longwear-foundation",
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Rose Bloom Supple Kiss Lip Glaze Deluxe Sample",
        url: "https://kyliecosmetics.com/products/rose-bloom-supple-kiss-lip-glaze-deluxe-sample",
        description_raw:
          "Shop Kylie Cosmetics by Kylie Jenner, Kylie Jenner Fragrances and Kylie Skin featuring makeup, fragrance, and skincare.",
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Gloss Bomb Ice Cooling Lip Luminizer — Melon Chillz",
        url: "https://fentybeauty.com/products/gloss-bomb-ice-cooling-lip-luminizer-melon-chillz",
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Cherry Dub Triple Action AHA Body Scrub",
        url: "https://fentybeauty.com/products/cherry-dub-triple-action-aha-body-scrub",
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Gloss Bomb Universal Lip Luminizer: Arcane Collection — Kaboom",
        url: "https://fentybeauty.com/products/gloss-bomb-universal-lip-luminizer-kaboom",
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Shadowstix Longwear Eyeshadow Stick — Sip & Sparkle",
        url: "https://fentybeauty.com/products/shadowstix-longwear-eyeshadow-stick-sip-sparkle",
        variants: [
          {
            id: "64844",
            sku: "64844",
            url: "https://fentybeauty.com/products/shadowstix-longwear-eyeshadow-stick-sip-sparkle",
            option_name: "Color",
            option_value: "Sip & Sparkle",
            price: "20.80",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/product.jpg",
            image_urls: ["https://cdn.example.com/product.jpg"],
            ad_copy: "",
          },
        ],
      }),
    ),
    "single_formula",
  );
  assert.deepEqual(
    getMissingPdpFieldReasons(
      makeProduct({
        title: "Shadowstix Longwear Eyeshadow Stick — Sip & Sparkle",
        url: "https://fentybeauty.com/products/shadowstix-longwear-eyeshadow-stick-sip-sparkle",
      }),
    ),
    ["ingredients"],
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Acne Healing Dots",
        url: "https://peaceoutskincare.com/products/peace-out-acne-dots",
      }),
    ),
    "single_formula",
  );
  assert.deepEqual(
    getMissingPdpFieldReasons(
      makeProduct({
        title: "Acne Healing Dots",
        url: "https://peaceoutskincare.com/products/peace-out-acne-dots",
      }),
    ),
    ["how_to_use", "ingredients"],
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Overnight Spot Stickers",
        url: "https://pixibeauty.com/products/overnight-spot-stickers",
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Hair Pins",
        url: "https://kyliecosmetics.com/products/hair-pins",
        description_raw:
          "A set of polished hair pins packed in a seasonal cosmetics gift box.",
      }),
    ),
    "accessory",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Match My Energy Gloss Drip & Iced Latte Lip Liner",
        url: "https://kyliecosmetics.com/products/match-my-energy-gloss-drip-iced-latte-lip-liner",
      }),
    ),
    "bundle",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Build Your Own 5-Piece Lip Gloss Vault",
        url: "https://fentybeauty.com/products/build-your-own-5-piece-lip-gloss-vault",
        details_sections: [
          {
            heading: "Product Type",
            body: "Bundle Builder",
            source_kind: "shopify_product_tags",
          },
        ],
      }),
    ),
    "bundle",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Programme Intensif Lift Regard 360°",
        url: "https://patyka.com/products/programme-intensif-lift-regard-360",
      }),
    ),
    "bundle",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "PLAY Lip Shield SPF 30 Coconut",
        url: "https://supergoop.com/products/play-lip-shield-spf-30-coconut-1",
        details_sections: [
          {
            heading: "How to Use",
            body: "Use PLAY Lip Shield as part of your daily beauty routine. Apply generously and evenly.",
            source_kind: "accordion_control",
          },
        ],
      }),
    ),
    "single_formula",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Moroccanoil Treatment Original",
        url: "https://www.moroccanoil.com/products/moroccanoil-treatment-original",
        details_sections: [
          {
            heading: "How to Use",
            body: "Apply a small amount throughout damp hair, then comb through and style as usual.",
            source_kind: "accordion_control",
          },
          {
            heading: "Fragrance",
            body: "The formula includes the brand's signature scent.",
            source_kind: "accordion_control",
          },
        ],
      }),
    ),
    "single_formula",
  );
  assert.deepEqual(
    extractBundleComponents(
      makeProduct({
        title: "Programme Intensif Lift Regard 360°",
        url: "https://patyka.com/products/programme-intensif-lift-regard-360",
      }),
    ),
    [],
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Patchs Lift Regard 360°",
        url: "https://patyka.com/products/patch-lift-regard-360",
      }),
    ),
    "single_formula",
  );
  assert.deepEqual(
    getMissingPdpFieldReasons(
      makeProduct({
        title: "Patchs Lift Regard 360°",
        url: "https://patyka.com/products/patch-lift-regard-360",
      }),
    ),
    ["how_to_use", "ingredients"],
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "BODY RELAX - Soin Corps Relaxant 50 minutes [Cabine]",
        url: "https://patyka.com/products/body-relax",
      }),
    ),
    "general_merchandise",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Kylie's Vacay Look",
        url: "https://kyliecosmetics.com/products/vacay-glam-look",
      }),
    ),
    "bundle",
  );
  assert.equal(
    classifyExtractedProductKind(
      makeProduct({
        title: "Compact Mirror & Pouch",
        url: "https://kyliecosmetics.com/products/compact-mirror-and-pouch",
      }),
    ),
    "accessory",
  );
  assert.deepEqual(
    extractBundleComponents(
      makeProduct({
        title: "Glow Routine Set",
        url: "https://kyliecosmetics.com/products/glow-routine-set",
        details_sections: [
          {
            heading: "What's Inside",
            body: "Includes: Vanilla Milk Toner, Hyaluronic Acid Serum, Face Cream.",
            source_kind: "shopify_body_html_labeled_sections",
          },
        ],
      }),
    ).map((component) => component.name),
    ["Vanilla Milk Toner", "Hyaluronic Acid Serum", "Face Cream"],
  );
  assert.deepEqual(
    extractBundleComponents(
      makeProduct({
        title: "Bold Eye Essentials Bundle: Eyeshadow Stick, Gel Eyeliner + Mascara",
        url: "https://fentybeauty.com/products/bold-eye-essentials-bundle-eyeshadow-stick-gel-eyeliner-mascara",
        details_sections: [
          {
            heading: "Product Type",
            body: "Bundle",
            source_kind: "shopify_product_tags",
          },
        ],
      }),
    ).map((component) => component.name),
    ["Eyeshadow Stick", "Gel Eyeliner", "Mascara"],
  );
  assert.deepEqual(
    extractBundleComponents(
      makeProduct({
        title: "Build Your Own SPF Moisturizer + Foundation Bundle",
        url: "https://fentybeauty.com/products/build-your-own-spf-moisturizer-foundation-bundle",
        details_sections: [
          {
            heading: "Product Type",
            body: "Bundle",
            source_kind: "shopify_product_tags",
          },
        ],
      }),
    ).map((component) => component.name),
    ["SPF Moisturizer", "Foundation"],
  );
});

test("extractShopifyEmbeddedProductPayloadPdpFields unwraps nested DCART product payloads", () => {
  const fields = extractShopifyEmbeddedProductPayloadPdpFields([
    `
      window.DCART = ${JSON.stringify({
        product: {
          title: "Tranexamic Acid Serum",
          description: "<p>Brightening serum with tranexamic acid.</p>",
          featured_image: "//www.theinkeylist.com/cdn/shop/files/tranexamic-main.png?v=1",
          images: [
            "//www.theinkeylist.com/cdn/shop/files/tranexamic-main.png?v=1",
            "//www.theinkeylist.com/cdn/shop/files/tranexamic-side.png?v=1",
          ],
          media: [
            {
              src: "//www.theinkeylist.com/cdn/shop/files/tranexamic-how-to-use.png?v=1",
              alt: "How to use Tranexamic Acid Serum",
            },
          ],
        },
      })};
    `,
  ]);

  assert.equal(fields.descriptionRaw, "Brightening serum with tranexamic acid.");
  assert.deepEqual(fields.imageUrls, [
    "//www.theinkeylist.com/cdn/shop/files/tranexamic-main.png?v=1",
    "//www.theinkeylist.com/cdn/shop/files/tranexamic-side.png?v=1",
    "//www.theinkeylist.com/cdn/shop/files/tranexamic-how-to-use.png?v=1",
  ]);
});

test("extractShopifyProductJsonAttributeScriptsFromHtml reads Fenty product hero payloads", () => {
  const productJson = {
    title: "Cherry Dub BHA Toner",
    handle: "cherry-dub-bha-toner",
    description:
      "<p>This BHA-powered purifying toner helps unclog pores, brighten, hydrate and keep surface oil in check.</p>",
    shortDescription: "Heavy-hitting salicylates and triple cherry complex help purify without leaving skin dull.",
    tags: ["Benefits:Brightening", "Benefits:Hydrating", "Skin Type:Oily", "catalog-exclude"],
    media: [
      {
        src: "//fentybeauty.com/cdn/shop/files/cherry-benefits.jpg?v=1",
        alt: "Controls surface oil, brightens skin, unclogs pores without stripping.",
      },
      {
        src: "//fentybeauty.com/cdn/shop/files/cherry-ingredients.jpg?v=1",
        alt: "Made with triple cherry complex, BHAs, zinc PCA, aloe juice, and willow bark extract.",
      },
      {
        src: "//fentybeauty.com/cdn/shop/files/cherry-results.jpg?v=1",
        alt: "Improves skin clarity in 1 week in a 4-week clinical study on 52 people.",
      },
    ],
  };
  const scripts = extractShopifyProductJsonAttributeScriptsFromHtml(`
    <section is="product-hero" product-json="${encodeURIComponent(JSON.stringify(productJson))}"></section>
  `);
  const fields = extractShopifyEmbeddedProductPayloadPdpFields(scripts);

  assert.equal(scripts.length, 1);
  assert.match(fields.descriptionRaw || "", /BHA-powered purifying toner/);
  assert.deepEqual(
    Array.from(new Set((fields.detailsSections || []).map((section) => section.heading))).sort(),
    ["Benefits", "Clinical Results", "Key Ingredients", "Skin Type"].sort(),
  );
  assert.deepEqual(fields.imageUrls, [
    "//fentybeauty.com/cdn/shop/files/cherry-benefits.jpg?v=1",
    "//fentybeauty.com/cdn/shop/files/cherry-ingredients.jpg?v=1",
    "//fentybeauty.com/cdn/shop/files/cherry-results.jpg?v=1",
  ]);
});

test("enrichDirectShopifyPdpResponse merges embedded product-json before browser enrichment", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const seedUrl = "https://fentybeauty.com/products/hydra-vizor";
  const productJson = {
    title: "Hydra Vizor",
    handle: "hydra-vizor",
    description:
      "<p>A daily SPF moisturizer that hydrates and supports smoother-looking skin.</p><p>How to Use: Apply generously before sun exposure and reapply every two hours.</p><p>Full Ingredients: Water, Glycerin, Homosalate, Octisalate, Octocrylene, Avobenzone, Niacinamide, Dimethicone, Phenoxyethanol.</p>",
    tags: ["Benefits:Hydrating", "Sun Protection:SPF 30"],
    media: [
      {
        src: "//fentybeauty.com/cdn/shop/files/hydra.jpg?v=1",
        alt: "Made with niacinamide and Kalahari melon for hydration support.",
      },
    ],
  };
  const response = {
    brand: "Fenty Skin",
    domain: "https://fentybeauty.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Hydra Vizor",
        url: seedUrl,
        image_url: "",
        image_urls: [],
        variant_skus: ["FS-HYDRA"],
        variants: [
          {
            id: "1",
            sku: "FS-HYDRA",
            url: seedUrl,
            option_name: "Title",
            option_value: "Default Title",
            price: "45.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "",
            image_urls: [],
            ad_copy: "",
          },
        ],
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 45, max: 45, avg: 45 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "fentybeauty.com",
      resolved_base_url: "https://fentybeauty.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  await withMockFetch(
    {
      [seedUrl]: {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: `<section is="product-hero" product-json="${encodeURIComponent(JSON.stringify(productJson))}"></section>`,
      },
    },
    async () => {
      const result = await enrichDirectShopifyPdpResponse({
        brand: "Fenty Skin",
        baseUrl: "https://fentybeauty.com",
        seedUrl,
        response,
        diagnostics: response.diagnostics,
        log: (type, msg) => logs.push({ type, msg }),
        browserRunner: async () => {
          throw new Error("browser should not run");
        },
      });

      assert.match(result.products[0]?.description_raw || "", /daily SPF moisturizer/);
      assert.equal(result.products[0]?.how_to_use_raw, "Apply generously before sun exposure and reapply every two hours.");
      assert.match(result.products[0]?.ingredients_raw || "", /Homosalate/);
      assert.deepEqual(result.products[0]?.image_urls, ["https://fentybeauty.com/cdn/shop/files/hydra.jpg?v=1"]);
      assert.match(logs.map((entry) => `${entry.type}:${entry.msg}`).join("\n"), /Recovered Shopify PDP fields via embedded product-json/);
      assert.doesNotMatch(logs.map((entry) => entry.msg).join("\n"), /requires browser enrichment/);
    },
  );
});

test("enrichDirectShopifyPdpResponse upgrades generic single variants from HTML product volume before browser enrichment", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const seedUrl = "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask";
  const response = {
    brand: "Laneige",
    domain: "https://us.laneige.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Bouncy & Firm Eye Sleeping Mask",
        url: seedUrl,
        image_url: "https://cdn.example.com/bouncy-eye.jpg",
        image_urls: ["https://cdn.example.com/bouncy-eye.jpg"],
        variant_skus: ["270284032"],
        variants: [
          {
            id: "42052027940916",
            sku: "270284032",
            url: seedUrl,
            option_name: "Title",
            option_value: "Default Title",
            price: "32.00",
            currency: "USD",
            stock: "In Stock",
            description: "An overnight eye treatment.",
            image_url: "https://cdn.example.com/bouncy-eye.jpg",
            image_urls: ["https://cdn.example.com/bouncy-eye.jpg"],
            ad_copy: "",
            hidden_from_selector: true,
          },
        ],
        description_raw: "An overnight eye treatment.",
        details_sections: [
          {
            heading: "Benefits",
            body: "Hydration, PM",
            source_kind: "embedded_product_json_tags",
          },
        ],
        field_sources: {
          description_raw: ["shopify_description"],
          details_sections: ["embedded_product_json_tags"],
          ingredients_raw: [],
          active_ingredients_raw: [],
          how_to_use_raw: [],
          faq_items: [],
        },
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 32, max: 32, avg: 32 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "us.laneige.com",
      resolved_base_url: "https://us.laneige.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  await withMockFetch(
    {
      [seedUrl]: {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: `<html><body><span class="product__volume">(0.70 fl. oz./20 mL)</span></body></html>`,
      },
    },
    async () => {
      const result = await enrichDirectShopifyPdpResponse({
        brand: "Laneige",
        baseUrl: "https://us.laneige.com",
        seedUrl,
        response,
        diagnostics: response.diagnostics,
        log: (type, msg) => logs.push({ type, msg }),
        browserRunner: async () => {
          throw new Error("browser should not run");
        },
      });

      assert.equal(result.products[0]?.variants[0]?.option_name, "Size");
      assert.equal(result.products[0]?.variants[0]?.option_value, "20ml");
      assert.equal(result.products[0]?.variants[0]?.hidden_from_selector, false);
    },
  );
});

test("normalizeStructuredProseDetailSections preserves benefits while relabeling Anua prose results and how-to blocks", () => {
  const result = normalizeStructuredProseDetailSections([
    {
      heading: "SOOTHES REDNESS",
      body: "Designed to soothe, tone, and hydrate the skin.",
      source_kind: "page_section_stack_prose",
    },
    {
      heading: "BALANCES pH LEVEL OF SKIN",
      body: "Helps maintain the skin's moisture barrier.",
      source_kind: "page_section_stack_prose",
    },
    {
      heading: "Skin Temperature Decreased by 14.52%",
      body: "Clinical testing showed a soothing effect that supports the skin barrier.",
      source_kind: "page_section_stack_prose",
    },
    {
      heading: "Tone Up Your Routine",
      body: "01 Cleanse your skin.\n02 Apply toner evenly.",
      source_kind: "page_image_with_text_prose",
    },
  ]);

  assert.deepEqual(result.map((section) => section.heading), [
    "SOOTHES REDNESS",
    "BALANCES pH LEVEL OF SKIN",
    "Testing Results",
    "How to Use",
  ]);
});

test("normalizeStructuredProseDetailSections derives how-to from Anua image-with-text prose blocks", () => {
  const result = normalizeStructuredProseDetailSections([
    {
      heading: "Niacinamide",
      body: "Helps revitalize and revive dull-looking skin.",
      source_kind: "page_section_stack_prose",
    },
    {
      heading: "Tranexamic Acid (TXA)",
      body: "Targets the appearance of post-breakout marks.",
      source_kind: "page_section_stack_prose",
    },
    {
      heading: "How to use",
      body: "1. Apply to areas of concern after cleansing and leave on for 10–20 minutes like a mask.\n2. Gently swipe remaining essence.",
      source_kind: "page_image_with_text_prose",
    },
  ]);

  assert.deepEqual(result.map((section) => section.heading), [
    "Niacinamide",
    "Tranexamic Acid (TXA)",
    "How to Use",
  ]);
  const howToSection = result.find((section) => section.heading === "How to Use");
  assert.ok(howToSection);
  assert.equal(howToSection.source_kind, "page_image_with_text_prose");
  assert.match(howToSection.body, /leave on for 10–20 minutes/i);
});

test("extractLikelyFullIngredientListText isolates full INCI from mixed accordion copy", () => {
  const text = `
    2% Tranexamic Acid helps reduce the appearance of hyperpigmentation and dark spots.

    2% Acai Berry Extract helps soothe skin and visibly improve uneven tone.

    Aqua (Water), Propanediol, Tranexamic Acid, Glycerin, Butylene Glycol, Acai Fruit Extract, Phenoxyethanol, Xanthan Gum, Sodium Hydroxide
  `;

  assert.equal(
    extractLikelyFullIngredientListText(text),
    "Aqua (Water), Propanediol, Tranexamic Acid, Glycerin, Butylene Glycol, Acai Fruit Extract, Phenoxyethanol, Xanthan Gum, Sodium Hydroxide",
  );
});

test("PuppeteerExtractor honors locale-prefixed Shopify direct PDP seed URLs", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 151,
    title: "Detox Cleansing Foam",
    handle: "detox-cleansing-foam",
    body_html: "<p>Detox cleansing foam</p>",
    variants: [
      {
        id: 1501,
        sku: "P1126",
        title: "150 ml",
        option1: "150 ml",
        price: 1590,
        available: true,
        inventory_quantity: 18,
      },
    ],
    options: [{ name: "Size" }],
    images: [{ src: "https://cdn.example.com/patyka-detox-1.jpg" }],
  };

  await withMockFetch(
    {
      "https://patyka.com/products/detox-cleansing-foam.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://patyka.com/en-eu/products/detox-cleansing-foam": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="EUR"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Patyka",
        domain: "https://patyka.com/en-eu/products/detox-cleansing-foam",
        market: "US",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://patyka.com/products/detox-cleansing-foam");
      assert.deepEqual(result.products[0]?.variant_skus, ["P1126"]);
      assert.equal(result.variants[0]?.price, "15.90");
      assert.equal(result.variants[0]?.currency, "EUR");
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
    },
  );
});

test("PuppeteerExtractor honors singular /product Shopify direct PDP seed URLs", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 191,
    title: "Neroli Portofino Hand and Body Moisturizer",
    handle: "neroli-portofino-hand-and-body-moisturizer",
    body_html: "<p>Hand and body moisturizer</p>",
    variants: [
      {
        id: 1901,
        sku: "TF-NP-001",
        title: "Default Title",
        option1: "Default Title",
        price: 9500,
        available: true,
        inventory_quantity: 4,
      },
    ],
    options: [{ name: "Title" }],
    images: [{ src: "https://cdn.example.com/tomford-neroli-1.jpg" }],
  };

  await withMockFetch(
    {
      "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://www.tomfordbeauty.com/product/neroli-portofino-hand-and-body-moisturizer": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Tom Ford Beauty",
        domain: "https://www.tomfordbeauty.com/product/neroli-portofino-hand-and-body-moisturizer",
        market: "US",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer");
      assert.deepEqual(result.products[0]?.variant_skus, ["TF-NP-001"]);
      assert.equal(result.variants[0]?.price, "95.00");
      assert.equal(result.variants[0]?.currency, "USD");
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
      assert.equal(result.platform, "Shopify (Direct PDP)");
    },
  );
});

test("pickBestJsonLdObjectForPage prefers the Product object that matches the current locale page", () => {
  const selected = pickBestJsonLdObjectForPage({
    candidates: [
      {
        "@type": "Product",
        name: "Espuma Limpiadora Detoxificante",
        url: "https://patyka.com/es-ad/products/espuma-limpiadora-detoxificante",
        "@id": "https://patyka.com/es-ad/products/espuma-limpiadora-detoxificante#product",
      },
      {
        "@type": "Product",
        name: "Detox Cleansing Foam",
        url: "https://patyka.com/en-eu/products/detox-cleansing-foam?variant=10169393250340",
        "@id": "https://patyka.com/en-eu/products/detox-cleansing-foam#product",
      },
    ],
    pageUrl: "https://patyka.com/en-eu/products/detox-cleansing-foam",
    canonicalUrl: "https://patyka.com/en-eu/products/detox-cleansing-foam",
    baseUrl: "https://patyka.com",
  });

  assert.equal(selected?.name, "Detox Cleansing Foam");
});

test("PuppeteerExtractor supports string-based Shopify direct PDP image arrays", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 202,
    title: "Glow Getter Set",
    handle: "glow-getter-set",
    body_html: "<p>Glow set</p>",
    featured_image: "//cdn.shopify.com/s/files/1/1463/5858/files/AAV1_PJUL02_BundlesMinis_01_Ruby_BaseBrush.jpg?v=1752708261",
    variants: [
      {
        id: 2001,
        title: "Default Title",
        option1: "Default Title",
        price: 6200,
        available: true,
        inventory_quantity: 8,
        featured_image: null,
      },
    ],
    options: [{ name: "Title" }],
    images: [
      "//cdn.shopify.com/s/files/1/1463/5858/files/AAV1_PJUL02_BundlesMinis_01_Ruby_BaseBrush.jpg?v=1752708261",
      "//cdn.shopify.com/s/files/1/1463/5858/files/Pixi_Makeup_OTG_Base_June_2025_01.jpg?v=1773267435",
      "//cdn.shopify.com/s/files/1/1463/5858/files/Colour-Swatches-on-Arm-OTG-BASE-800x800-31JAN25.jpg?v=1773267573",
    ],
  };

  await withMockFetch(
    {
      "https://pixibeauty.com/products/glow-getter-set.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://pixibeauty.com/products/glow-getter-set": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Pixi",
        domain: "https://pixibeauty.com/products/glow-getter-set",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.deepEqual(result.products[0]?.image_urls, [
        "https://cdn.shopify.com/s/files/1/1463/5858/files/AAV1_PJUL02_BundlesMinis_01_Ruby_BaseBrush.jpg?v=1752708261",
        "https://cdn.shopify.com/s/files/1/1463/5858/files/Pixi_Makeup_OTG_Base_June_2025_01.jpg?v=1773267435",
        "https://cdn.shopify.com/s/files/1/1463/5858/files/Colour-Swatches-on-Arm-OTG-BASE-800x800-31JAN25.jpg?v=1773267573",
      ]);
      assert.deepEqual(result.variants[0]?.image_urls, result.products[0]?.image_urls);
      assert.equal(
        result.products[0]?.image_url,
        "https://cdn.shopify.com/s/files/1/1463/5858/files/AAV1_PJUL02_BundlesMinis_01_Ruby_BaseBrush.jpg?v=1752708261",
      );
      assert.equal(result.variants[0]?.price, "62.00");
      assert.equal(result.variants[0]?.currency, "USD");
    },
  );
});

test("PuppeteerExtractor collapses Shopify CDN size variants and rejects cross-product support images", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 8052355530952,
    title: "Supersize Omega Water Cream - 100ml",
    handle: "omega-water-cream-100ml",
    body_html:
      "<p>Oil-free hydration.</p><p>How to Use: Apply after cleansing.</p><p>Ingredients: Water, Betaine, Niacinamide.</p>",
    variants: [
      {
        id: 91001,
        sku: "INK-OMEGA-100",
        title: "Default Title",
        option1: "Default Title",
        price: 1500,
        available: true,
        inventory_quantity: 12,
      },
    ],
    options: [{ name: "Title" }],
    featured_image: "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
    images: [
      "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
      "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot_grande.png?v=1727108488",
      "https://www.theinkeylist.com/cdn/shop/files/Hero_026c776c-16f5-483e-861b-331dea8c6a2f.jpg?v=1730483258",
      "https://www.theinkeylist.com/cdn/shop/files/Omega100_How_to_use_OptB_2000x2000_030fc11d-00c4-4749-a1e5-91b55b0782d5.jpg?v=1759262701",
      "https://www.theinkeylist.com/cdn/shop/files/ROW_Hydrocolloid_Invisible_Pimple_Patches_key_Claims_2000x2000_5ddcb091-d228-43aa-9cca-97c96797f481.jpg?v=1759262701",
    ],
  };

  await withMockFetch(
    {
      "https://www.theinkeylist.com/products/omega-water-cream-100ml.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://www.theinkeylist.com/products/omega-water-cream-100ml": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "The Inkey List",
        domain: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
        market: "US",
        limit: 1,
      });

      assert.equal(result.products.length, 1);
      assert.deepEqual(result.products[0]?.image_urls, [
        "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot_grande.png?v=1727108488",
        "https://www.theinkeylist.com/cdn/shop/files/Hero_026c776c-16f5-483e-861b-331dea8c6a2f.jpg?v=1730483258",
        "https://www.theinkeylist.com/cdn/shop/files/Omega100_How_to_use_OptB_2000x2000_030fc11d-00c4-4749-a1e5-91b55b0782d5.jpg?v=1759262701",
      ]);
      assert.ok(
        !result.products[0]?.image_urls.some((url) => url.includes("Hydrocolloid_Invisible_Pimple_Patches")),
      );
      assert.equal(result.products[0]?.variants.length, 1);
      assert.equal(result.products[0]?.variants[0]?.hidden_from_selector, false);
    },
  );
});

test("PuppeteerExtractor does not fabricate template descriptions when Shopify direct PDP overview is missing", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 252,
    title: "Blush Brush",
    handle: "blush-brush",
    body_html: "",
    featured_image: null,
    variants: [
      {
        id: 2501,
        title: "Default Title",
        option1: "Default Title",
        price: 1800,
        available: true,
        inventory_quantity: 4,
      },
    ],
    options: [{ name: "Title" }],
    images: ["//cdn.shopify.com/s/files/1/1463/5858/files/Blush-Brush.jpg?v=1773267573"],
  };

  await withMockFetch(
    {
      "https://pixibeauty.com/products/blush-brush.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://pixibeauty.com/products/blush-brush": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><head><meta property=\"og:price:currency\" content=\"USD\"></head><body></body></html>",
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Pixi",
        domain: "https://pixibeauty.com/products/blush-brush",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.variants.length, 1);
      assert.equal(result.variants[0]?.description, "");
    },
  );
});

test("PuppeteerExtractor normalizes Shopify direct PDP cents and HTML currency hints", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 303,
    title: "Mousse Nettoyante Détox",
    handle: "mousse-nettoyante-detox",
    body_html: "<p>Detox foam cleanser</p>",
    variants: [
      {
        id: 3001,
        sku: "P1126",
        title: "150 ml",
        option1: "150 ml",
        price: 1590,
        available: true,
        inventory_quantity: 60,
      },
      {
        id: 3002,
        sku: "P6126",
        title: "50 ml",
        option1: "50 ml",
        price: 790,
        available: true,
        inventory_quantity: 60,
      },
    ],
    options: [{ name: "Size" }],
    images: [{ src: "https://cdn.example.com/patyka-1.jpg" }],
  };

  await withMockFetch(
    {
      "https://patyka.com/products/mousse-nettoyante-detox.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
      "https://patyka.com/products/mousse-nettoyante-detox": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: `
          <html>
            <head>
              <meta property="og:price:currency" content="EUR">
              <script>Shopify.currency = {"active":"EUR","rate":"1.0"};</script>
            </head>
            <body></body>
          </html>
        `,
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Patyka",
        domain: "https://patyka.com/products/mousse-nettoyante-detox",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.variants.length, 2);
      assert.equal(result.variants[0]?.price, "15.90");
      assert.equal(result.variants[0]?.currency, "EUR");
      assert.equal(result.variants[1]?.price, "7.90");
      assert.equal(result.variants[1]?.currency, "EUR");
      assert.equal(result.pricing.currency, "EUR");
      assert.equal(result.pricing.min, 7.9);
      assert.equal(result.pricing.max, 15.9);
    },
  );
});

test("mergeShopifyDirectPdpFallback fills Shopify direct PDP images from fallback scrape data", () => {
  const response = {
    brand: "Pixi",
    domain: "pixibeauty.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Glow Getter Set",
        url: "https://pixibeauty.com/products/glow-getter-set",
        image_url: "",
        image_urls: [],
        variant_skus: ["84357"],
        variants: [
          {
            id: "v1",
            sku: "84357",
            url: "https://pixibeauty.com/products/glow-getter-set",
            option_name: "Title",
            option_value: "Default Title",
            price: "62.00",
            currency: "USD",
            stock: "In Stock",
            description: "desc",
            image_url: "",
            image_urls: [],
            ad_copy: "copy",
          },
        ],
      },
    ],
    variants: [
      {
        id: "v1",
        sku: "84357",
        url: "https://pixibeauty.com/products/glow-getter-set",
        option_name: "Title",
        option_value: "Default Title",
        price: "62.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "",
        image_urls: [],
        ad_copy: "copy",
        brand: "Pixi",
        product_title: "Glow Getter Set",
        product_url: "https://pixibeauty.com/products/glow-getter-set",
        deep_link: "https://pixibeauty.com/products/glow-getter-set?variant=v1",
        simulated: false,
      },
    ],
    pricing: { currency: "USD" as const, min: 62, max: 62, avg: 62 },
    ad_copy: { by_variant_id: { v1: "copy" } },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "pixibeauty.com",
      resolved_base_url: "https://pixibeauty.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Glow Getter Set",
    url: "https://pixibeauty.com/products/glow-getter-set",
    image_url: "https://cdn.shopify.com/glow-getter-set-main.jpg",
    image_urls: [
      "https://cdn.shopify.com/glow-getter-set-main.jpg",
      "https://cdn.shopify.com/glow-getter-set-side.jpg",
    ],
    variant_skus: ["84357"],
    variants: [
      {
        id: "fallback-v1",
        sku: "84357",
        url: "https://pixibeauty.com/products/glow-getter-set",
        option_name: "Title",
        option_value: "Default Title",
        price: "62.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "https://cdn.shopify.com/glow-getter-set-main.jpg",
        image_urls: [
          "https://cdn.shopify.com/glow-getter-set-main.jpg",
          "https://cdn.shopify.com/glow-getter-set-side.jpg",
        ],
        ad_copy: "copy",
      },
    ],
  };

  const merged = mergeShopifyDirectPdpFallback("Pixi", response, fallbackProduct);

  assert.deepEqual(merged.products[0]?.image_urls, [
    "https://cdn.shopify.com/glow-getter-set-main.jpg",
    "https://cdn.shopify.com/glow-getter-set-side.jpg",
  ]);
  assert.equal(merged.products[0]?.image_url, "https://cdn.shopify.com/glow-getter-set-main.jpg");
  assert.deepEqual(merged.products[0]?.variants[0]?.image_urls, [
    "https://cdn.shopify.com/glow-getter-set-main.jpg",
    "https://cdn.shopify.com/glow-getter-set-side.jpg",
  ]);
  assert.equal(merged.variants[0]?.image_url, "https://cdn.shopify.com/glow-getter-set-main.jpg");
});

test("mergeShopifyDirectPdpFallback upgrades generic single-variant display fields from fallback scrape data", () => {
  const response = {
    brand: "Laneige",
    domain: "us.laneige.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Bouncy & Firm Eye Sleeping Mask",
        url: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
        image_url: "https://cdn.shopify.com/bouncy-eye-main.jpg",
        image_urls: ["https://cdn.shopify.com/bouncy-eye-main.jpg"],
        variant_skus: ["270284032"],
        variants: [
          {
            id: "v1",
            sku: "270284032",
            url: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
            option_name: "Title",
            option_value: "Default Title",
            price: "32.00",
            currency: "USD",
            stock: "In Stock",
            description: "desc",
            image_url: "https://cdn.shopify.com/bouncy-eye-main.jpg",
            image_urls: ["https://cdn.shopify.com/bouncy-eye-main.jpg"],
            ad_copy: "copy",
          },
        ],
      },
    ],
    variants: [
      {
        id: "v1",
        sku: "270284032",
        url: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
        option_name: "Title",
        option_value: "Default Title",
        price: "32.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "https://cdn.shopify.com/bouncy-eye-main.jpg",
        image_urls: ["https://cdn.shopify.com/bouncy-eye-main.jpg"],
        ad_copy: "copy",
        brand: "Laneige",
        product_title: "Bouncy & Firm Eye Sleeping Mask",
        product_url: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
        deep_link: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask?variant=v1",
        simulated: false,
      },
    ],
    pricing: { currency: "USD" as const, min: 32, max: 32, avg: 32 },
    ad_copy: { by_variant_id: { v1: "copy" } },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "us.laneige.com",
      resolved_base_url: "https://us.laneige.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Bouncy & Firm Eye Sleeping Mask",
    url: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
    image_url: "https://cdn.shopify.com/bouncy-eye-main.jpg",
    image_urls: ["https://cdn.shopify.com/bouncy-eye-main.jpg"],
    variant_skus: ["270284032"],
    variants: [
      {
        id: "fallback-v1",
        sku: "270284032",
        url: "https://us.laneige.com/products/bouncy-firm-eye-sleeping-mask",
        option_name: "Size",
        option_value: "20mL",
        price: "32.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "https://cdn.shopify.com/bouncy-eye-main.jpg",
        image_urls: ["https://cdn.shopify.com/bouncy-eye-main.jpg"],
        ad_copy: "copy",
      },
    ],
  };

  const merged = mergeShopifyDirectPdpFallback("Laneige", response, fallbackProduct);

  assert.equal(merged.products[0]?.variants[0]?.option_name, "Size");
  assert.equal(merged.products[0]?.variants[0]?.option_value, "20mL");
  assert.equal(merged.variants[0]?.option_name, "Size");
  assert.equal(merged.variants[0]?.option_value, "20mL");
});

test("mergeShopifyDirectPdpFallback rejects site-level meta descriptions", () => {
  const seedUrl = "https://tirtir.global/products/reflect-glow-prep-primer";
  const response = {
    brand: "TIRTIR",
    domain: "tirtir.global",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Reflect Glow Prep Primer",
        url: seedUrl,
        image_url: "https://cdn.example.com/primer.png",
        image_urls: ["https://cdn.example.com/primer.png"],
        variant_skus: ["TIRTIR-PRIMER"],
        variants: [
          {
            id: "v1",
            sku: "TIRTIR-PRIMER",
            url: seedUrl,
            option_name: "Title",
            option_value: "Default Title",
            price: "20.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/primer.png",
            image_urls: ["https://cdn.example.com/primer.png"],
            ad_copy: "copy",
          },
        ],
      },
    ],
    variants: [],
    pricing: { currency: "USD" as const, min: 20, max: 20, avg: 20 },
    ad_copy: { by_variant_id: {} },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "tirtir.global",
      resolved_base_url: "https://tirtir.global",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };
  const fallbackProduct = {
    title: "Reflect Glow Prep Primer",
    url: seedUrl,
    image_url: "https://cdn.example.com/primer.png",
    image_urls: ["https://cdn.example.com/primer.png"],
    variant_skus: ["TIRTIR-PRIMER"],
    description_raw:
      "Shop Top Selling Skincare Brand For Healthy, Glowing Skin Packed With High Performing Ingredients. Take Your Skincare Quiz & Find Your Perfect Skincare Routine to Find Skin Radiance.",
    variants: response.products[0]!.variants,
  };

  const merged = mergeShopifyDirectPdpFallback("TIRTIR", response, fallbackProduct);

  assert.equal(merged.products[0]?.description_raw, undefined);
  assert.equal(merged.products[0]?.field_capture_status?.description_raw, "missing");
});

test("mergeShopifyDirectPdpFallback discards unrelated fallback page images", () => {
  const response = {
    brand: "PATYKA",
    domain: "patyka.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Peeling Nuit Renovateur Eclat 10ml",
        url: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml",
        image_url: "",
        image_urls: [],
        variant_skus: ["PATYKA-PEELING"],
        variants: [
          {
            id: "v1",
            sku: "PATYKA-PEELING",
            url: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml",
            option_name: "Title",
            option_value: "Default Title",
            price: "0.00",
            currency: "USD",
            stock: "In Stock",
            description: "desc",
            image_url: "",
            image_urls: [],
            ad_copy: "copy",
          },
        ],
      },
    ],
    variants: [
      {
        id: "v1",
        sku: "PATYKA-PEELING",
        url: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml",
        option_name: "Title",
        option_value: "Default Title",
        price: "0.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "",
        image_urls: [],
        ad_copy: "copy",
        brand: "PATYKA",
        product_title: "Peeling Nuit Renovateur Eclat 10ml",
        product_url: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml",
        deep_link: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml?variant=v1",
        simulated: false,
      },
    ],
    pricing: { currency: "USD" as const, min: 0, max: 0, avg: 0 },
    ad_copy: { by_variant_id: { v1: "copy" } },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "patyka.com",
      resolved_base_url: "https://patyka.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Peeling Nuit Renovateur Eclat 10ml",
    url: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml",
    image_url: "https://patyka.com/cdn/shop/files/Header_Solaire-Teinte-Mobile-2.png?v=1",
    image_urls: [
      "https://patyka.com/cdn/shop/files/Header_Solaire-Teinte-Mobile-2.png?v=1",
      "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
      "https://patyka.com/cdn/shop/files/PATYKA_2025_institute_card.jpg?v=1",
    ],
    variant_skus: ["PATYKA-PEELING"],
    variants: [
      {
        id: "fallback-v1",
        sku: "PATYKA-PEELING",
        url: "https://patyka.com/products/peeling-nuit-renovateur-eclat-10ml",
        option_name: "Title",
        option_value: "Default Title",
        price: "0.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
        image_urls: [
          "https://patyka.com/cdn/shop/files/Header_Solaire-Teinte-Mobile-2.png?v=1",
          "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
          "https://patyka.com/cdn/shop/files/PATYKA_2025_institute_card.jpg?v=1",
        ],
        ad_copy: "copy",
      },
    ],
  };

  const merged = mergeShopifyDirectPdpFallback("PATYKA", response, fallbackProduct);

  assert.deepEqual(merged.products[0]?.image_urls, [
    "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
  ]);
  assert.equal(
    merged.products[0]?.image_url,
    "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
  );
  assert.deepEqual(merged.products[0]?.variants[0]?.image_urls, [
    "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
  ]);
  assert.equal(
    merged.variants[0]?.image_url,
    "https://patyka.com/cdn/shop/files/02-RechargePeeling-beauty.jpg?v=1",
  );
});

test("mergeShopifyDirectPdpFallback collapses Shopify CDN resized derivatives from browser enrichment", () => {
  const response = {
    brand: "The Inkey List",
    domain: "www.theinkeylist.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Supersize Omega Water Cream - 100ml",
        url: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
        image_url: "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
        image_urls: ["https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488"],
        variant_skus: ["INK-OMEGA-100"],
        variants: [
          {
            id: "v1",
            sku: "INK-OMEGA-100",
            url: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
            option_name: "Size",
            option_value: "100ml",
            price: "15.00",
            currency: "USD",
            stock: "In Stock",
            description: "desc",
            image_url: "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
            image_urls: ["https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488"],
            ad_copy: "copy",
          },
        ],
      },
    ],
    variants: [
      {
        id: "v1",
        sku: "INK-OMEGA-100",
        url: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
        option_name: "Size",
        option_value: "100ml",
        price: "15.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url: "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
        image_urls: ["https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488"],
        ad_copy: "copy",
        brand: "The Inkey List",
        product_title: "Supersize Omega Water Cream - 100ml",
        product_url: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
        deep_link: "https://www.theinkeylist.com/products/omega-water-cream-100ml?variant=v1",
        simulated: false,
      },
    ],
    pricing: { currency: "USD" as const, min: 15, max: 15, avg: 15 },
    ad_copy: { by_variant_id: { v1: "copy" } },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "www.theinkeylist.com",
      resolved_base_url: "https://www.theinkeylist.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Supersize Omega Water Cream - 100ml",
    url: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
    image_url:
      "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_300x_crop_center.png?v=1759406160",
    image_urls: [
      "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_300x_crop_center.png?v=1759406160",
      "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_600x_crop_center.png?v=1759406160",
      "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_1600x.png?v=1759406160",
      "https://www.theinkeylist.com/cdn/shop/files/03_KAFFYA_07_HOLDING_OMEGA_SUPERSIZE_001_300x_crop_center.jpg?v=1759406160",
      "https://www.theinkeylist.com/cdn/shop/files/03_KAFFYA_07_HOLDING_OMEGA_SUPERSIZE_001_1600x.jpg?v=1759406160",
    ],
    variant_skus: ["INK-OMEGA-100"],
    variants: [
      {
        id: "fallback-v1",
        sku: "INK-OMEGA-100",
        url: "https://www.theinkeylist.com/products/omega-water-cream-100ml",
        option_name: "Size",
        option_value: "100ml",
        price: "15.00",
        currency: "USD",
        stock: "In Stock",
        description: "desc",
        image_url:
          "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_300x_crop_center.png?v=1759406160",
        image_urls: [
          "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_300x_crop_center.png?v=1759406160",
          "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_3200x_crop_center.png?v=1759406160",
        ],
        ad_copy: "copy",
      },
    ],
  };

  const merged = mergeShopifyDirectPdpFallback("The Inkey List", response, fallbackProduct);

  assert.deepEqual(merged.products[0]?.image_urls, [
    "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
    "https://www.theinkeylist.com/cdn/shop/files/03_KAFFYA_07_HOLDING_OMEGA_SUPERSIZE_001_1600x.jpg?v=1759406160",
    "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_3200x_crop_center.png?v=1759406160",
  ]);
  assert.deepEqual(merged.products[0]?.variants[0]?.image_urls, [
    "https://www.theinkeylist.com/cdn/shop/files/OMEGASUPERSIZE_Packshot.png?v=1727108488",
    "https://www.theinkeylist.com/cdn/shop/files/03_KAFFYA_07_HOLDING_OMEGA_SUPERSIZE_001_1600x.jpg?v=1759406160",
    "https://www.theinkeylist.com/cdn/shop/files/Omega_Water_Cream_Size_Range_2000x2000_1a037cd9-3bfb-476f-b36e-0b30a39876e5_3200x_crop_center.png?v=1759406160",
  ]);
});

test("mergeShopifyDirectPdpFallback quarantines fallback PDP fields even when no new images are contributed", () => {
  const response = {
    brand: "Tom Ford Beauty",
    domain: "www.tomfordbeauty.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Neroli Portofino Hand and Body Moisturizer",
        url: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer",
        image_url: "https://cdn.example.com/tomford-neroli-1.jpg",
        image_urls: ["https://cdn.example.com/tomford-neroli-1.jpg"],
        variant_skus: ["TF-NP-001"],
        details_sections: [
          {
            heading: "Format",
            body: "Body Moisturizer",
            source_kind: "embedded_product_json_tags" as const,
          },
        ],
        variants: [
          {
            id: "v1",
            sku: "TF-NP-001",
            url: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer",
            option_name: "Title",
            option_value: "Default Title",
            price: "95.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/tomford-neroli-1.jpg",
            image_urls: ["https://cdn.example.com/tomford-neroli-1.jpg"],
            ad_copy: "copy",
          },
        ],
      },
    ],
    variants: [
      {
        id: "v1",
        sku: "TF-NP-001",
        url: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer",
        option_name: "Title",
        option_value: "Default Title",
        price: "95.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/tomford-neroli-1.jpg",
        image_urls: ["https://cdn.example.com/tomford-neroli-1.jpg"],
        ad_copy: "copy",
        brand: "Tom Ford Beauty",
        product_title: "Neroli Portofino Hand and Body Moisturizer",
        product_url: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer",
        deep_link: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer?variant=v1",
        simulated: false,
      },
    ],
    pricing: { currency: "USD" as const, min: 95, max: 95, avg: 95 },
    ad_copy: { by_variant_id: { v1: "copy" } },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "www.tomfordbeauty.com",
      resolved_base_url: "https://www.tomfordbeauty.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Neroli Portofino Hand and Body Moisturizer",
    url: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer",
    image_url: "",
    image_urls: [],
    variant_skus: ["TF-NP-001"],
    details_sections: [
      {
        heading: "Ingredients and Safety",
        body: "Ingredients: Water Aqua Eau, Glycerin, Panthenol.",
        source_kind: "details_summary" as const,
      },
      {
        heading: "How to Use",
        body: "Smooth into skin as needed.",
        source_kind: "details_summary" as const,
      },
    ],
    ingredients_raw: "Ingredients: Water Aqua Eau, Glycerin, Panthenol.",
    how_to_use_raw: "Smooth into skin as needed.",
    faq_items: [
      {
        question: "Can I use this every day?",
        answer: "Yes, smooth into skin as needed.",
        source_kind: "faq_section" as const,
      },
    ],
    variants: [
      {
        id: "fallback-v1",
        sku: "TF-NP-001",
        url: "https://www.tomfordbeauty.com/products/neroli-portofino-hand-and-body-moisturizer",
        option_name: "Title",
        option_value: "Default Title",
        price: "95.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "",
        image_urls: [],
        ad_copy: "copy",
      },
    ],
  };

  const merged = mergeShopifyDirectPdpFallback("Tom Ford Beauty", response, fallbackProduct);

  assert.equal(merged.products[0]?.ingredients_raw, undefined);
  assert.equal(merged.products[0]?.how_to_use_raw, undefined);
  assert.equal(merged.products[0]?.details_sections?.length, 1);
  assert.deepEqual(merged.products[0]?.details_sections?.map((section) => section.heading), ["Format"]);
  assert.equal(merged.products[0]?.faq_items, undefined);
  assert.equal(merged.products[0]?.field_quality_summary?.ingredients_raw?.source_quality_status, "quarantined");
  assert.equal(merged.products[0]?.field_quality_summary?.how_to_use_raw?.source_origin, "browser_fallback");
  assert.equal(merged.products[0]?.quarantined_pdp_fields?.details_sections?.length, 2);
  assert.equal(merged.products[0]?.quarantined_pdp_fields?.faq_items?.length, 1);
  assert.equal(merged.products[0]?.image_url, "https://cdn.example.com/tomford-neroli-1.jpg");
});

test("mergeShopifyDirectPdpFallback can preserve same-PDP browser field provenance for authoritative DOM recovery", () => {
  const response = {
    brand: "Anua",
    domain: "https://anua.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Heartleaf 77 Soothing Toner",
        description_raw: "A calming toner for sensitive skin.",
        details_sections: [],
        ingredients_raw: undefined,
        active_ingredients_raw: undefined,
        how_to_use_raw: undefined,
        faq_items: undefined,
        field_sources: {
          description_raw: ["shopify_description"],
          details_sections: [],
          ingredients_raw: [],
          active_ingredients_raw: [],
          how_to_use_raw: [],
          faq_items: [],
        },
        field_quality_summary: undefined,
        image_url: "https://cdn.example.com/anua-heartleaf77.jpg",
        image_urls: ["https://cdn.example.com/anua-heartleaf77.jpg"],
        title_override: undefined,
        price_amount: "23.00",
        price_currency: "USD" as const,
        availability: "In Stock" as const,
        url: "https://anua.com/products/heartleaf-77-soothing-toner",
        variant_skus: ["AA000043"],
        variants: [
          {
            id: "v1",
            sku: "AA000043",
            url: "https://anua.com/products/heartleaf-77-soothing-toner?variant=v1",
            option_name: "Size",
            option_value: "250ml",
            price: "23.00",
            currency: "USD" as const,
            stock: "In Stock" as const,
            description: "",
            image_url: "https://cdn.example.com/anua-heartleaf77.jpg",
            image_urls: ["https://cdn.example.com/anua-heartleaf77.jpg"],
            ad_copy: "",
          },
        ],
        simulated: false,
      },
    ],
    variants: [],
    pricing: { currency: "USD" as const, min: 23, max: 23, avg: 23 },
    ad_copy: { by_variant_id: {} },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "anua.com",
      resolved_base_url: "https://anua.com",
      discovery_strategy: "shopify_json" as const,
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const browserProduct = {
    title: "Heartleaf 77 Soothing Toner",
    url: "https://anua.com/products/heartleaf-77-soothing-toner",
    image_url: "https://cdn.example.com/anua-heartleaf77.jpg",
    image_urls: ["https://cdn.example.com/anua-heartleaf77.jpg"],
    variant_skus: ["AA000043"],
    details_sections: [
      {
        heading: "SOOTHES REDNESS",
        body: "Designed to soothe, tone, and hydrate the skin.",
        source_kind: "page_section_stack_prose" as const,
      },
      {
        heading: "How to Use",
        body: "01 Cleanse your skin. 02 Apply toner evenly.",
        source_kind: "page_image_with_text_prose" as const,
      },
      {
        heading: "Ingredients",
        body: "Water, Butylene Glycol, Houttuynia Cordata Extract.",
        source_kind: "details_summary" as const,
      },
    ],
    ingredients_raw: "Water, Butylene Glycol, Houttuynia Cordata Extract.",
    how_to_use_raw: "01 Cleanse your skin. 02 Apply toner evenly.",
    faq_items: [
      {
        question: "Can I use this every day?",
        answer: "Yes, use after cleansing.",
        source_kind: "faq_section" as const,
      },
    ],
    field_sources: {
      description_raw: [],
      details_sections: ["page_section_stack_prose", "page_image_with_text_prose", "details_summary"],
      ingredients_raw: ["details_summary"],
      active_ingredients_raw: [],
      how_to_use_raw: ["page_image_with_text_prose"],
      faq_items: ["faq_section"],
    },
    variants: [
      {
        id: "fallback-v1",
        sku: "AA000043",
        url: "https://anua.com/products/heartleaf-77-soothing-toner?variant=v1",
        option_name: "Size",
        option_value: "250ml",
        price: "23.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/anua-heartleaf77.jpg",
        image_urls: ["https://cdn.example.com/anua-heartleaf77.jpg"],
        ad_copy: "",
      },
    ],
  };

  const merged = mergeShopifyDirectPdpFallback("Anua", response, browserProduct, {
    preservePdpFieldSourceKinds: true,
  });

  assert.equal(merged.products[0]?.field_quality_summary?.details_sections?.source_origin, "retail_pdp");
  assert.equal(merged.products[0]?.field_quality_summary?.details_sections?.source_quality_status, "medium");
  assert.equal(merged.products[0]?.field_quality_summary?.ingredients_raw?.source_origin, "retail_pdp");
  assert.equal(merged.products[0]?.field_quality_summary?.how_to_use_raw?.source_origin, "retail_pdp");
  assert.equal(merged.products[0]?.field_quality_summary?.faq_items?.source_origin, "retail_pdp");
  assert.equal(merged.products[0]?.quarantined_pdp_fields?.details_sections?.length || 0, 0);
  assert.equal(merged.products[0]?.how_to_use_raw, "01 Cleanse your skin. 02 Apply toner evenly.");
  assert.deepEqual(merged.products[0]?.field_sources?.details_sections, [
    "page_section_stack_prose",
    "page_image_with_text_prose",
    "details_summary",
  ]);
});

test("choosePreferredProductOverview prefers expanded product details over short structured blurbs", () => {
  const overview = choosePreferredProductOverview({
    structured: "A 3-step regimen with Salicylic Acid 2% Solution for clearer skin",
    detailed:
      "The Acne Set offers a targeted skincare regimen featuring Salicylic Acid 2% Solution for treating acne.\n\nThis set includes...\n\nGlucoside Foaming Cleanser removes dirt and environmental impurities.\nSalicylic Acid 2% Solution exfoliates and helps clear pores.",
    meta: "Fight acne with The Acne Set.",
  });

  assert.equal(
    overview,
    "The Acne Set offers a targeted skincare regimen featuring Salicylic Acid 2% Solution for treating acne.\n\nThis set includes...\n\nGlucoside Foaming Cleanser removes dirt and environmental impurities.\nSalicylic Acid 2% Solution exfoliates and helps clear pores.",
  );
});
