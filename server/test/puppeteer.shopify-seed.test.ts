import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

import {
  PuppeteerExtractor,
  buildProductPdpFields,
  canReturnHtmlProductsWithoutBrowser,
  chooseDiscoveryBatchCandidates,
  choosePreferredProductOverview,
  deriveProductPdpModuleBodies,
  discoverProductUrls,
  enrichDirectShopifyPdpResponse,
  extractProductFromHtmlSnapshot,
  extractPaulasChoiceAppDataPdpFields,
  extractStaticHtmlPdpFallbackProduct,
  fetchHtmlViaNativeRequest,
  filterShopifyCatalogProducts,
  isNonProductShopifyFeedProduct,
  mergeShopifyDirectPdpFallback,
  pickBestJsonLdObjectForPage,
  resolveDirectPdpEnrichmentUrl,
} from "../src/services/extractors/puppeteer";

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
    const directRequest = requests.find((entry) => entry.url === "https://olehenriksen.com/products/henriksen-tote.js");
    assert.ok(directRequest);
    assert.match(directRequest!.cookie, /localization=US/i);
    assert.match(directRequest!.cookie, /cart_currency=USD/i);
    assert.match(directRequest!.acceptLanguage, /en-US/i);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("PuppeteerExtractor honors direct Shopify PDP seed URLs", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 101,
    title: "Banana Bright 15% Vitamin C Dark Spot Serum",
    handle: "banana-bright-vitamin-c-serum",
    body_html:
      "<p>Brightening serum</p><p>Ingredients: Aqua/Water, Vitamin C, Hyaluronic Acid.</p><p>How to Use: Apply evenly to face and neck daily.</p>",
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

test("filterShopifyCatalogProducts removes obvious gift and sample feed rows while keeping regular products", () => {
  const filtered = filterShopifyCatalogProducts([
    {
      id: 1,
      title: "Welcome Gift - Gift",
      handle: "welcome-gift-gift",
      body_html: "<p>Complimentary gift with purchase.</p>",
      variants: [],
    },
    {
      id: 2,
      title: "Smart Response Serum",
      handle: "smart-response-serum",
      body_html: "<p>Adaptive serum with peptides.</p>",
      variants: [],
    },
    {
      id: 3,
      title: "Deluxe Sample",
      handle: "deluxe-sample",
      body_html: "<p>Complimentary sample while supplies last.</p>",
      variants: [],
    },
    {
      id: 4,
      title: "Pro-Collagen Banking Serum - Gift",
      handle: "pro-collagen-banking-serum-gift",
      body_html: "<p>Powerful serum gift.</p>",
      variants: [
        {
          id: 401,
          title: "Default Title",
          option1: "Default Title",
          price: 0,
          available: true,
          inventory_quantity: 12,
        },
      ],
    },
    {
      id: 5,
      title: "Intensive Moisture Balance - Travel",
      handle: "intensive-moisture-balance-travel",
      body_html: "<p>Mini travel size.</p>",
      variants: [
        {
          id: 502,
          title: "Default Title",
          option1: "Default Title",
          price: 0,
          available: true,
          inventory_quantity: 12,
        },
      ],
    },
  ] as any);

  assert.deepEqual(
    filtered.map((product) => product.handle),
    ["smart-response-serum"],
  );
  assert.equal(
    isNonProductShopifyFeedProduct({
      id: 4,
      title: "Gift Card",
      handle: "gift-card",
      body_html: "",
      variants: [],
    } as any),
    true,
  );
  assert.equal(
    isNonProductShopifyFeedProduct({
      id: 6,
      title: "Pro-Collagen Banking Serum - Gift",
      handle: "pro-collagen-banking-serum-gift",
      body_html: "<p>Powerful serum gift.</p>",
      variants: [
        {
          id: 601,
          title: "Default Title",
          option1: "Default Title",
          price: 0,
          available: true,
          inventory_quantity: 10,
        },
      ],
    } as any),
    true,
  );
  assert.equal(
    isNonProductShopifyFeedProduct({
      id: 7,
      title: "Intensive Moisture Balance - Travel",
      handle: "intensive-moisture-balance-travel",
      body_html: "<p>Mini travel size.</p>",
      variants: [
        {
          id: 701,
          title: "Default Title",
          option1: "Default Title",
          price: 0,
          available: true,
          inventory_quantity: 10,
        },
      ],
    } as any),
    true,
  );
});

test("PuppeteerExtractor filters non-product Shopify feed rows before picking domain-root products", async () => {
  const extractor = new PuppeteerExtractor();

  await withMockFetch(
    {
      "https://www.dermalogica.com/products.json?limit=1": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          products: [
            {
              id: 1,
              title: "Welcome Gift - Gift",
              handle: "welcome-gift-gift",
              body_html: "<p>Complimentary gift with purchase.</p>",
              variants: [
                {
                  id: 101,
                  sku: "",
                  title: "Default Title",
                  option1: "Default Title",
                  price: 0,
                  available: true,
                  inventory_quantity: 999,
                },
              ],
              options: [{ name: "Title" }],
              images: [{ src: "https://cdn.example.com/welcome-gift.jpg" }],
            },
          ],
        }),
      },
      "https://www.dermalogica.com": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: '<html><head><meta property="og:price:currency" content="USD"></head><body></body></html>',
      },
      "https://www.dermalogica.com/products.json?limit=250&page=1": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          products: [
            {
              id: 1,
              title: "Welcome Gift - Gift",
              handle: "welcome-gift-gift",
              body_html: "<p>Complimentary gift with purchase.</p>",
              variants: [
                {
                  id: 101,
                  sku: "",
                  title: "Default Title",
                  option1: "Default Title",
                  price: 0,
                  available: true,
                  inventory_quantity: 999,
                },
              ],
              options: [{ name: "Title" }],
              images: [{ src: "https://cdn.example.com/welcome-gift.jpg" }],
            },
            {
              id: 2,
              title: "Smart Response Serum",
              handle: "smart-response-serum",
              body_html:
                "<p>Adaptive serum.</p><p>Ingredients: Aqua, Glycerin, Peptides.</p><p>How to Use: Apply after cleansing.</p>",
              variants: [
                {
                  id: 202,
                  sku: "DL-SRS-001",
                  title: "Default Title",
                  option1: "Default Title",
                  price: 16500,
                  available: true,
                  inventory_quantity: 15,
                },
              ],
              options: [{ name: "Title" }],
              images: [{ src: "https://cdn.example.com/smart-response-serum.jpg" }],
            },
          ],
        }),
      },
      "https://www.dermalogica.com/products.json?limit=250&page=2": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify({ products: [] }),
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Dermalogica",
        domain: "https://www.dermalogica.com",
        market: "US",
        limit: 1,
      });

      assert.equal(result.platform, "Shopify");
      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.title, "Smart Response Serum");
      assert.equal(result.products[0]?.url, "https://www.dermalogica.com/products/smart-response-serum");
      assert.match(result.products[0]?.ingredients_raw || "", /Peptides/i);
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
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

test("resolveDirectPdpEnrichmentUrl prefers canonical /products PDPs over stale singular /product seeds", () => {
  const resolved = resolveDirectPdpEnrichmentUrl({
    seedUrl: "https://www.tomfordbeauty.com/product/oud-wood-parfum?size=50_ml",
    productUrl: "https://www.tomfordbeauty.com/products/oud-wood-parfum",
    baseUrl: "https://www.tomfordbeauty.com",
  });

  assert.equal(resolved, "https://www.tomfordbeauty.com/products/oud-wood-parfum");
});

test("chooseDiscoveryBatchCandidates caps direct seed rediscovery windows to the top seed-affine candidates", () => {
  const selected = chooseDiscoveryBatchCandidates({
    productUrls: [
      "https://www.tomfordbeauty.com/products/architecture-radiance-hydrating-foundation-broad-spectrum-spf-50",
      "https://www.tomfordbeauty.com/products/shade-and-illuminate-concealer",
      "https://www.tomfordbeauty.com/products/shade-and-illuminate-contour-duo",
      "https://www.tomfordbeauty.com/products/architecture-soft-matte-blurring-foundation",
      "https://www.tomfordbeauty.com/products/brow-sculptor",
      "https://www.tomfordbeauty.com/products/ombre-leather-eau-de-parfum",
    ],
    offset: 0,
    limit: 10,
    reserve: 4,
    seedUrl: "https://www.tomfordbeauty.com/product/shade-and-illuminate-soft-radiance-foundation-spf-50?shade=11.0_Dusk",
    baseUrl: "https://www.tomfordbeauty.com",
  });

  assert.deepEqual(selected, [
    "https://www.tomfordbeauty.com/products/architecture-radiance-hydrating-foundation-broad-spectrum-spf-50",
    "https://www.tomfordbeauty.com/products/shade-and-illuminate-concealer",
    "https://www.tomfordbeauty.com/products/shade-and-illuminate-contour-duo",
    "https://www.tomfordbeauty.com/products/architecture-soft-matte-blurring-foundation",
  ]);
});

test("enrichDirectShopifyPdpResponse recovers PDP modules from native HTML before browser enrichment", async () => {
  const response = {
    brand: "NUXE",
    domain: "https://us.nuxe.com/products/face-cleansing-and-make-up-removing-gel",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Face Cleansing and Make-Up Removing Gel",
        url: "https://us.nuxe.com/products/face-cleansing-and-make-up-removing-gel",
        image_url: "https://cdn.example.com/nuxe-gel.jpg",
        image_urls: ["https://cdn.example.com/nuxe-gel.jpg"],
        variant_skus: ["NX9702910"],
        variants: [
          {
            id: "v1",
            sku: "NX9702910",
            url: "https://us.nuxe.com/products/face-cleansing-and-make-up-removing-gel",
            option_name: "Title",
            option_value: "Default Title",
            price: "21.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/nuxe-gel.jpg",
            image_urls: ["https://cdn.example.com/nuxe-gel.jpg"],
            ad_copy: "copy",
          },
        ],
        field_capture_status: {
          description_raw: "missing" as const,
          details_sections: "missing" as const,
          ingredients_raw: "missing" as const,
          active_ingredients_raw: "missing" as const,
          how_to_use_raw: "missing" as const,
        },
        field_sources: {
          description_raw: [],
          details_sections: [],
          ingredients_raw: [],
          active_ingredients_raw: [],
          how_to_use_raw: [],
        },
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 21, max: 21, avg: 21 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: 0,
      limit: 1,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: {
      requested_domain: "us.nuxe.com",
      resolved_base_url: "https://us.nuxe.com",
      discovery_strategy: "shopify_json",
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  let browserCalled = false;
  const merged = await enrichDirectShopifyPdpResponse({
    brand: "NUXE",
    baseUrl: "https://us.nuxe.com",
    seedUrl: "https://us.nuxe.com/products/face-cleansing-and-make-up-removing-gel",
    response,
    diagnostics: response.diagnostics,
    log: () => undefined,
    htmlFetcher: async () => ({
      status: 200,
      finalUrl: "https://us.nuxe.com/products/face-cleansing-and-make-up-removing-gel",
      body: `
        <html>
          <head>
            <meta name="description" content="Gentle cleansing gel for dry and sensitive skin.">
          </head>
          <body>
            <h1>Face Cleansing and Make-Up Removing Gel</h1>
            <details>
              <summary>Description</summary>
              <div class="accordion__content">
                <p>Gentle cleansing gel for dry and sensitive skin.</p>
              </div>
            </details>
            <details>
              <summary>Beauty tips</summary>
              <div class="accordion__content">
                <p>Apply to damp face, massage, then rinse.</p>
              </div>
            </details>
            <details>
              <summary>Formula</summary>
              <div class="accordion__content">
                <p>Ingredients: Aqua/Water, Glycerin, Honey Extract, Sunflower Seed Oil.</p>
              </div>
            </details>
          </body>
        </html>
      `,
    }),
    browserRunner: async () => {
      browserCalled = true;
      throw new Error("browser should not be called");
    },
  });

  assert.equal(browserCalled, false);
  assert.equal(merged.products[0]?.field_capture_status?.description_raw, "present");
  assert.equal(merged.products[0]?.field_capture_status?.details_sections, "present");
  assert.equal(merged.products[0]?.field_capture_status?.ingredients_raw, "present");
  assert.equal(merged.products[0]?.field_capture_status?.how_to_use_raw, "present");
  assert.match(merged.products[0]?.ingredients_raw || "", /Honey Extract/i);
  assert.match(merged.products[0]?.how_to_use_raw || "", /Apply to damp face/i);
});

test("enrichDirectShopifyPdpResponse merges Pixi-style direct PDP fallback modules", async () => {
  const logs: Array<{ type: string; msg: string }> = [];
  const response = {
    brand: "Pixi",
    domain: "https://pixibeauty.com/products/glow-tonic-250ml",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Glow Tonic Original Size",
        url: "https://pixibeauty.com/products/glow-tonic-250ml",
        image_url: "https://cdn.example.com/glow-tonic-1.jpg",
        image_urls: ["https://cdn.example.com/glow-tonic-1.jpg"],
        variant_skus: ["PIXI-GLOW-250"],
        variants: [
          {
            id: "v1",
            sku: "PIXI-GLOW-250",
            url: "https://pixibeauty.com/products/glow-tonic-250ml",
            option_name: "Size",
            option_value: "250 ml",
            price: "15.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/glow-tonic-1.jpg",
            image_urls: ["https://cdn.example.com/glow-tonic-1.jpg"],
            ad_copy: "",
          },
        ],
        ...buildProductPdpFields({}),
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 15, max: 15, avg: 15 },
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
      discovery_strategy: "shopify_json",
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Glow Tonic Original Size",
    url: "https://pixibeauty.com/products/glow-tonic-250ml",
    image_url: "https://cdn.example.com/glow-tonic-hero.jpg",
    image_urls: [
      "https://cdn.example.com/glow-tonic-hero.jpg",
      "https://cdn.example.com/glow-tonic-2.jpg",
    ],
    variant_skus: ["PIXI-GLOW-250"],
    variants: [
      {
        id: "v1",
        sku: "PIXI-GLOW-250",
        url: "https://pixibeauty.com/products/glow-tonic-250ml",
        option_name: "Size",
        option_value: "250 ml",
        price: "15.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/glow-tonic-hero.jpg",
        image_urls: ["https://cdn.example.com/glow-tonic-hero.jpg"],
        ad_copy: "",
      },
    ],
    ...buildProductPdpFields({
      descriptionRaw: "Our bestselling toner gently exfoliates to reveal brighter-looking skin.",
      detailsSections: [
        {
          heading: "How To Apply",
          body: "Use AM or PM after cleansing. Saturate a cotton pad and sweep across face.",
          source_kind: "accordion_button",
        },
        {
          heading: "Ingredients",
          body: "Aqua/Water/Eau, Aloe Barbadensis Leaf Juice, Glycolic Acid, Glycerin.",
          source_kind: "accordion_button",
        },
      ],
      ingredientsRaw: "Aqua/Water/Eau, Aloe Barbadensis Leaf Juice, Glycolic Acid, Glycerin.",
      howToUseRaw: "Use AM or PM after cleansing. Saturate a cotton pad and sweep across face.",
      fieldSources: {
        description_raw: ["structured_overview"],
        details_sections: ["accordion_button"],
        ingredients_raw: ["details_section_ingredients"],
        how_to_use_raw: ["details_section_how_to_use"],
      },
    }),
  };

  let result:
    | Awaited<ReturnType<typeof enrichDirectShopifyPdpResponse>>
    | undefined;
  await withMockFetch(
    {
      "https://pixibeauty.com/products/glow-tonic-250ml": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><head><title>Glow Tonic</title></head><body></body></html>",
      },
    },
    async () => {
      result = await enrichDirectShopifyPdpResponse({
        brand: "Pixi",
        baseUrl: "https://pixibeauty.com",
        seedUrl: "https://pixibeauty.com/products/glow-tonic-250ml",
        response,
        diagnostics: response.diagnostics,
        log: (type, msg) => logs.push({ type, msg }),
        browserRunner: async () => ({
          result: fallbackProduct,
          mode: "local",
        }),
      });
    },
  );

  assert.equal(result?.products[0]?.description_raw?.includes("bestselling toner"), true);
  assert.equal(result?.products[0]?.details_sections?.length, 2);
  assert.equal(result?.products[0]?.ingredients_raw?.includes("Glycolic Acid"), true);
  assert.equal(result?.products[0]?.how_to_use_raw?.includes("cotton pad"), true);
  assert.equal(result?.products[0]?.field_capture_status?.ingredients_raw, "present");
  assert.equal(result?.products[0]?.field_capture_status?.how_to_use_raw, "present");
  assert.match(logs.map((entry) => `${entry.type}:${entry.msg}`).join("\n"), /Attempting browser enrichment/);
});

test("mergeShopifyDirectPdpFallback preserves Fenty-style description and active ingredient sections", () => {
  const response = {
    brand: "Fenty Beauty",
    domain: "https://fentybeauty.com/products/butta-drop-hydrating-body-milk-vanilla-dream",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Butta Drop Hydrating Body Milk — Vanilla Dream",
        url: "https://fentybeauty.com/products/butta-drop-hydrating-body-milk-vanilla-dream",
        image_url: "https://cdn.example.com/butta-1.jpg",
        image_urls: ["https://cdn.example.com/butta-1.jpg"],
        variant_skus: ["FB-BUTTA-001"],
        variants: [
          {
            id: "v1",
            sku: "FB-BUTTA-001",
            url: "https://fentybeauty.com/products/butta-drop-hydrating-body-milk-vanilla-dream",
            option_name: "Size",
            option_value: "200 ml",
            price: "34.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "https://cdn.example.com/butta-1.jpg",
            image_urls: ["https://cdn.example.com/butta-1.jpg"],
            ad_copy: "",
          },
        ],
        ...buildProductPdpFields({}),
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 34, max: 34, avg: 34 },
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
      discovery_strategy: "shopify_json",
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Butta Drop Hydrating Body Milk — Vanilla Dream",
    url: "https://fentybeauty.com/products/butta-drop-hydrating-body-milk-vanilla-dream",
    image_url: "https://cdn.example.com/butta-hero.jpg",
    image_urls: ["https://cdn.example.com/butta-hero.jpg"],
    variant_skus: ["FB-BUTTA-001"],
    variants: [],
    ...buildProductPdpFields({
      descriptionRaw: "A rich body moisturizer that softens and replenishes dry skin.",
      detailsSections: [
        {
          heading: "Benefits",
          body: "Hydrating, moisturizing, and leaves skin looking healthy.",
          source_kind: "heading_sibling",
        },
        {
          heading: "Active Ingredients",
          body: "Barbados Cherry Extract, Coconut Oil.",
          source_kind: "heading_sibling",
        },
      ],
      activeIngredientsRaw: "Barbados Cherry Extract, Coconut Oil.",
      fieldSources: {
        description_raw: ["structured_overview"],
        details_sections: ["heading_sibling"],
        active_ingredients_raw: ["details_section_active_ingredients"],
      },
    }),
  };

  const merged = mergeShopifyDirectPdpFallback("Fenty Beauty", response, fallbackProduct);

  assert.equal(merged.products[0]?.description_raw?.includes("rich body moisturizer"), true);
  assert.equal(merged.products[0]?.details_sections?.length, 2);
  assert.equal(merged.products[0]?.active_ingredients_raw, "Barbados Cherry Extract, Coconut Oil.");
  assert.equal(merged.products[0]?.field_capture_status?.active_ingredients_raw, "present");
});

test("mergeShopifyDirectPdpFallback tolerates raw percent signs in Fenty titles", () => {
  const response = {
    brand: "Fenty Beauty",
    domain: "https://fentybeauty.com/products/mini-killawatt-freestyle-highlighter-wattab",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "Mini Killawatt Freestyle Highlighter — Wattab!*%#",
        url: "https://fentybeauty.com/products/mini-killawatt-freestyle-highlighter-wattab",
        image_url: "",
        image_urls: [],
        variant_skus: ["FB-KILLA-001"],
        variants: [
          {
            id: "v1",
            sku: "FB-KILLA-001",
            url: "https://fentybeauty.com/products/mini-killawatt-freestyle-highlighter-wattab",
            option_name: "Shade",
            option_value: "Wattab!*%#",
            price: "25.00",
            currency: "USD",
            stock: "In Stock",
            description: "",
            image_url: "",
            image_urls: [],
            ad_copy: "",
          },
        ],
        ...buildProductPdpFields({}),
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 25, max: 25, avg: 25 },
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
      discovery_strategy: "shopify_json",
      failure_category: null,
      block_provider: null,
      http_trace: [],
    },
  };

  const fallbackProduct = {
    title: "Mini Killawatt Freestyle Highlighter — Wattab!*%#",
    url: "https://fentybeauty.com/products/mini-killawatt-freestyle-highlighter-wattab",
    image_url: "https://cdn.example.com/fenty-wattab-hero.jpg?v=1",
    image_urls: ["https://cdn.example.com/fenty-wattab-hero.jpg?v=1"],
    variant_skus: ["FB-KILLA-001"],
    variants: [
      {
        id: "fv1",
        sku: "FB-KILLA-001",
        url: "https://fentybeauty.com/products/mini-killawatt-freestyle-highlighter-wattab",
        option_name: "Shade",
        option_value: "Wattab!*%#",
        price: "25.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/fenty-wattab-hero.jpg?v=1",
        image_urls: ["https://cdn.example.com/fenty-wattab-hero.jpg?v=1"],
        ad_copy: "",
      },
    ],
    ...buildProductPdpFields({
      descriptionRaw: "Travel-size shimmer highlighter.",
      detailsSections: [
        {
          heading: "Benefits",
          body: "Smooths and illuminates skin.",
          source_kind: "heading_sibling",
        },
      ],
    }),
  };

  const merged = mergeShopifyDirectPdpFallback("Fenty Beauty", response, fallbackProduct);

  assert.equal(merged.products[0]?.description_raw, "Travel-size shimmer highlighter.");
  assert.deepEqual(merged.products[0]?.image_urls, ["https://cdn.example.com/fenty-wattab-hero.jpg?v=1"]);
  assert.deepEqual(merged.products[0]?.variants[0]?.image_urls, ["https://cdn.example.com/fenty-wattab-hero.jpg?v=1"]);
});

test("extractStaticHtmlPdpFallbackProduct captures Fenty-style snapshot content and ingredients modal", () => {
  const fallback = extractStaticHtmlPdpFallbackProduct({
    brand: "Fenty Beauty",
    url: "https://fentybeauty.com/products/hydra-vizor",
    html: `
      <html>
        <head>
          <meta property="og:title" content="Hydra Vizor Invisible Moisturizer Broad Spectrum SPF 30 Sunscreen with Niacinamide + Kalahari Melon" />
          <meta name="description" content="Hydrating SPF moisturizer for daily wear." />
          <meta property="og:image" content="https://cdn.example.com/hydra-vizor.jpg" />
        </head>
        <body>
          <div class="product__content">
            <p>Instantly hydrates and improves the look of fine lines.</p>
            <p><strong>Fill Weight:</strong> 50 ml / 1.7 fl. oz.</p>
            <modal handle="productIngredients" title="Full ingredients" role="dialog">
              <div class="product-ingredients-modal__content OneLinkNoTx">
                <p><strong>ACTIVE INGREDIENTS:</strong> AVOBENZONE 3%, HOMOSALATE 9%</p>
                <p><strong>INACTIVE INGREDIENTS:</strong> WATER, GLYCERIN, NIACINAMIDE.</p>
              </div>
            </modal>
          </div>
        </body>
      </html>
    `,
  });

  assert.ok(fallback);
  assert.equal(fallback?.title.includes("Hydra Vizor"), true);
  assert.equal(fallback?.description_raw?.includes("Instantly hydrates"), true);
  assert.equal(fallback?.details_sections?.length, 2);
  assert.equal(fallback?.active_ingredients_raw?.includes("AVOBENZONE"), true);
  assert.equal(fallback?.ingredients_raw?.includes("WATER, GLYCERIN"), true);
  assert.deepEqual(fallback?.image_urls, ["https://cdn.example.com/hydra-vizor.jpg"]);
});

test("deriveProductPdpModuleBodies recognizes Bioderma-style Usage instructions and Composition headings", () => {
  const derived = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Usage instructions",
        body: "Morning and/or evening - 7 days a week. Soak a cotton pad with Sensibio H2O. No rinsing required.",
        source_kind: "accordion_button",
      },
      {
        heading: "Composition",
        body:
          "AQUA/WATER/EAU - PEG-6 CAPRYLIC/CAPRIC GLYCERIDES - FRUCTOOLIGOSACCHARIDES - MANNITOL - XYLITOL. The ingredients listed here are those contained in the most recent formulation of this product.",
        source_kind: "accordion_button",
      },
    ],
  });

  assert.match(derived.howToUseRaw || "", /Morning and\/or evening/i);
  assert.match(derived.ingredientsRaw || "", /AQUA\/WATER\/EAU/i);
});

test("extractStaticHtmlPdpFallbackProduct captures Bioderma-style accordions", () => {
  const fallback = extractStaticHtmlPdpFallbackProduct({
    brand: "Bioderma",
    url: "https://www.bioderma.us/en/p/sensibio-h2o-micellar-water.html",
    html: `
      <html>
        <head>
          <meta property="og:title" content="Sensibio H2O Micellar Water" />
          <meta name="description" content="Micellar water for sensitive skin." />
        </head>
        <body>
          <div class="product-infos__accordion__category accordion">
            <h2>
              <button class="product-infos__accordion__category__title accordion-trigger">
                Usage instructions
              </button>
            </h2>
            <div class="collapse d-flex flex-column gap--16 body--m">
              <div class="bold"><p>Morning and/or evening - 7 days a week</p></div>
              <ol>
                <li>Soak a cotton pad with Sensibio H2O.</li>
                <li>No rinsing required.</li>
              </ol>
            </div>
          </div>
          <div class="product-infos__accordion__category accordion">
            <h2>
              <button class="product-infos__accordion__category__title accordion-trigger">
                Composition
              </button>
            </h2>
            <div class="collapse d-flex flex-column gap--16">
              <p>AQUA/WATER/EAU - PEG-6 CAPRYLIC/CAPRIC GLYCERIDES - FRUCTOOLIGOSACCHARIDES - MANNITOL - XYLITOL</p>
              <p>The ingredients listed here are those contained in the most recent formulation of this product.</p>
            </div>
          </div>
        </body>
      </html>
    `,
  });

  assert.ok(fallback);
  assert.equal(fallback?.field_capture_status.how_to_use_raw, "present");
  assert.equal(fallback?.field_capture_status.ingredients_raw, "present");
  assert.match(fallback?.how_to_use_raw || "", /Soak a cotton pad/i);
  assert.match(fallback?.ingredients_raw || "", /AQUA\/WATER\/EAU/i);
  assert.ok((fallback?.details_sections || []).some((section) => /Usage instructions/i.test(section.heading)));
  assert.ok((fallback?.details_sections || []).some((section) => /Composition/i.test(section.heading)));
});

test("fetchHtmlViaNativeRequest follows direct PDP redirects and preserves final URL", async () => {
  const server = http.createServer((req, res) => {
    if (req.url === "/products/fragrance-free-seed") {
      res.statusCode = 301;
      res.setHeader("Location", "/products/hydra-vizor-invisible-moisturizer-us");
      res.end();
      return;
    }

    if (req.url === "/products/hydra-vizor-invisible-moisturizer-us") {
      const port = (server.address() as any).port;
      res.statusCode = 200;
      res.setHeader("Content-Type", "text/html; charset=utf-8");
      res.end(`<!DOCTYPE html>
        <html lang="en">
          <head>
            <link rel="canonical" href="http://127.0.0.1:${port}/products/hydra-vizor-invisible-moisturizer-us" />
            <title>Hydra Vizor Invisible Moisturizer</title>
          </head>
          <body>
            <h1>Hydra Vizor Invisible Moisturizer</h1>
          </body>
        </html>`);
      return;
    }

    res.statusCode = 404;
    res.end("not found");
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as any).port;

  try {
    const diagnostics = {
      requested_domain: "127.0.0.1",
      resolved_base_url: `http://127.0.0.1:${port}`,
      discovery_strategy: null,
      failure_category: null,
      block_provider: null,
      http_trace: [] as Array<{ url: string; status: number | null }>,
    };
    const result = await fetchHtmlViaNativeRequest(
      `http://127.0.0.1:${port}/products/fragrance-free-seed`,
      diagnostics as any,
    );

    assert.equal(result.status, 200);
    assert.equal(
      result.finalUrl,
      `http://127.0.0.1:${port}/products/hydra-vizor-invisible-moisturizer-us`,
    );
    assert.match(result.body || "", /Hydra Vizor Invisible Moisturizer/);
    assert.deepEqual(diagnostics.http_trace.map((entry) => entry.status), [301, 200]);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
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

test("discoverProductUrls re-discovers Tom Ford foundation PDPs when stale /product seeds redirect to makeup collections", async () => {
  await withMockFetch(
    {
      "https://www.tomfordbeauty.com/products/shade-and-illuminate-soft-radiance-foundation-spf-50.js": {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: "{}",
      },
      "https://www.tomfordbeauty.com/product/shade-and-illuminate-soft-radiance-foundation-spf-50?shade=12.5_Walnut": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        responseUrl: "https://www.tomfordbeauty.com/collections/makeup",
        body: `
          <html>
            <head><title>Makeup</title></head>
            <body>
              <h1>Makeup</h1>
              <a href="/products/ombre-leather-parfum">Ombre Leather Parfum</a>
              <a href="/products/shade-and-illuminate-soft-radiance-foundation-spf-50">Soft Radiance Foundation SPF 50</a>
            </body>
          </html>
        `,
      },
    },
    async () => {
      const result = await discoverProductUrls({
        baseUrl: "https://www.tomfordbeauty.com",
        seedUrl: "https://www.tomfordbeauty.com/product/shade-and-illuminate-soft-radiance-foundation-spf-50?shade=12.5_Walnut",
        maxProducts: 5,
        log: () => undefined,
      });

      assert.deepEqual(result.productUrls.slice(0, 2), [
        "https://www.tomfordbeauty.com/products/shade-and-illuminate-soft-radiance-foundation-spf-50",
        "https://www.tomfordbeauty.com/products/ombre-leather-parfum",
      ]);
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

test("mergeShopifyDirectPdpFallback preserves fallback PDP fields even when no new images are contributed", () => {
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

  assert.equal(merged.products[0]?.ingredients_raw, "Ingredients: Water Aqua Eau, Glycerin, Panthenol.");
  assert.equal(merged.products[0]?.how_to_use_raw, "Smooth into skin as needed.");
  assert.equal(merged.products[0]?.details_sections?.length, 2);
  assert.equal(merged.products[0]?.image_url, "https://cdn.example.com/tomford-neroli-1.jpg");
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

test("extractProductFromHtmlSnapshot parses The Ordinary ingredients and usage without a browser", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>Niacinamide 10% + Zinc 1% Serum</title>
          <meta property="og:price:amount" content="6.50">
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Product",
              "name": "Niacinamide 10% + Zinc 1% Serum",
              "url": "https://theordinary.com/en-us/niacinamide-10-zinc-1-serum-100436.html",
              "description": "A universal serum for blemish-prone skin that smooths, brightens, and supports.",
              "image": ["https://theordinary.com/images/niacinamide.jpg"],
              "offers": {
                "@type": "Offer",
                "price": "6.50",
                "priceCurrency": "USD",
                "availability": "https://schema.org/InStock"
              }
            }
          </script>
        </head>
        <body>
          <h1>Niacinamide 10% + Zinc 1% Serum</h1>
          <div class="active-ingredient-flyout-root">
            <aside class="active-ingredient-flyout">
              <div class="title">Ingredients</div>
              <p class="ingredients-flyout-content">Aqua (Water), Niacinamide, Pentylene Glycol, Zinc PCA.</p>
            </aside>
          </div>
          <div class="directions-overview-flyout-container-root">
            <div class="product-flyout-content">
              <p class="title">How to Use</p>
              <div class="product-flyout-directions-list">
                <ul><li>Apply a few drops to the face in the morning and evening.</li></ul>
              </div>
            </div>
          </div>
          <div class="product-info-description">
            <div class="description">A universal serum for blemish-prone skin that smooths, brightens, and supports.</div>
          </div>
        </body>
      </html>
    `,
    url: "https://theordinary.com/en-us/niacinamide-10-zinc-1-serum-100436.html",
    baseUrl: "https://theordinary.com",
  });

  assert.ok(product);
  assert.equal(product?.title, "Niacinamide 10% + Zinc 1% Serum");
  assert.match(product?.ingredients_raw || "", /Niacinamide/i);
  assert.match(product?.how_to_use_raw || "", /Apply a few drops/i);
  assert.equal(product?.field_capture_status?.description_raw, "present");
  assert.equal(product?.field_capture_status?.details_sections, "present");
});

test("extractProductFromHtmlSnapshot parses Jurlique ingredient accordions and key ingredients", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>Radiant Skin Foaming Cleanser</title>
          <meta property="og:price:amount" content="34.00">
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Product",
              "name": "Radiant Skin Foaming Cleanser",
              "url": "https://www.jurlique.com/products/radiant-skin-foam-cleanser",
              "description": "This foaming cleanser delivers a powerful clean without drying."
            }
          </script>
        </head>
        <body>
          <h1>Radiant Skin Foaming Cleanser</h1>
          <div class="product-accordion ingredients_list">
            <div class="product-accordion-header"><h2>ingredients List</h2></div>
            <div class="accordion-panel">
              <p>Aqua (Water), Glycerin, Decyl Glucoside, Rosa canina Fruit Oil, Calendula officinalis Flower Extract.</p>
            </div>
          </div>
          <div class="description-container">
            <h2 class="description-header">How to Use</h2>
            <div class="description-body">
              Gently lather a small amount between damp hands and massage over face.
            </div>
          </div>
          <div class="product-ingredient-section">
            <div class="product-key-ingredients" id="key-ingredients">
              <div class="child-ingredient">
                <div class="name">Calendula</div>
                <div class="description">Calendula extracts provide soothing, environmental protection and moisturisation to the skin.</div>
              </div>
              <div class="child-ingredient">
                <div class="name">Lemon Balm</div>
                <div class="description">Lemon Balm has a refreshing effect on the skin.</div>
              </div>
            </div>
          </div>
        </body>
      </html>
    `,
    url: "https://www.jurlique.com/products/radiant-skin-foam-cleanser",
    baseUrl: "https://www.jurlique.com",
  });

  assert.ok(product);
  assert.match(product?.ingredients_raw || "", /Aqua \(Water\)/i);
  assert.match(product?.active_ingredients_raw || "", /Calendula/i);
  assert.match(product?.how_to_use_raw || "", /Gently lather/i);
  assert.equal(product?.field_capture_status?.ingredients_raw, "present");
  assert.equal(product?.field_capture_status?.active_ingredients_raw, "present");
  assert.equal(product?.field_capture_status?.how_to_use_raw, "present");
});

test("extractProductFromHtmlSnapshot normalizes HOW TO accordions from Shopify HTML snapshots", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>BeamCream Smoothing Body Moisturizer</title>
          <meta property="og:price:amount" content="38.00">
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Product",
              "name": "BeamCream Smoothing Body Moisturizer",
              "url": "https://olehenriksen.com/products/beamcream-smoothing-body-moisturizer",
              "description": "Finally, a face-worthy body cream."
            }
          </script>
        </head>
        <body>
          <h1>BeamCream Smoothing Body Moisturizer</h1>
          <div class="product-form__accordion" data-accordion-item>
            <h2><button aria-label="HOW TO">HOW TO</button></h2>
            <div class="accordion__content rte p4">
              <p>Once a day, apply generously to body after cleansing &amp; massage into skin.</p>
            </div>
          </div>
          <div class="product-form__accordion" data-accordion-item>
            <h2><button aria-label="INGREDIENTS">INGREDIENTS</button></h2>
            <div class="accordion__content rte p4">
              <p><strong>Full Ingredients List:</strong> AQUA/WATER/EAU, GLYCERIN.</p>
            </div>
          </div>
        </body>
      </html>
    `,
    url: "https://olehenriksen.com/products/beamcream-smoothing-body-moisturizer",
    baseUrl: "https://olehenriksen.com",
  });

  assert.ok(product);
  assert.match(product?.ingredients_raw || "", /AQUA\/WATER\/EAU/i);
  assert.match(product?.how_to_use_raw || "", /apply generously to body/i);
  assert.ok(
    (product?.details_sections || []).some(
      (section) => section.heading === "How to Use" && /apply generously/i.test(section.body),
    ),
  );
});

test("PuppeteerExtractor returns a generic product from static HTML without launching a browser", async () => {
  const server = http.createServer((req, res) => {
    const url = req.url || "/";

    if (url === "/products.json?limit=1") {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not_found" }));
      return;
    }

    if (url === "/niacinamide-10-zinc-1-serum-100436.html") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(`
        <html>
          <head>
            <title>Niacinamide 10% + Zinc 1% Serum</title>
            <meta property="og:price:amount" content="6.50">
            <meta property="og:image" content="http://127.0.0.1/static/niacinamide.jpg">
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "Product",
                "name": "Niacinamide 10% + Zinc 1% Serum",
                "url": "http://127.0.0.1:${(server.address() as any)?.port || 0}/niacinamide-10-zinc-1-serum-100436.html",
                "description": "A universal serum for blemish-prone skin that smooths, brightens, and supports.",
                "image": ["http://127.0.0.1/static/niacinamide.jpg"],
                "offers": {
                  "@type": "Offer",
                  "price": "6.50",
                  "priceCurrency": "USD",
                  "availability": "https://schema.org/InStock"
                }
              }
            </script>
          </head>
          <body>
            <h1>Niacinamide 10% + Zinc 1% Serum</h1>
            <div class="title">Ingredients</div>
            <p class="ingredients-flyout-content">Aqua (Water), Niacinamide, Pentylene Glycol, Zinc PCA.</p>
            <div class="product-flyout-content">
              <p class="title">How to Use</p>
              <div class="product-flyout-directions-list">
                <ul><li>Apply a few drops to the face in the morning and evening.</li></ul>
              </div>
            </div>
            <div class="product-info-description">
              <div class="description">A universal serum for blemish-prone skin that smooths, brightens, and supports.</div>
            </div>
          </body>
        </html>
      `);
      return;
    }

    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const port = address.port;

  try {
    const extractor = new PuppeteerExtractor();
    const result = await extractor.extract({
      brand: "The Ordinary",
      domain: `http://127.0.0.1:${port}/niacinamide-10-zinc-1-serum-100436.html`,
      market: "US",
      limit: 5,
    });

    assert.equal(result.platform, "Generic Website");
    assert.equal(result.products.length, 1);
    assert.match(result.products[0]?.ingredients_raw || "", /Niacinamide/i);
    assert.match(result.products[0]?.how_to_use_raw || "", /Apply a few drops/i);
    assert.equal(result.products[0]?.field_capture_status?.description_raw, "present");
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});

test("canReturnHtmlProductsWithoutBrowser rejects HTML-only products that still need PDP enrichment", () => {
  const incompleteProduct = {
    title: "Abeille Royale Youth Repair Eye Care",
    url: "https://www.guerlain.com/us/en-us/p/abeille-royale-youth-repair-eye-care-P062209.html",
    image_url: "https://cdn.example.com/guerlain-eye.jpg",
    image_urls: ["https://cdn.example.com/guerlain-eye.jpg"],
    variant_skus: ["GR-062209"],
    variants: [
      {
        id: "1",
        sku: "GR-062209",
        url: "https://www.guerlain.com/us/en-us/p/abeille-royale-youth-repair-eye-care-P062209.html",
        option_name: "Size",
        option_value: "15 ml",
        price: "145.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/guerlain-eye.jpg",
        image_urls: ["https://cdn.example.com/guerlain-eye.jpg"],
        ad_copy: "",
      },
    ],
    description_raw: "",
    details_sections: [
      {
        heading: "Key Ingredients",
        body: "Black Bee Honey: Helps support skin repair.",
        source_kind: "guerlain_ingredients_carousel",
      },
    ],
    active_ingredients_raw: "Black Bee Honey: Helps support skin repair.",
    field_capture_status: {
      description_raw: "missing",
      details_sections: "present",
      ingredients_raw: "missing",
      active_ingredients_raw: "present",
      how_to_use_raw: "missing",
    },
    field_sources: {
      description_raw: [],
      details_sections: ["guerlain_ingredients_carousel"],
      ingredients_raw: [],
      active_ingredients_raw: ["guerlain_ingredients_carousel"],
      how_to_use_raw: [],
    },
  };

  assert.equal(
    canReturnHtmlProductsWithoutBrowser({
      products: [incompleteProduct as any],
      candidateCount: 1,
    }),
    false,
  );
});

test("canReturnHtmlProductsWithoutBrowser accepts HTML-only products once PDP fields are complete", () => {
  const completeProduct = {
    title: "Abeille Royale Youth Watery Oil Serum",
    url: "https://www.guerlain.com/us/en-us/p/abeille-royale-youth-watery-oil-serum-P062033.html",
    image_url: "https://cdn.example.com/guerlain-serum.jpg",
    image_urls: ["https://cdn.example.com/guerlain-serum.jpg"],
    variant_skus: ["GR-062033"],
    variants: [
      {
        id: "1",
        sku: "GR-062033",
        url: "https://www.guerlain.com/us/en-us/p/abeille-royale-youth-watery-oil-serum-P062033.html",
        option_name: "Size",
        option_value: "50 ml",
        price: "165.00",
        currency: "USD",
        stock: "In Stock",
        description: "",
        image_url: "https://cdn.example.com/guerlain-serum.jpg",
        image_urls: ["https://cdn.example.com/guerlain-serum.jpg"],
        ad_copy: "",
      },
    ],
    description_raw: "A replenishing serum powered by honey actives.",
    details_sections: [
      {
        heading: "Ingredients",
        body: "Aqua (Water), Glycerin, Squalane, Parfum (Fragrance).",
        source_kind: "guerlain_ingredients_modal",
      },
      {
        heading: "Key Ingredients",
        body: "Black Bee Honey: Helps support skin repair.",
        source_kind: "guerlain_ingredients_carousel",
      },
    ],
    ingredients_raw: "Aqua (Water), Glycerin, Squalane, Parfum (Fragrance).",
    active_ingredients_raw: "Black Bee Honey: Helps support skin repair.",
    field_capture_status: {
      description_raw: "present",
      details_sections: "present",
      ingredients_raw: "present",
      active_ingredients_raw: "present",
      how_to_use_raw: "missing",
    },
    field_sources: {
      description_raw: ["page_product_details"],
      details_sections: ["guerlain_ingredients_modal", "guerlain_ingredients_carousel"],
      ingredients_raw: ["guerlain_ingredients_modal"],
      active_ingredients_raw: ["guerlain_ingredients_carousel"],
      how_to_use_raw: [],
    },
  };

  assert.equal(
    canReturnHtmlProductsWithoutBrowser({
      products: [completeProduct as any],
      candidateCount: 1,
    }),
    true,
  );
});

test("extractPaulasChoiceAppDataPdpFields recovers product modules from appData payloads", () => {
  const fields = extractPaulasChoiceAppDataPdpFields(
    JSON.stringify({
      common: {
        strings: {
          whatDoesItDo: "Benefits",
          whyIsItDifferent: "What is it",
          howToUse: "How to use",
          research: "Research",
          keyIngredients: "Key Ingredients",
          allIngredients: "All Ingredients",
        },
      },
      page: {
        product: {
          whyIsItDifferent: "<p>BHA works within pores to clear congestion.</p>",
          whatDoesItDo: "Skin looks brighter and smoother.",
          keyIngredients: "Salicylic Acid, Green Tea",
          howToUse: "<p>Apply after cleansing and do not rinse.</p>",
        },
        ingredientsData: [
          { name: "Water" },
          { name: "Methylpropanediol" },
          { name: "Salicylic Acid" },
        ],
        research: [
          {
            paragraph: 1,
            text: ["Journal A", { break: 1 }, "Journal B"],
          },
        ],
      },
    }),
  );

  assert.ok(fields);
  assert.equal(fields?.descriptionRaw, "BHA works within pores to clear congestion.\n\nSkin looks brighter and smoother.");
  assert.equal(fields?.ingredientsRaw, "Water, Methylpropanediol, Salicylic Acid");
  assert.equal(fields?.activeIngredientsRaw, "Salicylic Acid, Green Tea");
  assert.equal(fields?.howToUseRaw, "Apply after cleansing and do not rinse.");
  assert.deepEqual(
    fields?.detailsSections.map((section) => section.heading),
    ["What is it", "Benefits", "Key Ingredients", "All Ingredients", "How to use", "Research"],
  );
});

test("extractProductFromHtmlSnapshot parses Estee Lauder ProductGroup accordions", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>Advanced Night Repair Serum Synchronized Multi-Recovery Complex | Estée Lauder</title>
          <link rel="canonical" href="https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex" />
          <script type="application/ld+json">
            {
              "@context": "http://schema.org/",
              "@type": "ProductGroup",
              "name": "Advanced Night Repair Serum Synchronized Multi-Recovery Complex",
              "description": "Wake up radiant. Visibly reduce signs of aging.",
              "image": "https://www.esteelauder.com/media/export/cms/products/640x640/el_sku_PG5001_640x640_0.jpg",
              "url": "https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex",
              "brand": { "@type": "Brand", "name": "Estée Lauder" },
              "hasVariant": [
                {
                  "@type": "Product",
                  "sku": "PG5001",
                  "name": "Advanced Night Repair Serum Synchronized Multi-Recovery Complex 1.0 oz.",
                  "offers": {
                    "@type": "Offer",
                    "priceCurrency": "USD",
                    "price": "50.00",
                    "availability": "http://schema.org/InStock",
                    "url": "https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex"
                  }
                }
              ]
            }
          </script>
        </head>
        <body>
          <h1>Advanced Night Repair Serum Synchronized Multi-Recovery Complex</h1>
          <div>$50.00</div>
          <details open>
            <summary>Product Details</summary>
            <div>The super serum for deep nightly renewal and unfiltered morning radiance.</div>
          </details>
          <details open>
            <summary>How To Use</summary>
            <div>Apply on clean skin before your moisturizer, AM and PM.</div>
          </details>
          <details open>
            <summary>Ingredients</summary>
            <div>Water, Bifida Ferment Lysate, Sodium Hyaluronate.</div>
          </details>
        </body>
      </html>
    `,
    url: "https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex",
    baseUrl: "https://www.esteelauder.com",
  });

  assert.ok(product);
  assert.equal(product?.title, "Advanced Night Repair Serum Synchronized Multi-Recovery Complex");
  assert.equal(product?.variants.length, 1);
  assert.equal(product?.variant_skus[0], "PG5001");
  assert.match(product?.description_raw || "", /wake up radiant/i);
  assert.match(product?.ingredients_raw || "", /Bifida Ferment Lysate/i);
  assert.match(product?.how_to_use_raw || "", /before your moisturizer/i);
  assert.equal(product?.details_sections.length, 3);
});
