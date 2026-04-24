import assert from "node:assert/strict";
import test from "node:test";

import {
  PuppeteerExtractor,
  choosePreferredProductOverview,
  classifyExtractedProductKind,
  enrichDirectShopifyPdpResponse,
  extractBundleComponents,
  extractLikelyFullIngredientListText,
  extractShopifyEmbeddedProductPayloadPdpFields,
  mergeShopifyDirectPdpFallback,
  pickBestJsonLdObjectForPage,
  productHasMissingPdpFields,
} from "../src/services/extractors/puppeteer";

type MockRoute = {
  status?: number;
  headers?: Record<string, string>;
  body?: string;
};

function createMockResponse(route: MockRoute): Response {
  return new Response(route.body ?? "", {
    status: route.status ?? 200,
    headers: route.headers,
  });
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
    body_html: "<p>Brightening serum</p>",
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
                  reviewAggregate: {
                    subscriberId: "store-123",
                    productId: "shopify-456",
                    subscriberId_productId: "store-123:shopify-456",
                  },
                  questionCount: 1,
                })}
              </script>
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
      assert.doesNotMatch(logs.map((entry) => entry.msg).join("\n"), /Attempting browser enrichment/);
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

  assert.equal(merged.products[0]?.ingredients_raw, "Ingredients: Water Aqua Eau, Glycerin, Panthenol.");
  assert.equal(merged.products[0]?.how_to_use_raw, "Smooth into skin as needed.");
  assert.equal(merged.products[0]?.details_sections?.length, 2);
  assert.deepEqual(merged.products[0]?.faq_items, [
    {
      question: "Can I use this every day?",
      answer: "Yes, smooth into skin as needed.",
      source_kind: "faq_section",
    },
  ]);
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
