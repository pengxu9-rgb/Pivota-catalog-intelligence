import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

import puppeteer from "puppeteer";

import {
  BotChallengeError,
  createDiagnostics,
  detectBlockProvider,
  discoverProductUrls,
  isCookieActionLabel,
  looksLikeStorefrontPasswordPage,
  looksLikeProductPageHtml,
  parseTarget,
  resolveStorefrontFromHtml,
  resolveStorefrontTarget,
  runBrowserTaskWithFallback,
} from "../src/services/extractors/shared";
import {
  buildProductPdpFields,
  deriveProductPdpModuleBodies,
  looksLikeFullIngredientListText,
} from "../src/services/extractors/puppeteer";

type MockRoute = {
  status?: number;
  headers?: Record<string, string>;
  body?: string;
  responseUrl?: string;
};

function readFixture(name: string): string {
  return readFileSync(join(__dirname, "fixtures", name), "utf8");
}

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

test("buildProductPdpFields preserves explicit PDP module fields and sources", () => {
  const fields = buildProductPdpFields({
    descriptionRaw: "A lightweight moisturizer with SPF 20.",
    detailsSections: [
      {
        heading: "Ingredients",
        body: "Titanium Dioxide, Zinc Oxide, Glycerin",
        source_kind: "accordion_ingredients",
      },
      {
        heading: "How to Use",
        body: "Apply evenly before sun exposure.",
        source_kind: "accordion_how_to_use",
      },
    ],
    ingredientsRaw: "Titanium Dioxide, Zinc Oxide, Glycerin",
    activeIngredientsRaw: "Titanium Dioxide, Zinc Oxide",
    howToUseRaw: "Apply evenly before sun exposure.",
    faqItems: [
      {
        question: "Can I use this every day?",
        answer: "Yes, it is suitable for daily use.",
        source_kind: "faq_section",
      },
    ],
    fieldSources: {
      description_raw: ["structured_overview"],
      details_sections: ["accordion_ingredients", "accordion_how_to_use"],
      ingredients_raw: ["page_ingredients_section"],
      active_ingredients_raw: ["page_active_ingredients_section"],
      how_to_use_raw: ["page_how_to_use_section"],
      faq_items: ["page_faq_section"],
    },
  });

  assert.equal(fields.description_raw, "A lightweight moisturizer with SPF 20.");
  assert.equal(fields.field_capture_status?.ingredients_raw, "present");
  assert.equal(fields.field_capture_status?.how_to_use_raw, "present");
  assert.equal(fields.field_capture_status?.faq_items, "present");
  assert.equal(fields.details_sections?.length, 2);
  assert.deepEqual(fields.field_sources?.ingredients_raw, ["page_ingredients_section"]);
  assert.deepEqual(fields.faq_items, [
    {
      question: "Can I use this every day?",
      answer: "Yes, it is suitable for daily use.",
      source_kind: "faq_section",
    },
  ]);
});

test("buildProductPdpFields does not fabricate ingredient fields from generic copy", () => {
  const fields = buildProductPdpFields({
    descriptionRaw: "Hydrates and smooths for softer-feeling skin.",
    fieldSources: {
      description_raw: ["structured_overview"],
    },
  });

  assert.equal(fields.description_raw, "Hydrates and smooths for softer-feeling skin.");
  assert.equal(fields.ingredients_raw, undefined);
  assert.equal(fields.active_ingredients_raw, undefined);
  assert.equal(fields.how_to_use_raw, undefined);
  assert.equal(fields.faq_items, undefined);
  assert.equal(fields.field_capture_status?.ingredients_raw, "missing");
  assert.equal(fields.field_capture_status?.faq_items, "missing");
});

test("deriveProductPdpModuleBodies extracts full modal ingredients and active ingredients separately", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Full Ingredients",
        body:
          "Active ingredients: Homosalate: 9.0%, Titanium Dioxide: 1.8%, Zinc Oxide: 0.9%. Inactive ingredients: Water/Aqua, Dimethicone, Talc, Iron Oxides (CI 77491, CI 77492, CI 77499).",
        source_kind: "modal_content",
      },
      {
        heading: "How to use",
        body: "Shake well and blend with fingers.",
        source_kind: "accordion_button",
      },
    ],
  });

  assert.equal(
    bodies.ingredientsRaw,
    "Active ingredients: Homosalate: 9.0%, Titanium Dioxide: 1.8%, Zinc Oxide: 0.9%. Inactive ingredients: Water/Aqua, Dimethicone, Talc, Iron Oxides (CI 77491, CI 77492, CI 77499).",
  );
  assert.equal(bodies.activeIngredientsRaw, "Homosalate: 9.0%, Titanium Dioxide: 1.8%, Zinc Oxide: 0.9%.");
  assert.equal(bodies.howToUseRaw, "Shake well and blend with fingers.");
});

test("deriveProductPdpModuleBodies keeps summary-style ingredient accordions out of ingredients_raw", () => {
  assert.equal(looksLikeFullIngredientListText("Rose Flower Oil nourishes & restores. Ceramide provides time-release moisture."), false);

  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Ingredients",
        body: "Rose Flower Oil nourishes & restores. Ceramide provides time-release moisture. Probiotics protect & balance.",
        source_kind: "accordion_button",
      },
      {
        heading: "How To Apply",
        body: "Use daily after cleansing and serum.",
        source_kind: "accordion_button",
      },
    ],
  });

  assert.equal(bodies.ingredientsRaw, undefined);
  assert.equal(
    bodies.activeIngredientsRaw,
    "Rose Flower Oil nourishes & restores. Ceramide provides time-release moisture. Probiotics protect & balance.",
  );
  assert.equal(bodies.howToUseRaw, "Use daily after cleansing and serum.");
});

test("deriveProductPdpModuleBodies prefers full INCI over key ingredient summaries", () => {
  const fullInci =
    "Water, Methylpropanediol, Propanediol, 1,2-Hexanediol, Glycerin, Panthenol, Oryza Sativa (Rice) Extract, Rice Amino Acids";
  assert.equal(
    looksLikeFullIngredientListText("Rice Extract\n\nRice Amino Acids\n\nSebum Control Complex\n\nFull Ingredient List"),
    false,
  );
  assert.equal(looksLikeFullIngredientListText(fullInci), true);

  const bodies = deriveProductPdpModuleBodies({
    ingredientsMarkdownText: "Rice Extract\n\nRice Amino Acids\n\nSebum Control Complex\n\nFull Ingredient List",
    detailsSections: [
      {
        heading: "Key Ingredients",
        body: "Rice Extract\n\nRice Amino Acids\n\nSebum Control Complex\n\nFull Ingredient List",
        source_kind: "heading_sibling",
      },
      {
        heading: "Full Ingredient List",
        body: fullInci,
        source_kind: "product_modal_content",
      },
    ],
  });

  assert.equal(bodies.ingredientsRaw, fullInci);
  assert.equal(bodies.activeIngredientsRaw, undefined);
});

test("deriveProductPdpModuleBodies extracts Tom Ford-style details summary accordions", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Product Details",
        body: "Key Notes\nTunisian Neroli, Italian Bergamot\nBenefits\nLeaves skin feeling soft and smooth without a greasy residue",
        source_kind: "details_summary",
      },
      {
        heading: "How to Use",
        body: "Hands - After washing hands, smooth into the skin as needed.",
        source_kind: "details_summary",
      },
      {
        heading: "Ingredients and Safety",
        body:
          "Ingredients: Water Aqua Eau, Sodium Laureth Sulfate, Squalane, Panthenol, Glycerin. Please refer to the ingredient list on the product package you receive for the most up-to-date information.",
        source_kind: "details_summary",
      },
    ],
  });

  assert.equal(
    bodies.ingredientsRaw,
    "Ingredients: Water Aqua Eau, Sodium Laureth Sulfate, Squalane, Panthenol, Glycerin.",
  );
  assert.equal(
    bodies.howToUseRaw,
    "Hands - After washing hands, smooth into the skin as needed.",
  );
  assert.equal(bodies.activeIngredientsRaw, undefined);
});

test("deriveProductPdpModuleBodies keeps Guerlain key ingredients separate from full ingredients", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Key Ingredients",
        body:
          "Black Bee Honey: Helps support skin repair.\n\nExclusive Royal Jelly: Helps nourish and smooth the skin.",
        source_kind: "guerlain_ingredients_carousel",
      },
      {
        heading: "Ingredients",
        body: "Aqua (Water), Glycerin, Squalane, Caprylic/Capric Triglyceride, Parfum (Fragrance).",
        source_kind: "guerlain_ingredients_modal",
      },
    ],
  });

  assert.equal(
    bodies.ingredientsRaw,
    "Aqua (Water), Glycerin, Squalane, Caprylic/Capric Triglyceride, Parfum (Fragrance).",
  );
  assert.equal(
    bodies.activeIngredientsRaw,
    "Black Bee Honey: Helps support skin repair.\n\nExclusive Royal Jelly: Helps nourish and smooth the skin.",
  );
});

test("deriveProductPdpModuleBodies prefers the richer duplicate hero-ingredients accordion body", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "HERO INGREDIENTS",
        body: "HERO INGREDIENTS",
        source_kind: "accordion_control",
      },
      {
        heading: "HERO INGREDIENTS",
        body:
          "• Hyaluronic Acid, Sea Moss, Centella\nMoisturized and hydrate the skin all day long\n• EGT (L-Ergothioneine), Carnosine\nProtect the skin against free radicals.",
        source_kind: "accordion_control",
      },
    ],
  });

  assert.equal(
    bodies.activeIngredientsRaw,
    "• Hyaluronic Acid, Sea Moss, Centella\nMoisturized and hydrate the skin all day long\n• EGT (L-Ergothioneine), Carnosine\nProtect the skin against free radicals.",
  );
  assert.equal(bodies.ingredientsRaw, undefined);
});

test("deriveProductPdpModuleBodies extracts The Ordinary-style ingredient flyout blocks", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Ingredients",
        body:
          "Active ingredients:\nAvobenzone 3.0% (UV Filter)\nHomosalate 7.0% (UV Filter)\nOctisalate 4.5% (UV Filter)\nOctocrylene 10.0% (UV Filter)\n\nInactive ingredients:\nWater, Glycerin, Ceramide NP.",
        source_kind: "ingredients_flyout",
      },
      {
        heading: "Key Ingredients",
        body: "Homosalate, Octisalate, Octocrylene, Avobenzone, Ceramides",
        source_kind: "page_key_ingredients",
      },
    ],
  });

  assert.match(bodies.ingredientsRaw || "", /Avobenzone 3\.0%/);
  assert.match(bodies.ingredientsRaw || "", /Inactive ingredients:/i);
  assert.match(bodies.activeIngredientsRaw || "", /Homosalate 7\.0%/);
});

test("resolveStorefrontFromHtml resolves selector roots to the requested market storefront", () => {
  const html = readFixture("caudalie-selector.html");
  const resolved = resolveStorefrontFromHtml(html, "https://caudalie.com", "US");

  assert.equal(resolved.selectorRoot, true);
  assert.equal(resolved.url, "https://us.caudalie.com/");
});

test("resolveStorefrontFromHtml ignores same-brand service links without market storefront signals", () => {
  const html = `
    <html>
      <body>
        <h1>Select your country</h1>
        <a href="https://fentybeauty.setmore.com/">Still confused? Book an appointment</a>
      </body>
    </html>
  `;

  const resolved = resolveStorefrontFromHtml(html, "https://fentybeauty.com", "US");

  assert.equal(resolved.selectorRoot, true);
  assert.equal(resolved.url, null);
});

test("resolveStorefrontTarget normalizes locale-prefixed seed URLs to the requested market", async () => {
  const diagnostics = createDiagnostics("theordinary.com", "https://theordinary.com");
  const resolved = await resolveStorefrontTarget({
    target: parseTarget("https://theordinary.com/de-de/uv-filters-spf-45-serum-100720.html"),
    marketId: "US",
    context: {},
    diagnostics,
  });

  assert.equal(resolved.target.seedUrl, "https://theordinary.com/en-us/uv-filters-spf-45-serum-100720.html");
  assert.equal(resolved.target.baseUrl, "https://theordinary.com");
});

test("resolveStorefrontTarget preserves language-compatible seed locales when the exact market locale differs", async () => {
  const diagnostics = createDiagnostics("patyka.com", "https://patyka.com");
  const resolved = await resolveStorefrontTarget({
    target: parseTarget("https://patyka.com/en-eu/products/detox-cleansing-foam"),
    marketId: "US",
    context: {},
    diagnostics,
  });

  assert.equal(resolved.target.seedUrl, "https://patyka.com/en-eu/products/detox-cleansing-foam");
  assert.equal(resolved.target.baseUrl, "https://patyka.com");
});

test("discoverProductUrls uses landing-page HTML discovery for slug PDPs", async () => {
  const homepageHtml = readFixture("augustinus-homepage.html");
  const diagnostics = createDiagnostics("augustinusbader.com", "https://augustinusbader.com");

  await withMockFetch(
    {
      "https://augustinusbader.com": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: homepageHtml,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://augustinusbader.com",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "seed_page");
      assert.deepEqual(discovered.productUrls, [
        "https://augustinusbader.com/the-geranium-rose-body-cream",
        "https://augustinusbader.com/the-rich-cream",
      ]);
    },
  );
});

test("discoverProductUrls does not treat a homepage as a direct PDP when it only has merchandising signals", async () => {
  const diagnostics = createDiagnostics("www.guerlain.com", "https://www.guerlain.com");
  const homepageHtml = `
    <html>
      <body>
        <h1>Guerlain</h1>
        <button>Buy now</button>
        <span class="price">$165.00</span>
        <a href="/us/en-us/p/abeille-royale-youth-watery-oil-serum-P062033.html">Abeille Royale</a>
      </body>
    </html>
  `;

  await withMockFetch(
    {
      "https://www.guerlain.com": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: homepageHtml,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://www.guerlain.com",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "seed_page");
      assert.deepEqual(discovered.productUrls, ["https://www.guerlain.com/us/en-us/p/abeille-royale-youth-watery-oil-serum-P062033.html"]);
    },
  );
});

test("discoverProductUrls falls back to default sitemap paths after a dead robots sitemap", async () => {
  const diagnostics = createDiagnostics("augustinusbader.com", "https://augustinusbader.com");

  await withMockFetch(
    {
      "https://augustinusbader.com": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><body><a href=\"/about\">About</a></body></html>",
      },
      "https://augustinusbader.com/robots.txt": {
        status: 200,
        headers: { "content-type": "text/plain; charset=utf-8" },
        body: readFixture("dead-sitemap-robots.txt"),
      },
      "https://augustinusbader.com/media/sitemap/sitemap_main_index.xml": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><title>Not Found</title></html>",
      },
      "https://augustinusbader.com/sitemap.xml": {
        status: 200,
        headers: { "content-type": "application/xml; charset=utf-8" },
        body: readFixture("fallback-sitemap.xml"),
      },
      "https://augustinusbader.com/sitemap_index.xml": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><title>Not Found</title></html>",
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://augustinusbader.com",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "sitemap");
      assert.equal(diagnostics.failure_category, null);
      assert.equal(discovered.sitemapUrl, "https://augustinusbader.com/sitemap.xml");
      assert.deepEqual(discovered.productUrls, ["https://augustinusbader.com/the-geranium-rose-body-cream"]);
      assert.ok(
        diagnostics.http_trace.some(
          (entry) => entry.url === "https://augustinusbader.com/media/sitemap/sitemap_main_index.xml" && entry.status === 404,
        ),
      );
      assert.ok(
        diagnostics.http_trace.some((entry) => entry.url === "https://augustinusbader.com/sitemap.xml" && entry.status === 200),
      );
    },
  );
});

test("discoverProductUrls treats a direct PDP input as a product page", async () => {
  const diagnostics = createDiagnostics("augustinusbader.com", "https://augustinusbader.com");

  await withMockFetch(
    {
      "https://augustinusbader.com/the-rich-cream": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: readFixture("direct-product-page.html"),
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://augustinusbader.com",
        seedUrl: "https://augustinusbader.com/the-rich-cream",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "seed_page");
      assert.deepEqual(discovered.productUrls, ["https://augustinusbader.com/the-rich-cream"]);
    },
  );
});

test("discoverProductUrls does not fall through from an invalid direct PDP to unrelated page links", async () => {
  const diagnostics = createDiagnostics("theordinary.com", "https://theordinary.com");

  await withMockFetch(
    {
      "https://theordinary.com/en-us/the-clear-set-100630.html": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: `
          <html>
            <body>
              <h1>Page not found</h1>
              <a href="/contact-us.html">Contact us</a>
            </body>
          </html>
        `,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://theordinary.com",
        seedUrl: "https://theordinary.com/en-us/the-clear-set-100630.html",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "seed_page");
      assert.equal(diagnostics.failure_category, "no_product_urls");
      assert.deepEqual(discovered.productUrls, []);
    },
  );
});

test("discoverProductUrls re-discovers a target PDP when a stale direct seed redirects to a collection page", async () => {
  const diagnostics = createDiagnostics("www.tomfordbeauty.com", "https://www.tomfordbeauty.com");

  await withMockFetch(
    {
      "https://www.tomfordbeauty.com/product/shade-and-illuminate-soft-radiance-foundation-spf-50": {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
        responseUrl: "https://www.tomfordbeauty.com/collections/makeup",
        body: `
          <html>
            <head><title>Makeup</title></head>
            <body>
              <h1>Makeup</h1>
              <a href="/products/ombre-leather-parfum">Ombre Leather Parfum</a>
              <a href="/products/shade-and-illuminate-soft-radiance-foundation-spf-50">Soft Radiance Foundation</a>
            </body>
          </html>
        `,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://www.tomfordbeauty.com",
        seedUrl: "https://www.tomfordbeauty.com/product/shade-and-illuminate-soft-radiance-foundation-spf-50",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "seed_page");
      assert.deepEqual(discovered.productUrls, [
        "https://www.tomfordbeauty.com/products/shade-and-illuminate-soft-radiance-foundation-spf-50",
        "https://www.tomfordbeauty.com/products/ombre-leather-parfum",
      ]);
      assert.equal(diagnostics.failure_category, null);
    },
  );
});

test("discoverProductUrls classifies blocked direct seed PDPs as bot challenges", async () => {
  const diagnostics = createDiagnostics("www.esteelauder.com", "https://www.esteelauder.com");

  await withMockFetch(
    {
      "https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex": {
        status: 403,
        headers: {
          "content-type": "text/html; charset=utf-8",
          warning: "299 Akamai",
          "akamai-grn": "0.59ce2d17.1774455343.cef8a5a9",
          "x-akamai-devicedetected": "Desktop",
        },
        body: `
          <html>
            <head><title>Access Denied</title></head>
            <body>
              <h1>Access Denied</h1>
              You don't have permission to access this page on this server.
              Reference #18.59ce2d17.1774455343.cef8a5a9
            </body>
          </html>
        `,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://www.esteelauder.com",
        seedUrl: "https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "seed_page");
      assert.equal(diagnostics.block_provider, "akamai");
      assert.equal(diagnostics.failure_category, "bot_challenge");
      assert.deepEqual(discovered.productUrls, []);
    },
  );
});
test("looksLikeProductPageHtml distinguishes PDPs from price-only non-product pages", () => {
  assert.equal(looksLikeProductPageHtml(readFixture("direct-product-page.html")), true);
  assert.equal(
    looksLikeProductPageHtml(
      "<html><head><title>Spa Vinotherapie</title></head><body><h1>Spa Vinotherapie</h1><p>Starting at $250</p></body></html>",
    ),
    false,
  );
});

test("isCookieActionLabel does not confuse 'Book' CTAs with cookie consent buttons", () => {
  assert.equal(isCookieActionLabel("Book a Spa Treatment"), false);
  assert.equal(isCookieActionLabel("OK"), true);
  assert.equal(isCookieActionLabel("Accept all cookies"), true);
});

test("detectBlockProvider classifies Cloudflare challenge pages", () => {
  const body = readFixture("cloudflare-challenge.html");
  const provider = detectBlockProvider({
    status: 403,
    headers: {
      "cf-mitigated": "challenge",
      server: "cloudflare",
    },
    body,
    title: "Just a moment...",
    url: "https://www.laroche-posay.us/",
  });

  assert.equal(provider, "cloudflare");
});

test("detectBlockProvider does not classify normal Cloudflare-served pages from cf-ray alone", () => {
  const provider = detectBlockProvider({
    status: 200,
    headers: {
      "cf-ray": "1234567890-SJC",
      server: "cloudflare",
    },
    body: "<html><head><title>Shop</title></head><body><h1>Products</h1></body></html>",
    title: "Shop",
    url: "https://example.com/products",
  });

  assert.equal(provider, null);
});

test("detectBlockProvider classifies Akamai access denied pages from response headers", () => {
  const provider = detectBlockProvider({
    status: 403,
    headers: {
      warning: "299 Akamai",
      "akamai-grn": "0.59ce2d17.1774455343.cef8a5a9",
      "x-akamai-devicedetected": "Desktop",
    },
    body: `You don't have permission to access this page on this server.\nReference #18.59ce2d17.1774455343.cef8a5a9`,
    title: "Access Denied",
    url: "https://www.esteelauder.com/product/689/77491/product-catalog/skincare/repair-serum/advanced-night-repair-serum/synchronized-multi-recovery-complex",
  });

  assert.equal(provider, "akamai");
});

test("looksLikeStorefrontPasswordPage detects Shopify password gates", () => {
  assert.equal(
    looksLikeStorefrontPasswordPage({
      url: "https://pivota-market.myshopify.com/password",
      title: "– Pivota Market",
      content: "<h1>Opening soon</h1><button>Enter using password</button>",
    }),
    true,
  );
});

test("looksLikeStorefrontPasswordPage ignores normal product pages", () => {
  assert.equal(
    looksLikeStorefrontPasswordPage({
      url: "https://pivota-market.myshopify.com/products/winona-soothing-repair-serum",
      title: "Winona Soothing Repair Serum – Pivota Market",
      content: "<h1>Winona Soothing Repair Serum</h1><button>Add to cart</button>",
    }),
    false,
  );
});

test("runBrowserTaskWithFallback retries once with a managed browser after a bot challenge", async () => {
  const diagnostics = createDiagnostics("www.laroche-posay.us", "https://www.laroche-posay.us");
  const originalLaunch = puppeteer.launch;
  const originalConnect = puppeteer.connect;
  const originalEndpoint = process.env.REMOTE_BROWSER_WS_ENDPOINT;
  const originalEnabled = process.env.REMOTE_BROWSER_ENABLED;
  const calls: string[] = [];
  let attempts = 0;

  const localBrowser = {
    close: async () => {
      calls.push("local-close");
    },
  };

  const managedBrowser = {
    disconnect: () => {
      calls.push("managed-disconnect");
    },
  };

  process.env.REMOTE_BROWSER_WS_ENDPOINT = "wss://browserless.example/ws";
  process.env.REMOTE_BROWSER_ENABLED = "1";
  (puppeteer as typeof puppeteer & { launch: typeof puppeteer.launch }).launch = async () => {
    calls.push("launch");
    return localBrowser as never;
  };
  (puppeteer as typeof puppeteer & { connect: typeof puppeteer.connect }).connect = async () => {
    calls.push("connect");
    return managedBrowser as never;
  };

  try {
    const result = await runBrowserTaskWithFallback(
      async (browser, mode) => {
        attempts += 1;
        if (mode === "local") {
          assert.equal(browser, localBrowser);
          throw new BotChallengeError("cloudflare", "https://www.laroche-posay.us/");
        }

        assert.equal(browser, managedBrowser);
        return "ok";
      },
      { diagnostics },
    );

    assert.equal(result.mode, "managed");
    assert.equal(result.result, "ok");
    assert.equal(diagnostics.discovery_strategy, "managed_browser");
    assert.equal(attempts, 2);
    assert.deepEqual(calls, ["launch", "local-close", "connect", "managed-disconnect"]);
  } finally {
    puppeteer.launch = originalLaunch;
    puppeteer.connect = originalConnect;

    if (originalEndpoint === undefined) {
      delete process.env.REMOTE_BROWSER_WS_ENDPOINT;
    } else {
      process.env.REMOTE_BROWSER_WS_ENDPOINT = originalEndpoint;
    }

    if (originalEnabled === undefined) {
      delete process.env.REMOTE_BROWSER_ENABLED;
    } else {
      process.env.REMOTE_BROWSER_ENABLED = originalEnabled;
    }
  }
});

test("runBrowserTaskWithFallback launches local Chrome with hardened Railway-safe flags", async () => {
  const diagnostics = createDiagnostics("pixibeauty.com", "https://pixibeauty.com");
  const originalLaunch = puppeteer.launch;
  const captured: { args?: string[] } = {};
  const calls: string[] = [];

  const localBrowser = {
    close: async () => {
      calls.push("local-close");
    },
  };

  (puppeteer as typeof puppeteer & { launch: typeof puppeteer.launch }).launch = async (options?: Parameters<typeof puppeteer.launch>[0]) => {
    captured.args = options?.args ? [...options.args] : [];
    calls.push("launch");
    return localBrowser as never;
  };

  try {
    const result = await runBrowserTaskWithFallback(
      async (browser, mode) => {
        assert.equal(mode, "local");
        assert.equal(browser, localBrowser);
        return "ok";
      },
      { diagnostics },
    );

    assert.equal(result.mode, "local");
    assert.equal(result.result, "ok");
    assert.deepEqual(calls, ["launch", "local-close"]);
    assert.ok(captured.args?.includes("--no-sandbox"));
    assert.ok(captured.args?.includes("--disable-setuid-sandbox"));
    assert.ok(captured.args?.includes("--disable-dev-shm-usage"));
    assert.ok(captured.args?.includes("--disable-breakpad"));
    assert.ok(captured.args?.includes("--disable-crash-reporter"));
    assert.ok(captured.args?.includes("--no-zygote"));
  } finally {
    puppeteer.launch = originalLaunch;
  }
});
