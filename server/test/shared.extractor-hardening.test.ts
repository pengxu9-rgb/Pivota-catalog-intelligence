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
  extractDelimitedLabeledSectionText,
  extractProductFromHtmlSnapshot,
  extractShopifyBodyHtmlPdpTextFields,
  extractShopifyEmbeddedProductPayloadPdpFields,
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

test("deriveProductPdpModuleBodies does not treat active ingredient lists as full INCI", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Active Ingredients",
        body: "Hippophae Rhamnoides Water, Niacinamide, 3-O-Ethyl Ascorbic Acid, Panthenol",
        source_kind: "shopify_collapsible_tab_html",
      },
      {
        heading: "How to Use",
        body: "Apply after cleansing and toning.",
        source_kind: "shopify_collapsible_tab_html",
      },
    ],
  });

  assert.equal(bodies.ingredientsRaw, undefined);
  assert.match(bodies.activeIngredientsRaw || "", /Hippophae Rhamnoides Water/i);
  assert.equal(bodies.howToUseRaw, "Apply after cleansing and toning.");
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

test("extractDelimitedLabeledSectionText supports labels split across Shopify body_html blocks", () => {
  const text = `
    This mask is made with a pure cotton sheet mask.

    How it works
    Hydrating sheet mask that contains 91.45% pure Mugwort extract.

    Ingredients
    Active Ingredients: Glycine, Madecassoside
    Full Ingredients: Water, Glycine, Methylpropanediol, Artemisia Vulgaris Extract
    How to use
    After cleansing the face, tidy up the skin texture using toner.
  `;

  assert.equal(
    extractDelimitedLabeledSectionText(text, ["How to use"], ["FAQ", "Frequently Asked Questions"]),
    "After cleansing the face, tidy up the skin texture using toner.",
  );
  assert.match(
    extractDelimitedLabeledSectionText(
      text,
      ["Ingredients"],
      ["How to use", "FAQ", "Frequently Asked Questions"],
    ),
    /Full Ingredients:/,
  );
});

test("extractProductFromHtmlSnapshot parses BYOMA routine rich-text how-to sections", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>Balancing Face Mist</title>
          <meta property="og:price:amount" content="15.99" />
          <link rel="canonical" href="https://byoma.com/products/balancing-face-mist" />
        </head>
        <body>
          <h1>Balancing Face Mist</h1>
          <section class="routine-section">
            <div class="left-section-routine">
              <div class="routine-content">
                <h2>HOW TO USE</h2>
                <div class="metafield-rich_text_field">
                  <ol>
                    <li>Shake well before each use and mist onto clean, dry skin</li>
                    <li>Follow with your favorite serums and moisturizer</li>
                  </ol>
                </div>
              </div>
            </div>
          </section>
        </body>
      </html>
    `,
    url: "https://byoma.com/products/balancing-face-mist",
    baseUrl: "https://byoma.com",
  });

  assert.match(product?.how_to_use_raw || "", /Shake well before each use/i);
  assert.match(product?.field_sources?.how_to_use_raw?.join(","), /page_how_to_use_section/);
});

test("extractProductFromHtmlSnapshot parses SKIN1004 prhow and prinfo PDP sections", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>Azelaic Acid 10 Ampoule</title>
          <meta property="og:price:amount" content="16.80" />
          <link rel="canonical" href="https://www.skin1004.com/products/azelaic-acid-10-ampoule" />
        </head>
        <body>
          <h1>Azelaic Acid 10 Ampoule</h1>
          <div class="prhow-flex">
            <div class="prhow-section-title txt_style_five">HOW TO USE</div>
            <div class="swiper-container prhow-swiper-container">
              <div class="swiper-slide">
                <div class="prhow-txt txt_style_four">
                  <div class="metafield-rich_text_field">
                    <p><strong>[SKINCARE ROUTINE]</strong><br />Gently apply along the skin texture, then lightly pat to aid absorption.</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="prinfo-content-ingrewrapper">
            <div class="prinfo-ingre-title txt_style_three">FULL INGREDIENTS</div>
            <div id="prinfo-tab3-body2" class="prinfo-content-body txt_style_four">
              <div class="metafield-rich_text_field">
                <p>Water, Azelaic Acid, Hydroxypropyl Cyclodextrin, Panthenol, Centella Asiatica Extract</p>
              </div>
            </div>
          </div>
        </body>
      </html>
    `,
    url: "https://www.skin1004.com/products/azelaic-acid-10-ampoule",
    baseUrl: "https://www.skin1004.com",
  });

  assert.match(product?.how_to_use_raw || "", /Gently apply along the skin texture/i);
  assert.match(product?.ingredients_raw || "", /Water, Azelaic Acid/i);
  assert.match(product?.field_sources?.details_sections?.join(","), /skin1004_pr/);
});

test("extractShopifyEmbeddedProductPayloadPdpFields promotes inline Shopify product payloads into structured PDP fields", () => {
  const script = `window.reelUp_productJSON = ${JSON.stringify({
    description: `
      <p>This mask is made with a pure cotton sheet mask soaked in clean, natural ingredients.</p>
      <p><strong>How it works</strong></p>
      <ul><li>Hydrating sheet mask that contains 91.45% pure Mugwort extract.</li></ul>
      <p><strong>Ingredients</strong></p>
      <p><strong>Active Ingredients:</strong> Glycine, Madecassoside</p>
      <p><strong>Full Ingredients:</strong> Water, Glycine, Methylpropanediol, Artemisia Vulgaris Extract</p>
      <p><strong>How to use</strong></p>
      <ol><li>After cleansing the face, tidy up the skin texture using toner.</li></ol>
    `,
    images: ["//roundlab.com/cdn/shop/files/mugwort-calming-sheet-mask-round-lab-1.png?v=1772849529"],
  })};`;

  const fields = extractShopifyEmbeddedProductPayloadPdpFields([script]);

  assert.match(fields.descriptionRaw || "", /pure cotton sheet mask/i);
  assert.match(fields.detailsSections[0]?.body || "", /91\.45% pure Mugwort extract/i);
  assert.equal(fields.activeIngredientsRaw, "Glycine, Madecassoside");
  assert.match(fields.ingredientsRaw || "", /Water, Glycine, Methylpropanediol/i);
  assert.equal(fields.howToUseRaw, "After cleansing the face, tidy up the skin texture using toner.");
  assert.deepEqual(fields.imageUrls, ["//roundlab.com/cdn/shop/files/mugwort-calming-sheet-mask-round-lab-1.png?v=1772849529"]);
});

test("extractShopifyBodyHtmlPdpTextFields splits Round Lab-style How to Use, Good For, and Full INCI sections", () => {
  const fields = extractShopifyBodyHtmlPdpTextFields(`
    <p><strong>How to Use</strong></p>
    <p>(Short) After cleansing, apply an appropriate amount to the face. Gently pat until fully absorbed.</p>
    <p><strong>Good For</strong></p>
    <p>Dry or dehydrated skin. Daily barrier support care.</p>
    <p><strong>Full INCI</strong></p>
    <p>Water, Camellia Japonica Flower Extract, Glycerin, Butylene Glycol, Collagen Extract, Sodium DNA (PDRN), Niacinamide</p>
    <p><strong>Key Benefits</strong></p>
    <p>Milky hydration and elasticity support.</p>
  `);

  assert.equal(
    fields.howToUseRaw,
    "(Short) After cleansing, apply an appropriate amount to the face. Gently pat until fully absorbed.",
  );
  assert.match(fields.ingredientsRaw || "", /Water, Camellia Japonica Flower Extract/i);
  assert.doesNotMatch(fields.howToUseRaw || "", /Full INCI|Camellia Japonica Flower Extract/i);
});

test("extractShopifyBodyHtmlPdpTextFields parses Round Lab uppercase h3 labels with narrow spaces", () => {
  const fields = extractShopifyBodyHtmlPdpTextFields(`
    <p><strong>Sculpt, cool, and revive—one mask does it all.</strong></p>
    <hr>
    <h3>ACTIVE INGREDIENTS</h3>
    <p>Jeju Camellia Flower Extract • Multi‑Weight Collagen • 8‑Peptide Complex • Caffeine • Niacinamide</p>
    <hr>
    <h3>HOW TO USE</h3>
    <ol>
      <li>After cleansing and toning, stretch the mask and hook over each ear.</li>
      <li>Smooth upward along the jawline for a snug fit.</li>
      <li>Relax for 20-30 minutes.</li>
    </ol>
    <p><strong>Size:</strong> 1 lifting hydrogel mask</p>
  `);

  assert.match(fields.activeIngredientsRaw || "", /Jeju Camellia Flower Extract/i);
  assert.match(fields.howToUseRaw || "", /hook over each ear/i);
  assert.match(fields.howToUseRaw || "", /Relax for 20-30 minutes/i);
  assert.doesNotMatch(fields.howToUseRaw || "", /Size|hydrogel mask/i);
  assert.doesNotMatch(fields.activeIngredientsRaw || "", /HOW TO USE|hook over each ear/i);
});

test("extractProductFromHtmlSnapshot normalizes CN yen price text to CNY", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>帧颜淡纹修护精华水 - Pechoin</title>
          <meta name="description" content="中国官方站商品页。">
        </head>
        <body>
          <h1>帧颜淡纹修护精华水 - Pechoin</h1>
          <div class="price">价格：¥298 / 100ml</div>
          <img src="/images/pechoin-serum-water.jpg" alt="帧颜淡纹修护精华水">
          <h2>Details</h2>
          <p>Peptide-focused essence water for daily facial care.</p>
        </body>
      </html>
    `,
    url: "https://www.pechoin.com/products/peptide-essence-water/",
    baseUrl: "https://www.pechoin.com",
    marketId: "CN",
  });

  assert.equal(product?.variants[0]?.price, "298.00");
  assert.equal(product?.variants[0]?.currency, "CNY");
});

test("extractShopifyBodyHtmlPdpTextFields does not promote active-only sections to full INCI", () => {
  const fields = extractShopifyBodyHtmlPdpTextFields(`
    <p><strong>Active Ingredients</strong></p>
    <p>Hippophae Rhamnoides Water, Niacinamide, 3-O-Ethyl Ascorbic Acid, Panthenol</p>
    <p><strong>How to Use</strong></p>
    <p>Apply a moderate amount after cleansing and toning.</p>
  `);

  assert.equal(fields.ingredientsRaw, "");
  assert.match(fields.activeIngredientsRaw || "", /Hippophae Rhamnoides Water/i);
  assert.equal(fields.howToUseRaw, "Apply a moderate amount after cleansing and toning.");
});

test("extractShopifyBodyHtmlPdpTextFields does not treat ingredient mentions as INCI labels", () => {
  const fields = extractShopifyBodyHtmlPdpTextFields(`
    <h3>HOW IT WORKS</h3>
    <ul>
      <li>Hypoallergenic formula is free from 19 flagged ingredients and certified gentle for sensitive skin.</li>
    </ul>
    <h3>ACTIVE INGREDIENTS</h3>
    <p>Jeju Camellia Flower Extract • Multi-Weight Collagen • Caffeine</p>
  `);

  assert.equal(fields.ingredientsRaw, "");
  assert.match(fields.activeIngredientsRaw || "", /Jeju Camellia Flower Extract/i);
});

test("extractProductFromHtmlSnapshot parses Shopify collapsible PDP ingredients and how-to tabs", () => {
  const product = extractProductFromHtmlSnapshot({
    html: `
      <html>
        <head>
          <title>Soybean Panthenol Cleanser</title>
          <meta property="og:price:amount" content="17.00" />
          <link rel="canonical" href="https://roundlab.com/products/soybean-panthenol-cleanser" />
        </head>
        <body>
          <h1>Soybean Panthenol Cleanser</h1>
          <collapsible-tab class="m-collapsible no-js-hidden">
            <button class="m-collapsible--button" data-trigger><span>ACTIVE INGREDIENTS</span></button>
            <div class="m-collapsible--content" data-content hidden>
              <div class="m-collapsible--content__inner rte">
                Fermented Soybean Extract: Rich in amino acids and antioxidants to nourish skin.
              </div>
            </div>
          </collapsible-tab>
          <collapsible-tab class="m-collapsible no-js-hidden">
            <button class="m-collapsible--button" data-trigger><span>HOW TO USE</span></button>
            <div class="m-collapsible--content" data-content hidden>
              <div class="m-collapsible--content__inner rte">
                Dispense an appropriate amount into wet hands. Rinse thoroughly.
              </div>
            </div>
          </collapsible-tab>
          <div class="ingredients-tabs">
            <div class="tab-panel">
              <h4>Full Ingredients</h4>
              <p class="full_ingredients">
                Water(Aqua), Sodium Cocoyl Isethionate, Glycerin, Panthenol, Ceramide NP.
              </p>
            </div>
          </div>
        </body>
      </html>
    `,
    url: "https://roundlab.com/products/soybean-panthenol-cleanser",
    baseUrl: "https://roundlab.com",
  });

  assert.match(product?.how_to_use_raw || "", /Dispense an appropriate amount/i);
  assert.match(product?.ingredients_raw || "", /Water\(Aqua\), Sodium Cocoyl Isethionate/i);
  assert.match(product?.active_ingredients_raw || "", /Fermented Soybean Extract/i);
  assert.match(product?.field_sources?.details_sections?.join(","), /shopify_collapsible_tab_html/);
  assert.match(product?.field_sources?.details_sections?.join(","), /shopify_ingredients_tabs_html/);
});

test("extractShopifyEmbeddedProductPayloadPdpFields parses customMetafields from inline product scripts", () => {
  const script = `window.corner.sessionData.product = ${JSON.stringify({
    customMetafields: {
      how_to_use_1_: {
        type: "root",
        children: [
          {
            type: "paragraph",
            children: [{ type: "text", value: "After cleansing, apply evenly and pat to absorb." }],
          },
        ],
      },
      product_info_tab_1_body: {
        type: "root",
        children: [
          {
            type: "paragraph",
            children: [{ type: "text", value: "Brightens dull skin while supporting the barrier." }],
          },
        ],
      },
      product_info_tab_3_full_ingredients: {
        type: "root",
        children: [
          {
            type: "paragraph",
            children: [{ type: "text", value: "Water, Glycerin, Niacinamide, Centella Asiatica Extract" }],
          },
        ],
      },
    },
  })};`;

  const fields = extractShopifyEmbeddedProductPayloadPdpFields([script]);

  assert.match(fields.howToUseRaw || "", /apply evenly and pat to absorb/i);
  assert.match(fields.ingredientsRaw || "", /Water, Glycerin, Niacinamide/i);
  assert.deepEqual(
    fields.detailsSections.map((section) => section.heading),
    ["Benefits", "Ingredients", "How to Use"],
  );
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

test("discoverProductUrls filters Korean legal and company sitemap pages before PDPs", async () => {
  const diagnostics = createDiagnostics("roundlab.co.kr", "https://roundlab.co.kr");

  await withMockFetch(
    {
      "https://roundlab.co.kr/robots.txt": {
        status: 200,
        headers: { "content-type": "text/plain" },
        body: "Sitemap: https://roundlab.co.kr/sitemap.xml",
      },
      "https://roundlab.co.kr/sitemap.xml": {
        status: 200,
        headers: { "content-type": "application/xml" },
        body: `
          <urlset>
            <url><loc>https://roundlab.co.kr/member/privacy.html</loc></url>
            <url><loc>https://roundlab.co.kr/member/agreement.html</loc></url>
            <url><loc>https://roundlab.co.kr/shopinfo/company.html</loc></url>
            <url><loc>https://roundlab.co.kr/product/1025-독도-토너-200ml/22/</loc></url>
          </urlset>
        `,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://roundlab.co.kr",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "sitemap");
      assert.deepEqual(discovered.productUrls, ["https://roundlab.co.kr/product/1025-독도-토너-200ml/22/"]);
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
