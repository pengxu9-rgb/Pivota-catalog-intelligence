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
  resolveLocalBrowserConfig,
  resolveStorefrontFromHtml,
  resolveStorefrontTarget,
  runBrowserTaskWithFallback,
} from "../src/services/extractors/shared";
import {
  buildProductPdpFields,
  deriveProductPdpModuleBodies,
  extractDelimitedLabeledSectionText,
  extractInlineFaqItemsFromHtml,
  extractShopifyEmbeddedProductPayloadPdpFields,
  extractShopifyBodyHtmlPdpFields,
  filterUsefulFaqItems,
  looksLikeFullIngredientListText,
  productHasMissingPdpFields,
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

test("looksLikeFullIngredientListText does not misclassify prose-heavy overview copy as INCI", () => {
  assert.equal(
    looksLikeFullIngredientListText(
      "Cloud-like hydrating mist instantly calms and nourishes skin, while making it look and feel more plump, smooth, and ready for makeup.",
    ),
    false,
  );
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

test("deriveProductPdpModuleBodies does not turn long ingredient disclaimers into active ingredients", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "Ingredients",
        body:
          "PETA-certified vegan and cruelty-free.\n\n100% Cold Pressed Organic Tamanu Oil:\n\nContains antioxidants to protect skin from premature aging and skin damage\nImproves skin hydration, reduces dryness and inflammation, and prevents moisture loss\nPromotes skin regeneration to reduce the appearance of scars and stretch marks\nHydrates hair strands and soothes sensitive scalps to encourage stronger, longer, & healthier hair growth\n\nNOTE:\nTamanu Oil is derived from Tamanu nuts. Though most people do not have issues using this ingredient, people with nut allergies could have a potential reaction.",
        source_kind: "accordion_ingredients",
      },
    ],
  });

  assert.equal(bodies.ingredientsRaw, undefined);
  assert.equal(bodies.activeIngredientsRaw, undefined);
});

test("deriveProductPdpModuleBodies prefers instructional how-to text over mislabeled marketing blurbs", () => {
  const bodies = deriveProductPdpModuleBodies({
    howToUseText:
      "Cloud-like hydrating mist instantly calms and nourishes skin, while making it look and feel more plump, smooth, and ready for makeup.",
    detailsSections: [
      {
        heading: "How to Use",
        body: "Shake well to fully mix. With eyes closed, spritz 2-4 times at least 10 inches away from your face.",
        source_kind: "modal_content",
      },
    ],
  });

  assert.match(bodies.howToUseRaw || "", /Shake well to fully mix/i);
  assert.doesNotMatch(bodies.howToUseRaw || "", /Cloud-like hydrating mist/i);
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

test("deriveProductPdpModuleBodies ignores 'What's in it' marketing blurbs when a modal INCI list exists", () => {
  const fullInci =
    "Water/Aqua/Eau, Caprylic/Capric Triglyceride, Glycerin, Dipropylene Glycol, Niacinamide, 1,2-Hexanediol, Butylene Glycol, Panthenol";
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "What's in it?",
        body:
          "Cloud-like hydrating mist instantly calms and nourishes skin, while making it look and feel more plump, smooth, and ready for makeup.",
        source_kind: "accordion_button",
      },
      {
        heading: "Ingredients",
        body: fullInci,
        source_kind: "modal_content",
      },
    ],
  });

  assert.equal(bodies.ingredientsRaw, fullInci);
  assert.equal(bodies.activeIngredientsRaw, undefined);
});

test("filterUsefulFaqItems removes promo and pseudo-faq noise but keeps product questions", () => {
  const items = filterUsefulFaqItems([
    {
      question: "What's in it?",
      answer: "Cloud-like hydrating mist instantly calms and nourishes skin.",
      source_kind: "accordion_question_answer",
    },
    {
      question: "How to Pair",
      answer: "Shop Now with our matching serum.",
      source_kind: "accordion_question_answer",
    },
    {
      question: "Be the first to be in the know, y’know?",
      answer: "Plus save 10% on your first order.",
      source_kind: "accordion_question_answer",
    },
    {
      question: "Can I use Always an Optimist 4-in-1 Mist as a setting spray?",
      answer: "You can. It helps extend makeup wear with a natural radiant finish.",
      source_kind: "inline_html_faq",
    },
  ]);

  assert.deepEqual(items, [
    {
      question: "Can I use Always an Optimist 4-in-1 Mist as a setting spray?",
      answer: "You can. It helps extend makeup wear with a natural radiant finish.",
      source_kind: "inline_html_faq",
    },
  ]);
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

test("extractDelimitedLabeledSectionText supports Shopify labels split across headings and paragraphs", () => {
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

test("extractShopifyBodyHtmlPdpFields parses Round Lab-style body_html sections", () => {
  const fields = extractShopifyBodyHtmlPdpFields(`
    <p>This mask is made with a pure cotton sheet mask.</p>
    <p><strong>How it works</strong></p>
    <ul><li>Hydrating sheet mask that contains 91.45% pure Mugwort extract.</li></ul>
    <p><strong>Ingredients</strong></p>
    <p><strong>Active Ingredients:</strong> Glycine, Madecassoside</p>
    <p><strong>Full Ingredients:</strong> Water, Glycine, Methylpropanediol, Artemisia Vulgaris Extract</p>
    <p><strong>How to use</strong></p>
    <ol><li>After cleansing the face, tidy up the skin texture using toner.</li></ol>
  `);

  assert.equal(fields.detailsSections.length, 1);
  assert.equal(fields.detailsSections[0]?.heading, "Benefits");
  assert.match(fields.detailsSections[0]?.body || "", /91\.45% pure Mugwort extract/i);
  assert.match(fields.ingredientsRaw || "", /Water, Glycine, Methylpropanediol/i);
  assert.equal(fields.activeIngredientsRaw, "Glycine, Madecassoside");
  assert.equal(fields.howToUseRaw, "After cleansing the face, tidy up the skin texture using toner.");
});

test("extractShopifyBodyHtmlPdpFields parses Round Lab uppercase h3 PDP labels", () => {
  const fields = extractShopifyBodyHtmlPdpFields(`
    <p><strong>Sculpt, cool, and revive—one mask does it all.</strong></p>
    <hr>
    <h3>HOW IT WORKS</h3>
    <ul>
      <li><strong>Thread‑Lift Fabric</strong> — Hugs the jawline and pulls upward for a lifted V-shape.</li>
      <li><strong>Instant Cooling Hydrogel</strong> — Cools on contact to soothe heat and reduce puffiness.</li>
    </ul>
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
  `);

  assert.equal(fields.detailsSections.length, 1);
  assert.equal(fields.detailsSections[0]?.heading, "Benefits");
  assert.match(fields.detailsSections[0]?.body || "", /Thread‑Lift Fabric/i);
  assert.match(fields.activeIngredientsRaw || "", /Jeju Camellia Flower Extract/i);
  assert.match(fields.howToUseRaw || "", /hook over each ear/i);
  assert.match(fields.howToUseRaw || "", /Relax for 20-30 minutes/i);
});

test("extractShopifyBodyHtmlPdpFields treats usage headings as how-to labels", () => {
  const fields = extractShopifyBodyHtmlPdpFields(`
    <h3>Usage</h3>
    <p>Shake well before use. Spritz two to four times across the face.</p>
    <h3>Full Ingredients</h3>
    <p>Water, Glycerin, Niacinamide, Panthenol</p>
  `);

  assert.equal(fields.howToUseRaw, "Shake well before use. Spritz two to four times across the face.");
  assert.equal(fields.ingredientsRaw, "Water, Glycerin, Niacinamide, Panthenol");
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
  assert.equal(fields.detailsSections.length, 1);
  assert.equal(fields.detailsSections[0]?.heading, "Benefits");
  assert.match(fields.detailsSections[0]?.body || "", /91\.45% pure Mugwort extract/i);
  assert.equal(fields.activeIngredientsRaw, "Glycine, Madecassoside");
  assert.match(fields.ingredientsRaw || "", /Water, Glycine, Methylpropanediol/i);
  assert.equal(fields.howToUseRaw, "After cleansing the face, tidy up the skin texture using toner.");
  assert.deepEqual(fields.imageUrls, ["//roundlab.com/cdn/shop/files/mugwort-calming-sheet-mask-round-lab-1.png?v=1772849529"]);
});

test("extractShopifyEmbeddedProductPayloadPdpFields parses sgGlobalVars currentProduct payloads", () => {
  const script = `sgGlobalVars.currentProduct = ${JSON.stringify({
    description: "<p>A multipurpose oil that promotes healthy skin and hair.</p><p><strong>Intention Til the Last Drop</strong></p><p>Supports the local community where the oil is sourced.</p>",
    content: "<p>A multipurpose oil that promotes healthy skin and hair.</p>",
    images: ["//kravebeauty.com/cdn/shop/files/topdp1.png?v=1699393313"],
  })};`;

  const fields = extractShopifyEmbeddedProductPayloadPdpFields([script]);

  assert.match(fields.descriptionRaw || "", /promotes healthy skin and hair/i);
  assert.deepEqual(fields.imageUrls, ["//kravebeauty.com/cdn/shop/files/topdp1.png?v=1699393313"]);
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
      product_info_tab_2_body: {
        type: "root",
        children: [
          {
            type: "paragraph",
            children: [{ type: "text", value: "Lightweight gel texture for daily use." }],
          },
        ],
      },
      product_info_tab_3_key_ingredients: {
        type: "root",
        children: [
          {
            type: "paragraph",
            children: [{ type: "text", value: "Niacinamide, Centella Asiatica Extract" }],
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
    ["Benefits", "Details", "Key Ingredients", "Ingredients", "How to Use"],
  );
});

test("extractInlineFaqItemsFromHtml parses Rare-style inline FAQ blocks inside usage content", () => {
  const items = extractInlineFaqItemsFromHtml(`
    <p>Shake well before use.<br><br><h3>Face Mist FAQs</h3><br>
    <b>What does Always an Optimist 4-in-1 Mist do?</b><br>
    This face mist instantly hydrates skin and refreshes your look.<br><br>
    <b>Can I use Always an Optimist 4-in-1 Mist as a setting spray?</b><br>
    You can. It helps foundation look smooth and seamless.</p>
  `);

  assert.equal(items.length, 2);
  assert.equal(items[0]?.question, "What does Always an Optimist 4-in-1 Mist do?");
  assert.match(items[0]?.answer || "", /instantly hydrates skin/i);
  assert.equal(items[1]?.question, "Can I use Always an Optimist 4-in-1 Mist as a setting spray?");
  assert.match(items[1]?.answer || "", /smooth and seamless/i);
});

test("deriveProductPdpModuleBodies does not misclassify glossary-style INCI decode as active ingredients", () => {
  const bodies = deriveProductPdpModuleBodies({
    detailsSections: [
      {
        heading: "The Inci Decoded",
        body:
          "Carrier Water Solvent Butylene Glycol Humectant Glycerin Skin Conditioner Ceramide NP Phytosphingosine",
        source_kind: "details_summary",
      },
    ],
  });

  assert.equal(bodies.ingredientsRaw, undefined);
  assert.equal(bodies.activeIngredientsRaw, undefined);
});

test("productHasMissingPdpFields treats partial Shopify PDP modules as incomplete", () => {
  assert.equal(
    productHasMissingPdpFields({
      title: "Balancing Face Mist",
      url: "https://byoma.com/products/balancing-face-mist",
      image_url: "https://cdn.example.com/a.jpg",
      image_urls: ["https://cdn.example.com/a.jpg"],
      variant_skus: [],
      variants: [],
      description_raw: "Face mist overview",
      details_sections: [
        {
          heading: "Benefits",
          body: "Hydrates and supports the barrier.",
          source_kind: "details_summary",
        },
      ],
      ingredients_raw: "Water, Glycerin, Ceramide NP",
      field_capture_status: {
        description_raw: "present",
        details_sections: "present",
        ingredients_raw: "present",
        active_ingredients_raw: "missing",
        how_to_use_raw: "missing",
        faq_items: "missing",
      },
      field_sources: {
        description_raw: ["shopify_body_html"],
        details_sections: ["details_summary"],
        ingredients_raw: ["page_ingredients_section"],
        active_ingredients_raw: [],
        how_to_use_raw: [],
        faq_items: [],
      },
    } as any),
    true,
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

test("discoverProductUrls falls through from a 404 direct PDP to sitemap and ranks the closest replacement first", async () => {
  const diagnostics = createDiagnostics("sigmabeauty.com", "https://sigmabeauty.com");

  await withMockFetch(
    {
      "https://sigmabeauty.com/products/the-award-winning-brush-set": {
        status: 404,
        headers: { "content-type": "text/html; charset=utf-8" },
        body: "<html><body><h1>Not found</h1></body></html>",
      },
      "https://sigmabeauty.com/robots.txt": {
        status: 200,
        headers: { "content-type": "text/plain; charset=utf-8" },
        body: "Sitemap: https://sigmabeauty.com/sitemap_products_1.xml?from=1&to=9999999999\n",
      },
      "https://sigmabeauty.com/sitemap_products_1.xml?from=1&to=9999999999": {
        status: 200,
        headers: { "content-type": "application/xml; charset=utf-8" },
        body: `
          <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://sigmabeauty.com/products/e33-detail-diffused-crease</loc></url>
            <url><loc>https://sigmabeauty.com/products/the-award-winning-brush-set-1</loc></url>
            <url><loc>https://sigmabeauty.com/products/f80-flat-kabuki-brush</loc></url>
          </urlset>
        `,
      },
    },
    async () => {
      const discovered = await discoverProductUrls({
        baseUrl: "https://sigmabeauty.com",
        seedUrl: "https://sigmabeauty.com/products/the-award-winning-brush-set",
        maxProducts: 5,
        context: {},
        diagnostics,
      });

      assert.equal(diagnostics.discovery_strategy, "sitemap");
      assert.equal(diagnostics.failure_category, null);
      assert.equal(discovered.productUrls[0], "https://sigmabeauty.com/products/the-award-winning-brush-set-1");
      assert.ok(
        diagnostics.http_trace.some(
          (entry) => entry.url === "https://sigmabeauty.com/products/the-award-winning-brush-set" && entry.status === 404,
        ),
      );
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

test("resolveLocalBrowserConfig defaults Linux runtimes to chrome-headless-shell", () => {
  const config = resolveLocalBrowserConfig("linux", {});
  assert.equal(config.launchBrowser, undefined);
  assert.equal(config.headless, "shell");
});

test("resolveLocalBrowserConfig respects explicit chrome override", () => {
  const config = resolveLocalBrowserConfig("linux", {
    PUPPETEER_BROWSER: "chrome",
    PUPPETEER_EXECUTABLE_PATH: "/tmp/custom-chrome",
  });
  assert.equal(config.launchBrowser, "chrome");
  assert.equal(config.headless, true);
  assert.equal(config.executablePath, "/tmp/custom-chrome");
});

test("resolveLocalBrowserConfig respects explicit chrome-headless-shell override", () => {
  const config = resolveLocalBrowserConfig("darwin", {
    PUPPETEER_BROWSER: "chrome-headless-shell",
  });
  assert.equal(config.launchBrowser, undefined);
  assert.equal(config.headless, "shell");
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
