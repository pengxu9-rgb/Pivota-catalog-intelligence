import assert from "node:assert/strict";
import test from "node:test";

import { enrichDirectShopifyPdpResponse } from "../src/services/extractors/puppeteer";
import {
  THEME_SECTION_SOURCE_KIND,
  extractShopifyThemeBrandSections,
  hasBrandDescriptiveSection,
  parseHtmlTree,
} from "../src/services/extractors/themeSections";

// The markup below is shaped like the prod pages measured 2026-09-29 (class names and nesting kept,
// copy replaced): supergoop.com, meritbeauty.com, jurlique.com / sokoglam.com, anua.us, celimax.us,
// tatcha.com, makeupforever.sg, us.glasshousefragrances.com.
const WHY = "This silky, ultra-light formula protects skin and is made for year-round daily wear under makeup.";
const FEEL = "A watery lotion that feels lightweight and hydrating with a natural finish on every skin type.";

const page = (body: string) => `<!doctype html><html><head><title>P</title>
  <script>window.x = "<script>not a tag</script>"; if (a < b) { document.write("<div>"); }</script>
  <style>.h2 { color: red } </style></head><body class="template-product">${body}</body></html>`;

test("supergoop-style tabs: a heading followed by its text, rendered twice, is one section", () => {
  const block = `<div class="flex flex-col"><div class="w-full"><h3 class="sub-h4 mt-[40px]">Why we made it</h3>
      <div class="rte"><p>${WHY}</p></div></div>
    <div class="w-full"><h3 class="sub-h4">How it feels</h3><div class="rte"><p>${FEEL}</p></div></div></div>`;
  const sections = extractShopifyThemeBrandSections(page(`
    <main><section class="bg-grey-FA"><div class="lg:py-[60px]"><tab-list class="hidden lg:block">${block}</tab-list>
      <div class="space-y-[10px]">${block}</div></div></section></main>`));
  assert.deepEqual(sections, [
    { heading: "Why we made it", body: WHY, source_kind: THEME_SECTION_SOURCE_KIND },
    { heading: "How it feels", body: FEEL, source_kind: THEME_SECTION_SOURCE_KIND },
  ]);
});

test("merit-style cards: the mobile heading inside a button is skipped, the desktop one reads the card", () => {
  const sections = extractShopifyThemeBrandSections(page(`
    <ul class="ProductInfoCards"><li class="ProductInfoCards__card">
      <button class="ProductInfoCards__button"><h3 class="ProductInfoCards__title--mobile">WHAT IT IS</h3></button>
      <h3 class="ProductInfoCards__title--desktop">WHAT IT IS</h3>
      <div class="ProductInfoCards__content"><p>Get compliments on your skin, not your makeup: a sheer tint for small edits only where you need them.</p></div>
    </li></ul>`));
  assert.equal(sections.length, 1);
  assert.equal(sections[0].heading, "WHAT IT IS");
  assert.match(sections[0].body, /^Get compliments on your skin/);
});

test("details/summary accordions read the panel, whatever element carries the label", () => {
  const sections = extractShopifyThemeBrandSections(page(`
    <div class="product__accordion"><details id="Details-collapsible_tab"><summary><div class="summary__title"><h2 class="h4">Details</h2></div></summary>
      <div class="accordion__content"><p>Revitalizing lightweight gel cream delivers a burst of hydration to boost the skin barrier all day.</p></div></details></div>
    <accordion-disclosure class="accordion"><details class="accordion__disclosure"><summary><span class="accordion__toggle"><span class="text-with-icon">Benefits</span></span></summary>
      <div class="accordion__content"><p>● No White Cast ─ Clear non greasy formula blends into all skin tones and works as a makeup base.</p></div></details></accordion-disclosure>
    <details><summary><h2>How to Use</h2></summary><p>Apply a pearl-sized amount to damp skin and massage in gentle circles.</p></details>`));
  assert.deepEqual(sections.map((s) => s.heading), ["Details", "Benefits"]);
  assert.match(sections[0].body, /^Revitalizing lightweight gel cream/);
  assert.doesNotMatch(sections[0].body, /Details/);
});

test("theme prose blocks: a class-styled heading reads the paragraphs after it", () => {
  const sections = extractShopifyThemeBrandSections(page(`
    <image-with-text class="image-with-text"><div class="prose"><p class="h2">What It Is</p>
      <p>This creamy toner replenishes the skin with essential ceramides, lipids and hyaluronic acid for soft, hydrated skin.</p>
      <p>Use it morning and night after cleansing.</p></div></image-with-text>
    <div class="section-stack"><div class="prose"><span class="subheading">Benefits</span>
      <ul><li>Featherweight nourishment rich in fatty acids</li><li>Imparts a subtle gleam on skin</li><li>Locks in moisture on face, hair and body</li></ul></div></div>`));
  assert.deepEqual(sections.map((s) => s.heading), ["What It Is", "Benefits"]);
  assert.equal(sections[0].body.split("\n").length, 2);
  assert.equal(sections[1].body, "Featherweight nourishment rich in fatty acids\nImparts a subtle gleam on skin\nLocks in moisture on face, hair and body");
});

test("a section heading's following siblings stop at the next heading", () => {
  const sections = extractShopifyThemeBrandSections(page(`
    <section class="good-to-know"><h2 class="good-to-know__title">Why you will love it</h2>
      <p>This multi-purpose brush's soft, long fibers blend and lightly apply a range of eyeshadow formulas.</p>
      <h2>How to use</h2><p>Sweep over the lid and blend the edges outward for a diffused finish.</p></section>`));
  assert.equal(sections.length, 1);
  assert.doesNotMatch(sections[0].body, /Sweep over the lid/);
});

for (const [name, region] of [
  ["a header", (inner: string) => `<header class="site-header">${inner}</header>`],
  ["a nav", (inner: string) => `<nav>${inner}</nav>`],
  ["a footer", (inner: string) => `<footer>${inner}</footer>`],
  ["a mega menu", (inner: string) => `<div class="mega-menu__navigation">${inner}</div>`],
  ["a reviews widget", (inner: string) => `<div data-oke-widget class="okendo-reviews">${inner}</div>`],
  ["a recommendations rail", (inner: string) => `<div class="product-recommendations">${inner}</div>`],
  ["a cart drawer", (inner: string) => `<cart-drawer class="drawer">${inner}</cart-drawer>`],
] as const) {
  test(`a descriptive heading inside ${name} is not the product's copy`, () => {
    const inner = `<h3>Details</h3><p>A section about something else entirely, long enough to be taken as real prose copy.</p>`;
    assert.deepEqual(extractShopifyThemeBrandSections(page(region(inner))), []);
  });
}

for (const heading of ["How to Use", "Ingredients", "Key Ingredients", "FAQ", "Reviews", "Shipping & Returns", "You may also like"]) {
  test(`"${heading}" is not a descriptive heading`, () => {
    const html = page(`<h3>${heading}</h3><p>Prose long enough to be kept as copy if the heading were descriptive at all.</p>`);
    assert.deepEqual(extractShopifyThemeBrandSections(html), []);
  });
}

for (const heading of ["Details", "Product Details", "Overview", "Description", "Product Description", "Benefits",
  "Key Benefits", "Product Benefit", "What it is", "What it does", "Why we made it", "Why you'll love it",
  "Why You Will Love It", "Why we love it", "How it feels", "Highlights", "Inspiration", "DETAILS:"]) {
  test(`"${heading}" is a descriptive heading`, () => {
    const html = page(`<h3>${heading}</h3><p>Prose long enough to be kept as copy for this descriptive heading here.</p>`);
    assert.equal(extractShopifyThemeBrandSections(html).length, 1);
  });
}

test("a section needs 60 chars and 8 words of its own text", () => {
  assert.deepEqual(extractShopifyThemeBrandSections(page(`<h3>Details</h3><p>Net wt. 30 ml / 1.01 fl oz.</p>`)), []);
  assert.deepEqual(extractShopifyThemeBrandSections(page(`<h3>Details</h3><p>It is a tint for the lips and the cheeks.</p>`)), []);
  assert.deepEqual(
    extractShopifyThemeBrandSections(page(`<h3>Details</h3><p>Supercalifragilisticexpialidocious-extraordinarily-long-compound.</p>`)),
    [],
  );
});

test("a long section is clipped at a sentence under 2,000 chars", () => {
  const sentence = "The buildable formula blends out easily and stays put all day long. ";
  const [section] = extractShopifyThemeBrandSections(page(`<h3>Details</h3><p>${sentence.repeat(60)}</p>`));
  assert.ok(section.body.length <= 2000);
  assert.ok(section.body.endsWith("."));
});

test("entities are decoded and an inline script cannot swallow the page", () => {
  const [section] = extractShopifyThemeBrandSections(page(
    `<h3>Details</h3><p>Rich in fatty acids &amp; vitamin&nbsp;E, it&#39;s made for dry skin &ndash; day and night &#8212; all year.</p>`));
  assert.equal(section.body, "Rich in fatty acids & vitamin E, it's made for dry skin – day and night — all year.");
});

test("markup inside a script's strings is never read as the page", () => {
  const html = page(`<script>document.write('<h3>Details</h3><p>Script-only copy that must never be read as a section of this page.</p>');</script>
    <h3>Overview</h3><p>The real overview of the product, long enough to be kept as this page's own copy.</p>`);
  const sections = extractShopifyThemeBrandSections(html);
  assert.deepEqual(sections.map((s) => s.heading), ["Overview"]);
});

test("text inside a button is interface, not copy", () => {
  const html = page(`<button class="toggle"><h3>Details</h3><span>Tap to expand the full description of this product and its long list of benefits.</span></button>`);
  assert.deepEqual(extractShopifyThemeBrandSections(html), []);
});

test("an accordion panel is read whole, its own subheadings included", () => {
  const [section] = extractShopifyThemeBrandSections(page(`<details><summary>Details</summary>
    <div class="panel"><h3>The formula</h3><p>A lightweight gel with squalane and ceramides that absorbs in seconds without residue.</p></div></details>`));
  assert.equal(section.heading, "Details");
  assert.match(section.body, /The formula\nA lightweight gel/);
});

test("a heading alone in its wrapper reads the wrapper's following siblings", () => {
  const [section] = extractShopifyThemeBrandSections(page(`<div class="block"><div class="block__head"><h3>Why we made it</h3></div>
    <div class="block__text"><p>${WHY}</p></div></div>`));
  assert.equal(section.body, WHY);
});

test("the tree survives unclosed and stray tags", () => {
  const root = parseHtmlTree(`<div><p>one<p>two</span></div><br><img src=x>three`);
  // the div (holding both paragraphs; the stray </span> is ignored), the two void tags, the text
  assert.deepEqual(root.children.map((c) => (typeof c === "string" ? c : c.tag)), ["div", "br", "img", "three"]);
  const div = root.children[0] as { children: Array<{ tag: string } | string> };
  assert.deepEqual(div.children.map((c) => (typeof c === "string" ? c : c.tag)), ["p"]);
});

test("hasBrandDescriptiveSection needs a descriptive heading with real text", () => {
  const body = "Prose long enough to be kept as copy for this descriptive heading here.";
  assert.equal(hasBrandDescriptiveSection([{ heading: "Details", body, source_kind: "details_summary" }]), true);
  assert.equal(hasBrandDescriptiveSection([{ heading: "How to Use", body, source_kind: "details_summary" }]), false);
  assert.equal(hasBrandDescriptiveSection([{ heading: "Details", body: "Short.", source_kind: "x" }]), false);
  assert.equal(hasBrandDescriptiveSection(undefined), false);
});

// ----------------------------------------------------------------- through enrichDirectShopifyPdpResponse

const SEED_URL = "https://supergoop.com/products/city-sunscreen-serum";
const IMG = (n: number) => `https://cdn.shopify.com/s/files/1/0000/0000/files/city_${n}.jpg?v=1`;

function directResponse(detailsSections: Array<{ heading: string; body: string; source_kind: string }> = []) {
  return {
    brand: "Supergoop!",
    domain: "https://supergoop.com",
    mode: "puppeteer" as const,
    platform: "Shopify (Direct PDP)",
    products: [
      {
        title: "City Sunscreen Serum SPF 30",
        url: SEED_URL,
        image_url: IMG(0),
        image_urls: [IMG(0), IMG(1), IMG(2)],
        variant_skus: ["CITY30"],
        variants: [
          {
            id: "1", sku: "CITY30", url: SEED_URL, option_name: "Size", option_value: "50 ml", price: "36.00",
            currency: "USD", stock: "In Stock", description: "", image_url: IMG(0), image_urls: [IMG(0), IMG(1), IMG(2)],
            ad_copy: "",
          },
        ],
        description_raw: "A silky, ultra-light daily serum with broad spectrum SPF 30 for every skin type.",
        details_sections: detailsSections,
        faq_items: [],
        field_sources: {
          description_raw: ["shopify_body_html"],
          details_sections: detailsSections.map((s) => s.source_kind),
          ingredients_raw: [], active_ingredients_raw: [], how_to_use_raw: [], faq_items: [],
        },
      },
    ],
    variants: [],
    pricing: { currency: "USD", min: 36, max: 36, avg: 36 },
    ad_copy: { by_variant_id: {} },
    pagination: { offset: 0, limit: 1, next_offset: null, has_more: false, discovered_urls: 1 },
    diagnostics: {
      requested_domain: "supergoop.com", resolved_base_url: "https://supergoop.com",
      discovery_strategy: "shopify_json" as const, failure_category: null, block_provider: null, http_trace: [],
    },
  } as any;
}

async function withPage(html: string, fn: (fetches: string[]) => Promise<void>) {
  const original = globalThis.fetch;
  const fetches: string[] = [];
  globalThis.fetch = (async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    fetches.push(url);
    if (url !== SEED_URL) throw new Error(`Unexpected fetch: ${url}`);
    return new Response(html, { status: 200, headers: { "content-type": "text/html" } });
  }) as typeof fetch;
  try {
    await fn(fetches);
  } finally {
    globalThis.fetch = original;
  }
}

const HOW_TO = { heading: "How to Use", body: "Apply generously 15 minutes before sun exposure and reapply every 2 hours.", source_kind: "accordion_how_to_use" };
const PAGE = page(`<main><div class="w-full"><h3 class="sub-h4">Why we made it</h3><div><p>${WHY}</p></div></div>
  <section><h2>How to Use</h2><p>Apply generously before sun exposure.</p></section></main>`);

test("the enrichment adds the theme sections last, after the browser pass it would otherwise have suppressed", async () => {
  const logs: string[] = [];
  const response = directResponse();
  await withPage(PAGE, async (fetches) => {
    const result = await enrichDirectShopifyPdpResponse({
      brand: "Supergoop!", baseUrl: "https://supergoop.com", seedUrl: SEED_URL, response,
      diagnostics: response.diagnostics, log: (_t, msg) => logs.push(msg),
      browserRunner: async () => ({ mode: "managed", result: { ...response.products[0], details_sections: [HOW_TO] } }) as any,
    });
    const sections = result.products[0].details_sections || [];
    assert.deepEqual(sections.map((s: any) => [s.heading, s.source_kind]), [
      ["How to Use", "accordion_how_to_use"],
      ["Why we made it", THEME_SECTION_SOURCE_KIND],
    ]);
    // the product had no sections, so the browser pass still ran
    assert.match(logs.join("\n"), /requires browser enrichment for .*pdp_structured_sections/);
    assert.match(logs.join("\n"), /Recovered 1 Shopify theme brand sections/);
    assert.equal(fetches.filter((u) => u === SEED_URL).length, 1); // the page HTML is fetched once
    assert.ok(result.products[0].field_sources?.details_sections?.includes(THEME_SECTION_SOURCE_KIND));
  });
});

test("a product that already has a descriptive section is left as it is", async () => {
  const details = { heading: "Details", body: "A silky, ultra-light daily serum that sits invisibly under makeup all day.", source_kind: "details_summary" };
  const response = directResponse([details, HOW_TO]);
  await withPage(PAGE, async () => {
    const result = await enrichDirectShopifyPdpResponse({
      brand: "Supergoop!", baseUrl: "https://supergoop.com", seedUrl: SEED_URL, response,
      diagnostics: response.diagnostics, log: () => {},
      browserRunner: async () => ({ mode: "managed", result: null }) as any,
    });
    const headings = (result.products[0].details_sections || []).map((s: any) => s.heading);
    assert.ok(!headings.includes("Why we made it"));
  });
});

test("a page with no descriptive section changes nothing", async () => {
  const response = directResponse([HOW_TO]);
  await withPage(page(`<main><h2>How to Use</h2><p>Apply generously before sun exposure every day.</p></main>`), async () => {
    const result = await enrichDirectShopifyPdpResponse({
      brand: "Supergoop!", baseUrl: "https://supergoop.com", seedUrl: SEED_URL, response,
      diagnostics: response.diagnostics, log: () => {},
      browserRunner: async () => ({ mode: "managed", result: null }) as any,
    });
    assert.ok(!(result.products[0].field_sources?.details_sections || []).includes(THEME_SECTION_SOURCE_KIND));
  });
});
