import assert from "node:assert/strict";
import test from "node:test";

import {
  buildOffersFromScrapedPage,
  buildSourceProductId,
  computeCounters,
  parsePrice,
  resolveCurrency,
  resolveMarketSwitchStatus,
} from "../src/services/extractors/extractV2";
import type { MarketProfile, OfferV2 } from "../src/services/extractors/types";

test("resolveCurrency prefers structured priceCurrency with high confidence", () => {
  const out = resolveCurrency({
    structuredCurrency: "jpy",
    metaCurrencyCandidates: ["USD"],
    priceDisplayRaw: "$19.99",
    marketId: "JP",
  });

  assert.equal(out.code, "JPY");
  assert.equal(out.confidence, "high");
});

test("source_product_id generation is stable and does not embed price", () => {
  const id1 = buildSourceProductId({
    sourceSite: "www.example.com",
    canonicalUrl: "https://www.example.com/products/a?utm_source=x",
    sku: "SKU-123",
  });

  const id2 = buildSourceProductId({
    sourceSite: "www.example.com",
    canonicalUrl: "https://www.example.com/products/a?utm_source=y",
    sku: "SKU-123",
  });

  assert.equal(id1.includes("19.99"), false);
  assert.equal(id2.includes("19.99"), false);
  assert.notEqual(id1.length, 0);
  assert.notEqual(id2.length, 0);
});

test("market mismatch flags JP expected JPY but observed non-JPY", () => {
  const status = resolveMarketSwitchStatus("USD", "JPY", false);
  assert.equal(status, "mismatch");
});

test("symbol fallback maps $ to SGD under SG market and remains low confidence", () => {
  const out = resolveCurrency({
    structuredCurrency: null,
    metaCurrencyCandidates: [],
    priceDisplayRaw: "$25.00",
    marketId: "SG",
  });

  assert.equal(out.code, "SGD");
  assert.equal(out.confidence, "low");
});

test("symbol fallback maps ¥ to JPY under JP market and remains low confidence", () => {
  const out = resolveCurrency({
    structuredCurrency: null,
    metaCurrencyCandidates: [],
    priceDisplayRaw: "¥3,200",
    marketId: "JP",
  });

  assert.equal(out.code, "JPY");
  assert.equal(out.confidence, "low");
});

test("computeCounters aggregates by site+market dimensions", () => {
  const profiles: MarketProfile[] = [
    {
      market_id: "US",
      country: "US",
      currency_target: "USD",
      locale: "en-US",
      headers: {},
      cookies: {},
      url_params: {},
    },
    {
      market_id: "JP",
      country: "JP",
      currency_target: "JPY",
      locale: "ja-JP",
      headers: {},
      cookies: {},
      url_params: {},
    },
  ];

  const offers: OfferV2[] = [
    {
      source_site: "example.com",
      source_product_id: "example.com:p1",
      url_canonical: "https://example.com/p1",
      market_id: "US",
      price_amount: 10,
      price_currency: "USD",
      price_display_raw: "$10",
      price_type: "list",
      tax_included: "unknown",
      captured_at: new Date().toISOString(),
      currency_confidence: "high",
      market_switch_status: "ok",
      market_context_debug: {
        headers: {},
        cookies: {},
        url_params: {},
        expected_currency: "USD",
        observed_currency: "USD",
      },
    },
    {
      source_site: "example.com",
      source_product_id: "example.com:p1",
      url_canonical: "https://example.com/p1",
      market_id: "JP",
      price_amount: 1000,
      price_currency: "USD",
      price_display_raw: "$1000",
      price_type: "list",
      tax_included: "unknown",
      captured_at: new Date().toISOString(),
      currency_confidence: "low",
      market_switch_status: "mismatch",
      market_context_debug: {
        headers: {},
        cookies: {},
        url_params: {},
        expected_currency: "JPY",
        observed_currency: "USD",
      },
    },
  ];

  const rows = computeCounters({
    sourceSite: "example.com",
    profiles,
    offers,
    marketFailures: new Map<string, boolean>(),
  });

  assert.equal(rows.length, 2);

  const us = rows.find((row) => row.market_id === "US");
  const jp = rows.find((row) => row.market_id === "JP");
  assert.ok(us);
  assert.ok(jp);
  assert.equal(us?.native_currency_hit_rate, 1);
  assert.equal(jp?.market_switch_fail_rate, 1);
});

test("parsePrice detects range", () => {
  const out = parsePrice("From $12 - $20");
  assert.equal(out.price_type, "range");
  assert.equal(out.range_min, 12);
  assert.equal(out.range_max, 20);
});

test("buildOffersFromScrapedPage extracts variant SKUs from ProductGroup hasVariant JSON-LD", () => {
  const offers = buildOffersFromScrapedPage({
    baseUrl: "https://www.guerlain.com",
    sourceSite: "www.guerlain.com",
    context: {
      market_id: "SG",
      headers: {},
      cookies: {},
      url_params: {},
      expected_currency: "SGD",
    },
    extracted: {
      title: "PARURE GOLD SKIN MESH CUSHION",
      canonical: "https://www.guerlain.com/sg/en-sg/p/parure-gold-skin-mesh-cushion-P062104.html",
      metaDescription: "Foundation",
      scripts: [
        JSON.stringify({
          "@context": "https://schema.org/",
          "@type": "ProductGroup",
          name: "PARURE GOLD SKIN MESH CUSHION",
          url: "https://www.guerlain.com/sg/en-sg/p/parure-gold-skin-mesh-cushion-P062104.html",
          hasVariant: [
            {
              "@type": "Product",
              name: "PARURE GOLD SKIN MESH CUSHION 00N Beige",
              sku: "G062104",
              color: "00N Beige",
              offers: {
                "@type": "Offer",
                url: "https://www.guerlain.com/sg/en-sg/p/parure-gold-skin-mesh-cushion-P062104.html?v=G062104",
                availability: "http://schema.org/InStock",
              },
            },
            {
              "@type": "Product",
              name: "PARURE GOLD SKIN MESH CUSHION 01N Pale Beige",
              sku: "G062105",
              color: "01N Pale Beige",
              offers: {
                "@type": "Offer",
                url: "https://www.guerlain.com/sg/en-sg/p/parure-gold-skin-mesh-cushion-P062104.html?v=G062105",
                availability: "http://schema.org/OutOfStock",
              },
            },
          ],
        }),
      ],
      metaCurrencies: ["SGD"],
      priceTexts: ["SGD 92"],
    },
    pageHtml: "<html><body><h1>Parure Gold</h1></body></html>",
    capturedAt: "2026-03-09T00:00:00.000Z",
  });

  assert.equal(offers.length, 2);
  assert.deepEqual(
    offers.map((offer) => offer.variant_sku),
    ["G062104", "G062105"],
  );
  assert.deepEqual(
    offers.map((offer) => offer.url_canonical),
    [
      "https://www.guerlain.com/sg/en-sg/p/parure-gold-skin-mesh-cushion-P062104.html?v=G062104",
      "https://www.guerlain.com/sg/en-sg/p/parure-gold-skin-mesh-cushion-P062104.html?v=G062105",
    ],
  );
});

test("buildOffersFromScrapedPage falls back to Product.sku when Offer.sku is missing", () => {
  const offers = buildOffersFromScrapedPage({
    baseUrl: "https://www.guerlain.com",
    sourceSite: "www.guerlain.com",
    context: {
      market_id: "US",
      headers: {},
      cookies: {},
      url_params: {},
      expected_currency: "USD",
    },
    extracted: {
      title: "AQUA ALLEGORIA PERLE",
      canonical: "https://www.guerlain.com/us/en-us/p/aqua-allegoria-perle-florabloom-perle---eau-de-parfum-P062203.html",
      metaDescription: "Fragrance",
      scripts: [
        JSON.stringify({
          "@context": "http://schema.org/",
          "@type": "Product",
          name: "AQUA ALLEGORIA PERLE Florabloom Perle - Eau de Parfum 125 ML / 4.22 OZ",
          sku: "G062203",
          url: "https://www.guerlain.com/us/en-us/p/aqua-allegoria-perle-florabloom-perle---eau-de-parfum-P062203.html",
          offers: {
            "@type": "Offer",
            url: "https://www.guerlain.com/us/en-us/p/aqua-allegoria-perle-florabloom-perle---eau-de-parfum-P062203.html?v=G062203",
            priceCurrency: "USD",
            price: "176.00",
            availability: "http://schema.org/InStock",
          },
        }),
      ],
      metaCurrencies: ["USD"],
      priceTexts: ["$176.00"],
    },
    pageHtml: "<html><body><h1>AQUA ALLEGORIA PERLE</h1></body></html>",
    capturedAt: "2026-03-09T00:00:00.000Z",
  });

  assert.equal(offers.length, 1);
  assert.equal(offers[0]?.variant_sku, "G062203");
  assert.equal(offers[0]?.price_amount, 176);
});
