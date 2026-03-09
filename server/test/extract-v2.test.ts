import assert from "node:assert/strict";
import test from "node:test";

import {
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
