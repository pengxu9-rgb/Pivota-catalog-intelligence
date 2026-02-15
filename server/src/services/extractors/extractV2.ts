import { createHash } from "crypto";
import puppeteer, { type Browser } from "puppeteer";

import { getMarketProfiles } from "./marketProfiles";
import type {
  CurrencyConfidence,
  ExtractV2RequestBody,
  ExtractV2Response,
  MarketId,
  MarketProfile,
  MarketSwitchStatus,
  OfferV2,
  PriceType,
  SiteMarketCounters,
} from "./types";

const DEFAULT_BATCH_LIMIT = 10;
const DEFAULT_MAX_TOTAL_PRODUCTS = 500;
const DEFAULT_CONCURRENCY = 3;
const DEFAULT_NAV_TIMEOUT_MS = 8_000;
const DEFAULT_FETCH_TIMEOUT_MS = 8_000;
const DEFAULT_LAUNCH_TIMEOUT_MS = 15_000;
const DEFAULT_SCRAPE_TIMEOUT_MS = 60_000;
const DEFAULT_PRODUCT_URL_RESERVE = 4;

const TRACKING_QUERY_PARAM_RE = /^(utm_|fbclid$|gclid$|mc_|_ga$|_gl$|ref$|source$)/i;

type Logger = (type: ExtractV2Response["logs"][number]["type"], msg: string) => void;

type RequestContext = {
  market_id: MarketId;
  headers: Record<string, string>;
  cookies: Record<string, string>;
  url_params: Record<string, string>;
  geo_hint?: string;
  expected_currency: string;
  shipping_destination?: string;
};

type ParseTargetResult = {
  domain: string;
  baseUrl: string;
  seedUrl?: string;
  collectionHandle?: string;
};

type PriceParseResult = {
  price_amount: number | null;
  price_type: PriceType;
  range_min?: number;
  range_max?: number;
};

type CurrencyResolution = {
  code: string | null;
  confidence: CurrencyConfidence;
};

type MarketExtractionResult = {
  offers: OfferV2[];
  failed: boolean;
};

type ShopifyProductsResponse = { products?: ShopifyProduct[] };

type ShopifyProduct = {
  id?: number;
  title?: string;
  handle?: string;
  variants?: ShopifyVariant[];
};

type ShopifyVariant = {
  id?: number;
  sku?: string | null;
  price?: string;
  available?: boolean;
  inventory_quantity?: number | null;
};

type ScrapedPageData = {
  title: string;
  canonical: string;
  scripts: string[];
  metaCurrencies: string[];
  priceTexts: string[];
};

type JsonObject = Record<string, unknown>;

export async function extractCatalogV2(input: ExtractV2RequestBody): Promise<ExtractV2Response> {
  const generatedAt = new Date().toISOString();
  const logs: ExtractV2Response["logs"] = [];
  const log: Logger = (type, msg) => {
    logs.push({ at: new Date().toISOString(), type, msg });
  };

  const target = parseTarget(input.domain);
  const baseUrl = target.baseUrl;
  const sourceSite = target.domain;
  const batchOffset = clampOptionalInt(input.offset, 0, 0, 100_000);
  const batchLimit = clampOptionalInt(
    input.limit,
    clampInt(process.env.BATCH_LIMIT || process.env.MAX_PRODUCTS, DEFAULT_BATCH_LIMIT, 1, 200),
    1,
    200,
  );
  const maxProductsTotal = clampInt(process.env.MAX_TOTAL_PRODUCTS, DEFAULT_MAX_TOTAL_PRODUCTS, 1, 10_000);
  const discoveryReserve = clampInt(process.env.PRODUCT_URL_RESERVE, DEFAULT_PRODUCT_URL_RESERVE, 0, 100);
  const discoveryLimit = Math.min(maxProductsTotal, batchOffset + batchLimit + discoveryReserve);

  const profiles = getMarketProfiles(input.markets);
  log("info", `V2 extraction start: ${sourceSite} | markets=${profiles.map((p) => p.market_id).join(",")}`);

  const offers: OfferV2[] = [];
  const marketFailures = new Map<string, boolean>();

  for (const profile of profiles) {
    const context = buildRequestContext(profile);
    log("info", `Extracting market=${context.market_id} expected=${context.expected_currency}`);

    try {
      const result = await extractSingleMarket({
        baseUrl,
        sourceSite,
        seedUrl: target.seedUrl,
        collectionHandle: target.collectionHandle,
        context,
        batchOffset,
        batchLimit,
        discoveryLimit,
        log,
      });
      offers.push(...result.offers);
      marketFailures.set(String(context.market_id), result.failed);
      log(
        result.failed ? "warn" : "success",
        `Market ${context.market_id} offers=${result.offers.length} failed=${result.failed ? "yes" : "no"}`,
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : "unknown";
      marketFailures.set(String(context.market_id), true);
      log("error", `Market ${context.market_id} extraction error: ${message}`);
    }
  }

  const countersBySiteMarket = computeCounters({ sourceSite, profiles, offers, marketFailures });

  return {
    brand: input.brand,
    domain: target.domain,
    generated_at: generatedAt,
    mode: "puppeteer",
    offers_v2: offers,
    counters_by_site_market: countersBySiteMarket,
    logs,
  };
}

export function buildRequestContext(profile: MarketProfile): RequestContext {
  const injectionEnabled = (process.env.EXTRACT_V2_MARKET_INJECTION_ENABLED || "1").toLowerCase() !== "0";
  return {
    market_id: profile.market_id,
    headers: injectionEnabled ? { ...profile.headers } : {},
    cookies: injectionEnabled ? { ...profile.cookies } : {},
    url_params: injectionEnabled ? { ...profile.url_params } : {},
    geo_hint: profile.geo_hint,
    expected_currency: profile.currency_target,
    shipping_destination: profile.shipping_destination,
  };
}

async function extractSingleMarket(params: {
  baseUrl: string;
  sourceSite: string;
  seedUrl?: string;
  collectionHandle?: string;
  context: RequestContext;
  batchOffset: number;
  batchLimit: number;
  discoveryLimit: number;
  log: Logger;
}): Promise<MarketExtractionResult> {
  const shopifyOffers = await tryExtractShopifyOffersV2({
    baseUrl: params.baseUrl,
    sourceSite: params.sourceSite,
    collectionHandle: params.collectionHandle,
    context: params.context,
    batchOffset: params.batchOffset,
    batchLimit: params.batchLimit,
    log: params.log,
  });

  if (shopifyOffers) {
    return {
      offers: shopifyOffers,
      failed: shopifyOffers.length === 0,
    };
  }

  const discovered = await discoverProductUrls({
    baseUrl: params.baseUrl,
    maxProducts: params.discoveryLimit,
    seedUrl: params.seedUrl,
    context: params.context,
    log: params.log,
  });

  const batchCandidates = discovered.productUrls.slice(
    params.batchOffset,
    params.batchOffset + params.batchLimit + DEFAULT_PRODUCT_URL_RESERVE,
  );

  if (batchCandidates.length === 0) {
    return { offers: [], failed: true };
  }

  const concurrency = clampInt(process.env.PUPPETEER_CONCURRENCY, DEFAULT_CONCURRENCY, 1, 6);
  const navigationTimeoutMs = clampInt(process.env.PUPPETEER_NAV_TIMEOUT_MS, DEFAULT_NAV_TIMEOUT_MS, 5_000, 120_000);
  const launchTimeoutMs = clampInt(process.env.PUPPETEER_LAUNCH_TIMEOUT_MS, DEFAULT_LAUNCH_TIMEOUT_MS, 5_000, 120_000);
  const scrapeTimeoutMs = clampInt(process.env.PUPPETEER_SCRAPE_TIMEOUT_MS, DEFAULT_SCRAPE_TIMEOUT_MS, 10_000, 300_000);

  const browser = await withTimeout(
    puppeteer.launch({
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
    }),
    launchTimeoutMs,
    "Puppeteer launch",
  );

  try {
    const capturedAt = new Date().toISOString();
    const productOfferBatches = await withTimeout(
      mapWithConcurrency(batchCandidates, concurrency, async (url) => {
        return scrapeProductPageV2({
          browser,
          url,
          baseUrl: params.baseUrl,
          sourceSite: params.sourceSite,
          context: params.context,
          navigationTimeoutMs,
          capturedAt,
        });
      }),
      scrapeTimeoutMs,
      "V2 product scraping",
    );

    const offers = productOfferBatches.flatMap((batch) => batch).slice(0, params.batchLimit * 20);
    return {
      offers,
      failed: offers.length === 0,
    };
  } finally {
    await browser.close().catch(() => undefined);
  }
}

async function tryExtractShopifyOffersV2(params: {
  baseUrl: string;
  sourceSite: string;
  collectionHandle?: string;
  context: RequestContext;
  batchOffset: number;
  batchLimit: number;
  log: Logger;
}): Promise<OfferV2[] | null> {
  const probeUrl = params.collectionHandle
    ? `${params.baseUrl}/collections/${params.collectionHandle}/products.json?limit=1`
    : `${params.baseUrl}/products.json?limit=1`;

  const probe = await fetchJson<ShopifyProductsResponse>(probeUrl, params.context);
  if (!probe || !Array.isArray(probe.products)) {
    return null;
  }

  const pageLimit = clampInt(process.env.SHOPIFY_MAX_PAGES, 20, 1, 200);
  const allProducts: ShopifyProduct[] = [];
  const feedPrefix = params.collectionHandle ? `/collections/${params.collectionHandle}` : "";

  for (let page = 1; page <= pageLimit; page++) {
    const url = `${params.baseUrl}${feedPrefix}/products.json?limit=250&page=${page}`;
    const batch = await fetchJson<ShopifyProductsResponse>(url, params.context);
    const products = batch?.products;
    if (!products || products.length === 0) break;
    allProducts.push(...products);
    if (products.length < 250) break;
  }

  const hintedCurrency = await fetchCurrencyHint(params.baseUrl, params.context);
  const capturedAt = new Date().toISOString();

  const offers: OfferV2[] = [];
  for (const product of allProducts.slice(params.batchOffset, params.batchOffset + params.batchLimit)) {
    const productHandle = (product.handle || "").trim();
    if (!productHandle) continue;
    const productTitle = (product.title || "").trim() || productHandle;

    const canonicalProductUrl = canonicalizeUrl(`${params.baseUrl}/products/${productHandle}`, params.baseUrl);
    const siteProductId = typeof product.id === "number" ? String(product.id) : "";

    for (const variant of product.variants || []) {
      const rawPrice = typeof variant.price === "string" ? variant.price.trim() : "";
      const priceDisplayRaw = rawPrice || null;
      const priceParsed = parsePrice(rawPrice || null);
      const resolvedCurrency = resolveCurrency({
        structuredCurrency: null,
        metaCurrencyCandidates: hintedCurrency ? [hintedCurrency] : [],
        priceDisplayRaw,
        marketId: params.context.market_id,
      });
      const status = resolveMarketSwitchStatus(resolvedCurrency.code, params.context.expected_currency, false);
      const sku = (variant.sku || "").trim();
      const variantSku = sku || (typeof variant.id === "number" ? String(variant.id) : "");

      const sourceProductId = buildSourceProductId({
        sourceSite: params.sourceSite,
        siteProductId,
        canonicalUrl: canonicalProductUrl,
        sku,
      });

      offers.push({
        source_site: params.sourceSite,
        source_product_id: sourceProductId,
        url_canonical: canonicalProductUrl,
        product_title: productTitle || null,
        variant_sku: variantSku || null,
        market_id: params.context.market_id,
        price_amount: priceParsed.price_amount,
        price_currency: resolvedCurrency.code,
        price_display_raw: priceDisplayRaw,
        price_type: priceParsed.price_type,
        range_min: priceParsed.range_min,
        range_max: priceParsed.range_max,
        tax_included: "unknown",
        availability: normalizeAvailabilityFromBoolean(variant.available, variant.inventory_quantity),
        captured_at: capturedAt,
        currency_confidence: resolvedCurrency.confidence,
        market_switch_status: status,
        market_context_debug: {
          headers: { ...params.context.headers },
          cookies: { ...params.context.cookies },
          url_params: { ...params.context.url_params },
          geo_hint: params.context.geo_hint,
          expected_currency: params.context.expected_currency,
          observed_currency: resolvedCurrency.code,
        },
      });
    }
  }

  params.log("data", `Shopify V2 offers=${offers.length} market=${params.context.market_id}`);
  return offers;
}

async function scrapeProductPageV2(params: {
  browser: Browser;
  url: string;
  baseUrl: string;
  sourceSite: string;
  context: RequestContext;
  navigationTimeoutMs: number;
  capturedAt: string;
}): Promise<OfferV2[]> {
  const page = await params.browser.newPage();
  await page.setUserAgent(process.env.PUPPETEER_USER_AGENT || "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36");
  page.setDefaultNavigationTimeout(params.navigationTimeoutMs);

  try {
    if (Object.keys(params.context.headers).length > 0) {
      await page.setExtraHTTPHeaders(params.context.headers);
    }

    const host = new URL(params.baseUrl).hostname;
    const cookieEntries = Object.entries(params.context.cookies);
    if (cookieEntries.length > 0) {
      await page.setCookie(
        ...cookieEntries.map(([name, value]) => ({
          name,
          value,
          domain: host,
          path: "/",
        })),
      );
    }

    const targetUrl = withUrlParams(params.url, params.context.url_params);
    await page.goto(targetUrl, { waitUntil: "domcontentloaded" });

    const extracted = await page.evaluate(() => {
      const title =
        document.querySelector("h1")?.textContent?.trim() ||
        document.querySelector('meta[property="og:title"]')?.getAttribute("content")?.trim() ||
        document.title ||
        "";

      const canonical =
        (document.querySelector('link[rel="canonical"]') as HTMLLinkElement | null)?.href ||
        document.querySelector('meta[property="og:url"]')?.getAttribute("content") ||
        location.href;

      const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
        .map((s) => s.textContent || "")
        .filter(Boolean);

      const metaCurrencySelectors = [
        'meta[property="og:price:currency"]',
        'meta[property="product:price:currency"]',
        'meta[name="priceCurrency"]',
      ];
      const metaCurrencies = metaCurrencySelectors
        .map((selector) => document.querySelector(selector)?.getAttribute("content")?.trim() || "")
        .filter(Boolean);

      const priceSelectors = [
        '[itemprop="price"]',
        'meta[property="og:price:amount"]',
        'meta[property="product:price:amount"]',
        '[class*="price"]',
        '[data-price]'
      ];
      const priceTexts: string[] = [];
      for (const selector of priceSelectors) {
        const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 5);
        for (const node of nodes) {
          const text =
            (node as HTMLElement).getAttribute?.("content") ||
            (node as HTMLElement).getAttribute?.("data-price") ||
            (node as HTMLElement).textContent ||
            "";
          const trimmed = text.trim();
          if (trimmed) priceTexts.push(trimmed);
        }
      }

      return {
        title,
        canonical,
        scripts,
        metaCurrencies,
        priceTexts,
      };
    });

    return buildOffersFromScrapedPage({
      baseUrl: params.baseUrl,
      sourceSite: params.sourceSite,
      context: params.context,
      extracted,
      capturedAt: params.capturedAt,
    });
  } catch {
    return [];
  } finally {
    await page.close().catch(() => undefined);
  }
}

function buildOffersFromScrapedPage(params: {
  baseUrl: string;
  sourceSite: string;
  context: RequestContext;
  extracted: ScrapedPageData;
  capturedAt: string;
}): OfferV2[] {
  const objects: JsonObject[] = [];

  for (const raw of params.extracted.scripts) {
    try {
      const parsed = JSON.parse(raw) as unknown;
      for (const obj of normalizeJsonLdValue(parsed)) {
        if (obj && typeof obj === "object") objects.push(obj as JsonObject);
      }
    } catch {
      // Ignore invalid JSON-LD blocks.
    }
  }

  const productObj = objects.find((obj) => isType(obj, "Product"));
  const productTitle =
    (typeof productObj?.name === "string" ? productObj.name.trim() : params.extracted.title.trim()) ||
    params.extracted.title.trim();

  const rawCanonical =
    (typeof productObj?.url === "string" ? productObj.url : params.extracted.canonical) || params.extracted.canonical;
  const canonicalUrl = canonicalizeUrl(toAbsoluteUrl(params.baseUrl, rawCanonical), params.baseUrl);
  const productIdCandidate = extractProductId(productObj);

  const offersRaw = normalizeJsonLdOffers(productObj?.offers);
  const fallbackPriceDisplay = params.extracted.priceTexts[0] || null;

  if (offersRaw.length === 0) {
    const parsed = parsePrice(fallbackPriceDisplay);
    const resolvedCurrency = resolveCurrency({
      structuredCurrency: null,
      metaCurrencyCandidates: params.extracted.metaCurrencies,
      priceDisplayRaw: fallbackPriceDisplay,
      marketId: params.context.market_id,
    });

    const status = resolveMarketSwitchStatus(resolvedCurrency.code, params.context.expected_currency, false);

    return [
      {
        source_site: params.sourceSite,
        source_product_id: buildSourceProductId({
          sourceSite: params.sourceSite,
          siteProductId: productIdCandidate,
          canonicalUrl,
          sku: productTitle,
        }),
        url_canonical: canonicalUrl,
        product_title: productTitle || null,
        variant_sku: null,
        market_id: params.context.market_id,
        price_amount: parsed.price_amount,
        price_currency: resolvedCurrency.code,
        price_display_raw: fallbackPriceDisplay,
        price_type: parsed.price_type,
        range_min: parsed.range_min,
        range_max: parsed.range_max,
        tax_included: "unknown",
        availability: undefined,
        captured_at: params.capturedAt,
        currency_confidence: resolvedCurrency.confidence,
        market_switch_status: status,
        market_context_debug: {
          headers: { ...params.context.headers },
          cookies: { ...params.context.cookies },
          url_params: { ...params.context.url_params },
          geo_hint: params.context.geo_hint,
          expected_currency: params.context.expected_currency,
          observed_currency: resolvedCurrency.code,
        },
      },
    ];
  }

  const offers: OfferV2[] = [];

  for (let idx = 0; idx < offersRaw.length; idx++) {
    const offer = offersRaw[idx]!;
    const skuRaw = typeof offer.sku === "string" ? offer.sku.trim() : "";
    const sku = skuRaw || `AUTO-${stableHash(`${canonicalUrl}|${idx}`)}`;

    const priceRawUnknown =
      offer.price ??
      ((offer.priceSpecification as Record<string, unknown> | undefined)?.price ??
        ((offer.priceSpecification as Record<string, unknown> | undefined)?.priceSpecification as
          | Record<string, unknown>
          | undefined)?.price);

    const priceDisplayRaw = stringifyPriceRaw(priceRawUnknown) || fallbackPriceDisplay;
    const parsed = parsePrice(priceDisplayRaw);

    const structuredCurrency =
      normalizeCurrencyCode(readCurrencyFromOffer(offer)) ||
      normalizeCurrencyCode(
        ((offer.priceSpecification as Record<string, unknown> | undefined)?.priceCurrency as string | undefined) || null,
      ) ||
      normalizeCurrencyCode(
        ((offer.priceSpecification as Record<string, unknown> | undefined)?.priceSpecification as
          | Record<string, unknown>
          | undefined)?.priceCurrency as string | undefined,
      );

    const resolvedCurrency = resolveCurrency({
      structuredCurrency,
      metaCurrencyCandidates: params.extracted.metaCurrencies,
      priceDisplayRaw,
      marketId: params.context.market_id,
    });

    const taxIncluded = normalizeTaxIncluded(
      (offer.priceSpecification as Record<string, unknown> | undefined)?.valueAddedTaxIncluded,
    );

    const status = resolveMarketSwitchStatus(resolvedCurrency.code, params.context.expected_currency, false);

    offers.push({
      source_site: params.sourceSite,
      source_product_id: buildSourceProductId({
        sourceSite: params.sourceSite,
        siteProductId: productIdCandidate,
        canonicalUrl,
        sku,
      }),
      url_canonical: canonicalUrl,
      product_title: productTitle || null,
      variant_sku: skuRaw || null,
      market_id: params.context.market_id,
      price_amount: parsed.price_amount,
      price_currency: resolvedCurrency.code,
      price_display_raw: priceDisplayRaw,
      price_type: parsed.price_type,
      range_min: parsed.range_min,
      range_max: parsed.range_max,
      tax_included: taxIncluded,
      availability: normalizeAvailability((offer.availability as string | undefined) || undefined),
      captured_at: params.capturedAt,
      currency_confidence: resolvedCurrency.confidence,
      market_switch_status: status,
      market_context_debug: {
        headers: { ...params.context.headers },
        cookies: { ...params.context.cookies },
        url_params: { ...params.context.url_params },
        geo_hint: params.context.geo_hint,
        expected_currency: params.context.expected_currency,
        observed_currency: resolvedCurrency.code,
      },
    });
  }

  return offers;
}

function readCurrencyFromOffer(offer: JsonObject): string | null {
  const direct = typeof offer.priceCurrency === "string" ? offer.priceCurrency : null;
  if (direct) return direct;

  const priceSpec = offer.priceSpecification;
  if (priceSpec && typeof priceSpec === "object") {
    const cur = (priceSpec as Record<string, unknown>).priceCurrency;
    if (typeof cur === "string") return cur;
    const nested = (priceSpec as Record<string, unknown>).priceSpecification;
    if (nested && typeof nested === "object") {
      const nestedCur = (nested as Record<string, unknown>).priceCurrency;
      if (typeof nestedCur === "string") return nestedCur;
    }
  }

  return null;
}

function stringifyPriceRaw(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) return value.toString();
  if (typeof value === "string" && value.trim()) return value.trim();
  return null;
}

export function buildSourceProductId(params: {
  sourceSite: string;
  siteProductId?: string | null;
  canonicalUrl: string;
  sku?: string | null;
}): string {
  const siteProductId = (params.siteProductId || "").trim();
  if (siteProductId) {
    return `${params.sourceSite}:${siteProductId}`;
  }

  const canonicalHash = stableHash(params.canonicalUrl);
  const sku = (params.sku || "").trim();
  if (sku) {
    return `${params.sourceSite}:${canonicalHash}:${stableHash(sku)}`;
  }

  return `${params.sourceSite}:${canonicalHash}`;
}

export function canonicalizeUrl(rawUrl: string, baseUrl?: string): string {
  try {
    const parsed = baseUrl ? new URL(rawUrl, baseUrl) : new URL(rawUrl);
    parsed.hash = "";

    const keys = Array.from(parsed.searchParams.keys());
    for (const key of keys) {
      if (TRACKING_QUERY_PARAM_RE.test(key)) {
        parsed.searchParams.delete(key);
      }
    }

    return parsed.toString();
  } catch {
    return rawUrl;
  }
}

export function resolveCurrency(params: {
  structuredCurrency: string | null;
  metaCurrencyCandidates: string[];
  priceDisplayRaw: string | null;
  marketId: MarketId;
}): CurrencyResolution {
  const structuredCurrency = normalizeCurrencyCode(params.structuredCurrency);
  if (structuredCurrency) return { code: structuredCurrency, confidence: "high" };

  const metaCurrency = params.metaCurrencyCandidates.map((v) => normalizeCurrencyCode(v)).find((v) => Boolean(v)) || null;
  if (metaCurrency) return { code: metaCurrency, confidence: "medium" };

  const symbolCurrency = inferCurrencyFromSymbol(params.priceDisplayRaw, params.marketId);
  if (symbolCurrency) return { code: symbolCurrency, confidence: "low" };

  return { code: null, confidence: "low" };
}

export function inferCurrencyFromSymbol(rawPrice: string | null, marketId: MarketId): string | null {
  if (!rawPrice) return null;

  if (/€/.test(rawPrice)) return "EUR";
  if (/£/.test(rawPrice)) return "GBP";

  const upperMarket = String(marketId || "").toUpperCase();

  if (/[$]/.test(rawPrice)) {
    if (upperMarket === "SG") return "SGD";
    if (upperMarket === "US") return "USD";
    if (upperMarket === "EU-DE") return "EUR";
    const fallback = fallbackCurrencyByMarket(upperMarket);
    return fallback;
  }

  if (/[¥￥]/.test(rawPrice)) {
    if (upperMarket === "JP") return "JPY";
    return null;
  }

  return null;
}

function fallbackCurrencyByMarket(marketId: string): string | null {
  if (marketId === "US") return "USD";
  if (marketId === "EU-DE") return "EUR";
  if (marketId === "SG") return "SGD";
  if (marketId === "JP") return "JPY";
  return null;
}

function normalizeCurrencyCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const upper = value.trim().toUpperCase();
  return /^[A-Z]{3}$/.test(upper) ? upper : null;
}

function normalizeAvailability(raw: string | undefined): string | undefined {
  if (!raw) return undefined;
  if (/InStock/i.test(raw)) return "in_stock";
  if (/OutOfStock/i.test(raw)) return "out_of_stock";
  if (/PreOrder/i.test(raw)) return "preorder";
  return raw;
}

function normalizeAvailabilityFromBoolean(available?: boolean, inventoryQty?: number | null): string {
  if (available === false) return "out_of_stock";
  if (typeof inventoryQty === "number" && inventoryQty <= 0) return "out_of_stock";
  if (typeof inventoryQty === "number" && inventoryQty <= 10) return "low_stock";
  return "in_stock";
}

function normalizeTaxIncluded(value: unknown): true | false | "unknown" {
  if (value === true) return true;
  if (value === false) return false;
  return "unknown";
}

export function parsePrice(raw: string | null): PriceParseResult {
  if (!raw) {
    return {
      price_amount: null,
      price_type: "unknown",
    };
  }

  const lower = raw.toLowerCase();
  const numbers = extractNumericCandidates(raw);

  const hasRangeToken = /\bto\b|[-–—~]/i.test(raw);
  const hasFrom = /\bfrom\b/i.test(lower);
  const hasMember = /\bmember\b/i.test(lower);
  const hasSale = /\bsale\b|\bdiscount\b|\bnow\b/i.test(lower);

  if (numbers.length >= 2 && hasRangeToken) {
    const min = Math.min(...numbers);
    const max = Math.max(...numbers);
    return {
      price_amount: min,
      price_type: "range",
      range_min: min,
      range_max: max,
    };
  }

  const priceAmount = numbers.length > 0 ? numbers[0]! : null;

  if (hasFrom) {
    return { price_amount: priceAmount, price_type: "from" };
  }
  if (hasMember) {
    return { price_amount: priceAmount, price_type: "member" };
  }
  if (hasSale) {
    return { price_amount: priceAmount, price_type: "sale" };
  }
  if (priceAmount !== null) {
    return { price_amount: priceAmount, price_type: "list" };
  }

  return { price_amount: null, price_type: "unknown" };
}

function extractNumericCandidates(raw: string): number[] {
  const matches = raw.match(/[0-9][0-9.,\s']*/g) || [];
  const values: number[] = [];

  for (const token of matches) {
    const normalized = normalizeNumericToken(token);
    if (!normalized) continue;
    const value = Number.parseFloat(normalized);
    if (Number.isFinite(value)) values.push(value);
  }

  return values;
}

function normalizeNumericToken(token: string): string {
  let out = token.replace(/[\s']/g, "");
  if (!out) return "";

  const hasComma = out.includes(",");
  const hasDot = out.includes(".");

  if (hasComma && hasDot) {
    if (out.lastIndexOf(".") > out.lastIndexOf(",")) {
      out = out.replace(/,/g, "");
    } else {
      out = out.replace(/\./g, "").replace(/,/g, ".");
    }
    return out;
  }

  if (hasComma && !hasDot) {
    const parts = out.split(",");
    if (parts.length === 2 && parts[1] && parts[1].length <= 2) {
      out = out.replace(/,/g, ".");
    } else {
      out = out.replace(/,/g, "");
    }
    return out;
  }

  return out;
}

export function resolveMarketSwitchStatus(
  observedCurrency: string | null,
  expectedCurrency: string,
  explicitFailed: boolean,
): MarketSwitchStatus {
  if (explicitFailed) return "failed";
  if (!observedCurrency) return "unknown";
  return observedCurrency.toUpperCase() === expectedCurrency.toUpperCase() ? "ok" : "mismatch";
}

export function computeCounters(params: {
  sourceSite: string;
  profiles: MarketProfile[];
  offers: OfferV2[];
  marketFailures: Map<string, boolean>;
}): SiteMarketCounters[] {
  const byKey = new Map<string, {
    source_site: string;
    market_id: string;
    total_offers: number;
    native_hits: number;
    price_parse_successes: number;
    low_confidence_count: number;
    switch_fail_count: number;
    run_failed: boolean;
  }>();

  for (const profile of params.profiles) {
    const key = `${params.sourceSite}|${profile.market_id}`;
    byKey.set(key, {
      source_site: params.sourceSite,
      market_id: profile.market_id,
      total_offers: 0,
      native_hits: 0,
      price_parse_successes: 0,
      low_confidence_count: 0,
      switch_fail_count: 0,
      run_failed: params.marketFailures.get(String(profile.market_id)) || false,
    });
  }

  for (const offer of params.offers) {
    const key = `${offer.source_site}|${offer.market_id}`;
    const bucket =
      byKey.get(key) || {
        source_site: offer.source_site,
        market_id: String(offer.market_id),
        total_offers: 0,
        native_hits: 0,
        price_parse_successes: 0,
        low_confidence_count: 0,
        switch_fail_count: 0,
        run_failed: false,
      };

    bucket.total_offers += 1;
    if (offer.price_amount !== null) bucket.price_parse_successes += 1;
    if (offer.currency_confidence === "low") bucket.low_confidence_count += 1;

    const expected = offer.market_context_debug.expected_currency;
    const observed = offer.price_currency;
    if (expected && observed && expected.toUpperCase() === observed.toUpperCase()) {
      bucket.native_hits += 1;
    }

    if (offer.market_switch_status === "failed" || offer.market_switch_status === "mismatch") {
      bucket.switch_fail_count += 1;
    }

    byKey.set(key, bucket);
  }

  const rows: SiteMarketCounters[] = [];
  for (const bucket of byKey.values()) {
    const total = bucket.total_offers;
    const divisor = total > 0 ? total : 1;
    rows.push({
      source_site: bucket.source_site,
      market_id: bucket.market_id,
      total_offers: total,
      native_currency_hit_rate: Number((bucket.native_hits / divisor).toFixed(4)),
      price_parse_success_rate: Number((bucket.price_parse_successes / divisor).toFixed(4)),
      currency_confidence_low_rate: Number((bucket.low_confidence_count / divisor).toFixed(4)),
      market_switch_fail_rate: Number(
        ((total > 0 ? bucket.switch_fail_count / divisor : bucket.run_failed ? 1 : 0)).toFixed(4),
      ),
    });
  }

  rows.sort((a, b) => {
    const siteCmp = a.source_site.localeCompare(b.source_site);
    if (siteCmp !== 0) return siteCmp;
    return String(a.market_id).localeCompare(String(b.market_id));
  });

  return rows;
}

async function fetchCurrencyHint(baseUrl: string, context: RequestContext): Promise<string | null> {
  const html = await fetchText(baseUrl, context);
  if (!html) return null;

  const regexes = [
    /"priceCurrency"\s*:\s*"([A-Za-z]{3})"/i,
    /meta[^>]+property=["']og:price:currency["'][^>]+content=["']([A-Za-z]{3})["']/i,
    /meta[^>]+property=["']product:price:currency["'][^>]+content=["']([A-Za-z]{3})["']/i,
    /data-currency=["']([A-Za-z]{3})["']/i,
    /cart_currency=([A-Za-z]{3})/i,
  ];

  for (const pattern of regexes) {
    const matched = html.match(pattern)?.[1];
    const normalized = normalizeCurrencyCode(matched || null);
    if (normalized) return normalized;
  }

  return null;
}

function extractProductId(productObj: JsonObject | undefined): string | null {
  if (!productObj) return null;
  const direct = productObj.productID;
  if (typeof direct === "string" && direct.trim()) return direct.trim();
  if (typeof direct === "number" && Number.isFinite(direct)) return String(direct);

  const sku = productObj.sku;
  if (typeof sku === "string" && sku.trim()) return sku.trim();

  return null;
}

function normalizeJsonLdValue(value: unknown): unknown[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.flatMap(normalizeJsonLdValue);
  if (typeof value === "object") {
    const candidate = value as Record<string, unknown>;
    if (Array.isArray(candidate["@graph"])) return normalizeJsonLdValue(candidate["@graph"]);
    return [candidate];
  }
  return [];
}

function normalizeJsonLdObjects(value: unknown): JsonObject[] {
  return normalizeJsonLdValue(value).filter((obj): obj is JsonObject => Boolean(obj && typeof obj === "object"));
}

function normalizeJsonLdOffers(value: unknown): JsonObject[] {
  const out: JsonObject[] = [];
  for (const offerish of normalizeJsonLdObjects(value)) {
    const nested = normalizeJsonLdObjects(offerish.offers);
    if (nested.length > 0) {
      out.push(...nested);
    } else {
      out.push(offerish);
    }
  }
  return out;
}

function isType(obj: JsonObject, typeName: string): boolean {
  const rawType = obj["@type"];
  if (typeof rawType === "string") return rawType === typeName;
  if (Array.isArray(rawType)) return rawType.includes(typeName);
  return false;
}

function toAbsoluteUrl(baseUrl: string, href: string): string {
  try {
    return new URL(href, baseUrl).toString();
  } catch {
    return href;
  }
}

function stableHash(input: string): string {
  return createHash("sha1").update(input).digest("hex").slice(0, 12);
}

function parseTarget(raw: string): ParseTargetResult {
  const trimmed = raw.trim();
  if (!trimmed) return { domain: "localhost", baseUrl: "https://localhost" };

  try {
    const parsed = new URL(trimmed.startsWith("http://") || trimmed.startsWith("https://") ? trimmed : `https://${trimmed}`);
    const hasPath = parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "";

    return {
      domain: parsed.host,
      baseUrl: parsed.origin,
      seedUrl: hasPath ? parsed.toString() : undefined,
      collectionHandle: getCollectionHandle(parsed.pathname),
    };
  } catch {
    const host = trimmed.replace(/^https?:\/\//i, "").split("/")[0];
    return {
      domain: host,
      baseUrl: `https://${host}`,
    };
  }
}

function getCollectionHandle(pathname: string): string | undefined {
  const matched = pathname.match(/^\/collections\/([^/]+)/i);
  return matched?.[1];
}

function clampInt(value: string | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

function clampOptionalInt(value: number | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
  });

  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T, idx: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let idx = 0;

  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const current = idx++;
      if (current >= items.length) break;
      results[current] = await fn(items[current] as T, current);
    }
  });

  await Promise.all(workers);
  return results;
}

function withUrlParams(rawUrl: string, urlParams: Record<string, string>): string {
  if (!urlParams || Object.keys(urlParams).length === 0) return rawUrl;

  try {
    const parsed = new URL(rawUrl);
    for (const [key, value] of Object.entries(urlParams)) {
      if (!key || !value) continue;
      if (!parsed.searchParams.has(key)) parsed.searchParams.set(key, value);
    }
    return parsed.toString();
  } catch {
    return rawUrl;
  }
}

function buildFetchHeaders(context: RequestContext, accept: string): Record<string, string> {
  const headers: Record<string, string> = {
    accept,
    "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
    ...context.headers,
  };

  const cookieString = Object.entries(context.cookies)
    .map(([name, value]) => `${name}=${value}`)
    .join("; ");

  if (cookieString) {
    headers.cookie = cookieString;
  }

  return headers;
}

async function fetchJson<T>(url: string, context: RequestContext): Promise<T | null> {
  const timeoutMs = clampInt(process.env.PUPPETEER_FETCH_TIMEOUT_MS, DEFAULT_FETCH_TIMEOUT_MS, 2_000, 120_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(withUrlParams(url, context.url_params), {
      redirect: "follow",
      signal: controller.signal,
      headers: buildFetchHeaders(context, "application/json"),
    });

    if (!response.ok) return null;
    return (await response.json()) as T;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchText(url: string, context: RequestContext): Promise<string | null> {
  const timeoutMs = clampInt(process.env.PUPPETEER_FETCH_TIMEOUT_MS, DEFAULT_FETCH_TIMEOUT_MS, 2_000, 120_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(withUrlParams(url, context.url_params), {
      redirect: "follow",
      signal: controller.signal,
      headers: buildFetchHeaders(context, "text/plain,text/html,application/xml;q=0.9,*/*;q=0.8"),
    });

    if (!response.ok) return null;
    return await response.text();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function discoverProductUrls(params: {
  baseUrl: string;
  maxProducts: number;
  seedUrl?: string;
  context: RequestContext;
  log: Logger;
}): Promise<{ sitemapUrl?: string; productUrls: string[] }> {
  if (params.seedUrl) {
    const seedHtml = await fetchText(params.seedUrl, params.context);
    if (seedHtml) {
      const seedUrls = extractProductUrlsFromHtml(seedHtml, params.baseUrl);
      if (seedUrls.length > 0) {
        return { sitemapUrl: undefined, productUrls: seedUrls.slice(0, params.maxProducts) };
      }
    }
  }

  const robotsUrl = `${params.baseUrl}/robots.txt`;
  const robotsText = (await fetchText(robotsUrl, params.context)) || "";
  const sitemapUrls = extractSitemapUrlsFromRobots(robotsText);

  const candidates =
    sitemapUrls.length > 0 ? sitemapUrls : [`${params.baseUrl}/sitemap.xml`, `${params.baseUrl}/sitemap_index.xml`];

  const visited = new Set<string>();
  const queue = [...candidates];
  const pageUrls: string[] = [];
  let chosenSitemap: string | undefined;

  const maxSitemaps = clampInt(process.env.MAX_SITEMAPS, 20, 1, 100);

  while (queue.length > 0 && visited.size < maxSitemaps) {
    const sitemapUrl = queue.shift();
    if (!sitemapUrl || visited.has(sitemapUrl)) continue;
    visited.add(sitemapUrl);

    const xml = await fetchText(sitemapUrl, params.context);
    if (!xml) continue;
    if (!chosenSitemap) chosenSitemap = sitemapUrl;

    const locs = extractLocUrlsFromSitemap(xml);
    const isIndex = /<sitemapindex/i.test(xml);

    if (isIndex) {
      for (const loc of locs) {
        if (!visited.has(loc)) queue.push(loc);
      }
    } else {
      pageUrls.push(...locs);
      const dedupedSoFar = Array.from(new Set(pageUrls)).filter((u) => u.startsWith("http"));
      const likelySoFar = dedupedSoFar.filter((u) => isLikelyProductUrl(u, params.baseUrl));
      if (likelySoFar.length >= params.maxProducts || dedupedSoFar.length >= params.maxProducts * 2) break;
    }
  }

  const deduped = Array.from(new Set(pageUrls)).filter((u) => u.startsWith("http"));
  const nonAsset = deduped.filter((u) => !isStaticAssetUrl(u, params.baseUrl));
  const productLike = nonAsset.filter((u) => isLikelyProductUrl(u, params.baseUrl));
  const selected = (productLike.length > 0 ? productLike : nonAsset).slice(0, params.maxProducts);

  params.log("data", `discovered_urls=${selected.length} market=${params.context.market_id}`);
  return { sitemapUrl: chosenSitemap, productUrls: selected };
}

function extractProductUrlsFromHtml(html: string, baseUrl: string): string[] {
  const hrefUrls = new Set<string>();
  for (const match of html.matchAll(/<a\b[^>]*\bhref\s*=\s*["']([^"']+)["']/gi)) {
    const rawHref = match[1]?.trim();
    if (!rawHref) continue;
    if (/^(#|mailto:|tel:|javascript:)/i.test(rawHref)) continue;
    hrefUrls.add(toAbsoluteUrl(baseUrl, rawHref.replace(/&amp;/gi, "&")));
  }

  const hrefProducts = Array.from(hrefUrls).filter((url) => isLikelyProductUrl(url, baseUrl));
  if (hrefProducts.length > 0) return hrefProducts;

  const urls = new Set<string>();
  const fallbackPatterns = [
    /["'](\/product\/[^"'?#\s<]+)["']/gi,
    /["'](\/products\/[^"'?#\s<]+)["']/gi,
    /["'](https?:\/\/[^"'?#\s<]+)["']/gi,
  ];

  for (const pattern of fallbackPatterns) {
    for (const match of html.matchAll(pattern)) {
      const candidate = match[1] || match[0];
      const absolute = toAbsoluteUrl(baseUrl, candidate);
      if (isLikelyProductUrl(absolute, baseUrl)) urls.add(absolute);
    }
  }

  return Array.from(urls);
}

function extractSitemapUrlsFromRobots(robotsText: string): string[] {
  const urls: string[] = [];
  for (const match of robotsText.matchAll(/^sitemap:\s*(.+)$/gim)) {
    const url = match[1]?.trim();
    if (url) urls.push(url);
  }
  return urls;
}

function extractLocUrlsFromSitemap(xml: string): string[] {
  const urls: string[] = [];
  for (const match of xml.matchAll(/<loc>([^<]+)<\/loc>/gim)) {
    const loc = match[1]?.trim();
    if (!loc) continue;
    const cleaned = loc.replace(/^<!\[CDATA\[/, "").replace(/\]\]>$/, "").trim();
    urls.push(cleaned);
  }
  return urls;
}

function parseHttpUrl(rawUrl: string, baseUrl: string): URL | null {
  try {
    const parsed = new URL(rawUrl, baseUrl);
    if (!/^https?:$/i.test(parsed.protocol)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function isStaticAssetUrl(rawUrl: string, baseUrl: string): boolean {
  const parsed = parseHttpUrl(rawUrl, baseUrl);
  if (!parsed) return true;
  return /\.(?:css|js|mjs|map|png|jpe?g|gif|webp|svg|ico|pdf|xml|txt|woff2?|ttf|eot|otf|mp3|wav|mp4|webm|zip|gz|tar|json)(?:$|[?#])/i.test(
    parsed.pathname,
  );
}

function isLikelyProductUrl(rawUrl: string, baseUrl: string): boolean {
  const parsed = parseHttpUrl(rawUrl, baseUrl);
  if (!parsed) return false;

  const baseHost = parseHttpUrl(baseUrl, baseUrl)?.host.toLowerCase();
  if (baseHost && parsed.host.toLowerCase() !== baseHost) return false;
  if (isStaticAssetUrl(parsed.toString(), baseUrl)) return false;

  const path = parsed.pathname.toLowerCase();
  if (path === "/" || path === "") return false;
  if (/\/products?\//.test(path)) return true;
  if (/[-_]\d{4,}\.html$/.test(path)) return true;
  if (/\/p\/[^/]+$/.test(path)) return true;

  return false;
}
