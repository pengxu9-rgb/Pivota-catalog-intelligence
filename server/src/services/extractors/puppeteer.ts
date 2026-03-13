import { createHash } from "crypto";
import { type Browser } from "puppeteer";

import type {
  ExtractInput,
  ExtractResponse,
  ExtractedProduct,
  ExtractedVariant,
  ExtractedVariantRow,
  Extractor,
  StockStatus,
} from "./types";
import {
  BotChallengeError,
  canonicalizeUrl as canonicalizeUrlShared,
  clampInt as clampIntShared,
  clampOptionalInt as clampOptionalIntShared,
  createDiagnostics,
  detectBlockProvider,
  discoverProductUrls as discoverProductUrlsShared,
  dismissCookieBanner,
  extractProductUrlsFromHtml as extractProductUrlsFromHtmlShared,
  fetchJsonTracked,
  fetchTextTracked,
  gotoPageOrThrow,
  isLikelyProductUrl as isLikelyProductUrlShared,
  isStaticAssetUrl as isStaticAssetUrlShared,
  looksLikeProductPageHtml,
  mapWithConcurrency as mapWithConcurrencyShared,
  normalizeMarketId,
  parseTarget as parseTargetShared,
  preparePage,
  resolveStorefrontTarget,
  runBrowserTaskWithFallback,
  setDiscoveryStrategy,
  setFailureCategory,
  toAbsoluteUrl as toAbsoluteUrlShared,
  withTimeout as withTimeoutShared,
  type LoggerFn,
} from "./shared";

const DEFAULT_BATCH_LIMIT = 10;
const DEFAULT_MAX_TOTAL_PRODUCTS = 500;
const DEFAULT_CONCURRENCY = 3;
const DEFAULT_NAV_TIMEOUT_MS = 8_000;
const DEFAULT_FETCH_TIMEOUT_MS = 8_000;
const DEFAULT_LAUNCH_TIMEOUT_MS = 15_000;
const DEFAULT_SCRAPE_TIMEOUT_MS = 60_000;
const DEFAULT_PRODUCT_URL_RESERVE = 4;

export class PuppeteerExtractor implements Extractor {
  async extract(input: ExtractInput): Promise<ExtractResponse> {
    const generatedAt = new Date().toISOString();

    const logs: ExtractResponse["logs"] = [];
    const log: LoggerFn = (type, msg) => {
      logs.push({ at: new Date().toISOString(), type, msg });
    };

    const requestedTarget = parseTargetShared(input.domain);
    const diagnostics = createDiagnostics(requestedTarget.domain, requestedTarget.baseUrl);
    const marketId = normalizeMarketId(input.market);
    const batchOffset = clampOptionalIntShared(input.offset, 0, 0, 100_000);
    const batchLimit = clampOptionalIntShared(
      input.limit,
      clampIntShared(process.env.BATCH_LIMIT || process.env.MAX_PRODUCTS, DEFAULT_BATCH_LIMIT, 1, 200),
      1,
      200,
    );
    const maxProductsTotal = clampIntShared(process.env.MAX_TOTAL_PRODUCTS, DEFAULT_MAX_TOTAL_PRODUCTS, 1, 10_000);
    const discoveryReserve = clampIntShared(process.env.PRODUCT_URL_RESERVE, DEFAULT_PRODUCT_URL_RESERVE, 0, 100);
    const discoveryLimit = Math.min(maxProductsTotal, batchOffset + batchLimit + discoveryReserve);

    log("info", `Initializing Puppeteer extraction for: ${input.brand}`);
    log("info", `Requested target: ${requestedTarget.baseUrl} (market=${marketId})`);
    log("info", `Batch window: offset=${batchOffset}, limit=${batchLimit}, max_total=${maxProductsTotal}`);
    if (requestedTarget.seedUrl) log("info", `Seed URL: ${requestedTarget.seedUrl}`);

    try {
      const resolved = await resolveStorefrontTarget({
        target: requestedTarget,
        marketId,
        context: {},
        diagnostics,
        log,
      });
      const target = resolved.target;
      const baseUrl = target.baseUrl;

      // 1) Fast path: Shopify JSON feed (no browser required).
      const shopify = await tryExtractShopify({
        brand: input.brand,
        domain: target.domain,
        baseUrl,
        seedUrl: target.seedUrl,
        collectionHandle: target.collectionHandle,
        maxProducts: maxProductsTotal,
        offset: batchOffset,
        limit: batchLimit,
        diagnostics,
        log,
      });
      if (shopify) {
        return {
          ...shopify,
          generated_at: generatedAt,
          logs,
          diagnostics,
        };
      }

      // 2) Generic path: direct PDP/seed discovery -> sitemaps -> browser fallback.
      log("info", "Shopify feed not detected. Falling back to direct page, sitemap, and browser discovery.");
      const discovered = await discoverProductUrlsShared({
        baseUrl,
        maxProducts: discoveryLimit,
        seedUrl: target.seedUrl,
        context: {},
        diagnostics,
        selectorRootDetected: resolved.selectorRootDetected && !resolved.storefrontResolved,
        log,
      });
      const batchCandidates = discovered.productUrls.slice(batchOffset, batchOffset + batchLimit + discoveryReserve);

      if (batchCandidates.length === 0) {
        log("error", "No product URLs discovered.");
        const nextOffset = batchOffset + batchLimit;
        const reachedDiscoveryCap = discovered.productUrls.length >= discoveryLimit && discoveryLimit < maxProductsTotal;
        const hasMore =
          nextOffset < maxProductsTotal && (nextOffset < discovered.productUrls.length || reachedDiscoveryCap);
        return {
          brand: input.brand,
          domain: target.domain,
          generated_at: generatedAt,
          mode: "puppeteer",
          platform: "Unknown",
          sitemap: discovered.sitemapUrl,
          products: [],
          variants: [],
          pricing: { currency: "USD", min: 0, max: 0, avg: 0 },
          ad_copy: { by_variant_id: {} },
          pagination: {
            offset: batchOffset,
            limit: batchLimit,
            next_offset: hasMore ? nextOffset : null,
            has_more: hasMore,
            discovered_urls: discovered.productUrls.length,
          },
          logs,
          diagnostics,
        };
      }

      log(
        "success",
        `Discovered ${discovered.productUrls.length} product URLs. Scraping batch candidates: ${batchCandidates.length}.`,
      );

      const concurrency = clampIntShared(process.env.PUPPETEER_CONCURRENCY, DEFAULT_CONCURRENCY, 1, 6);
      const navigationTimeoutMs = clampIntShared(process.env.PUPPETEER_NAV_TIMEOUT_MS, DEFAULT_NAV_TIMEOUT_MS, 5_000, 120_000);
      const scrapeTimeoutMs = clampIntShared(process.env.PUPPETEER_SCRAPE_TIMEOUT_MS, DEFAULT_SCRAPE_TIMEOUT_MS, 10_000, 300_000);

      const browserRun = await runBrowserTaskWithFallback(
        async (browser) =>
          withTimeoutShared(
            mapWithConcurrencyShared(batchCandidates, concurrency, async (url, idx) => {
              const verbose = idx < 3;
              return scrapeProductPage({
                browser,
                url,
                baseUrl,
                navigationTimeoutMs,
                verbose,
                log,
                diagnostics,
                context: {},
              });
            }),
            scrapeTimeoutMs,
            "Product scraping",
          ),
        { diagnostics, log },
      );

      const products = browserRun.result.filter((product): product is ExtractedProduct => Boolean(product)).slice(0, batchLimit);
      const { variants, adCopyById } = flattenVariants({
        brand: input.brand,
        products,
        simulated: false,
      });

      if (products.length === 0 && !diagnostics.failure_category && diagnostics.block_provider) {
        setFailureCategory(diagnostics, "bot_challenge");
      } else if (products.length === 0 && !diagnostics.failure_category) {
        setFailureCategory(diagnostics, "product_schema_missing");
      }

      const nextOffset = batchOffset + batchLimit;
      const reachedDiscoveryCap = discovered.productUrls.length >= discoveryLimit && discoveryLimit < maxProductsTotal;
      const hasMore = nextOffset < maxProductsTotal && (nextOffset < discovered.productUrls.length || reachedDiscoveryCap);
      const pricing = computePricingStats(variants);
      log("success", `Extraction Complete. ${variants.length} variants processed successfully.`);

      return {
        brand: input.brand,
        domain: target.domain,
        generated_at: generatedAt,
        mode: "puppeteer",
        platform: browserRun.mode === "managed" ? "Managed Browser / Generic" : "Generic Website",
        sitemap: discovered.sitemapUrl,
        products,
        variants,
        pricing,
        ad_copy: { by_variant_id: adCopyById },
        pagination: {
          offset: batchOffset,
          limit: batchLimit,
          next_offset: hasMore ? nextOffset : null,
          has_more: hasMore,
          discovered_urls: discovered.productUrls.length,
        },
        logs,
        diagnostics,
      };
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      if (!diagnostics.failure_category) {
        if (err instanceof BotChallengeError) {
          setFailureCategory(diagnostics, "bot_challenge");
        } else if (err instanceof Error && /timed out/i.test(err.message)) {
          setFailureCategory(diagnostics, "timeout");
        } else {
          setFailureCategory(diagnostics, "unknown");
        }
      }
      log("error", `Puppeteer extraction failed: ${msg}`);
      return {
        brand: input.brand,
        domain: requestedTarget.domain,
        generated_at: generatedAt,
        mode: "puppeteer",
        platform: "Error",
        products: [],
        variants: [],
        pricing: { currency: "USD", min: 0, max: 0, avg: 0 },
        ad_copy: { by_variant_id: {} },
        pagination: {
          offset: batchOffset,
          limit: batchLimit,
          next_offset: null,
          has_more: false,
          discovered_urls: 0,
        },
        logs,
        diagnostics,
      };
    }
  }
}

type Logger = (type: ExtractResponse["logs"][number]["type"], msg: string) => void;

type DomVariantMeta = {
  sku: string;
  option_name?: string;
  option_value?: string;
  url_path?: string;
  image_url?: string;
  image_urls?: string[];
  price?: string;
  ingredients?: string;
};

function clampInt(value: string | undefined, fallback: number, min: number, max: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function clampOptionalInt(value: number | undefined, fallback: number, min: number, max: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
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

function parseTarget(raw: string): {
  domain: string;
  baseUrl: string;
  seedUrl?: string;
  collectionHandle?: string;
} {
  const trimmed = raw.trim();
  if (!trimmed) return { domain: "localhost", baseUrl: "https://localhost" };

  try {
    const u = new URL(trimmed.startsWith("http://") || trimmed.startsWith("https://") ? trimmed : `https://${trimmed}`);
    const hasPath = u.pathname !== "/" || u.search !== "" || u.hash !== "";
    return {
      domain: u.host,
      baseUrl: u.origin,
      seedUrl: hasPath ? u.toString() : undefined,
      collectionHandle: getCollectionHandle(u.pathname),
    };
  } catch {
    const host = trimmed.replace(/^https?:\/\//i, "").split("/")[0];
    return { domain: host, baseUrl: `https://${host}` };
  }
}

function pick<T>(arr: readonly T[]) {
  return arr[Math.floor(Math.random() * arr.length)];
}

const SOCIAL_CONTENT_TEMPLATES = [
  "Trending on TikTok: 'The finish is absolutely unreal.' Users report all-day wear without touch-ups.",
  "Instagram favorite: Influencers are obsessed with the {variant} shade. 'My new holy grail,' says @BeautyGuru.",
  "Viral hit: This specific {variant} is selling out everywhere. 'Worth every penny for the glow alone.'",
  "Community top pick: 4.8/5 stars on social platforms. Fans love how it feels weightless yet powerful.",
  "As seen on #BeautyTok: 'Best investment for your routine.' The hype around {variant} is real.",
] as const;

const AD_SUBJECT_TEMPLATES = [
  "✨ Back in Stock: {title} in {variant}",
  "Why everyone is talking about {title} ({variant})",
  "Your new obsession: {title}",
  "Exclusive: The perfect {variant} shade is here",
  "Luxury Redefined: Meet {title}",
] as const;

const AD_CAPTION_TEMPLATES = [
  "Finally got my hands on {title} in {variant} and I'm obsessed! 😍 The texture is incredible and it lasts all day. \n\n#TomFordBeauty #LuxuryMakeup #BeautyFaves #{variant}",
  "Pov: You found the perfect {variant} shade. ✨ {title} is worth the hype. Tap the link to shop before it sells out! \n\n#MakeupAddict #SplurgeWorthy #{variant} #TomFord",
  "Elevate your routine with {title}. The shade {variant} is absolute perfection for any occasion. 🖤 \n\n#BeautyEssentials #LuxuryLife #{variant}",
  "Run don't walk! 🏃‍♀️ {title} in {variant} is the viral product of the season. \n\n#ViralBeauty #TomFord #{variant} #MakeupHaul",
] as const;

function getMergedDescription(params: {
  title: string;
  overview?: string;
  howToUse?: string;
  ingredientsAndSafety?: string;
}) {
  const overview = cleanText(params.overview);
  const parts = overview ? [overview] : [];

  const howToUse = cleanText(params.howToUse);
  if (howToUse) parts.push(`How to Use: ${howToUse}`);

  const ingredientsAndSafety = cleanText(params.ingredientsAndSafety);
  if (ingredientsAndSafety) parts.push(`Ingredients and Safety: ${ingredientsAndSafety}`);

  return parts.join("\n\n");
}

export function choosePreferredProductOverview(params: {
  structured?: string;
  detailed?: string;
  meta?: string;
}) {
  const structured = cleanText(params.structured);
  const detailed = cleanText(params.detailed);
  const meta = cleanText(params.meta);

  if (detailed) {
    if (!structured) return detailed;

    const structuredLower = structured.toLowerCase();
    const detailedLower = detailed.toLowerCase();
    const startsWithStructured = detailedLower.startsWith(structuredLower);
    const materiallyLonger = detailed.length >= Math.max(structured.length + 60, Math.round(structured.length * 1.35));
    const looksLikeExpandedOverview = /\bthis set includes\b|\bproduct details\b|\n|•|\bto use\b/i.test(detailed);

    if (startsWithStructured || (materiallyLonger && looksLikeExpandedOverview)) {
      return detailed;
    }
  }

  return structured || meta || undefined;
}

function generateMockAdCopy(title: string, variantValue: string, price: string) {
  const subject = pick(AD_SUBJECT_TEMPLATES).replace("{title}", title).replace("{variant}", variantValue);
  const caption = pick(AD_CAPTION_TEMPLATES).replace("{title}", title).replace("{variant}", variantValue);
  return `**Subject:** ${subject}\n\n**Instagram Caption:**\n${caption}\n\n**Price:** $${price}`;
}

function cleanText(text?: string) {
  if (!text) return "";
  const withNewlines = text
    .replace(/\u00a0/g, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p\s*>/gi, "\n")
    .replace(/<\/div\s*>/gi, "\n")
    .replace(/<\/?[a-z][^>]*>/g, " ");

  return withNewlines
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function getCollectionHandle(pathname: string): string | undefined {
  const m = pathname.match(/^\/collections\/([^/]+)/i);
  return m?.[1];
}

function splitTitleIntoBaseAndVariant(title: string):
  | { baseTitle: string; variantLabel: string; delimiter: string }
  | null {
  const t = title.trim();
  if (!t) return null;

  const delimiters = [" — ", " – ", " - ", " | ", ": "];
  for (const delimiter of delimiters) {
    const idx = t.indexOf(delimiter);
    if (idx <= 0) continue;
    const baseTitle = t.slice(0, idx).trim();
    const variantLabel = t.slice(idx + delimiter.length).trim();
    if (!baseTitle || !variantLabel) continue;
    return { baseTitle, variantLabel, delimiter };
  }

  return null;
}

function buildDeepLink(rawUrl: string, variantId: string) {
  try {
    const u = new URL(rawUrl);
    if (/\/products\//i.test(u.pathname) && /^\d+$/.test(variantId)) {
      u.searchParams.set("variant", variantId);
    }
    u.searchParams.set("utm_source", "pivota");
    u.searchParams.set("utm_medium", "affiliate");
    return u.toString();
  } catch {
    return rawUrl;
  }
}

function computePricingStats(variants: ExtractedVariantRow[]) {
  const nums = variants
    .map((v) => Number.parseFloat(v.price))
    .filter((n) => Number.isFinite(n));
  const currency = variants[0]?.currency || "USD";
  if (nums.length === 0) return { currency, min: 0, max: 0, avg: 0 };
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const avg = nums.reduce((a, b) => a + b, 0) / nums.length;
  return { currency, min, max, avg: Number(avg.toFixed(2)) };
}

function dedupeStringList(values: Array<string | undefined | null>) {
  const out: string[] = [];
  for (const value of values) {
    const trimmed = String(value || "").trim();
    if (!trimmed || out.includes(trimmed)) continue;
    out.push(trimmed);
  }
  return out;
}

const IMAGE_HINT_STOPWORDS = new Set([
  "with",
  "from",
  "your",
  "that",
  "this",
  "default",
  "title",
  "shop",
  "beauty",
  "cream",
  "serum",
  "body",
  "face",
  "gift",
  "card",
  "sample",
  "products",
  "product",
  "collections",
]);

function tokenizeImageHints(values: Array<string | undefined | null>) {
  const tokens = new Set<string>();
  for (const value of values) {
    const decoded = decodeURIComponent(String(value || "").toLowerCase());
    const matches = decoded.match(/[\p{L}\p{N}]+/gu) || [];
    for (const match of matches) {
      if (match.length < 4) continue;
      if (IMAGE_HINT_STOPWORDS.has(match)) continue;
      if (/^\d+$/.test(match)) continue;
      tokens.add(match);
    }
  }
  return Array.from(tokens);
}

function imageUrlMatchScore(url: string, tokens: string[]) {
  const haystack = decodeURIComponent(url.toLowerCase());
  let score = 0;
  for (const token of tokens) {
    if (haystack.includes(token)) score += token.length >= 8 ? 3 : 2;
  }
  return score;
}

function preferredImageVariant(existingUrl: string | undefined, candidateUrl: string) {
  if (!existingUrl) return candidateUrl;

  const readWidth = (rawUrl: string) => {
    try {
      const parsed = new URL(rawUrl);
      const width = Number(parsed.searchParams.get("width") || parsed.searchParams.get("w") || parsed.searchParams.get("sw") || 0);
      return Number.isFinite(width) ? width : 0;
    } catch {
      return 0;
    }
  };

  return readWidth(candidateUrl) >= readWidth(existingUrl) ? candidateUrl : existingUrl;
}

function selectRelevantFallbackImageUrls(product: { title: string; url: string }, candidates: string[]) {
  const hintValues = [product.title];
  try {
    const parsed = new URL(product.url);
    hintValues.push(parsed.pathname, parsed.search);
  } catch {
    hintValues.push(product.url);
  }

  const hintTokens = tokenizeImageHints(hintValues);
  if (hintTokens.length === 0) return [];

  const bestByCanonical = new Map<string, { url: string; score: number }>();
  for (const candidate of candidates) {
    const score = imageUrlMatchScore(candidate, hintTokens);
    if (score <= 0) continue;

    try {
      const parsed = new URL(candidate);
      parsed.searchParams.delete("width");
      parsed.searchParams.delete("w");
      parsed.searchParams.delete("sw");
      parsed.searchParams.delete("height");
      parsed.searchParams.delete("h");
      parsed.searchParams.delete("sh");
      const canonical = parsed.toString();
      const prev = bestByCanonical.get(canonical);
      if (!prev) {
        bestByCanonical.set(canonical, { url: candidate, score });
        continue;
      }
      bestByCanonical.set(canonical, {
        url: preferredImageVariant(prev.url, candidate),
        score: Math.max(prev.score, score),
      });
    } catch {
      const prev = bestByCanonical.get(candidate);
      if (!prev) {
        bestByCanonical.set(candidate, { url: candidate, score });
      }
    }
  }

  return Array.from(bestByCanonical.values())
    .sort((a, b) => b.score - a.score)
    .map((entry) => entry.url);
}

function flattenVariants(params: {
  brand: string;
  products: ExtractedProduct[];
  simulated: boolean;
}): { variants: ExtractedVariantRow[]; adCopyById: Record<string, string> } {
  const variants: ExtractedVariantRow[] = [];
  const adCopyById: Record<string, string> = {};

  for (const product of params.products) {
    for (const variant of product.variants) {
      const deepLink = buildDeepLink(variant.url, variant.id);
      const row: ExtractedVariantRow = {
        ...variant,
        brand: params.brand,
        product_title: product.title,
        product_url: product.url,
        deep_link: deepLink,
        simulated: params.simulated,
      };
      variants.push(row);
      adCopyById[variant.id] = variant.ad_copy;
    }
  }

  return { variants, adCopyById };
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
      results[current] = await fn(items[current], current);
    }
  });

  await Promise.all(workers);
  return results;
}

type ShopifyProductsResponse = { products?: ShopifyProduct[] };

type ShopifyProduct = {
  id: number;
  title: string;
  handle: string;
  body_html?: string;
  variants: ShopifyVariant[];
  options?: Array<{ name?: string }>;
  images?: Array<string | ShopifyImage>;
  featured_image?: string | ShopifyImage | null;
};

type ShopifyVariant = {
  id: number;
  sku?: string | null;
  title?: string;
  option1?: string | null;
  option2?: string | null;
  option3?: string | null;
  price?: string;
  available?: boolean;
  inventory_quantity?: number | null;
  featured_image?: string | ShopifyImage | null;
};

type ShopifyImage = {
  src?: string;
  url?: string;
  variant_ids?: number[];
};

const ZERO_DECIMAL_CURRENCIES = new Set(["JPY"]);

function normalizeCurrencyCode(raw: unknown): ExtractedVariant["currency"] | null {
  const normalized = String(raw || "").trim().toUpperCase();
  if (normalized === "USD" || normalized === "EUR" || normalized === "SGD" || normalized === "JPY") {
    return normalized;
  }
  return null;
}

function extractCurrencyHintFromHtml(html: string): ExtractedVariant["currency"] | null {
  const regexes = [
    /"priceCurrency"\s*:\s*"([A-Za-z]{3})"/i,
    /meta[^>]+property=["']og:price:currency["'][^>]+content=["']([A-Za-z]{3})["']/i,
    /meta[^>]+property=["']product:price:currency["'][^>]+content=["']([A-Za-z]{3})["']/i,
    /data-currency=["']([A-Za-z]{3})["']/i,
    /Shopify\.currency\s*=\s*\{[^}]*"active"\s*:\s*"([A-Za-z]{3})"/i,
    /currencyCode"\s*:\s*"([A-Za-z]{3})"/i,
    /window\.ShopifyAnalytics\.meta\.currency\s*=\s*['"]([A-Za-z]{3})['"]/i,
    /cart_currency=([A-Za-z]{3})/i,
  ];

  for (const pattern of regexes) {
    const matched = html.match(pattern)?.[1];
    const normalized = normalizeCurrencyCode(matched);
    if (normalized) return normalized;
  }

  return null;
}

async function fetchShopifyCurrencyHint(
  urlCandidates: Array<string | undefined>,
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>,
): Promise<ExtractedVariant["currency"] | null> {
  for (const candidate of urlCandidates) {
    const url = String(candidate || "").trim();
    if (!url) continue;
    const outcome = await fetchTextTracked(url, {}, diagnostics);
    if (!outcome.body) continue;
    const hint = extractCurrencyHintFromHtml(outcome.body);
    if (hint) return hint;
  }
  return null;
}

function normalizeShopifyPrice(raw: unknown, currency: ExtractedVariant["currency"]) {
  if (typeof raw === "number" && Number.isFinite(raw)) {
    if (Number.isInteger(raw) && !ZERO_DECIMAL_CURRENCIES.has(currency)) {
      return (raw / 100).toFixed(2);
    }
    return raw.toFixed(2);
  }

  if (typeof raw === "string" && raw.trim()) {
    const trimmed = raw.trim();
    if (/^-?\d+$/.test(trimmed) && !ZERO_DECIMAL_CURRENCIES.has(currency)) {
      return (Number(trimmed) / 100).toFixed(2);
    }
    return trimmed;
  }

  return "0.00";
}

function isDefaultShopifyVariant(variant: ShopifyVariant): boolean {
  const fields = [variant.title, variant.option1, variant.option2, variant.option3]
    .map((v) => (v || "").trim().toLowerCase())
    .filter(Boolean);
  return fields.length > 0 && fields.every((v) => v === "default title" || v === "default");
}

async function tryExtractShopify(params: {
  brand: string;
  domain: string;
  baseUrl: string;
  seedUrl?: string;
  collectionHandle?: string;
  maxProducts: number;
  offset: number;
  limit: number;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
}): Promise<Omit<ExtractResponse, "generated_at" | "logs"> | null> {
  const log = params.log;
  const directHandle = extractShopifyProductHandle(params.seedUrl, params.baseUrl);
  const currencyHintUrls = dedupeStringList([params.seedUrl, params.baseUrl]);

  if (directHandle) {
    const directUrl = `${params.baseUrl}/products/${directHandle}.js`;
    log("info", `Checking Shopify direct product feed: ${directUrl}`);
    const directProduct = await fetchJsonTracked<ShopifyProduct>(directUrl, {}, params.diagnostics!);
    if (directProduct.data && typeof directProduct.data.id === "number") {
      log("success", `Shopify direct product detected for handle: ${directHandle}`);
      setDiscoveryStrategy(params.diagnostics!, "shopify_json");
      const currencyHint = await fetchShopifyCurrencyHint(currencyHintUrls, params.diagnostics!);
      const response = buildShopifyResponse({
        ...params,
        currencyHint,
        products: [directProduct.data],
        platformLabel: "Shopify (Direct PDP)",
      });
      return enrichDirectShopifyPdpResponse({
        brand: params.brand,
        baseUrl: params.baseUrl,
        seedUrl: params.seedUrl,
        response,
        diagnostics: params.diagnostics,
        log,
      });
    }
    log("warn", `Shopify direct product feed not found for handle: ${directHandle}. Falling back to collection/site feed.`);
  }

  const probeUrl = params.collectionHandle
    ? `${params.baseUrl}/collections/${params.collectionHandle}/products.json?limit=1`
    : `${params.baseUrl}/products.json?limit=1`;

  log("info", `Checking Shopify feed: ${probeUrl}`);
  const probe = await fetchJsonTracked<ShopifyProductsResponse>(probeUrl, {}, params.diagnostics!);
  if (!probe.data || !Array.isArray(probe.data.products)) {
    log("warn", "Shopify feed not found.");
    return null;
  }

  log("success", "Shopify feed detected.");
  setDiscoveryStrategy(params.diagnostics!, "shopify_json");
  const currencyHint = await fetchShopifyCurrencyHint(currencyHintUrls, params.diagnostics!);

  const allProducts: ShopifyProduct[] = [];
  const maxPages = clampIntShared(process.env.SHOPIFY_MAX_PAGES, 20, 1, 200);
  const feedPrefix = params.collectionHandle ? `/collections/${params.collectionHandle}` : "";

  for (let page = 1; page <= maxPages; page++) {
    const url = `${params.baseUrl}${feedPrefix}/products.json?limit=250&page=${page}`;
    const batch = await fetchJsonTracked<ShopifyProductsResponse>(url, {}, params.diagnostics!);
    const products = batch.data?.products;
    if (!products || products.length === 0) break;
    allProducts.push(...products);
    if (products.length < 250) break;
  }

  const limitedProducts = allProducts.slice(0, params.maxProducts);
  log("data", `Loaded ${limitedProducts.length} products from Shopify feed.`);

  return buildShopifyResponse({
    ...params,
    currencyHint,
    products: limitedProducts,
    platformLabel: params.collectionHandle ? `Shopify (Collection: ${params.collectionHandle})` : "Shopify",
  });
}

async function enrichDirectShopifyPdpResponse(params: {
  brand: string;
  baseUrl: string;
  seedUrl?: string;
  response: Omit<ExtractResponse, "generated_at" | "logs">;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
}): Promise<Omit<ExtractResponse, "generated_at" | "logs">> {
  const product = params.response.products[0];
  if (!params.seedUrl || !product || params.response.products.length !== 1) return params.response;

  const productMissingImages = product.image_urls.length === 0;
  const variantMissingImages = product.variants.some((variant) => variant.image_urls.length === 0);
  if (!productMissingImages && !variantMissingImages) return params.response;

  params.log("info", `Shopify direct PDP returned incomplete image data. Attempting browser enrichment: ${params.seedUrl}`);

  const navigationTimeoutMs = clampIntShared(process.env.PUPPETEER_NAV_TIMEOUT_MS, DEFAULT_NAV_TIMEOUT_MS, 5_000, 120_000);
  const scrapeTimeoutMs = clampIntShared(process.env.PUPPETEER_SCRAPE_TIMEOUT_MS, DEFAULT_SCRAPE_TIMEOUT_MS, 10_000, 300_000);

  const browserRun = await runBrowserTaskWithFallback<ExtractedProduct | null>(
    async (browser) =>
      withTimeoutShared(
        scrapeProductPage({
          browser,
          url: params.seedUrl!,
          baseUrl: params.baseUrl,
          navigationTimeoutMs,
          verbose: false,
          log: params.log,
          diagnostics: params.diagnostics!,
          context: {},
        }),
        scrapeTimeoutMs,
        "Shopify direct PDP image enrichment",
      ),
    { diagnostics: params.diagnostics, log: params.log },
  );

  if (!browserRun.result) {
    params.log("warn", `Browser enrichment did not recover images for Shopify PDP: ${params.seedUrl}`);
    return params.response;
  }

  const merged = mergeShopifyDirectPdpFallback(params.brand, params.response, browserRun.result);
  if ((merged.products[0]?.image_urls.length || 0) > (product.image_urls.length || 0)) {
    params.log(
      "success",
      `Recovered ${merged.products[0]?.image_urls.length || 0} Shopify PDP images via browser enrichment: ${params.seedUrl}`,
    );
  }
  return merged;
}

function extractShopifyProductHandle(seedUrl: string | undefined, baseUrl: string): string | null {
  if (!seedUrl) return null;
  try {
    const parsed = new URL(seedUrl, baseUrl);
    const match = parsed.pathname.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products\/([^/?#]+)/i);
    return match?.[1] ? decodeURIComponent(match[1]) : null;
  } catch {
    return null;
  }
}

function buildShopifyResponse(params: {
  brand: string;
  domain: string;
  baseUrl: string;
  products: ShopifyProduct[];
  platformLabel: string;
  currencyHint: ExtractedVariant["currency"] | null;
  offset: number;
  limit: number;
  diagnostics: ExtractResponse["diagnostics"];
  log: Logger;
}) {
  const log = params.log;

  const variantDiscoverySetting = (process.env.SHOPIFY_VARIANT_DISCOVERY || "auto").toLowerCase().trim();
  const forceDiscoveryOff = ["0", "false", "no", "off", "none"].includes(variantDiscoverySetting);
  const forceDiscoveryOn = ["1", "true", "yes", "on", "title"].includes(variantDiscoverySetting);

  const discoveryCandidates = params.products
    .map((p) => {
      const split = splitTitleIntoBaseAndVariant(p.title);
      const isSingleDefault = (p.variants || []).length === 1 && isDefaultShopifyVariant(p.variants[0]!);
      return Boolean(split && isSingleDefault);
    })
    .filter(Boolean).length;

  const discoveryRate = params.products.length > 0 ? discoveryCandidates / params.products.length : 0;
  const autoDiscoveryOn = discoveryRate >= 0.2;

  const enableTitleDiscovery = !forceDiscoveryOff && (forceDiscoveryOn || (variantDiscoverySetting === "auto" && autoDiscoveryOn));
  if (enableTitleDiscovery && discoveryCandidates > 0) {
    log(
        "info",
      `Variant discovery enabled (mode=${variantDiscoverySetting}). Candidates: ${discoveryCandidates}/${params.products.length} (${Math.round(
        discoveryRate * 100,
      )}%).`,
    );
  } else {
    log(
      "info",
      `Variant discovery disabled (mode=${variantDiscoverySetting}). Candidates: ${discoveryCandidates}/${params.products.length} (${Math.round(
        discoveryRate * 100,
      )}%).`,
    );
  }

  const extractedByTitle = new Map<string, ExtractedProduct>();

  for (const product of params.products) {
    const productUrl = `${params.baseUrl}/products/${product.handle}`;
    const productImageUrls = resolveShopifyProductImageUrls(params.baseUrl, product);
    const titleSplit = enableTitleDiscovery ? splitTitleIntoBaseAndVariant(product.title) : null;
    const treatAsPseudoVariant =
      Boolean(titleSplit) && (product.variants || []).length === 1 && isDefaultShopifyVariant(product.variants[0]!);

    const canonicalProductTitle = treatAsPseudoVariant ? titleSplit!.baseTitle : product.title;
    const optionName = treatAsPseudoVariant
      ? "Variant"
      : product.options?.map((o) => o.name).filter((n): n is string => Boolean(n && n.trim())).join(" / ") || "Variant";
    const officialText = product.body_html;
    const currency = params.currencyHint || "USD";

    const extractedVariants: ExtractedVariant[] = (product.variants || []).map((v) => {
      const optionValue = treatAsPseudoVariant
        ? titleSplit!.variantLabel
        : [v.option1, v.option2, v.option3].filter((x): x is string => Boolean(x && x.trim())).join(" / ") ||
          v.title?.trim() ||
          "Default";

      const sku = (v.sku || "").trim() || `SHOPIFY-${v.id}`;
      const price = normalizeShopifyPrice(v.price, currency);
      const stock = toStockStatus(v.available, v.inventory_quantity);
      const imageUrls = resolveShopifyVariantImageUrls(params.baseUrl, product, v);
      const imageUrl = imageUrls[0] || "";
      const description = getMergedDescription({ title: canonicalProductTitle, overview: officialText });
      const adCopy = generateMockAdCopy(canonicalProductTitle, optionValue, price);

      return {
        id: String(v.id),
        sku,
        url: productUrl,
        option_name: optionName,
        option_value: optionValue,
        price,
        currency,
        stock,
        description,
        image_url: imageUrl,
        image_urls: imageUrls,
        ad_copy: adCopy,
      };
    });

    const existing =
      extractedByTitle.get(canonicalProductTitle) ||
      ({
        title: canonicalProductTitle,
        url: productUrl,
        image_url: productImageUrls[0] || "",
        image_urls: productImageUrls,
        variant_skus: [],
        variants: [],
      } satisfies ExtractedProduct);

    const seenVariants = new Set(existing.variants.map((variant) => `${variant.id}|${variant.sku}|${variant.url}`));
    for (const variant of extractedVariants) {
      const key = `${variant.id}|${variant.sku}|${variant.url}`;
      if (seenVariants.has(key)) continue;
      seenVariants.add(key);
      existing.variants.push(variant);
    }
    existing.image_urls = dedupeStringList([
      ...existing.image_urls,
      ...productImageUrls,
      ...extractedVariants.flatMap((variant) => variant.image_urls),
    ]);
    existing.image_url = existing.image_urls[0] || existing.image_url || "";
    existing.variant_skus = dedupeStringList([
      ...existing.variant_skus,
      ...extractedVariants.map((variant) => variant.sku),
    ]);
    extractedByTitle.set(canonicalProductTitle, existing);
  }

  const extractedProducts = Array.from(extractedByTitle.values());

  const pagedProducts = extractedProducts.slice(params.offset, params.offset + params.limit);
  const { variants, adCopyById } = flattenVariants({
    brand: params.brand,
    products: pagedProducts,
    simulated: false,
  });

  const productCount = extractedProducts.length;
  const variantCount = variants.length;
  const avg = pagedProducts.length > 0 ? (variantCount / pagedProducts.length).toFixed(2) : "0.00";
  const multi = extractedProducts.filter((p) => p.variants.length > 1).length;
  log(
    "data",
    `Summary: total_products=${productCount}, batch_products=${pagedProducts.length}, variants=${variantCount}, avg=${avg}, multi=${multi}`,
  );

  const pricing = computePricingStats(variants);
  log("success", `Extraction Complete. ${variants.length} variants processed successfully.`);
  const nextOffset = params.offset + params.limit;
  const hasMore = nextOffset < extractedProducts.length;

  return {
    brand: params.brand,
    domain: params.domain,
    mode: "puppeteer" as const,
    platform: params.platformLabel,
    products: pagedProducts,
    variants,
    pricing,
    ad_copy: { by_variant_id: adCopyById },
    pagination: {
      offset: params.offset,
      limit: params.limit,
      next_offset: hasMore ? nextOffset : null,
      has_more: hasMore,
      discovered_urls: extractedProducts.length,
    },
    diagnostics: params.diagnostics,
  };
}

export function mergeShopifyDirectPdpFallback(
  brand: string,
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  fallbackProduct: ExtractedProduct,
): Omit<ExtractResponse, "generated_at" | "logs"> {
  if (!response.products[0]) return response;

  const mergedProducts = response.products.map((product, idx) => {
    if (idx !== 0) return product;

    const mergedProduct: ExtractedProduct = {
      ...product,
      image_urls: [...product.image_urls],
      variant_skus: [...product.variant_skus],
      variants: product.variants.map((variant) => ({
        ...variant,
        image_urls: [...variant.image_urls],
      })),
    };

    const rawFallbackProductImages = dedupeStringList([
      ...fallbackProduct.image_urls,
      fallbackProduct.image_url,
      ...fallbackProduct.variants.flatMap((variant) => variant.image_urls),
      ...fallbackProduct.variants.map((variant) => variant.image_url),
    ]);
    const fallbackProductImages = selectRelevantFallbackImageUrls(
      {
        title: mergedProduct.title,
        url: mergedProduct.url,
      },
      rawFallbackProductImages,
    );

    if (fallbackProductImages.length === 0) return product;

    const fallbackBySku = new Map(
      fallbackProduct.variants
        .filter((variant) => variant.sku)
        .map((variant) => [variant.sku, variant] as const),
    );
    const fallbackByOption = new Map(
      fallbackProduct.variants
        .filter((variant) => variant.option_name || variant.option_value)
        .map((variant) => [`${variant.option_name}::${variant.option_value}`, variant] as const),
    );

    mergedProduct.variants = mergedProduct.variants.map((variant) => {
      const matchedFallback =
        fallbackBySku.get(variant.sku) ||
        fallbackByOption.get(`${variant.option_name}::${variant.option_value}`) ||
        fallbackProduct.variants[0];
      const relevantVariantFallbackImages = selectRelevantFallbackImageUrls(
        {
          title: [mergedProduct.title, variant.option_name, variant.option_value].filter(Boolean).join(" "),
          url: variant.url || mergedProduct.url,
        },
        dedupeStringList([
          ...(matchedFallback?.image_urls || []),
          matchedFallback?.image_url,
          ...fallbackProductImages,
        ]),
      );

      const mergedVariantImages = dedupeStringList([
        ...variant.image_urls,
        variant.image_url,
        ...relevantVariantFallbackImages,
      ]);

      return {
        ...variant,
        image_urls: mergedVariantImages,
        image_url: mergedVariantImages[0] || variant.image_url || mergedProduct.image_url,
      };
    });

    mergedProduct.image_urls = dedupeStringList([
      ...mergedProduct.image_urls,
      mergedProduct.image_url,
      ...fallbackProductImages,
      ...mergedProduct.variants.flatMap((variant) => variant.image_urls),
      ...mergedProduct.variants.map((variant) => variant.image_url),
    ]);
    mergedProduct.image_url = mergedProduct.image_urls[0] || mergedProduct.image_url || "";
    mergedProduct.variant_skus = dedupeStringList([
      ...mergedProduct.variant_skus,
      ...fallbackProduct.variant_skus,
      ...mergedProduct.variants.map((variant) => variant.sku),
    ]);

    return mergedProduct;
  });

  const { variants, adCopyById } = flattenVariants({
    brand,
    products: mergedProducts,
    simulated: false,
  });

  return {
    ...response,
    products: mergedProducts,
    variants,
    ad_copy: { by_variant_id: adCopyById },
  };
}

function resolveShopifyProductImageUrls(baseUrl: string, product: ShopifyProduct) {
  return dedupeStringList(resolveStructuredImageUrls(baseUrl, [product.featured_image, product.images]));
}

function resolveShopifyVariantImageUrls(baseUrl: string, product: ShopifyProduct, variant: ShopifyVariant) {
  const images = product.images || [];
  const matchedImages = images
    .filter((image) => typeof image === "object" && image !== null && (image.variant_ids || []).includes(variant.id));

  return dedupeStringList([
    ...resolveStructuredImageUrls(baseUrl, variant.featured_image),
    ...resolveStructuredImageUrls(baseUrl, matchedImages),
    ...resolveShopifyProductImageUrls(baseUrl, product),
  ]);
}

function resolveShopifyVariantImageUrl(baseUrl: string, product: ShopifyProduct, variant: ShopifyVariant): string | undefined {
  return resolveShopifyVariantImageUrls(baseUrl, product, variant)[0];
}

function toStockStatus(available?: boolean, inventoryQuantity?: number | null): StockStatus {
  if (available === false) return "Out of Stock";
  const qty = typeof inventoryQuantity === "number" ? inventoryQuantity : undefined;
  const lowStockThreshold = clampInt(process.env.LOW_STOCK_THRESHOLD, 10, 1, 9999);
  if (qty !== undefined && qty > 0 && qty <= lowStockThreshold) return "Low Stock";
  return "In Stock";
}

async function fetchJson<T>(url: string): Promise<T | null> {
  const timeoutMs = clampInt(process.env.PUPPETEER_FETCH_TIMEOUT_MS, DEFAULT_FETCH_TIMEOUT_MS, 2_000, 120_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        accept: "application/json",
        "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
      },
    });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchText(url: string): Promise<string | null> {
  const timeoutMs = clampInt(process.env.PUPPETEER_FETCH_TIMEOUT_MS, DEFAULT_FETCH_TIMEOUT_MS, 2_000, 120_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        accept: "text/plain,text/html,application/xml;q=0.9,*/*;q=0.8",
        "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
      },
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

export function extractProductUrlsFromHtml(html: string, baseUrl: string) {
  return extractProductUrlsFromHtmlShared(html, baseUrl);
}

const STATIC_ASSET_EXT_RE =
  /\.(?:css|js|mjs|map|png|jpe?g|gif|webp|svg|ico|pdf|xml|txt|woff2?|ttf|eot|otf|mp3|wav|mp4|webm|zip|gz|tar|json)(?:$|[?#])/i;

function parseHttpUrl(rawUrl: string, baseUrl: string): URL | null {
  try {
    const parsed = new URL(rawUrl, baseUrl);
    if (!/^https?:$/i.test(parsed.protocol)) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function isStaticAssetUrl(rawUrl: string, baseUrl: string) {
  return isStaticAssetUrlShared(rawUrl, baseUrl);
}

export function isLikelyProductUrl(rawUrl: string, baseUrl: string) {
  return isLikelyProductUrlShared(rawUrl, baseUrl);
}

function extractSitemapUrlsFromRobots(robotsText: string) {
  const urls: string[] = [];
  for (const match of robotsText.matchAll(/^sitemap:\s*(.+)$/gim)) {
    const url = match[1]?.trim();
    if (url) urls.push(url);
  }
  return urls;
}

function extractLocUrlsFromSitemap(xml: string) {
  const urls: string[] = [];
  for (const match of xml.matchAll(/<loc>([^<]+)<\/loc>/gim)) {
    const loc = match[1]?.trim();
    if (!loc) continue;
    const cleaned = loc.replace(/^<!\[CDATA\[/, "").replace(/\]\]>$/, "").trim();
    urls.push(cleaned);
  }
  return urls;
}

async function discoverProductUrls(params: { baseUrl: string; maxProducts: number; seedUrl?: string; log: Logger }) {
  if (params.seedUrl) {
    params.log("info", `GET ${params.seedUrl}`);
    const seedHtml = await fetchText(params.seedUrl);
    if (seedHtml) {
      const seedUrls = extractProductUrlsFromHtml(seedHtml, params.baseUrl);
      if (seedUrls.length > 0) {
        params.log("success", `Seed page yielded ${seedUrls.length} product links.`);
        return { sitemapUrl: undefined, productUrls: seedUrls.slice(0, params.maxProducts) };
      }
      params.log("warn", "Seed page did not yield product links; falling back to robots/sitemaps.");
    }
  }

  const robotsUrl = `${params.baseUrl}/robots.txt`;
  params.log("info", `GET ${robotsUrl}`);

  const robotsText = (await fetchText(robotsUrl)) || "";
  const sitemapUrls = extractSitemapUrlsFromRobots(robotsText);

  const candidates =
    sitemapUrls.length > 0
      ? sitemapUrls
      : [`${params.baseUrl}/sitemap.xml`, `${params.baseUrl}/sitemap_index.xml`];

  const visited = new Set<string>();
  const queue = [...candidates];
  const pageUrls: string[] = [];
  let chosenSitemap: string | undefined;

  const maxSitemaps = clampInt(process.env.MAX_SITEMAPS, 20, 1, 100);

  while (queue.length > 0 && visited.size < maxSitemaps) {
    const sitemapUrl = queue.shift();
    if (!sitemapUrl || visited.has(sitemapUrl)) continue;
    visited.add(sitemapUrl);

    const xml = await fetchText(sitemapUrl);
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
      const enoughLikely = likelySoFar.length >= params.maxProducts;
      const enoughAny = dedupedSoFar.length >= params.maxProducts * 2;
      if (enoughLikely || enoughAny) break;
    }
  }

  const deduped = Array.from(new Set(pageUrls)).filter((u) => u.startsWith("http"));
  const nonAsset = deduped.filter((u) => !isStaticAssetUrl(u, params.baseUrl));
  const productLike = nonAsset.filter((u) => isLikelyProductUrl(u, params.baseUrl));
  const selected = (productLike.length > 0 ? productLike : nonAsset).slice(0, params.maxProducts);

  return { sitemapUrl: chosenSitemap, productUrls: selected };
}

function normalizeJsonLdValue(value: unknown): unknown[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.flatMap(normalizeJsonLdValue);
  if (typeof value === "object") {
    const v = value as Record<string, unknown>;
    if (Array.isArray(v["@graph"])) return normalizeJsonLdValue(v["@graph"]);
    return [v];
  }
  return [];
}

function normalizeJsonLdObjects(value: unknown): Array<Record<string, unknown>> {
  return normalizeJsonLdValue(value).filter((obj): obj is Record<string, unknown> => Boolean(obj && typeof obj === "object"));
}

function normalizeJsonLdOffers(value: unknown): Array<Record<string, unknown>> {
  // `Product.offers` is often an `AggregateOffer` with a nested `offers: Offer[]`.
  // Unwrap that so we produce multiple variants instead of a single aggregated row.
  const out: Array<Record<string, unknown>> = [];
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

function isType(obj: Record<string, unknown>, typeName: string) {
  const t = obj["@type"];
  if (typeof t === "string") return t === typeName;
  if (Array.isArray(t)) return t.includes(typeName);
  return false;
}

function toAbsoluteUrl(baseUrl: string, href: string) {
  try {
    return new URL(href, baseUrl).toString();
  } catch {
    return href;
  }
}

const INVALID_IMAGE_URL_RE =
  /(placeholder\.svg|\/favicon|\/apple-touch-icon|\/logo(?:[._/-]|$)|\/sprite(?:[._/-]|$)|tracking|teads\.tv)/i;

function normalizeImageUrlCandidate(baseUrl: string, raw: string) {
  const trimmed = raw.trim();
  if (!trimmed) return "";

  const firstSrcsetEntry = trimmed.split(",")[0]?.trim().split(/\s+/)[0] || "";
  if (!firstSrcsetEntry) return "";

  const absolute = toAbsoluteUrl(baseUrl, firstSrcsetEntry);
  if (!/^https?:\/\//i.test(absolute)) return "";
  if (INVALID_IMAGE_URL_RE.test(absolute)) return "";
  return absolute;
}

export function resolveStructuredImageUrls(baseUrl: string, value: unknown): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  const visit = (candidate: unknown) => {
    if (!candidate) return;

    if (typeof candidate === "string") {
      const normalized = normalizeImageUrlCandidate(baseUrl, candidate);
      if (!normalized || seen.has(normalized)) return;
      seen.add(normalized);
      out.push(normalized);
      return;
    }

    if (Array.isArray(candidate)) {
      for (const item of candidate) visit(item);
      return;
    }

    if (typeof candidate !== "object") return;

    const obj = candidate as Record<string, unknown>;
    const directKeys = ["url", "src", "contentUrl", "contentURL", "secureUrl", "secure_url"] as const;
    const nestedKeys = ["thumbnail", "primaryImage", "image", "images"] as const;

    for (const key of directKeys) visit(obj[key]);
    for (const key of nestedKeys) visit(obj[key]);
  };

  visit(value);
  return out;
}

export function resolveStructuredImageUrl(baseUrl: string, value: unknown): string {
  return resolveStructuredImageUrls(baseUrl, value)[0] || "";
}

function stableId(input: string) {
  return createHash("sha1").update(input).digest("hex").slice(0, 12);
}

function stripProductTitlePrefix(productTitle: string, variantTitle: string): string {
  const normalizedProductTitle = productTitle.trim().toLowerCase();
  const normalizedVariantTitle = variantTitle.trim().toLowerCase();
  if (!normalizedProductTitle || !normalizedVariantTitle) return "";
  if (!normalizedVariantTitle.startsWith(normalizedProductTitle)) return variantTitle.trim();

  const suffix = variantTitle.slice(productTitle.length).trim().replace(/^[-–—:|/]+/, "").trim();
  return suffix || variantTitle.trim();
}

function normalizePrice(raw: unknown) {
  if (typeof raw === "number" && Number.isFinite(raw)) return raw.toFixed(2);
  if (typeof raw === "string" && raw.trim()) return raw.trim();
  return "0.00";
}

function stockFromAvailability(raw: unknown): StockStatus {
  const v = typeof raw === "string" ? raw : "";
  if (/OutOfStock/i.test(v)) return "Out of Stock";
  if (/InStock/i.test(v)) return "In Stock";
  return "In Stock";
}

async function scrapeProductPage(params: {
  browser: Browser;
  url: string;
  baseUrl: string;
  context: {};
  diagnostics: ExtractResponse["diagnostics"];
  navigationTimeoutMs: number;
  verbose: boolean;
  log: Logger;
}): Promise<ExtractedProduct | null> {
  const page = await params.browser.newPage();

  try {
    if (params.verbose) params.log("info", `Scraping: ${params.url}`);
    await preparePage(page, {
      baseUrl: params.baseUrl,
      context: params.context,
      navigationTimeoutMs: params.navigationTimeoutMs,
    });
    const visit = await gotoPageOrThrow(page, {
      url: params.url,
      baseUrl: params.baseUrl,
      context: params.context,
      diagnostics: params.diagnostics!,
    });
    const pageLooksLikeProduct = looksLikeProductPageHtml(visit.content);

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

      const metaDescription =
        document.querySelector('meta[name="description"]')?.getAttribute("content")?.trim() ||
        document.querySelector('meta[property="og:description"]')?.getAttribute("content")?.trim() ||
        "";

      const productDetailsText = (() => {
        const decodeHtmlText = (raw: string) => {
          const container = document.createElement("div");
          container.innerHTML = raw;
          return container.textContent?.trim() || "";
        };

        const normalize = (raw: string) =>
          raw
            .replace(/\r\n/g, "\n")
            .replace(/[ \t]+/g, " ")
            .replace(/\n[ \t]+/g, "\n")
            .replace(/\n{3,}/g, "\n\n")
            .trim();

        const hiddenOverview = document.getElementById("overview-about-text");
        const hiddenRaw = hiddenOverview?.getAttribute("value")?.trim() || "";
        if (hiddenRaw) {
          try {
            const decoded = decodeURIComponent(hiddenRaw);
            const text = normalize(decodeHtmlText(decoded));
            if (text) return text;
          } catch {
            const text = normalize(decodeHtmlText(hiddenRaw));
            if (text) return text;
          }
        }

        const moreAbout = document.querySelector(".more-about-product-content");
        if (moreAbout instanceof HTMLElement) {
          const text = normalize(moreAbout.innerText || moreAbout.textContent || "");
          if (text) return text;
        }

        return "";
      })();

      const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
        .map((s) => s.textContent || "")
        .filter(Boolean);

      const priceSelectors = [
        '[itemprop="price"]',
        '[class*="price"]',
        '[data-price]',
        'meta[property="og:price:amount"]',
        'meta[property="product:price:amount"]',
      ];
      const priceTexts: string[] = [];
      for (const selector of priceSelectors) {
        const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 8);
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

      const imageCandidates = (() => {
        const selectors = [
          "img.zoom-newPDPImage",
          "[zoom-src]",
          "[data-zoom-src]",
          "[data-zoom-image]",
          ".gallery-top-product img",
          ".gallery-thumbs-new-pdp img",
          '[class*="gallery"] img',
          '[class*="swiper"] img',
          'meta[property="og:image"]',
          'meta[name="twitter:image"]',
          'meta[itemprop="image"]',
          "img[data-src]",
          "img[srcset]",
          "img[src]",
        ];
        const invalidUrlRe =
          /(placeholder\.svg|\/favicon|\/apple-touch-icon|\/logo(?:[._/-]|$)|\/sprite(?:[._/-]|$)|tracking|teads\.tv|\/MenuBanner\/|\/Library-Sites-)/i;
        const seen = new Set<string>();
        const out: string[] = [];

        const push = (raw: string | null | undefined) => {
          const trimmed = typeof raw === "string" ? raw.trim() : "";
          if (!trimmed) return;

          const candidates = trimmed
            .split(",")
            .map((part) => part.trim().split(/\s+/)[0] || "")
            .filter(Boolean);

          for (const candidate of candidates) {
            try {
              const absolute = new URL(candidate, location.href).toString();
              if (!/^https?:\/\//i.test(absolute)) continue;
              if (invalidUrlRe.test(absolute)) continue;
              if (seen.has(absolute)) continue;
              seen.add(absolute);
              out.push(absolute);
            } catch {
              // ignore invalid image candidates
            }
          }
        };

        for (const selector of selectors) {
          const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 24);
          for (const node of nodes) {
            if (node instanceof HTMLMetaElement) {
              push(node.content);
              continue;
            }

            const el = node as HTMLElement;
            push(el.getAttribute("data-src"));
            push(el.getAttribute("zoom-src"));
            push(el.getAttribute("data-zoom-src"));
            push(el.getAttribute("data-zoom-image"));
            push(el.getAttribute("data-large-image"));
            push(el.getAttribute("srcset"));
            push(el.getAttribute("src"));
          }

          if (out.length >= 8) break;
        }

        return out;
      })();

      const domVariants = (() => {
        const el = document.querySelector("[data-product-skus-value]") as HTMLElement | null;
        const raw = el?.getAttribute("data-product-skus-value") || "";
        if (!raw) return [] as Array<{
          sku: string;
          option_name?: string;
          option_value?: string;
          url_path?: string;
          image_url?: string;
          image_urls?: string[];
          price?: string;
          ingredients?: string;
        }>;

        const textarea = document.createElement("textarea");
        textarea.innerHTML = raw;
        const decoded = textarea.value;

        try {
          const parsed = JSON.parse(decoded) as unknown;
          if (!Array.isArray(parsed)) return [];

          return parsed
            .map((item) => {
              const obj = item as Record<string, unknown>;
              const sku =
                (typeof obj.id === "string" && obj.id.trim()) ||
                (typeof obj.sku === "string" && obj.sku.trim()) ||
                "";

              const size = typeof obj.size === "string" ? obj.size.trim() : "";
              const shades = Array.isArray(obj.shades) ? obj.shades : [];
              const firstShade = shades[0] as Record<string, unknown> | undefined;
              const shadeTitle = typeof firstShade?.title === "string" ? firstShade.title.trim() : "";
              const multiShade = typeof obj.multi_shade_description === "string" ? obj.multi_shade_description.trim() : "";

              const optionName = size ? "Size" : shadeTitle || multiShade ? "Shade" : undefined;
              const optionValue = size || shadeTitle || multiShade || undefined;

              const urlPath = typeof obj.localized_path === "string" ? obj.localized_path.trim() : "";
              const ingredients = typeof obj.ingredients === "string" ? obj.ingredients.trim() : "";

              const images = Array.isArray(obj.images) ? obj.images : [];
              const imageUrls = images
                .map((image) => {
                  const next = image as Record<string, unknown>;
                  return typeof next?.src === "string" ? next.src.trim() : "";
                })
                .filter(Boolean);
              const imageUrl = imageUrls[0] || "";

              const price =
                (typeof obj.price_with_discount === "number" && Number.isFinite(obj.price_with_discount)
                  ? obj.price_with_discount.toFixed(2)
                  : "") ||
                (typeof obj.price === "number" && Number.isFinite(obj.price) ? obj.price.toFixed(2) : "") ||
                (typeof obj.price_with_discount === "string" && obj.price_with_discount.trim()) ||
                (typeof obj.price === "string" && obj.price.trim()) ||
                "";

              return {
                sku,
                option_name: optionName,
                option_value: optionValue,
                url_path: urlPath || undefined,
                image_url: imageUrl || undefined,
                image_urls: imageUrls.length > 0 ? imageUrls : undefined,
                price: price || undefined,
                ingredients: ingredients || undefined,
              };
            })
            .filter((v) => Boolean(v.sku));
        } catch {
          return [];
        }
      })();

      let howToUseContent = document.getElementById("accordion-toggle-How to Use");
      let ingredientsContent = document.getElementById("accordion-toggle-Ingredients and Safety");

      if (!howToUseContent || !ingredientsContent) {
        const buttons = Array.from(document.querySelectorAll("button[aria-controls]")) as HTMLButtonElement[];
        for (const button of buttons) {
          const titleText = (button.getAttribute("title") || button.textContent || "").trim().toLowerCase();
          if (!titleText) continue;

          const targetId = button.getAttribute("aria-controls") || "";
          if (!targetId) continue;

          if (!howToUseContent && titleText === "how to use") {
            howToUseContent = document.getElementById(targetId);
          } else if (!ingredientsContent && (titleText === "ingredients and safety" || titleText === "ingredients & safety")) {
            ingredientsContent = document.getElementById(targetId);
          }

          if (howToUseContent && ingredientsContent) break;
        }
      }

      const howToUseText = howToUseContent?.querySelector(".markdown")?.textContent?.trim() || undefined;
      const ingredientsMarkdownText = ingredientsContent?.querySelector(".markdown")?.textContent?.trim() || undefined;
      const ingredientsDisclaimerText =
        ingredientsContent?.querySelector(".product-details-accordions-ingredients-disclaimer")?.textContent?.trim() || undefined;

      return {
        title,
        canonical,
        metaDescription,
        priceTexts,
        imageCandidates,
        scripts,
        domVariants,
        productDetailsText,
        howToUseText,
        ingredientsMarkdownText,
        ingredientsDisclaimerText,
      };
    });

    const objects: Record<string, unknown>[] = [];
    for (const raw of extracted.scripts) {
      try {
        const parsed = JSON.parse(raw) as unknown;
        for (const obj of normalizeJsonLdValue(parsed)) {
          if (obj && typeof obj === "object") objects.push(obj as Record<string, unknown>);
        }
      } catch {
        // ignore invalid JSON-LD blocks
      }
    }

    const productObj = objects.find((o) => isType(o, "Product"));
    const productGroupObj = objects.find((o) => isType(o, "ProductGroup"));
    const variantProducts = normalizeJsonLdObjects(productGroupObj?.hasVariant).filter((o) => isType(o, "Product"));
    const primaryProductObj = productObj || variantProducts[0] || productGroupObj || null;
    if (!productObj && params.verbose) {
      params.log("warn", "> No JSON-LD Product schema found. Falling back to title/meta/price extraction.");
    }
    if (!primaryProductObj && !pageLooksLikeProduct) {
      if (params.verbose) {
        params.log("warn", `> Skipping non-product candidate: ${params.url}`);
      }
      return null;
    }

    const productTitle = (
      typeof productGroupObj?.name === "string" ? productGroupObj.name : typeof primaryProductObj?.name === "string" ? primaryProductObj.name : extracted.title
    ).trim() || extracted.title;
    const productUrl = canonicalizeUrlShared(
      toAbsoluteUrlShared(params.baseUrl, typeof primaryProductObj?.url === "string" ? primaryProductObj.url : extracted.canonical),
      params.baseUrl,
    );

    const imageRaw = primaryProductObj?.image ?? productGroupObj?.image;
    const productImageUrls = dedupeStringList([
      ...resolveStructuredImageUrls(params.baseUrl, [imageRaw, productGroupObj?.image, extracted.imageCandidates]),
      ...variantProducts.flatMap((variantProduct) => resolveStructuredImageUrls(params.baseUrl, variantProduct.image)),
    ]);
    const imageUrl = productImageUrls[0] || "";

    const officialText = choosePreferredProductOverview({
      structured:
        (typeof primaryProductObj?.description === "string" ? primaryProductObj.description : undefined) ||
        (typeof productGroupObj?.description === "string" ? productGroupObj.description : undefined),
      detailed: typeof extracted.productDetailsText === "string" ? extracted.productDetailsText : undefined,
      meta: extracted.metaDescription,
    });

    const offersRaw = primaryProductObj?.offers;
    const offers = normalizeJsonLdOffers(offersRaw);

    const domMetaBySku = new Map<string, DomVariantMeta>();
    for (const meta of extracted.domVariants || []) {
      if (!meta.sku) continue;
      domMetaBySku.set(meta.sku, meta);
    }

    const howToUseText = typeof extracted.howToUseText === "string" ? extracted.howToUseText.trim() : undefined;
    const ingredientsMarkdownText =
      typeof extracted.ingredientsMarkdownText === "string" ? extracted.ingredientsMarkdownText.trim() : undefined;
    const ingredientsDisclaimerText =
      typeof extracted.ingredientsDisclaimerText === "string" ? extracted.ingredientsDisclaimerText.trim() : undefined;

    const variants: ExtractedVariant[] =
      variantProducts.length > 1
        ? variantProducts.map((variantProduct, idx) => {
            const variantOffer = normalizeJsonLdOffers(variantProduct.offers)[0];
            const skuRaw =
              (typeof variantProduct.sku === "string" && variantProduct.sku.trim()) ||
              (typeof variantProduct.mpn === "string" && variantProduct.mpn.trim()) ||
              (typeof variantOffer?.sku === "string" && variantOffer.sku.trim()) ||
              "";
            const sku = skuRaw || `AUTO-${stableId(`${productUrl}|${idx}`)}`;
            const domMeta = domMetaBySku.get(sku);
            const variantName = typeof variantProduct.name === "string" ? variantProduct.name.trim() : "";
            const optionValue =
              (typeof variantProduct.color === "string" && variantProduct.color.trim()) ||
              stripProductTitlePrefix(productTitle, variantName) ||
              domMeta?.option_value ||
              variantName ||
              sku;
            const offerUrl = toAbsoluteUrlShared(
              params.baseUrl,
              typeof variantOffer?.url === "string"
                ? variantOffer.url
                : typeof variantProduct.url === "string"
                  ? variantProduct.url
                  : productUrl,
            );
            const price = normalizePrice(
              variantOffer?.price ??
                (variantOffer?.priceSpecification as any)?.price ??
                (variantOffer?.priceSpecification as any)?.priceSpecification?.price ??
                extracted.priceTexts[idx] ??
                extracted.priceTexts[0],
            );
            const stock = stockFromAvailability(variantOffer?.availability);
            const id = stableId(`${productUrl}|${sku}|${price}`);
            const variantImageRaw = variantProduct.image;
            const variantImageUrls = dedupeStringList([
              ...resolveStructuredImageUrls(params.baseUrl, [variantImageRaw, variantOffer?.image]),
              ...resolveStructuredImageUrls(params.baseUrl, [domMeta?.image_urls, domMeta?.image_url]),
              ...productImageUrls,
            ]);
            const variantImageUrl = variantImageUrls[0] || imageUrl;

            return {
              id,
              sku,
              url: offerUrl,
              option_name: domMeta?.option_name || "Variant",
              option_value: optionValue,
              price,
              currency: "USD",
              stock,
              description: getMergedDescription({
                title: productTitle,
                overview:
                  (typeof variantProduct.description === "string" ? variantProduct.description : undefined) || officialText,
                howToUse: howToUseText,
                ingredientsAndSafety:
                  [ingredientsMarkdownText, ingredientsDisclaimerText].filter(Boolean).join("\n\n") || undefined,
              }),
              image_url: variantImageUrl,
              image_urls: variantImageUrls,
              ad_copy: generateMockAdCopy(productTitle, optionValue, price),
            };
          })
        : offers.length > 0
        ? offers.map((offer, idx) => {
            const skuRaw =
              (typeof offer.sku === "string" && offer.sku.trim()) ||
              (typeof primaryProductObj?.sku === "string" && primaryProductObj.sku.trim()) ||
              (typeof primaryProductObj?.mpn === "string" && primaryProductObj.mpn.trim()) ||
              "";
            const sku = skuRaw || `AUTO-${stableId(`${productUrl}|${idx}`)}`;

            const domMeta = domMetaBySku.get(sku);

            const offerUrl = (() => {
              if (domMeta?.url_path) return toAbsoluteUrlShared(params.baseUrl, domMeta.url_path);
              return toAbsoluteUrlShared(params.baseUrl, typeof offer.url === "string" ? offer.url : productUrl);
            })();

            const price = normalizePrice(
              offer.price ??
                (offer.priceSpecification as any)?.price ??
                (offer.priceSpecification as any)?.priceSpecification?.price ??
                domMeta?.price,
            );
            const stock = stockFromAvailability(offer.availability);
            const optionValueFromOffer =
              (typeof offer.name === "string" && offer.name.trim()) || (typeof offer.description === "string" && offer.description.trim());

            const optionValue = optionValueFromOffer || domMeta?.option_value || sku;
            const optionName = domMeta?.option_name || "Offer";

            const id = stableId(`${productUrl}|${sku}|${price}`);
            const ingredientsText = domMeta?.ingredients || ingredientsMarkdownText;
            const ingredientsAndSafety = [ingredientsText, ingredientsDisclaimerText].filter(Boolean).join("\n\n") || undefined;
            const description = getMergedDescription({
              title: productTitle,
              overview: officialText,
              howToUse: howToUseText,
              ingredientsAndSafety,
            });
            const adCopy = generateMockAdCopy(productTitle, optionValue, price);

            const offerImageRaw = offer.image;
            const offerImageUrls = dedupeStringList([
              ...resolveStructuredImageUrls(params.baseUrl, [offerImageRaw, domMeta?.image_urls, domMeta?.image_url, imageRaw, extracted.imageCandidates]),
              ...productImageUrls,
            ]);
            const offerImageUrl = offerImageUrls[0] || imageUrl;

            return {
              id,
              sku,
              url: offerUrl,
              option_name: optionName,
              option_value: optionValue,
              price,
              currency: "USD",
              stock,
              description,
              image_url: offerImageUrl,
              image_urls: offerImageUrls,
              ad_copy: adCopy,
            };
          })
        : [
            {
              id: stableId(productUrl),
              sku: `AUTO-${stableId(productUrl).slice(0, 8)}`,
              url: productUrl,
              option_name: "Offer",
              option_value: "Default",
              price: normalizePrice(extracted.priceTexts[0]),
              currency: "USD",
              stock: "In Stock",
              description: getMergedDescription({
                title: productTitle,
                overview: officialText,
                howToUse: howToUseText,
                ingredientsAndSafety:
                  [ingredientsMarkdownText, ingredientsDisclaimerText].filter(Boolean).join("\n\n") || undefined,
              }),
              image_url: imageUrl,
              image_urls: productImageUrls,
              ad_copy: generateMockAdCopy(productTitle, "Default", normalizePrice(extracted.priceTexts[0])),
            },
          ];

    const finalProductImageUrls = dedupeStringList([
      ...productImageUrls,
      ...variants.flatMap((variant) => variant.image_urls),
      ...variants.map((variant) => variant.image_url),
    ]);
    const finalProductImageUrl = finalProductImageUrls[0] || imageUrl;

    if (params.verbose) {
      if (productObj) {
        params.log("data", "> Found JSON-LD 'Product' Schema");
      } else if (productGroupObj) {
        params.log("data", "> Found JSON-LD 'ProductGroup' Schema");
      }
      params.log("success", `> Extracted ${variants.length} offers/variants`);
    }

    return {
      title: productTitle,
      url: productUrl,
      image_url: finalProductImageUrl,
      image_urls: finalProductImageUrls,
      variant_skus: dedupeStringList(variants.map((variant) => variant.sku)),
      variants,
    };
  } catch (err) {
    if (err instanceof BotChallengeError) {
      throw err;
    }
    params.log("warn", `Failed to scrape ${params.url}`);
    return null;
  } finally {
    await page.close().catch(() => undefined);
  }
}
