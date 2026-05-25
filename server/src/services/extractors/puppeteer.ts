import { createHash } from "crypto";
import { type Browser, type HTTPRequest, type Page } from "puppeteer";

import type {
  ExtractInput,
  ExtractResponse,
  ExtractedBundleComponent,
  ExtractedProduct,
  ExtractedProductDetailSection,
  ExtractedProductFaqItem,
  ExtractedProductKind,
  ExtractedProductReviewSummary,
  ExtractedVariant,
  ExtractedVariantRow,
  Extractor,
  StockStatus,
} from "./types";
import { reviewedVariantOverrides } from "./reviewedVariantOverrides";
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
  isUnsafeSeedLocaleRedirect,
  looksLikeKnownNonProductUrl,
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
  type FetchContext,
  type LoggerFn,
} from "./shared";
import { getMarketProfile } from "./marketProfiles";

const DEFAULT_BATCH_LIMIT = 10;
const DEFAULT_MAX_TOTAL_PRODUCTS = 500;
const DEFAULT_CONCURRENCY = 3;
const DEFAULT_NAV_TIMEOUT_MS = 8_000;
const DEFAULT_FETCH_TIMEOUT_MS = 8_000;
const DEFAULT_LAUNCH_TIMEOUT_MS = 15_000;
const DEFAULT_SCRAPE_TIMEOUT_MS = 60_000;
const DEFAULT_PRODUCT_URL_RESERVE = 4;
const DEFAULT_IMAGE_VISION_TIMEOUT_MS = 45_000;
const DEFAULT_IMAGE_VISION_MAX_IMAGES = 6;
const DEFAULT_IMAGE_VISION_MAX_IMAGE_BYTES = 5_000_000;
const DEFAULT_BROWSERISH_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

type BrowserTaskRunner = typeof runBrowserTaskWithFallback;

export class PuppeteerExtractor implements Extractor {
  constructor(private readonly browserRunner: BrowserTaskRunner = runBrowserTaskWithFallback) {}

  async extract(input: ExtractInput): Promise<ExtractResponse> {
    const generatedAt = new Date().toISOString();

    const logs: ExtractResponse["logs"] = [];
    const log: LoggerFn = (type, msg) => {
      logs.push({ at: new Date().toISOString(), type, msg });
    };

    const requestedTarget = parseTargetShared(input.domain);
    const diagnostics = createDiagnostics(requestedTarget.domain, requestedTarget.baseUrl);
    const marketId = normalizeMarketId(input.market);
    const marketProfile = getMarketProfile(marketId);
    const browserContext = {
      headers: marketProfile.headers,
      cookies: marketProfile.cookies,
      storefront_password: typeof input.storefront_password === "string" ? input.storefront_password.trim() : undefined,
    };
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
        context: browserContext,
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
        marketId,
        seedUrl: target.seedUrl,
        productTitle: input.product_title,
        collectionHandle: target.collectionHandle,
        maxProducts: maxProductsTotal,
        offset: batchOffset,
        limit: batchLimit,
        diagnostics,
        log,
        browserRunner: this.browserRunner,
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
        context: browserContext,
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

      const browserRun = await this.browserRunner(
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
                context: browserContext,
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

type ScrapedPageSignals = {
  title: string;
  canonical: string;
  metaDescription: string;
  priceTexts: string[];
  imageCandidates: string[];
  scripts: string[];
  embeddedProductScripts: string[];
  domVariants: DomVariantMeta[];
  productVolumeText?: string;
  productDetailsText: string;
  howToUseText?: string;
  ingredientsMarkdownText?: string;
  ingredientsDisclaimerText?: string;
  activeIngredientsText?: string;
  detailsSections: ExtractedProductDetailSection[];
  faqItems: ExtractedProductFaqItem[];
  faqHtmlSnippets: string[];
  okendoMetafieldJson?: string;
  renderedReviewSummary?: ExtractedProductReviewSummary;
};

type RawScrapedPageSignals = Omit<ScrapedPageSignals, "renderedReviewSummary"> & {
  renderedReviewSummary?: RenderedReviewSummarySignal;
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
  "{title}",
  "{title} ({variant})",
  "Offer snapshot: {title}",
  "Catalog capture: {title}",
  "{title} - merchant PDP observation",
] as const;

const AD_CAPTION_TEMPLATES = [
  "Observed product: {title}\nObserved option: {variant}",
  "Merchant PDP capture for {title}\nOption observed: {variant}",
  "Catalog extraction snapshot for {title}\nObserved option: {variant}",
  "Observed sellable option for {title}: {variant}",
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
  void title;
  void variantValue;
  void price;
  return "";
}

function cleanText(text?: string) {
  if (!text) return "";
  const withNewlines = text
    .replace(/[\u00a0\u202f\u2007]/g, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\s*(?:h[1-6]|li|ul|ol|hr)\b[^>]*>/gi, "\n")
    .replace(/<\s*\/\s*(?:h[1-6]|li|ul|ol)\s*>/gi, "\n")
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

const PRODUCT_SIZE_OPTION_RE =
  /\b\d+(?:\.\d+)?\s*(?:fl\.?\s*oz\.?|fluid\s*ounces?|m\s*l|ml|g|kg|oz|l|lb|lbs|mm|cm|ea|ct|count|pcs?|pieces?|pads?|patches?)\b(?:\s*(?:x|×)\s*\d+)?/i;
const PRODUCT_SIZE_OPTION_GLOBAL_RE = new RegExp(PRODUCT_SIZE_OPTION_RE.source, "gi");

function getProductSizeUnitPriority(value: string) {
  const normalized = cleanText(value).toLowerCase();
  if (!normalized) return 0;
  if (/(?:\d|\b)(?:ml|m l|g|kg|l|mm|cm)\b/.test(normalized)) return 4;
  if (/(?:\d|\b)(?:ea|ct|count|pcs?|pieces?|pads?|patches?)\b/.test(normalized)) return 3;
  if (/(?:\d|\b)(?:fl\.?\s*oz\.?|fluid\s*ounces?|oz|lb|lbs)\b/.test(normalized)) return 2;
  return 1;
}

function pickPreferredProductSizeMatch(value?: string) {
  const normalized = cleanText(value);
  if (!normalized) return "";
  const matches = Array.from(normalized.matchAll(PRODUCT_SIZE_OPTION_GLOBAL_RE))
    .map((match) => cleanText(match[0]))
    .filter(Boolean);
  if (matches.length === 0) return "";
  return matches.reduce((best, current) =>
    getProductSizeUnitPriority(current) > getProductSizeUnitPriority(best) ? current : best,
  );
}

function normalizeProductSizeOptionValue(value?: string) {
  const raw = pickPreferredProductSizeMatch(value);
  if (!raw) return "";
  return raw
    .replace(/\s+/g, " ")
    .replace(/m\s*l/gi, "ml")
    .replace(/fluid\s*ounces?/gi, "fl oz")
    .replace(/fl\.?\s*oz\.?/gi, "fl oz")
    .replace(/\b(ml|g|kg|oz|l|lb|lbs|mm|cm|ea|ct|count|pcs?|pieces?|pads?|patches?)\b/gi, (unit) =>
      unit.toLowerCase(),
    )
    .replace(/(\d)\s+(ml|g|kg|oz|l|lb|lbs|mm|cm)\b/gi, "$1$2")
    .replace(/(\d)\s+(ea|ct|count|pcs?|pieces?|pads?|patches?)\b/gi, "$1 $2")
    .replace(/\s*(x|×)\s*/i, " x ")
    .trim();
}

function formatProductSizeDisplayUnit(unit: string) {
  const normalized = cleanText(unit)
    .replace(/fluid\s*ounces?/gi, "fl oz")
    .replace(/fl\.?\s*oz\.?/gi, "fl oz")
    .replace(/m\s*l/gi, "mL")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "";
  if (/^ml$/i.test(normalized)) return "mL";
  if (/^g$/i.test(normalized)) return "g";
  if (/^kg$/i.test(normalized)) return "kg";
  if (/^l$/i.test(normalized)) return "L";
  if (/^mm$/i.test(normalized)) return "mm";
  if (/^cm$/i.test(normalized)) return "cm";
  if (/^oz$/i.test(normalized)) return "oz";
  if (/^lb$/i.test(normalized)) return "lb";
  if (/^lbs$/i.test(normalized)) return "lbs";
  if (/^fl oz$/i.test(normalized)) return "fl oz";
  if (/^(ea|ct|count|pcs?|pieces?|pads?|patches?)$/i.test(normalized)) return normalized.toLowerCase();
  return normalized;
}

function formatProductSizeDisplayValue(raw: string) {
  const normalized = cleanText(raw).replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const match = normalized.match(
    /\b(\d+(?:\.\d+)?)\s*(fl\.?\s*oz\.?|fluid\s*ounces?|m\s*l|ml|g|kg|oz|l|lb|lbs|mm|cm|ea|ct|count|pcs?|pieces?|pads?|patches?)\b(?:\s*(?:x|×)\s*\d+)?/i,
  );
  if (!match) return "";
  const amount = cleanText(match[1]);
  const unit = formatProductSizeDisplayUnit(match[2] || "");
  if (!amount || !unit) return "";
  return `${amount} ${unit}`.trim();
}

type ProductSizeEvidence = {
  optionValue: string;
  detailLabel?: string;
  alternateOptionValue?: string;
};

function collectProductSizeEvidenceMatches(value?: string) {
  if (!value) return [];
  const rawText = decodeHtmlAttributeEntities(String(value).replace(/<[^>]+>/g, " "));
  const normalized = cleanText(rawText);
  if (!normalized) return [];
  const matches = Array.from(normalized.matchAll(PRODUCT_SIZE_OPTION_GLOBAL_RE))
    .map((match) => cleanText(match[0]))
    .filter(Boolean);
  return dedupeStringList(matches).map((match) => ({
    optionValue: normalizeProductSizeOptionValue(match),
    displayValue: formatProductSizeDisplayValue(match),
  }));
}

function getProductSizeDisplayPriority(value: string) {
  const normalized = cleanText(value).toLowerCase();
  if (!normalized) return 99;
  if (/\b(?:fl\.?\s*oz|oz|lb|lbs)\b/.test(normalized)) return 1;
  if (/\b(?:ml|m l|g|kg|l|mm|cm)\b/.test(normalized)) return 2;
  if (/\b(?:ea|ct|count|pcs?|pieces?|pads?|patches?)\b/.test(normalized)) return 3;
  return 4;
}

function extractProductSizeEvidence(...values: Array<string | undefined>): ProductSizeEvidence {
  const collected: Array<{ optionValue: string; displayValue: string }> = [];
  const seenOptionValues = new Set<string>();
  const pushMatches = (candidate?: string) => {
    for (const match of collectProductSizeEvidenceMatches(candidate)) {
      if (!match.optionValue || seenOptionValues.has(match.optionValue)) continue;
      seenOptionValues.add(match.optionValue);
      collected.push(match);
    }
  };

  for (const value of values) {
    pushMatches(value);
    if (!value) continue;
    try {
      const parsed = new URL(value);
      pushMatches(decodeURIComponent(parsed.pathname).replace(/[-_]+/g, " "));
      pushMatches(decodeURIComponent(parsed.search).replace(/[-_]+/g, " "));
    } catch {
      // Plain titles and non-URL fragments are handled above.
    }
  }

  if (collected.length === 0) {
    return { optionValue: "" };
  }

  const primary = collected.reduce((best, current) =>
    getProductSizeUnitPriority(current.optionValue) > getProductSizeUnitPriority(best.optionValue) ? current : best,
  );
  const alternate =
    collected.find((item) => item.optionValue !== primary.optionValue) || null;
  const detailParts = dedupeStringList(
    [primary, alternate]
      .filter(Boolean)
      .sort((left, right) => getProductSizeDisplayPriority(left!.displayValue) - getProductSizeDisplayPriority(right!.displayValue))
      .map((item) => item?.displayValue || "")
      .filter(Boolean),
  );

  return {
    optionValue: primary.optionValue,
    ...(detailParts.length > 0 ? { detailLabel: detailParts.join(" / ") } : {}),
    ...(alternate?.optionValue ? { alternateOptionValue: alternate.optionValue } : {}),
  };
}

function extractProductSizeOptionValue(...values: Array<string | undefined>) {
  return extractProductSizeEvidence(...values).optionValue;
}

function isGenericOfferOptionValue(value: string | undefined, productTitle: string) {
  const option = cleanText(value).toLowerCase();
  if (!option) return true;
  if (/^(?:default|default title|offer|variant)$/i.test(option)) return true;
  return option === cleanText(productTitle).toLowerCase();
}

function findReviewedVariantOverride(product: ExtractedProduct) {
  const productUrl = canonicalizeUrlShared(product.url || "");
  const productTitle = cleanText(product.title).toLowerCase();
  const variantSku = cleanText(product.variants[0]?.sku).toLowerCase();
  return reviewedVariantOverrides.find((override) => {
    if (canonicalizeUrlShared(override.product_url) !== productUrl) return false;
    if (cleanText(override.product_title).toLowerCase() !== productTitle) return false;
    if (override.sku && cleanText(override.sku).toLowerCase() !== variantSku) return false;
    return true;
  });
}

function applyReviewedVariantOverride(
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  seedUrl: string | undefined,
  log?: Logger,
): Omit<ExtractResponse, "generated_at" | "logs"> {
  const canonicalSeedUrl = canonicalizeUrlShared(seedUrl || "");
  if (!canonicalSeedUrl || !response.products[0]) return response;

  let changed = false;
  const products = response.products.map((product, idx) => {
    if (idx !== 0 || canonicalizeUrlShared(product.url || "") !== canonicalSeedUrl) return product;
    if (product.variants.length !== 1) return product;
    const onlyVariant = product.variants[0];
    if (!isGenericOfferOptionValue(onlyVariant?.option_value, product.title)) return product;

    const override = findReviewedVariantOverride(product);
    if (!override) return product;
    changed = true;
    return {
      ...product,
      variants: [
        {
          ...onlyVariant,
          option_name: override.option_name,
          option_value: override.option_value,
          source_origin: "manual_override" as const,
          source_quality_status: "medium" as const,
          hidden_from_selector: false,
        },
      ],
    };
  });

  if (!changed) return response;
  const { variants, adCopyById } = flattenVariants({
    brand: response.brand,
    products,
    simulated: false,
  });
  const override = reviewedVariantOverrides.find((item) => canonicalizeUrlShared(item.product_url) === canonicalSeedUrl);
  if (override && log) {
    log(
      "info",
      `Applied reviewed variant override for Shopify PDP: ${override.product_title} -> ${override.option_name}: ${override.option_value}`,
    );
  }
  return {
    ...response,
    products,
    variants,
    ad_copy: { by_variant_id: adCopyById },
  };
}

function isPdpContentNoiseText(text?: string) {
  const normalized = cleanText(text).toLowerCase();
  if (!normalized) return false;
  if (/\bsome tracking technologies\b/.test(normalized)) return true;
  if (/\b(?:accept all|privacy settings|cookie settings|privacy policy privacy settings)\b/.test(normalized)) {
    return true;
  }
  if (/\bwe'?ll never show your full name\b/.test(normalized)) return true;
  if (/\benter a valid email\b/.test(normalized)) return true;
  if (/\bplease fill all of the required fields\b/.test(normalized)) return true;
  if (/\b(?:submit your review|write a review|review submitted|loading reviews)\b/.test(normalized)) return true;
  return false;
}

function isLowQualityDetailSectionText(heading?: string, body?: string) {
  const normalizedHeading = cleanText(heading).toLowerCase();
  const normalizedBody = cleanText(body).toLowerCase();
  if (
    /^(?:tell us about yourself|write a review|submit your review|leave feedback(?: about this)?(?: cancel reply)?|cancel reply|privacy settings|cookie settings)$/.test(
      normalizedHeading,
    )
  ) {
    return true;
  }
  if (
    normalizedHeading === "less details" &&
    /\bage\b.*\bskin type\b|\bi was incentivized\b|\bskin concern\b/.test(normalizedBody)
  ) {
    return true;
  }
  if (/^ingredients?$/.test(normalizedHeading) && /^see full ingredients?$/.test(normalizedBody)) {
    return true;
  }
  return isPdpContentNoiseText(`${heading || ""}\n${body || ""}`);
}

function isTaxonomyOnlyDetailSection(section: ExtractedProductDetailSection | undefined) {
  const heading = cleanText(section?.heading).toLowerCase();
  const body = cleanText(section?.body);
  const sourceKind = cleanText(section?.source_kind).toLowerCase();
  if (!heading || !body) return false;
  const tagSource =
    sourceKind === "shopify_product_tags" ||
    sourceKind === "embedded_product_json_tags" ||
    sourceKind === "product_image_vision";
  if (!tagSource) return false;
  return heading === "product type";
}

function isStructuredProseSourceKind(sourceKind: string | undefined) {
  const normalized = cleanText(sourceKind).toLowerCase();
  return normalized === "page_section_stack_prose" || normalized === "page_image_with_text_prose";
}

export function normalizeStructuredProseDetailSection(
  section: ExtractedProductDetailSection,
): ExtractedProductDetailSection {
  if (!isStructuredProseSourceKind(section?.source_kind)) return section;
  const heading = cleanText(section?.heading);
  const body = cleanText(section?.body);
  if (!heading || !body) return { ...section, heading, body };
  const combined = `${heading}\n${body}`;
  const normalizedHeading =
    /^(?:how to use|how to apply|directions?|usage|suggested usage)$/i.test(heading) ||
    /(?:^|\n)\s*(?:\d+[.)]\s*|step\s*\d+|use after|apply to|smooth(?: it)? over|leave on|massage|finish by)\b/i.test(combined) ||
    /^tone up your routine$/i.test(heading)
      ? "How to Use"
      : /\b(?:clinical|testing?|results?|improvement|decrease(?:d)?|increase(?:d)?|after\s+\d+\s+(?:day|days|week|weeks)|study)\b/i.test(
            `${heading} ${body}`,
          )
        ? "Testing Results"
        : heading;
  return {
    ...section,
    heading: normalizedHeading,
    body,
  };
}

export function normalizeStructuredProseDetailSections(
  sections: ExtractedProductDetailSection[] | undefined,
): ExtractedProductDetailSection[] {
  return Array.isArray(sections) ? sections.map((section) => normalizeStructuredProseDetailSection(section)) : [];
}

function normalizeDetailSectionMediaUrls(section: ExtractedProductDetailSection | undefined) {
  return dedupeStringList(
    [
      ...(Array.isArray(section?.media_urls) ? section.media_urls : []),
      ...((section as { mediaUrls?: string[] } | undefined)?.mediaUrls || []),
    ]
      .map((value) => cleanText(value))
      .filter(Boolean),
  );
}

function dedupeDetailSections(sections: ExtractedProductDetailSection[]) {
  const out: ExtractedProductDetailSection[] = [];
  const seen = new Map<string, number>();
  for (const section of Array.isArray(sections) ? sections : []) {
    let heading = normalizeDetailSectionHeading(section?.heading);
    const body = cleanText(section?.body);
    const sourceKind = cleanText(section?.source_kind) || "unknown";
    const mediaUrls = normalizeDetailSectionMediaUrls(section);
    if (!heading || !body) continue;
    if (heading === "Ingredients" && !looksLikeFullIngredientListText(body)) {
      heading = "Key Ingredients";
    }
    if (isTaxonomyOnlyDetailSection({ heading, body, source_kind: sourceKind })) continue;
    if (isLowQualityDetailSectionText(heading, body)) continue;
    const keyBody = heading === "Ingredients" ? cleanText(body.replace(/^full ingredients?\s*/i, "")) : body;
    const key = `${heading.toLowerCase()}|${keyBody.toLowerCase()}`;
    const existingIndex = seen.get(key);
    if (existingIndex != null) {
      const existing = out[existingIndex];
      if (!existing) continue;
      const mergedMediaUrls = dedupeStringList([
        ...normalizeDetailSectionMediaUrls(existing),
        ...mediaUrls,
      ]);
      if (mergedMediaUrls.length > 0) existing.media_urls = mergedMediaUrls;
      continue;
    }
    seen.set(key, out.length);
    out.push({
      heading,
      body,
      source_kind: sourceKind,
      ...(mediaUrls.length > 0 ? { media_urls: mediaUrls } : {}),
    });
  }
  return out;
}

function normalizeDetailSectionHeading(value: string | undefined) {
  const heading = cleanText(value);
  if (!heading) return "";
  if (/^(?:product details?|details?|about(?: the product)?|description)$/i.test(heading)) return "Details";
  if (/^(?:benefits?|why it works|what it does|why we love it)$/i.test(heading)) return "Benefits";
  if (/^(?:key ingredients?|highlight(?:ed)? ingredients?|ingredients story)$/i.test(heading)) {
    return "Key Ingredients";
  }
  if (/^(?:clinical(?: results?| claims?)?|results?|proven results?)$/i.test(heading)) {
    return "Clinical Results";
  }
  if (/^(?:how to use|how to apply|directions?|usage|suggested usage|application|application tips?)$/i.test(heading)) {
    return "How to Use";
  }
  if (/^(?:ingredients?|ingredients and safety|ingredient list|full ingredients?|full ingredient list|inci)$/i.test(heading)) {
    return "Ingredients";
  }
  if (/^(?:faq|frequently asked questions?|q(?:uestions)?\s*&\s*a|questions?)$/i.test(heading)) {
    return "FAQ";
  }
  return heading;
}

function normalizeFaqQuestion(value: string | undefined) {
  return cleanText(value)
    .replace(/^(?:q(?:uestion)?\s*[:/-]\s*)/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeFaqAnswer(value: string | undefined) {
  return cleanText(value)
    .replace(/^(?:a(?:nswer)?\s*[:/-]\s*)/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeFaqItems(items: ExtractedProductFaqItem[]) {
  const out: ExtractedProductFaqItem[] = [];
  const seen = new Set<string>();
  for (const item of Array.isArray(items) ? items : []) {
    const question = normalizeFaqQuestion(item?.question);
    const answer = normalizeFaqAnswer(item?.answer);
    const sourceKind = cleanText(item?.source_kind) || "unknown";
    const sourceUrl = cleanText(item?.source_url);
    const sourceTitle = cleanText(item?.source_title);
    if (!question || !answer) continue;
    const key = `${question.toLowerCase()}|${answer.toLowerCase()}|${sourceKind.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      question,
      answer,
      source_kind: sourceKind,
      ...(sourceUrl ? { source_url: sourceUrl } : {}),
      ...(sourceTitle ? { source_title: sourceTitle } : {}),
    });
  }
  return out;
}

function getDetailSectionSourcePriority(sourceKind: string | undefined) {
  const normalized = cleanText(sourceKind).toLowerCase();
  if (!normalized) return 0;
  if (
    normalized === "modal_content" ||
    normalized === "product_modal_content" ||
    normalized.startsWith("embedded_custom_metafield_") ||
    normalized.startsWith("custom_metafield_")
  ) {
    return 6;
  }
  if (
    normalized === "accordion_ingredients" ||
    normalized === "accordion_how_to_use" ||
    normalized === "ingredients_flyout" ||
    normalized === "details_summary"
  ) {
    return 5;
  }
  if (
    normalized === "page_section_stack_prose" ||
    normalized === "page_image_with_text_prose"
  ) {
    return 4;
  }
  if (normalized === "accordion_control" || normalized === "pdp_content_heading" || normalized === "page_product_details") {
    return 4;
  }
  if (normalized === "accordion_button") return 2;
  if (normalized === "heading_sibling") return 1;
  return 3;
}

function pickBestDetailSection(
  sections: ExtractedProductDetailSection[],
  predicate: (section: ExtractedProductDetailSection) => boolean,
) {
  return [...sections]
    .filter(predicate)
    .sort((left, right) => {
      const priorityDelta =
        getDetailSectionSourcePriority(right.source_kind) - getDetailSectionSourcePriority(left.source_kind);
      if (priorityDelta !== 0) return priorityDelta;
      return cleanText(right.body).length - cleanText(left.body).length;
    })[0];
}

function isLowQualityFaqItem(item: ExtractedProductFaqItem) {
  const question = normalizeFaqQuestion(item.question).toLowerCase();
  const answer = normalizeFaqAnswer(item.answer).toLowerCase();
  if (!question || !answer) return true;
  if (/\bloading questions\b/.test(answer)) return true;
  if (/^(?:what(?:'|’)s in it|what else[!?]*|how to(?: use| apply)?)\??$/.test(question)) return true;
  if (/^why .+ loves it$/.test(question)) return true;
  if (/^how to pair$/.test(question)) return true;
  if (/be the first to be in the know/.test(question)) return true;
  if (
    /\b(?:forgot your password|reset your password|create an account|create account|sign in|log in|login|track(?: my)? order|where(?:'|’)s my order|order status|returns?|refunds?|shipping policy|contact us|customer service)\b/.test(
      question,
    )
  ) {
    return true;
  }
  if (
    /\b(?:enter your email|check your inbox|follow the reset link|sign in to your account|log in to your account|contact customer service)\b/.test(
      answer,
    )
  ) {
    return true;
  }
  if (
    /\b(?:are you sure you want to quit|booking request will be made|your current selections will be lost|review our privacy policy)\b/.test(
      `${question} ${answer}`,
    )
  ) {
    return true;
  }
  if (
    /^(?:how to build a skincare(?: regimen)?|regimen guide)\b/.test(question) &&
    /^(?:regimen guide\.?)$/i.test(answer)
  ) {
    return true;
  }
  if (/\b(?:shop now|save \d+%|sign up|subscribe|newsletter|join our list|text .*join)\b/.test(`${question} ${answer}`)) {
    return true;
  }
  if (/\bmore\b$/.test(answer)) return true;
  return false;
}

export function filterUsefulFaqItems(items: ExtractedProductFaqItem[]) {
  return dedupeFaqItems(items).filter((item) => !isLowQualityFaqItem(item));
}

type RenderedReviewSummarySignal = {
  text?: string;
  aria_labels?: string[];
};

export function parseRenderedBazaarvoiceReviewSummary(
  signal: RenderedReviewSummarySignal | null | undefined,
): ExtractedProductReviewSummary | undefined {
  const text = cleanText(signal?.text);
  const ariaLabels = dedupeStringList(
    (Array.isArray(signal?.aria_labels) ? signal!.aria_labels : []).map((item) => cleanText(item)),
  );
  const corpus = [text, ...ariaLabels].filter(Boolean).join(" | ");
  if (!corpus) return undefined;

  const ariaMatch = corpus.match(/(\d+(?:\.\d+)?)\s*out of 5 stars\D+(\d[\d,]*)\s+reviews?/i);
  const inlineMatch = corpus.match(/(\d+(?:\.\d+)?)\s*read\s*(\d[\d,]*)\s*reviews?/i);
  const fallbackRatingMatch = corpus.match(/\b(\d+(?:\.\d+)?)\b/);
  const fallbackCountMatch = corpus.match(/\b(?:read\s*)?(\d[\d,]*)\s*reviews?\b/i);

  const rating = Number.parseFloat(
    cleanText(ariaMatch?.[1] || inlineMatch?.[1] || fallbackRatingMatch?.[1] || ""),
  );
  const reviewCount = Number.parseInt(
    cleanText(ariaMatch?.[2] || inlineMatch?.[2] || fallbackCountMatch?.[1] || "").replace(/,/g, ""),
    10,
  );

  if (!Number.isFinite(rating) || rating <= 0) return undefined;
  if (!Number.isFinite(reviewCount) || reviewCount <= 0) return undefined;

  return {
    rating: Number(rating.toFixed(2)),
    review_count: Math.floor(reviewCount),
    scale: 5,
    aggregation_scope: "product",
    exact_item_review_count: Math.floor(reviewCount),
  };
}

const CROSS_PRODUCT_MISMATCH_HOST_RE = /(?:^|\.)theordinary\.com$/i;
const PRODUCT_SLUG_STOPWORDS = new Set([
  "the",
  "ordinary",
  "serum",
  "solution",
  "cream",
  "cleanser",
  "moisturizer",
  "moisturiser",
  "treatment",
  "face",
  "spf",
  "and",
  "with",
  "for",
  "plus",
  "ml",
  "oz",
]);

function extractMeaningfulProductTokens(raw: string | undefined) {
  return dedupeStringList(
    cleanText(raw)
      .toLowerCase()
      .replace(/https?:\/\/[^/]+/g, " ")
      .replace(/\.html?$/g, " ")
      .split(/[^a-z0-9]+/i)
      .map((token) => token.trim())
      .filter((token) => token.length >= 3 && !PRODUCT_SLUG_STOPWORDS.has(token) && !/^\d+$/.test(token)),
  );
}

export function isKnownCrossProductResolutionMismatch(params: {
  sourceUrl: string;
  extractedUrl?: string;
  extractedTitle?: string;
}) {
  try {
    const source = new URL(params.sourceUrl);
    const extracted = params.extractedUrl ? new URL(params.extractedUrl) : null;
    if (!CROSS_PRODUCT_MISMATCH_HOST_RE.test(source.hostname)) return false;
    if (!extracted || !CROSS_PRODUCT_MISMATCH_HOST_RE.test(extracted.hostname)) return false;
    const sourceSlug = decodeURIComponent(source.pathname.split("/").filter(Boolean).pop() || "");
    const extractedSlug = decodeURIComponent(extracted.pathname.split("/").filter(Boolean).pop() || "");
    const sourceTokens = extractMeaningfulProductTokens(sourceSlug);
    const extractedTokens = extractMeaningfulProductTokens(`${extractedSlug} ${params.extractedTitle || ""}`);
    if (sourceTokens.length === 0 || extractedTokens.length === 0) return false;
    return sourceTokens.every((token) => !extractedTokens.includes(token));
  } catch {
    return false;
  }
}

const QUARANTINED_PDP_SOURCE_KIND_RE =
  /^(?:product_image_vision|simulation|browser_fallback(?::.*|$)|.*(?:mock|synthetic).*)$/i;
const SHOPIFY_PDP_SOURCE_KIND_RE =
  /^(?:shopify_|embedded_shopify|embedded_product_json|shopify_body_html|structured_overview)/i;
const JSONLD_PDP_SOURCE_KIND_RE = /^jsonld/i;
const LINEAR_NARRATIVE_PDP_SOURCE_KIND_RE = /^(?:drjart_linear_(?:details|story))$/i;
const RETAIL_PDP_SOURCE_KIND_RE =
  /^(?:page_|accordion_|details_|faq_|merchant_faq|inline_html_faq|okendo_|modal_content|pdp_content_heading|custom_metafield_|embedded_custom_metafield_)/i;

function normalizePdpSourceKinds(sourceKinds: string[] | undefined) {
  return dedupeStringList(
    (Array.isArray(sourceKinds) ? sourceKinds : [])
      .map((item) => cleanText(item))
      .filter(Boolean),
  );
}

function classifyPdpFieldQuality(sourceKinds: string[] | undefined) {
  const normalized = normalizePdpSourceKinds(sourceKinds);
  const reasonCodes: string[] = [];
  if (normalized.length === 0) {
    return {
      source_origin: "unknown" as const,
      source_quality_status: "low" as const,
      source_kinds: [],
      reason_codes: ["missing_source_kind"],
    };
  }

  const safeKinds = normalized.filter((kind) => !QUARANTINED_PDP_SOURCE_KIND_RE.test(kind));
  const quarantinedKinds = normalized.filter((kind) => QUARANTINED_PDP_SOURCE_KIND_RE.test(kind));
  if (safeKinds.length > 0 && quarantinedKinds.length > 0) {
    reasonCodes.push("mixed_with_quarantined_sources");
  }

  const basis = safeKinds.length > 0 ? safeKinds : normalized;
  const firstKind = basis[0] || "";
  const fallbackSummary = {
    source_origin: "unknown" as const,
    source_quality_status: "low" as const,
    source_kinds: normalized,
    reason_codes: reasonCodes,
  };

  if (QUARANTINED_PDP_SOURCE_KIND_RE.test(firstKind) && safeKinds.length === 0) {
    return {
      source_origin:
        firstKind === "product_image_vision"
          ? ("image_vision" as const)
          : firstKind.startsWith("browser_fallback")
            ? ("browser_fallback" as const)
            : firstKind === "simulation"
              ? ("simulation" as const)
              : ("unknown" as const),
      source_quality_status: "quarantined" as const,
      source_kinds: normalized,
      reason_codes: [...reasonCodes, "quarantined_source_kind"],
    };
  }
  if (basis.some((kind) => SHOPIFY_PDP_SOURCE_KIND_RE.test(kind))) {
    return {
      source_origin: "shopify_json" as const,
      source_quality_status: "high" as const,
      source_kinds: normalized,
      reason_codes: reasonCodes,
    };
  }
  if (basis.some((kind) => JSONLD_PDP_SOURCE_KIND_RE.test(kind))) {
    return {
      source_origin: "jsonld" as const,
      source_quality_status: "high" as const,
      source_kinds: normalized,
      reason_codes: reasonCodes,
    };
  }
  if (basis.some((kind) => LINEAR_NARRATIVE_PDP_SOURCE_KIND_RE.test(kind))) {
    return {
      source_origin: "retail_pdp" as const,
      source_quality_status: "medium" as const,
      source_kinds: normalized,
      reason_codes: reasonCodes,
    };
  }
  if (basis.some((kind) => RETAIL_PDP_SOURCE_KIND_RE.test(kind))) {
    return {
      source_origin: "retail_pdp" as const,
      source_quality_status: "medium" as const,
      source_kinds: normalized,
      reason_codes: reasonCodes,
    };
  }
  return fallbackSummary;
}

function isQuarantinedDetailSourceKind(sourceKind: string | undefined) {
  return QUARANTINED_PDP_SOURCE_KIND_RE.test(cleanText(sourceKind));
}

function tagFallbackSourceKind(sourceKind: string | undefined) {
  const normalized = cleanText(sourceKind) || "unknown";
  return normalized.startsWith("browser_fallback") ? normalized : `browser_fallback:${normalized}`;
}

function tagFallbackFieldSources(sourceKinds: string[] | undefined) {
  return normalizePdpSourceKinds(sourceKinds).map((kind) => tagFallbackSourceKind(kind));
}

function fallbackFieldSourceKinds(
  sourceKinds: string[] | undefined,
  fallbackLabel: string,
): string[] {
  const tagged = tagFallbackFieldSources(sourceKinds);
  return tagged.length > 0 ? tagged : [tagFallbackSourceKind(fallbackLabel)];
}

type OkendoMetafieldSnapshot = {
  subscriberId: string;
  productId: string;
  questionCount: number;
  reviewCount: number;
  averageRating?: number;
  ratingDistribution: Array<{ stars: number; count: number; percent?: number }>;
  reviewsNextUrl?: string;
  reviewsOrderBy?: string;
  areReviewsGrouped?: boolean;
};

type OkendoQuestionAnswer = {
  body?: string;
  status?: string;
  isPrivate?: boolean;
  isStoreAnswer?: boolean;
};

type OkendoQuestion = {
  body?: string;
  status?: string;
  answers?: OkendoQuestionAnswer[];
};

type OkendoQuestionsResponse = {
  questions?: OkendoQuestion[];
};

type OkendoReviewMedia = {
  fullSizeUrl?: string;
  largeUrl?: string;
  mediumUrl?: string;
  smallUrl?: string;
  thumbnailUrl?: string;
  type?: string;
};

type OkendoReviewer = {
  displayName?: string;
  isVerified?: boolean;
};

type OkendoReview = {
  reviewId?: string;
  rating?: number;
  title?: string;
  body?: string;
  status?: string;
  helpfulCount?: number;
  reviewer?: OkendoReviewer;
  media?: OkendoReviewMedia[];
};

type OkendoReviewsResponse = {
  areReviewsGrouped?: boolean;
  reviews?: OkendoReview[];
};

function buildOkendoDistributionRows(
  reviewAggregate: Record<string, unknown> | null,
  reviewCount: number,
): Array<{ stars: number; count: number; percent?: number }> {
  const rawDistribution =
    reviewAggregate?.reviewCountByLevel && typeof reviewAggregate.reviewCountByLevel === "object"
      ? (reviewAggregate.reviewCountByLevel as Record<string, unknown>)
      : reviewAggregate?.ratingAndReviewCountByLevel && typeof reviewAggregate.ratingAndReviewCountByLevel === "object"
        ? (reviewAggregate.ratingAndReviewCountByLevel as Record<string, unknown>)
        : null;
  if (!rawDistribution) return [];

  const rows: Array<{ stars: number; count: number; percent?: number }> = [];
  for (let stars = 5; stars >= 1; stars -= 1) {
    const key = `level${stars}Count`;
    const count = Number(rawDistribution[key]);
    if (!Number.isFinite(count) || count < 0) continue;
    rows.push({
      stars,
      count: Math.floor(count),
      ...(reviewCount > 0 ? { percent: Math.max(0, Math.min(1, count / reviewCount)) } : {}),
    });
  }
  return rows;
}

function resolveOkendoAverageRating(reviewAggregate: Record<string, unknown> | null, reviewCount: number) {
  const directAverage = Number(reviewAggregate?.averageRating);
  if (Number.isFinite(directAverage) && directAverage > 0) return directAverage;
  const reviewRatingValuesTotal = Number(reviewAggregate?.reviewRatingValuesTotal);
  if (Number.isFinite(reviewRatingValuesTotal) && reviewCount > 0) {
    return Number((reviewRatingValuesTotal / reviewCount).toFixed(2));
  }
  const ratingAndReviewValuesTotal = Number(reviewAggregate?.ratingAndReviewValuesTotal);
  const ratingAndReviewCount = Number(reviewAggregate?.ratingAndReviewCount);
  if (
    Number.isFinite(ratingAndReviewValuesTotal) &&
    Number.isFinite(ratingAndReviewCount) &&
    ratingAndReviewCount > 0
  ) {
    return Number((ratingAndReviewValuesTotal / ratingAndReviewCount).toFixed(2));
  }
  return undefined;
}

function resolveOkendoReviewsOrderBy(
  reviewsNextUrl: string | undefined,
  defaultSort: string | undefined,
) {
  const sortFromSnapshot = cleanText(defaultSort);
  if (sortFromSnapshot) return sortFromSnapshot;
  const normalizedUrl = cleanText(reviewsNextUrl);
  if (!normalizedUrl) return "date desc";
  try {
    const parsed = new URL(normalizedUrl.startsWith("http") ? normalizedUrl : `https://api.okendo.io/v1${normalizedUrl}`);
    return cleanText(parsed.searchParams.get("orderBy") || "") || "date desc";
  } catch {
    return "date desc";
  }
}

function parseOkendoMetafieldSnapshot(raw: string | undefined): OkendoMetafieldSnapshot | null {
  const normalized = cleanText(raw);
  if (!normalized) return null;
  try {
    const parsed = JSON.parse(normalized) as Record<string, unknown>;
    const reviewAggregate =
      parsed.reviewAggregate && typeof parsed.reviewAggregate === "object"
        ? (parsed.reviewAggregate as Record<string, unknown>)
        : null;
    const subscriberProductKey = cleanText(
      typeof reviewAggregate?.subscriberId_productId === "string" ? reviewAggregate.subscriberId_productId : undefined,
    );
    const reviewsNextUrl = cleanText(typeof parsed.reviewsNextUrl === "string" ? parsed.reviewsNextUrl : undefined);
    const sortConfig =
      parsed.sort && typeof parsed.sort === "object" ? (parsed.sort as Record<string, unknown>) : null;
    const subscriberIdFromKey = subscriberProductKey.split(":")[0] || "";
    const productIdFromKey = subscriberProductKey.split(":").slice(1).join(":") || "";
    const subscriberIdFromReviewsUrl =
      reviewsNextUrl.match(/\/stores\/([^/]+)\/products\/([^/?#]+)/)?.[1] || "";
    const productIdFromReviewsUrl =
      reviewsNextUrl.match(/\/stores\/([^/]+)\/products\/([^/?#]+)/)?.[2] || "";
    const subscriberId = cleanText(
      typeof parsed.subscriberId === "string"
        ? parsed.subscriberId
        : typeof reviewAggregate?.subscriberId === "string"
          ? reviewAggregate.subscriberId
          : subscriberIdFromKey || subscriberIdFromReviewsUrl,
    );
    const productId = cleanText(
      typeof parsed.productId === "string"
        ? parsed.productId
        : typeof reviewAggregate?.productId === "string"
          ? reviewAggregate.productId
          : productIdFromKey || productIdFromReviewsUrl,
    );
    const questionCountRaw = Number(parsed.questionCount);
    const reviewCountRaw = Number(
      parsed.reviewCount ??
        reviewAggregate?.reviewCount ??
        reviewAggregate?.ratingAndReviewCount ??
        reviewAggregate?.ratingCount,
    );
    if (!subscriberId || !productId) return null;
    const reviewCount = Number.isFinite(reviewCountRaw) ? Math.max(0, Math.floor(reviewCountRaw)) : 0;
    const averageRatingFromParsed = Number(parsed.averageRating);
    const averageRating =
      (Number.isFinite(averageRatingFromParsed) && averageRatingFromParsed > 0 ? averageRatingFromParsed : undefined) ||
      resolveOkendoAverageRating(reviewAggregate, reviewCount);
    return {
      subscriberId,
      productId,
      questionCount: Number.isFinite(questionCountRaw) ? Math.max(0, Math.floor(questionCountRaw)) : 0,
      reviewCount,
      ...(Number.isFinite(averageRating) ? { averageRating } : {}),
      ratingDistribution: buildOkendoDistributionRows(reviewAggregate, reviewCount),
      ...(reviewsNextUrl ? { reviewsNextUrl } : {}),
      ...(resolveOkendoReviewsOrderBy(reviewsNextUrl, cleanText(typeof sortConfig?.defaultSort === "string" ? sortConfig.defaultSort : undefined))
        ? { reviewsOrderBy: resolveOkendoReviewsOrderBy(reviewsNextUrl, cleanText(typeof sortConfig?.defaultSort === "string" ? sortConfig.defaultSort : undefined)) }
        : {}),
      ...(typeof parsed.areReviewsGrouped === "boolean" ? { areReviewsGrouped: parsed.areReviewsGrouped } : {}),
    };
  } catch {
    return null;
  }
}

function pickBestOkendoAnswer(answers: OkendoQuestionAnswer[] | undefined): OkendoQuestionAnswer | null {
  const approvedAnswers = (Array.isArray(answers) ? answers : []).filter((answer) => {
    const body = normalizeFaqAnswer(answer?.body);
    const status = cleanText(answer?.status).toLowerCase();
    if (!body) return false;
    if (answer?.isPrivate) return false;
    if (status && status !== "approved") return false;
    return true;
  });
  if (approvedAnswers.length === 0) return null;
  approvedAnswers.sort((left, right) => Number(Boolean(right?.isStoreAnswer)) - Number(Boolean(left?.isStoreAnswer)));
  return approvedAnswers[0] || null;
}

function parseOkendoLooseSummary(raw: string | undefined) {
  const normalized = cleanText(raw);
  if (!normalized) return {} as { averageRating?: number; reviewCount?: number; questionCount?: number };
  try {
    const parsed = JSON.parse(normalized) as Record<string, unknown>;
    const averageRating = Number(parsed.averageRating);
    const reviewCount = Number(parsed.reviewCount);
    const questionCount = Number(parsed.questionCount);
    return {
      ...(Number.isFinite(averageRating) && averageRating > 0 ? { averageRating } : {}),
      ...(Number.isFinite(reviewCount) && reviewCount >= 0 ? { reviewCount: Math.floor(reviewCount) } : {}),
      ...(Number.isFinite(questionCount) && questionCount >= 0 ? { questionCount: Math.floor(questionCount) } : {}),
    };
  } catch {
    return {} as { averageRating?: number; reviewCount?: number; questionCount?: number };
  }
}

function decodeEmbeddedJsonString(value: string | undefined) {
  const normalized = typeof value === "string" ? value : "";
  if (!normalized) return "";
  const htmlDecoded = decodeHtmlAttributeEntities(normalized);
  try {
    return JSON.parse(`"${htmlDecoded.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`);
  } catch {
    return cleanText(htmlDecoded.replace(/\\\//g, "/").replace(/\\u0026/g, "&"));
  }
}

function extractOkendoSettingsFromHtml(html: string) {
  const match = html.match(/<script[^>]*id=["']oke-reviews-settings["'][^>]*>([\s\S]*?)<\/script>/i);
  if (!match) return { subscriberId: "", defaultSort: "" };
  try {
    const parsed = JSON.parse(cleanText(match[1])) as Record<string, unknown>;
    const widgetSettings =
      parsed.widgetSettings && typeof parsed.widgetSettings === "object"
        ? (parsed.widgetSettings as Record<string, unknown>)
        : null;
    const homepageCarousel =
      widgetSettings?.homepageCarousel && typeof widgetSettings.homepageCarousel === "object"
        ? (widgetSettings.homepageCarousel as Record<string, unknown>)
        : null;
    return {
      subscriberId: cleanText(typeof parsed.subscriberId === "string" ? parsed.subscriberId : undefined),
      defaultSort: cleanText(typeof homepageCarousel?.defaultSort === "string" ? homepageCarousel.defaultSort : undefined),
    };
  } catch {
    return { subscriberId: "", defaultSort: "" };
  }
}

function extractOkendoProductIdFromHtml(html: string) {
  const match = html.match(/data-oke-reviews-product-id=["']([^"']+)["']/i);
  return cleanText(match?.[1]);
}

function extractOkendoReviewsNextUrlFromHtml(html: string) {
  const match = html.match(/"reviewsNextUrl"\s*:\s*"([^"]+)"/i);
  return decodeEmbeddedJsonString(match?.[1]);
}

function extractOkendoAreReviewsGroupedFromHtml(html: string) {
  const match = html.match(/"areReviewsGrouped"\s*:\s*(true|false)/i);
  if (!match) return undefined;
  return match[1] === "true";
}

export function extractOkendoMetafieldJsonFromHtml(html: string | undefined) {
  const normalized = typeof html === "string" ? html : "";
  if (!normalized.trim()) return undefined;
  const matches = Array.from(normalized.matchAll(/<script[^>]*data-oke-metafield-data[^>]*>([\s\S]*?)<\/script>/gi));
  let fallbackSummary: { averageRating?: number; reviewCount?: number; questionCount?: number } | null = null;
  for (const match of matches.reverse()) {
    const raw = cleanText(match[1]);
    if (!raw) continue;
    if (parseOkendoMetafieldSnapshot(raw)) return raw;
    const looseSummary = parseOkendoLooseSummary(raw);
    if (!fallbackSummary && Object.keys(looseSummary).length > 0) fallbackSummary = looseSummary;
  }

  const { subscriberId, defaultSort } = extractOkendoSettingsFromHtml(normalized);
  const productId = extractOkendoProductIdFromHtml(normalized);
  if (!subscriberId || !productId) return undefined;

  const synthesized = {
    subscriberId,
    productId,
    ...(fallbackSummary || {}),
    ...(extractOkendoReviewsNextUrlFromHtml(normalized)
      ? { reviewsNextUrl: extractOkendoReviewsNextUrlFromHtml(normalized) }
      : {}),
    ...(defaultSort ? { sort: { defaultSort } } : {}),
    ...(typeof extractOkendoAreReviewsGroupedFromHtml(normalized) === "boolean"
      ? { areReviewsGrouped: extractOkendoAreReviewsGroupedFromHtml(normalized) }
      : {}),
  };
  const serialized = JSON.stringify(synthesized);
  if (parseOkendoMetafieldSnapshot(serialized)) {
    return serialized;
  }
  return undefined;
}

export async function fetchOkendoFaqItemsFromMetafieldJson(raw: string | undefined, sourceUrl: string) {
  const snapshot = parseOkendoMetafieldSnapshot(raw);
  if (!snapshot || snapshot.questionCount <= 0) return [] as ExtractedProductFaqItem[];

  const limit = Math.min(Math.max(snapshot.questionCount, 1), 12);
  const endpoint = `https://api.okendo.io/v1/stores/${encodeURIComponent(snapshot.subscriberId)}/products/${encodeURIComponent(snapshot.productId)}/questions?limit=${limit}`;

  try {
    const response = await withTimeout(fetch(endpoint), 10000, "okendo_questions_fetch");
    if (!response.ok) return [] as ExtractedProductFaqItem[];
    const payload = (await response.json()) as OkendoQuestionsResponse;
    const items: ExtractedProductFaqItem[] = [];

    for (const questionRow of Array.isArray(payload.questions) ? payload.questions : []) {
      const question = normalizeFaqQuestion(questionRow?.body);
      const status = cleanText(questionRow?.status).toLowerCase();
      if (!question) continue;
      if (status && status !== "approved") continue;
      if (!looksLikeFaqQuestionText(question)) continue;
      const answerRow = pickBestOkendoAnswer(questionRow?.answers);
      const answer = normalizeFaqAnswer(answerRow?.body);
      if (!answer) continue;
      items.push({
        question,
        answer,
        source_kind: "okendo_questions_api",
        source_url: sourceUrl,
        source_title: "Product Questions",
      });
    }

    return filterUsefulFaqItems(items);
  } catch {
    return [] as ExtractedProductFaqItem[];
  }
}

function mergeOkendoReviewPreviewItems(
  values: Array<NonNullable<ExtractedProductReviewSummary["preview_items"]> | undefined>,
) {
  const out: NonNullable<ExtractedProductReviewSummary["preview_items"]> = [];
  const seen = new Set<string>();
  for (const value of values) {
    for (const item of Array.isArray(value) ? value : []) {
      const reviewId = cleanText(item?.review_id).toLowerCase();
      if (!reviewId || seen.has(reviewId)) continue;
      seen.add(reviewId);
      out.push(item);
    }
  }
  return out;
}

function mergeOkendoReviewQuestions(
  values: Array<NonNullable<ExtractedProductReviewSummary["questions"]> | undefined>,
) {
  const out: NonNullable<ExtractedProductReviewSummary["questions"]> = [];
  const seen = new Set<string>();
  for (const value of values) {
    for (const item of Array.isArray(value) ? value : []) {
      const question = cleanText(item?.question).toLowerCase();
      if (!question || seen.has(question)) continue;
      seen.add(question);
      out.push(item);
    }
  }
  return out;
}

function mergeOkendoReviewSummary(
  existing: ExtractedProductReviewSummary | undefined,
  incoming: ExtractedProductReviewSummary | undefined,
): ExtractedProductReviewSummary | undefined {
  const left = existing && typeof existing === "object" ? existing : undefined;
  const right = incoming && typeof incoming === "object" ? incoming : undefined;
  if (!left) return right;
  if (!right) return left;

  const previewItems = mergeOkendoReviewPreviewItems([left.preview_items, right.preview_items]);
  const questions = mergeOkendoReviewQuestions([left.questions, right.questions]);
  const starDistribution =
    (Array.isArray(left.star_distribution) && left.star_distribution.length > 0 ? left.star_distribution : undefined) ||
    (Array.isArray(left.rating_distribution) && left.rating_distribution.length > 0 ? left.rating_distribution : undefined) ||
    (Array.isArray(right.star_distribution) && right.star_distribution.length > 0 ? right.star_distribution : undefined) ||
    (Array.isArray(right.rating_distribution) && right.rating_distribution.length > 0 ? right.rating_distribution : undefined);

  return {
    ...(left.scale != null ? { scale: left.scale } : right.scale != null ? { scale: right.scale } : {}),
    ...(left.rating != null ? { rating: left.rating } : right.rating != null ? { rating: right.rating } : {}),
    ...(left.review_count != null
      ? { review_count: left.review_count }
      : right.review_count != null
        ? { review_count: right.review_count }
        : {}),
    ...(left.aggregation_scope
      ? { aggregation_scope: left.aggregation_scope }
      : right.aggregation_scope
        ? { aggregation_scope: right.aggregation_scope }
        : {}),
    ...(left.exact_item_review_count != null
      ? { exact_item_review_count: left.exact_item_review_count }
      : right.exact_item_review_count != null
        ? { exact_item_review_count: right.exact_item_review_count }
        : {}),
    ...(left.product_line_review_count != null
      ? { product_line_review_count: left.product_line_review_count }
      : right.product_line_review_count != null
        ? { product_line_review_count: right.product_line_review_count }
        : {}),
    ...(left.scope_label ? { scope_label: left.scope_label } : right.scope_label ? { scope_label: right.scope_label } : {}),
    ...(starDistribution ? { star_distribution: starDistribution, rating_distribution: starDistribution } : {}),
    ...(previewItems.length > 0 ? { preview_items: previewItems } : {}),
    ...(questions.length > 0 ? { questions } : {}),
    ...(left.brand_card
      ? { brand_card: left.brand_card }
      : right.brand_card
        ? { brand_card: right.brand_card }
        : {}),
  };
}

function buildOkendoReviewSummaryFromSnapshot(snapshot: OkendoMetafieldSnapshot) {
  const distribution = snapshot.ratingDistribution;
  const summary: ExtractedProductReviewSummary = {
    ...(snapshot.averageRating != null ? { rating: snapshot.averageRating } : {}),
    ...(snapshot.reviewCount > 0 ? { review_count: snapshot.reviewCount, scale: 5 } : {}),
    ...(distribution.length > 0 ? { star_distribution: distribution, rating_distribution: distribution } : {}),
    ...(snapshot.areReviewsGrouped === true ? { aggregation_scope: "group", product_line_review_count: snapshot.reviewCount } : {}),
    ...(snapshot.areReviewsGrouped === false ? { aggregation_scope: "product", exact_item_review_count: snapshot.reviewCount } : {}),
  };
  return Object.keys(summary).length > 0 ? summary : undefined;
}

function resolveOkendoReviewMediaItems(media: OkendoReviewMedia[] | undefined) {
  const out: NonNullable<NonNullable<ExtractedProductReviewSummary["preview_items"]>[number]["media"]> = [];
  const seen = new Set<string>();
  for (const item of Array.isArray(media) ? media : []) {
    const url = cleanText(
      typeof item?.fullSizeUrl === "string"
        ? item.fullSizeUrl
        : typeof item?.largeUrl === "string"
          ? item.largeUrl
          : typeof item?.mediumUrl === "string"
            ? item.mediumUrl
            : typeof item?.smallUrl === "string"
              ? item.smallUrl
              : undefined,
    );
    if (!url || seen.has(url.toLowerCase())) continue;
    seen.add(url.toLowerCase());
    const thumbnailUrl = cleanText(
      typeof item?.thumbnailUrl === "string"
        ? item.thumbnailUrl
        : typeof item?.smallUrl === "string"
          ? item.smallUrl
          : typeof item?.mediumUrl === "string"
            ? item.mediumUrl
            : undefined,
    );
    out.push({
      type: cleanText(typeof item?.type === "string" ? item.type : undefined) || "image",
      url,
      ...(thumbnailUrl ? { thumbnail_url: thumbnailUrl } : {}),
      source: "merchant_public",
      source_kind: "okendo_reviews_api",
      source_scope: "merchant_public",
      content_review_state: "approved",
      public_visible: true,
    });
  }
  return out;
}

function buildOkendoReviewsEndpoint(snapshot: OkendoMetafieldSnapshot, limit: number) {
  const normalizedLimit = Math.min(Math.max(limit, 1), 6);
  const orderBy = cleanText(snapshot.reviewsOrderBy) || "date desc";
  return `https://api.okendo.io/v1/stores/${encodeURIComponent(snapshot.subscriberId)}/products/${encodeURIComponent(snapshot.productId)}/reviews?limit=${normalizedLimit}&orderBy=${encodeURIComponent(orderBy)}`;
}

export async function fetchOkendoReviewSummaryFromMetafieldJson(raw: string | undefined, sourceUrl: string) {
  const snapshot = parseOkendoMetafieldSnapshot(raw);
  if (!snapshot) return null as ExtractedProductReviewSummary | null;

  const baseSummary = buildOkendoReviewSummaryFromSnapshot(snapshot);
  if (snapshot.reviewCount <= 0) return baseSummary || null;

  const limit = Math.min(Math.max(snapshot.reviewCount, 1), 6);
  const endpoint = buildOkendoReviewsEndpoint(snapshot, limit);

  try {
    const response = await withTimeout(fetch(endpoint), 10000, "okendo_reviews_fetch");
    if (!response.ok) return baseSummary || null;
    const payload = (await response.json()) as OkendoReviewsResponse;
    const previewItems: NonNullable<ExtractedProductReviewSummary["preview_items"]> = [];

    for (const reviewRow of Array.isArray(payload.reviews) ? payload.reviews : []) {
      const reviewId = cleanText(reviewRow?.reviewId);
      const status = cleanText(reviewRow?.status).toLowerCase();
      const rawBody = typeof reviewRow?.body === "string" ? reviewRow.body : "";
      const rawTitle = typeof reviewRow?.title === "string" ? reviewRow.title : "";
      const rawReviewerLabel = typeof reviewRow?.reviewer?.displayName === "string" ? reviewRow.reviewer.displayName : "";
      const textSnippet = cleanText(decodeHtmlAttributeEntities(rawBody));
      const title = cleanText(decodeHtmlAttributeEntities(rawTitle));
      const authorLabel = cleanText(decodeHtmlAttributeEntities(rawReviewerLabel));
      if (!reviewId || (!textSnippet && !title)) continue;
      if (status && status !== "approved") continue;
      previewItems.push({
        review_id: reviewId,
        ...(Number.isFinite(Number(reviewRow?.rating)) ? { rating: Number(reviewRow!.rating) } : {}),
        ...(authorLabel ? { author_label: authorLabel } : {}),
        ...(title ? { title } : {}),
        ...(textSnippet ? { text_snippet: textSnippet } : {}),
        ...(resolveOkendoReviewMediaItems(reviewRow?.media).length > 0
          ? { media: resolveOkendoReviewMediaItems(reviewRow?.media) }
          : {}),
        source: "merchant_public",
        source_kind: "okendo_reviews_api",
        source_scope: "merchant_public",
        content_review_state: "approved",
        public_visible: true,
        ...(reviewRow?.reviewer?.isVerified === true ? { verified_buyer: true } : {}),
      });
    }

    return mergeOkendoReviewSummary(
      baseSummary,
      previewItems.length > 0
        ? {
            ...(payload.areReviewsGrouped === true
              ? { aggregation_scope: "group", product_line_review_count: snapshot.reviewCount }
              : payload.areReviewsGrouped === false
                ? { aggregation_scope: "product", exact_item_review_count: snapshot.reviewCount }
                : {}),
            preview_items: previewItems,
          }
        : undefined,
    ) || null;
  } catch {
    return baseSummary || null;
  }
}

async function enrichExtractedFaqItemsWithOkendoQuestions(extracted: ScrapedPageSignals, sourceUrl: string) {
  const okendoFaqItems = await fetchOkendoFaqItemsFromMetafieldJson(extracted.okendoMetafieldJson, sourceUrl);
  if (okendoFaqItems.length === 0) return extracted;
  return {
    ...extracted,
    faqItems: dedupeFaqItems([...(extracted.faqItems || []), ...okendoFaqItems]),
  };
}

function uniqueFieldSources(values: Array<string | undefined | null>) {
  return dedupeStringList(values.map((value) => cleanText(typeof value === "string" ? value : undefined)));
}

function firstMatchingSectionBody(
  sections: ExtractedProductDetailSection[],
  patterns: RegExp[],
): ExtractedProductDetailSection | null {
  for (const section of Array.isArray(sections) ? sections : []) {
    const heading = cleanText(section?.heading);
    if (!heading) continue;
    if (patterns.some((pattern) => pattern.test(heading))) return section;
  }
  return null;
}

function extractLabeledSectionText(text: string | undefined, labels: string[]) {
  const normalized = cleanText(text);
  if (!normalized) return "";
  const escapedLabels = labels.map((label) => label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
  if (!escapedLabels) return "";
  const match = normalized.match(new RegExp(`(?:${escapedLabels})\\s*:\\s*([\\s\\S]+)$`, "i"));
  return cleanText(match?.[1]);
}

export function extractDelimitedLabeledSectionText(
  text: string | undefined,
  labels: string[],
  stopLabels: string[] = [],
) {
  const normalized = cleanText(text);
  if (!normalized) return "";
  const escapedLabels = labels.map((label) => label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
  if (!escapedLabels) return "";
  const escapedStopLabels = stopLabels.map((label) => label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
  const pattern = escapedStopLabels
    ? new RegExp(
        `(?:^|\\n)\\s*(?:${escapedLabels})(?:\\s*:\\s*|\\s*\\n+)([\\s\\S]+?)(?=(?:(?:\\n+|\\s+(?=[A-Z]))\\s*(?:${escapedStopLabels})(?:\\s*:\\s*|\\s*\\n+)|$))`,
        "i",
      )
    : new RegExp(`(?:^|\\n)\\s*(?:${escapedLabels})(?:\\s*:\\s*|\\s*\\n+)([\\s\\S]+)$`, "i");
  const match = normalized.match(pattern);
  return cleanText(match?.[1]);
}

const SHOPIFY_PDP_TEXT_STOP_LABELS = [
  "What It Is",
  "Product Benefits",
  "Benefits",
  "How it works",
  "Skin Type",
  "Key Ingredients",
  "Ingredient Highlights",
  "Highlighted Ingredients",
  "How to Use",
  "How to use",
  "How To Use",
  "How To Apply",
  "Directions",
  "Usage",
  "Suggested Usage",
  "Application",
  "Application Tips",
  "Ingredients and Safety",
  "Ingredients",
  "Ingredient List",
  "Full Ingredients",
  "Full Ingredient List",
  "Active Ingredients",
  "Active Ingredient",
  "Inactive Ingredients",
  "FAQ",
  "Frequently Asked Questions",
];

function stopLabelsExcluding(excluded: string[]) {
  const excludedSet = new Set(excluded.map((value) => value.toLowerCase()));
  return SHOPIFY_PDP_TEXT_STOP_LABELS.filter((label) => !excludedSet.has(label.toLowerCase()));
}

function firstDelimitedSection(text: string | undefined, labels: string[], stopLabels?: string[]) {
  return extractDelimitedLabeledSectionText(text, labels, stopLabels || stopLabelsExcluding(labels));
}

function looksLikeFaqQuestionText(value: string | undefined) {
  const normalized = cleanText(value)
    .replace(/^(?:q(?:uestion)?\s*[:/-]\s*)/i, "")
    .trim();
  if (!normalized) return false;
  return /[?？]$/.test(normalized) || /^(?:can|is|are|do|does|did|will|would|should|could|where|when|why|how|what|who|which)\b/i.test(normalized);
}

export function extractInlineFaqItemsFromHtml(
  html: string | undefined,
  options?: {
    sourceKind?: string;
    sourceUrl?: string;
    sourceTitle?: string;
  },
) {
  if (!html || !html.trim()) return [] as ExtractedProductFaqItem[];

  const withLineBreaks = html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p\s*>/gi, "\n")
    .replace(/<\/div\s*>/gi, "\n")
    .replace(/<\/li\s*>/gi, "\n")
    .replace(/<\s*h[1-6][^>]*>/gi, "\n")
    .replace(/<\/\s*h[1-6]\s*>/gi, "\n")
    .replace(/<\s*b[^>]*>/gi, "\n")
    .replace(/<\/\s*b\s*>/gi, "\n");
  const normalized = cleanText(withLineBreaks);
  if (!normalized) return [] as ExtractedProductFaqItem[];

  const lines = normalized
    .split(/\n+/)
    .map((line) => cleanText(line))
    .filter(Boolean);
  const faqStart = lines.findIndex((line) => /\b(?:faqs?|frequently asked questions?|q\s*&\s*a)\b/i.test(line));
  if (faqStart < 0) return [] as ExtractedProductFaqItem[];

  const items: ExtractedProductFaqItem[] = [];
  let currentQuestion = "";
  let currentAnswer: string[] = [];
  const sourceKind = cleanText(options?.sourceKind) || "inline_html_faq";
  const sourceUrl = cleanText(options?.sourceUrl);
  const sourceTitle = cleanText(options?.sourceTitle) || "FAQ";

  const pushCurrent = () => {
    const question = cleanText(currentQuestion);
    const answer = cleanText(currentAnswer.join(" "));
    if (!looksLikeFaqQuestionText(question) || !answer) return;
    items.push({
      question,
      answer,
      source_kind: sourceKind,
      ...(sourceUrl ? { source_url: sourceUrl } : {}),
      ...(sourceTitle ? { source_title: sourceTitle } : {}),
    });
  };

  for (const line of lines.slice(faqStart + 1)) {
    if (looksLikeFaqQuestionText(line)) {
      pushCurrent();
      currentQuestion = line;
      currentAnswer = [];
      continue;
    }
    if (!currentQuestion) continue;
    currentAnswer.push(line);
  }
  pushCurrent();

  return dedupeFaqItems(items);
}

function stripInlineFaqText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return "";
  return cleanText(normalized.replace(/\n*\b(?:faqs?|frequently asked questions?)\b[\s\S]*$/i, ""));
}

export function extractShopifyBodyHtmlPdpFields(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) {
    return {
      detailsSections: [] as ExtractedProductDetailSection[],
      ingredientsRaw: undefined,
      activeIngredientsRaw: undefined,
      howToUseRaw: undefined,
    };
  }

  const detailsSections: ExtractedProductDetailSection[] = [];
  const pushDetailSection = (heading: string, labels: string[], sourceKind: string) => {
    const body = firstDelimitedSection(normalized, labels);
    if (!body) return;
    detailsSections.push({
      heading,
      body,
      source_kind: sourceKind,
    });
  };

  pushDetailSection("Benefits", ["Product Benefits", "Benefits", "How it works"], "shopify_body_html_labeled_section");
  pushDetailSection("Key Ingredients", ["Key Ingredients", "Ingredient Highlights", "Highlighted Ingredients"], "shopify_body_html_labeled_section");
  pushDetailSection("Skin Type", ["Skin Type"], "shopify_body_html_labeled_section");

  const howToUseRaw =
    firstDelimitedSection(
      normalized,
      ["How to Use", "How to use", "How To Use", "How To Apply", "Directions", "Usage", "Suggested Usage", "Application"],
    ) || undefined;
  const ingredientsRaw =
    firstDelimitedSection(normalized, ["Ingredients and Safety", "Full Ingredients", "Full Ingredient List"]) ||
    extractDelimitedLabeledSectionText(
      normalized,
      ["Ingredients", "Ingredient List"],
      [
        "How to Use",
        "How to use",
        "How To Use",
        "How To Apply",
        "Directions",
        "Usage",
        "Suggested Usage",
        "Application",
        "FAQ",
        "Frequently Asked Questions",
      ],
    ) ||
    undefined;
  const activeIngredientsRaw =
    firstDelimitedSection(
      normalized,
      ["Active Ingredients", "Active Ingredient"],
      [
        "Inactive Ingredients",
        "Full Ingredients",
        "Full Ingredient List",
        "How to Use",
        "How to use",
        "How To Use",
        "How To Apply",
        "Directions",
        "Usage",
        "Suggested Usage",
        "Application",
      ],
    ) ||
    (() => {
      const keyIngredients = firstDelimitedSection(
        normalized,
        ["Key Ingredients", "Ingredient Highlights", "Highlighted Ingredients"],
      );
      const cleaned = cleanText(keyIngredients);
      const commaCount = (cleaned.match(/,/g) || []).length;
      if (!cleaned || cleaned.length > 700 || (looksLikeFullIngredientListText(cleaned) && (commaCount >= 8 || cleaned.length > 350))) {
        return "";
      }
      return cleaned;
    })() ||
    undefined;

  return {
    detailsSections: dedupeDetailSections(detailsSections),
    ingredientsRaw: cleanText(ingredientsRaw) || undefined,
    activeIngredientsRaw: cleanText(activeIngredientsRaw) || undefined,
    howToUseRaw: stripInlineFaqText(howToUseRaw) || undefined,
  };
}

function extractFirstParagraphAfterMarker(html: string | undefined, marker: RegExp) {
  const source = typeof html === "string" ? html : "";
  const match = marker.exec(source);
  if (!match || typeof match.index !== "number") return "";
  const slice = source.slice(match.index, match.index + 6000);
  const paragraph = slice.match(/<p[^>]*>([\s\S]*?)<\/p>/i);
  return paragraph ? cleanText(paragraph[1]) : "";
}

function extractInlineHtmlTextAfterMarker(
  html: string | undefined,
  marker: RegExp,
  stopRe = /<section\b|<script\b|document\.addEventListener|<\/section>|<\/body>/i,
) {
  const source = typeof html === "string" ? html : "";
  const match = marker.exec(source);
  if (!match || typeof match.index !== "number") return "";
  const slice = source.slice(match.index + match[0].length, match.index + match[0].length + 3500);
  const stop = slice.search(stopRe);
  const raw = stop >= 0 ? slice.slice(0, stop) : slice;
  return cleanText(raw);
}

export function extractShopifyDirectPdpHtmlPdpFields(html: string | undefined) {
  const source = typeof html === "string" ? html : "";
  if (!source.trim()) {
    return {
      detailsSections: [] as ExtractedProductDetailSection[],
      ingredientsRaw: undefined,
      activeIngredientsRaw: undefined,
      howToUseRaw: undefined,
    };
  }

  const normalized = cleanText(source);
  const genericFields = extractShopifyBodyHtmlPdpFields(normalized);
  const fullIngredientsCandidate =
    extractFirstParagraphAfterMarker(source, /\bFULL INGREDIENTS\b/i) ||
    extractInlineHtmlTextAfterMarker(
      source,
      /\bFULL INGREDIENTS\b/i,
      /\b(?:HOW TO USE|KEY INGREDIENTS|PRODUCT BENEFITS|REVIEWS?|CUSTOMER REVIEWS?|FAQ)\b|<script\b|<\/section>|<\/body>/i,
    ) ||
    genericFields.ingredientsRaw ||
    "";
  const ingredientsRaw = stripIngredientPackageDisclaimer(extractLikelyFullIngredientListText(fullIngredientsCandidate));

  const howToHtmlMatch = source.match(
    /\bHOW TO USE\b[\s\S]{0,5000}?<div[^>]*class=["'][^"']*\bprhow-txt\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i,
  );
  const howToCandidate =
    (howToHtmlMatch ? cleanText(howToHtmlMatch[1]) : "") ||
    genericFields.howToUseRaw ||
    extractInlineHtmlTextAfterMarker(
      source,
      /\bHOW TO USE\b/i,
      /\b(?:FULL INGREDIENTS|KEY INGREDIENTS|PRODUCT BENEFITS|REVIEWS?|CUSTOMER REVIEWS?|FAQ)\b|<script\b|<\/section>|<\/body>/i,
    );
  const howToUseRaw = looksLikeHowToUseInstructionText(howToCandidate)
    ? stripProductRegulatoryTail(howToCandidate)
    : "";

  return {
    detailsSections: genericFields.detailsSections,
    ingredientsRaw: ingredientsRaw || undefined,
    activeIngredientsRaw: genericFields.activeIngredientsRaw,
    howToUseRaw: howToUseRaw || undefined,
  };
}

type ShopifyEmbeddedProductPayload = {
  title?: string;
  handle?: string;
  description?: string;
  content?: string;
  body_html?: string;
  shortDescription?: string;
  collectionShortDescription?: string;
  tags?: unknown;
  categories?: unknown;
  featured_image?: unknown;
  image?: unknown;
  images?: unknown;
  media?: unknown;
  customMetafields?: Record<string, unknown>;
  product?: unknown;
};

function extractEmbeddedJsonObject(scriptText: string, anchorPattern: RegExp) {
  const anchor = scriptText.match(anchorPattern);
  if (!anchor || typeof anchor.index !== "number") return null;
  const start = scriptText.indexOf("{", anchor.index);
  if (start < 0) return null;

  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let idx = start; idx < scriptText.length; idx += 1) {
    const char = scriptText[idx];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === "\"") {
        inString = false;
      }
      continue;
    }

    if (char === "\"") {
      inString = true;
      continue;
    }
    if (char === "{") {
      depth += 1;
      continue;
    }
    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return scriptText.slice(start, idx + 1);
      }
    }
  }

  return null;
}

function decodeHtmlAttributeEntities(value: string): string {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_match, hex) => {
      const codePoint = Number.parseInt(hex, 16);
      return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : _match;
    })
    .replace(/&#([0-9]+);/g, (_match, decimal) => {
      const codePoint = Number.parseInt(decimal, 10);
      return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : _match;
    })
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&plus;/gi, "+")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function parseJsonObjectText(value: string): Record<string, unknown> | null {
  const raw = value.trim();
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function decodeProductJsonAttributeValue(raw: string): Record<string, unknown> | null {
  const htmlDecoded = decodeHtmlAttributeEntities(raw);
  const formEncoded = raw.replace(/\+/g, " ");
  const htmlDecodedFormEncoded = htmlDecoded.replace(/\+/g, " ");
  const candidates = [
    raw,
    htmlDecoded,
    safeDecodeURIComponent(raw),
    safeDecodeURIComponent(htmlDecoded),
    safeDecodeURIComponent(formEncoded),
    safeDecodeURIComponent(htmlDecodedFormEncoded),
  ]
    .map((value) => value.trim())
    .filter(Boolean);

  for (const candidate of candidates) {
    const parsed = parseJsonObjectText(candidate);
    if (parsed) return parsed;
  }
  return null;
}

function readHtmlAttribute(tag: string, attrName: string): string {
  const escapedAttr = attrName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const doubleMatch = tag.match(new RegExp(`\\b${escapedAttr}\\s*=\\s*"([\\s\\S]*?)"`, "i"));
  if (doubleMatch?.[1]) return doubleMatch[1];
  const singleMatch = tag.match(new RegExp(`\\b${escapedAttr}\\s*=\\s*'([\\s\\S]*?)'`, "i"));
  return singleMatch?.[1] || "";
}

export function extractShopifyProductJsonAttributeScriptsFromHtml(html: string | undefined): string[] {
  if (!html || !html.trim()) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  const productHeroTagRe =
    /<section\b(?=[^>]*\bis\s*=\s*(?:"product-hero"|'product-hero'))[^>]*\bproduct-json\s*=\s*(?:"[\s\S]*?"|'[\s\S]*?')[^>]*>/gi;
  const tags = html.match(productHeroTagRe) || [];

  for (const tag of tags.slice(0, 4)) {
    const rawAttr = readHtmlAttribute(tag, "product-json");
    const parsed = decodeProductJsonAttributeValue(rawAttr);
    if (!parsed) continue;
    const key = JSON.stringify([
      typeof parsed.handle === "string" ? parsed.handle : "",
      typeof parsed.title === "string" ? parsed.title : "",
      typeof parsed.id === "string" || typeof parsed.id === "number" ? parsed.id : "",
    ]);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(`window.__PIVOTA_PRODUCT_JSON__ = ${JSON.stringify(parsed)};`);
  }

  return out;
}

function extractShopifyProductVolumeTextFromHtml(html: string | undefined) {
  const source = typeof html === "string" ? html : "";
  if (!source.trim()) return "";
  const match =
    source.match(/<[^>]+class=["'][^"']*\bproduct__volume\b[^"']*["'][^>]*>([\s\S]*?)<\/[^>]+>/i) ||
    source.match(/<[^>]+data-product-volume(?:=["'][^"']*["'])?[^>]*>([\s\S]*?)<\/[^>]+>/i) ||
    source.match(/<[^>]+class=["'][^"']*\bproduct-volume\b[^"']*["'][^>]*>([\s\S]*?)<\/[^>]+>/i) ||
    source.match(/\bNet\s*(?:Wt|Weight)\.?\s*[:\-]?\s*([^<\n]{0,120})/i);
  if (!match?.[1]) return "";
  return cleanText(decodeHtmlAttributeEntities(match[1].replace(/<[^>]+>/g, " ")));
}

function extractRichTextJsonToText(value: unknown): string {
  const walk = (node: unknown): string[] => {
    if (!node || typeof node !== "object") return [];
    const record = node as Record<string, unknown>;
    const nodeType = typeof record.type === "string" ? record.type : "";
    const textValue = typeof record.value === "string" ? record.value : "";
    const children = Array.isArray(record.children) ? record.children : [];
    const childText = children.flatMap((child) => walk(child)).filter(Boolean);

    if (nodeType === "text") {
      const text = cleanText(textValue);
      return text ? [text] : [];
    }
    if (nodeType === "list-item") {
      const text = cleanText(childText.join(" ").trim());
      return text ? [`- ${text}`] : [];
    }
    if (nodeType === "paragraph" || nodeType === "heading" || nodeType === "root" || nodeType === "list") {
      const text = cleanText(childText.join(nodeType === "list" ? "\n" : " ").trim());
      return text ? [text] : [];
    }

    const text = cleanText([textValue, ...childText].join(" ").trim());
    return text ? [text] : [];
  };

  return cleanText(walk(value).join("\n\n"));
}

function extractShopifyEmbeddedProductPayloadsFromScripts(scriptTexts: string[]): ShopifyEmbeddedProductPayload[] {
  if (!Array.isArray(scriptTexts) || scriptTexts.length === 0) return [];
  const payloads: ShopifyEmbeddedProductPayload[] = [];
  const seen = new Set<string>();
  const patterns = [
    /window\.reelUp_productJSON\s*=/i,
    /_RSConfig\.product\s*=/i,
    /window\.corner\.sessionData\.product\s*=/i,
    /corner\.sessionData\.product\s*=/i,
    /window\.DCART\s*=/i,
    /sgGlobalVars\.currentProduct\s*=/i,
    /window\.theme\.product\s*=/i,
    /theme\.product\s*=/i,
    /window\.__PIVOTA_PRODUCT_JSON__\s*=/i,
  ];

  const unwrapPayload = (value: unknown): ShopifyEmbeddedProductPayload | null => {
    if (!value || typeof value !== "object") return null;

    const looksLikePayload = (candidate: Record<string, unknown>) =>
      typeof candidate.description === "string" ||
      typeof candidate.content === "string" ||
      typeof candidate.body_html === "string" ||
      Boolean(candidate.customMetafields && typeof candidate.customMetafields === "object") ||
      Boolean(candidate.featured_image) ||
      Boolean(candidate.image) ||
      Boolean(candidate.images) ||
      Boolean(candidate.media);

    const record = value as Record<string, unknown>;
    if (looksLikePayload(record)) return record as ShopifyEmbeddedProductPayload;

    const nestedProduct =
      record.product && typeof record.product === "object" ? (record.product as Record<string, unknown>) : null;
    if (nestedProduct && looksLikePayload(nestedProduct)) {
      return nestedProduct as ShopifyEmbeddedProductPayload;
    }

    return null;
  };

  for (const scriptText of scriptTexts) {
    if (!scriptText) continue;
    for (const pattern of patterns) {
      const raw = extractEmbeddedJsonObject(scriptText, pattern);
      if (!raw) continue;
      try {
        const parsed = unwrapPayload(JSON.parse(raw));
        if (!parsed) continue;
        const key = JSON.stringify([
          typeof parsed.description === "string" ? parsed.description.slice(0, 200) : "",
          typeof parsed.content === "string" ? parsed.content.slice(0, 200) : "",
          typeof parsed.body_html === "string" ? parsed.body_html.slice(0, 200) : "",
          parsed.customMetafields && typeof parsed.customMetafields === "object"
            ? JSON.stringify(parsed.customMetafields).slice(0, 200)
            : "",
          typeof parsed.featured_image === "string" ? parsed.featured_image : "",
        ]);
        if (seen.has(key)) continue;
        seen.add(key);
        payloads.push(parsed);
      } catch {
        // ignore invalid embedded JSON
      }
    }
  }

  return payloads;
}

function extractEmbeddedProductPayloadImageUrls(payloads: ShopifyEmbeddedProductPayload[]): string[] {
  const rawUrls: string[] = [];

  const visit = (value: unknown) => {
    if (!value) return;
    if (typeof value === "string") {
      const cleaned = value.trim();
      if (cleaned) rawUrls.push(cleaned);
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    if (typeof value === "object") {
      const record = value as Record<string, unknown>;
      visit(record.src);
      visit(record.url);
      visit(record.image);
      visit(record.featured_image);
      visit(record.preview_image);
    }
  };

  for (const payload of payloads) {
    visit(payload.featured_image);
    visit(payload.image);
    visit(payload.images);
    visit(payload.media);
  }

  return dedupeStringList(rawUrls);
}

function cleanMerchantPayloadString(value: unknown): string {
  if (typeof value !== "string") return "";
  const normalized = cleanText(decodeHtmlAttributeEntities(value));
  if (!normalized || /^(?:false|null|undefined)$/i.test(normalized)) return "";
  return normalized;
}

function collectPayloadStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => cleanMerchantPayloadString(item)).filter(Boolean);
}

function extractPayloadTagDetailSections(payloads: ShopifyEmbeddedProductPayload[]): ExtractedProductDetailSection[] {
  const grouped = new Map<string, string[]>();
  const push = (heading: string, value: string) => {
    const normalized = cleanMerchantPayloadString(value);
    if (!normalized) return;
    if (
      /^(?:badge|catalog-exclude|cdp|ycrf_|ygroup_|bb::|is batch controlled|ordergroove|tax class id|color id|size id|show-size-selector|shipping-restricted|full-size|mini|status:hidden)$/i.test(
        normalized,
      )
    ) {
      return;
    }
    const items = grouped.get(heading) || [];
    if (!items.some((item) => item.toLowerCase() === normalized.toLowerCase())) {
      items.push(normalized);
      grouped.set(heading, items);
    }
  };

  for (const payload of payloads) {
    for (const rawTag of [...collectPayloadStringArray(payload.tags), ...collectPayloadStringArray(payload.categories)]) {
      const match = rawTag.match(/^([^:|]{2,40})[:|]\s*(.+)$/);
      if (!match) continue;
      const label = cleanMerchantPayloadString(match[1]);
      const value = cleanMerchantPayloadString(match[2]);
      if (!label || !value) continue;

      if (/^benefits?$/i.test(label)) push("Benefits", value);
      else if (/^concerns?$/i.test(label)) push("Concerns", value);
      else if (/^skin type$/i.test(label)) push("Skin Type", value);
      else if (/^finish$/i.test(label)) push("Finish", value);
      else if (/^coverage$/i.test(label)) push("Coverage", value);
      else if (/^formulation$/i.test(label)) push("Format", value);
      else if (/^sun protection$/i.test(label)) push("Sun Protection", value);
      else if (/^product type$/i.test(label)) push("Product Type", value);
      else if (/^ingredient preferences?$/i.test(label)) push("Ingredient Preferences", value);
    }
  }

  return Array.from(grouped.entries())
    .filter(([, values]) => values.length > 0)
    .map(([heading, values]) => ({
      heading,
      body: values.slice(0, 8).join(", "),
      source_kind: "embedded_product_json_tags",
    }));
}

function collectPayloadMediaAltTexts(payloads: ShopifyEmbeddedProductPayload[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const visit = (value: unknown) => {
    if (!value) return;
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    if (typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    const alt = cleanMerchantPayloadString(record.alt);
    if (alt && !seen.has(alt.toLowerCase())) {
      seen.add(alt.toLowerCase());
      out.push(alt);
    }
    visit(record.preview_image);
  };

  for (const payload of payloads) {
    visit(payload.media);
    visit(payload.images);
  }
  return out;
}

function extractPayloadMediaAltDetailSections(payloads: ShopifyEmbeddedProductPayload[]): ExtractedProductDetailSection[] {
  const byHeading = new Map<string, string[]>();
  const payloadTitles = new Set(
    payloads
      .map((payload) => cleanMerchantPayloadString(payload.title))
      .filter(Boolean)
      .map((title) => title.toLowerCase()),
  );

  const push = (heading: string, body: string) => {
    const normalized = cleanMerchantPayloadString(body);
    if (!normalized || normalized.length < 24) return;
    if (payloadTitles.has(normalized.toLowerCase())) return;
    if (/\b(?:product image|editorial image|on a .*background|on a .*backdrop|smear of|person applying|model)\b/i.test(normalized)) {
      return;
    }
    const items = byHeading.get(heading) || [];
    if (!items.some((item) => item.toLowerCase() === normalized.toLowerCase())) {
      items.push(normalized);
      byHeading.set(heading, items);
    }
  };

  for (const alt of collectPayloadMediaAltTexts(payloads)) {
    if (/\b(?:clinical study|before and after|agree\b|agreed\b|\d+%|after \d+ weeks?)\b/i.test(alt)) {
      push("Clinical Results", alt);
    } else if (
      /\b(?:made with|key ingredients?|ingredients?:|niacinamide|hyaluronic|salicylic|zinc pca|aloe|cherry|willow bark|kalahari melon|barbados cherry)\b/i.test(
        alt,
      )
    ) {
      push("Key Ingredients", alt);
    } else if (
      /\b(?:controls?|brightens?|unclogs?|hydrates?|moisturi[sz]es?|pores?|shine|texture|spf|sunscreen|smooths?|plumps?)\b/i.test(
        alt,
      )
    ) {
      push("Benefits", alt);
    }
  }

  return Array.from(byHeading.entries()).map(([heading, values]) => ({
    heading,
    body: values.slice(0, 4).join("\n"),
    source_kind: "embedded_product_json_media_alt",
  }));
}

export function extractShopifyEmbeddedProductPayloadPdpFields(scriptTexts: string[]) {
  const payloads = extractShopifyEmbeddedProductPayloadsFromScripts(scriptTexts);
  const mergedHtml = cleanText(
    payloads
      .flatMap((payload) => [
        payload.content,
        payload.description,
        payload.body_html,
        payload.shortDescription,
        payload.collectionShortDescription,
      ])
      .filter((value): value is string => Boolean(value && value.trim()))
      .map((value) => cleanMerchantPayloadString(value))
      .filter(Boolean)
      .join("\n\n"),
  );
  const bodyHtmlFields = mergedHtml
    ? extractShopifyBodyHtmlPdpFields(mergedHtml)
    : {
        detailsSections: [] as ExtractedProductDetailSection[],
        ingredientsRaw: undefined,
        activeIngredientsRaw: undefined,
        howToUseRaw: undefined,
      };

  const customMetafieldDetails: ExtractedProductDetailSection[] = [];
  const customMetafieldIngredients: string[] = [];
  const customMetafieldHowTo: string[] = [];

  const pushCustomSection = (heading: string, body: string | undefined, sourceKind: string) => {
    const cleaned = cleanText(body);
    if (!cleaned) return;
    customMetafieldDetails.push({
      heading,
      body: cleaned,
      source_kind: sourceKind,
    });
  };

  for (const payload of payloads) {
    const customMetafields =
      payload.customMetafields && typeof payload.customMetafields === "object" ? payload.customMetafields : null;
    if (!customMetafields) continue;

    const howToUseText = extractRichTextJsonToText(customMetafields.how_to_use_1_);
    const tab1Text = extractRichTextJsonToText(customMetafields.product_info_tab_1_body);
    const tab2Text = extractRichTextJsonToText(customMetafields.product_info_tab_2_body);
    const fullIngredientsText = extractRichTextJsonToText(customMetafields.product_info_tab_3_full_ingredients);
    const keyIngredientsText = extractRichTextJsonToText(customMetafields.product_info_tab_3_key_ingredients);

    pushCustomSection("Benefits", tab1Text, "embedded_custom_metafield_tab_1");
    pushCustomSection("Details", tab2Text, "embedded_custom_metafield_tab_2");
    pushCustomSection("Key Ingredients", keyIngredientsText, "embedded_custom_metafield_key_ingredients");
    pushCustomSection("Ingredients", fullIngredientsText, "embedded_custom_metafield_full_ingredients");
    pushCustomSection("How to Use", howToUseText, "embedded_custom_metafield_how_to_use");

    if (fullIngredientsText) customMetafieldIngredients.push(fullIngredientsText);
    if (howToUseText) customMetafieldHowTo.push(howToUseText);
  }

  return {
    descriptionRaw: mergedHtml || undefined,
    detailsSections: dedupeDetailSections([
      ...customMetafieldDetails,
      ...bodyHtmlFields.detailsSections,
      ...extractPayloadTagDetailSections(payloads),
      ...extractPayloadMediaAltDetailSections(payloads),
    ]),
    ingredientsRaw: cleanText(bodyHtmlFields.ingredientsRaw || customMetafieldIngredients[0]) || undefined,
    activeIngredientsRaw: cleanText(bodyHtmlFields.activeIngredientsRaw) || undefined,
    howToUseRaw: cleanText(bodyHtmlFields.howToUseRaw || customMetafieldHowTo[0]) || undefined,
    imageUrls: extractEmbeddedProductPayloadImageUrls(payloads),
  };
}

export function looksLikeFullIngredientListText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return false;
  const commaCount = (normalized.match(/,/g) || []).length;
  const proseSignal =
    /\b(?:nourishes?|provides?|helps?|boosts?|refines?|hydrates?|absorbs?|soothes?|calms?|brightens?|moisturiz(?:es?|ing)|balances?)\b/i.test(
      normalized,
    );
  return (
    /\b(active ingredients?|inactive ingredients?|full ingredients?|ingredient list|inci)\s*:/i.test(normalized) ||
    /\bwater\/aqua\b/i.test(normalized) ||
    /\bci\s*\d{5}\b/i.test(normalized) ||
    (commaCount >= 6 && !proseSignal) ||
    (commaCount >= 2 && !proseSignal && normalized.length < 500)
  );
}

export function extractLikelyFullIngredientListText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return undefined;
  if (looksLikeFullIngredientListText(normalized)) return normalized;

  const blocks = normalized
    .split(/\n{2,}/)
    .map((block) => cleanText(block))
    .filter(Boolean);
  const exactMatch = blocks
    .filter((block) => looksLikeFullIngredientListText(block))
    .sort((left, right) => right.length - left.length)[0];
  if (exactMatch) return exactMatch;

  const commaDenseFallback = blocks
    .filter((block) => {
      const commaCount = (block.match(/,/g) || []).length;
      return commaCount >= 6 && /\b(?:water\/aqua|aqua\b|ci\s*\d{5}|phenoxyethanol|butylene glycol)\b/i.test(block);
    })
    .sort((left, right) => right.length - left.length)[0];
  return commaDenseFallback || undefined;
}

function normalizeVariantIngredientLabel(value: string | undefined) {
  return cleanText(decodeHtmlAttributeEntities(value || ""))
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’`]/g, "'")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/gi, "")
    .toLowerCase();
}

export function extractVariantScopedIngredientListText(text: string | undefined, labels: string[]) {
  const normalized = cleanText(text);
  const labelKeys = dedupeStringList(labels.map(normalizeVariantIngredientLabel).filter(Boolean));
  if (!normalized || labelKeys.length === 0) return undefined;

  const matches = Array.from(
    normalized.matchAll(/(?:^|\n|\. )([A-Z0-9][A-Z0-9 $&'’+./-]{1,80})\s*:\s*/g),
  ).filter((match) => {
    const label = cleanText(match[1]);
    if (!label) return false;
    if (/^(?:full ingredients?|ingredients?|active ingredients?|inactive ingredients?)$/i.test(label)) return false;
    return normalizeVariantIngredientLabel(label).length >= 3;
  });
  if (matches.length < 2) return undefined;

  for (let idx = 0; idx < matches.length; idx += 1) {
    const match = matches[idx]!;
    const label = cleanText(match[1]);
    const labelKey = normalizeVariantIngredientLabel(label);
    if (!labelKeys.some((candidate) => candidate === labelKey || candidate.endsWith(labelKey))) continue;

    const bodyStart = (match.index || 0) + match[0].length;
    const bodyEnd = matches[idx + 1]?.index ?? normalized.length;
    const body = cleanText(normalized.slice(bodyStart, bodyEnd).replace(/^\.\s*/, ""));
    if (!looksLikeFullIngredientListText(body)) continue;
    return `${label}: ${body}`;
  }

  return undefined;
}

function looksLikeIngredientSummaryText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return false;
  if (looksLikeFullIngredientListText(normalized)) return false;
  if (/\bcarrier\b|\bhumectant\b|\bsolvent\b|\bemollient\b|\bfilm former\b|\bchelating agent\b|\bviscosity controller\b/i.test(normalized)) {
    return false;
  }
  return /\b(?:nourishes?|provides?|helps?|boosts?|refines?|hydrates?|absorbs?|soothes?|calms?|brightens?|balances?|protects?)\b/i.test(
    normalized,
  );
}

function looksLikeActiveIngredientSummaryText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!looksLikeIngredientSummaryText(normalized)) return false;
  if (normalized.length > 450) return false;
  if (/\bnote:\b/i.test(normalized)) return false;
  const namedIngredientActionMatches =
    normalized.match(
      /(?:^|[\n.])\s*[A-Z][A-Za-z0-9 ()/&'-]{2,40}\s+(?:nourishes?|provides?|helps?|boosts?|refines?|hydrates?|absorbs?|soothes?|calms?|brightens?|balances?|protects?)\b/g,
    ) || [];
  return (
    /\b(?:key ingredients?|highlighted ingredients?|contains|includes|powered by|formulated with|star ingredients?)\b/i.test(
      normalized,
    ) ||
    /(?:^|\n)\s*[A-Z][A-Za-z0-9 ()/&'-]{2,80}:\s*/.test(normalized) ||
    namedIngredientActionMatches.length >= 2
  );
}

function looksLikeHowToUseInstructionText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return false;
  if (/\bloading questions\b/i.test(normalized)) return false;
  if (/^\s*(?:step\s*\d+[:.-]?\s*)?(?:shake|spritz|apply|smooth|massage|dispense|cleanse|rinse|pat|layer|reapply|leave|wear|mix)\b/i.test(normalized)) {
    return true;
  }
  if (/\b(?:with eyes closed|at least \d+ inches away|after cleansing|before sun exposure|apply generously|apply (?:your|to|onto|with|a|an|the)\b|shake well|reapply every)\b/i.test(normalized)) {
    return true;
  }
  if (/^\s*(?:\d+\.|- )/m.test(normalized)) return true;
  return false;
}

function stripProductRegulatoryTail(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return "";
  return cleanText(
    normalized
      .replace(/\bClose\s+LONGWEAR EYESHADOW STICK\b[\s\S]*$/i, "")
      .replace(/\bRP:\s+(?:CLEAR|KENDO HOLDINGS)\b[\s\S]*$/i, ""),
  );
}

function stripIngredientPackageDisclaimer(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return "";
  return cleanText(
    normalized.replace(
      /\bplease refer to the ingredient list on the product package you receive for the most up-to-date information\.?$/i,
      "",
    ),
  );
}

export function deriveProductPdpModuleBodies(params: {
  ingredientsMarkdownText?: string;
  activeIngredientsText?: string;
  howToUseText?: string;
  detailsSections?: ExtractedProductDetailSection[];
}) {
  const detailsSections = dedupeDetailSections(params.detailsSections || []);
  const ingredientSections = detailsSections.filter((section) =>
    /\b(ingredients?|ingredient list|inci|what(?:'|’)s in it\??)\b/i.test(section.heading),
  );
  const fullIngredientSection = pickBestDetailSection(
    ingredientSections,
    (section) => !!extractLikelyFullIngredientListText(section.body),
  );
  const ingredientSummarySection = pickBestDetailSection(
    ingredientSections,
    (section) =>
      normalizeDetailSectionHeading(section.heading) !== "Key Ingredients" && looksLikeIngredientSummaryText(section.body),
  );
  const ingredientSectionBody = cleanText(ingredientSummarySection?.body);
  const explicitIngredients = cleanText(params.ingredientsMarkdownText);
  const explicitFullIngredients = stripIngredientPackageDisclaimer(extractLikelyFullIngredientListText(explicitIngredients));
  const sectionFullIngredients = stripIngredientPackageDisclaimer(extractLikelyFullIngredientListText(fullIngredientSection?.body));
  const activeIngredients = cleanText(params.activeIngredientsText);
  const ingredientsRaw =
    explicitFullIngredients || sectionFullIngredients || undefined;
  const ingredientSummaryBody = !explicitFullIngredients && explicitIngredients ? explicitIngredients : ingredientSectionBody;
  const activeIngredientsRaw =
    activeIngredients ||
    extractDelimitedLabeledSectionText(
      ingredientsRaw || ingredientSummaryBody,
      ["Active Ingredients", "Active Ingredient"],
      ["Inactive Ingredients", "Ingredient List", "Ingredients"],
    ) ||
    (!ingredientsRaw && looksLikeActiveIngredientSummaryText(ingredientSummaryBody) ? ingredientSummaryBody : "") ||
    undefined;
  const explicitHowTo = cleanText(params.howToUseText);
  const usableExplicitHowTo = isPdpContentNoiseText(explicitHowTo) ? "" : explicitHowTo;
  const instructionalHowToSection = pickBestDetailSection(
    detailsSections,
    (section) =>
      /\bhow to (?:use|apply)\b/i.test(section.heading) && looksLikeHowToUseInstructionText(section.body),
  );
  const fallbackHowToSection = pickBestDetailSection(
    detailsSections,
    (section) => /\bhow to (?:use|apply)\b/i.test(section.heading),
  );
  const brandedApplicationHowToSection = pickBestDetailSection(
    detailsSections,
    (section) =>
      /\b(?:application|routine|tutorial|eye look|everyday eye|look|pro tip)\b/i.test(section.heading) &&
      looksLikeHowToUseInstructionText(section.body),
  );
  const howToUseRaw =
    stripProductRegulatoryTail(
      (looksLikeHowToUseInstructionText(usableExplicitHowTo) ? usableExplicitHowTo : "") ||
        instructionalHowToSection?.body ||
        fallbackHowToSection?.body ||
        brandedApplicationHowToSection?.body,
    ) ||
    undefined;

  return {
    ingredientsRaw,
    activeIngredientsRaw,
    howToUseRaw,
  };
}

export function buildProductPdpFields(params: {
  descriptionRaw?: string;
  detailsSections?: ExtractedProductDetailSection[];
  ingredientsRaw?: string;
  activeIngredientsRaw?: string;
  howToUseRaw?: string;
  faqItems?: ExtractedProductFaqItem[];
  fieldSources?: Partial<Record<keyof NonNullable<ExtractedProduct["field_capture_status"]>, string[]>>;
}) {
  const descriptionRaw = cleanText(params.descriptionRaw);
  const detailsSections = dedupeDetailSections(params.detailsSections || []);
  const ingredientsRaw = cleanText(params.ingredientsRaw);
  const activeIngredientsRaw = cleanText(params.activeIngredientsRaw);
  const cleanedHowToUseRaw = cleanText(params.howToUseRaw);
  const howToUseRaw = isPdpContentNoiseText(cleanedHowToUseRaw) ? "" : cleanedHowToUseRaw;
  const faqItems = dedupeFaqItems(params.faqItems || []);
  const fieldSources = {
    description_raw: normalizePdpSourceKinds(params.fieldSources?.description_raw || []),
    details_sections: normalizePdpSourceKinds(params.fieldSources?.details_sections || []),
    ingredients_raw: normalizePdpSourceKinds(params.fieldSources?.ingredients_raw || []),
    active_ingredients_raw: normalizePdpSourceKinds(params.fieldSources?.active_ingredients_raw || []),
    how_to_use_raw: normalizePdpSourceKinds(params.fieldSources?.how_to_use_raw || []),
    faq_items: normalizePdpSourceKinds(params.fieldSources?.faq_items || []),
  };
  const surfacedDetailsSections = detailsSections.filter((section) => !isQuarantinedDetailSourceKind(section.source_kind));
  const quarantinedDetailsSections = detailsSections.filter((section) => isQuarantinedDetailSourceKind(section.source_kind));
  const surfacedFaqItems = faqItems.filter((item) => !isQuarantinedDetailSourceKind(item.source_kind));
  const quarantinedFaqItems = faqItems.filter((item) => isQuarantinedDetailSourceKind(item.source_kind));
  const baseDescriptionSummary = classifyPdpFieldQuality(fieldSources.description_raw);
  const baseIngredientsSummary = classifyPdpFieldQuality(fieldSources.ingredients_raw);
  const baseActiveIngredientsSummary = classifyPdpFieldQuality(fieldSources.active_ingredients_raw);
  const baseHowToUseSummary = classifyPdpFieldQuality(fieldSources.how_to_use_raw);
  const surfacedDescriptionRaw =
    baseDescriptionSummary.source_quality_status === "quarantined" ? "" : descriptionRaw;
  const surfacedIngredientsRaw =
    baseIngredientsSummary.source_quality_status === "quarantined" ? "" : ingredientsRaw;
  const surfacedActiveIngredientsRaw =
    baseActiveIngredientsSummary.source_quality_status === "quarantined" ? "" : activeIngredientsRaw;
  const surfacedHowToUseRaw =
    baseHowToUseSummary.source_quality_status === "quarantined" ? "" : howToUseRaw;
  const surfacedFieldSources = {
    description_raw: surfacedDescriptionRaw
      ? fieldSources.description_raw.filter((kind) => !isQuarantinedDetailSourceKind(kind))
      : [],
    details_sections:
      surfacedDetailsSections.length > 0
        ? normalizePdpSourceKinds(surfacedDetailsSections.map((section) => section.source_kind))
        : [],
    ingredients_raw: surfacedIngredientsRaw
      ? fieldSources.ingredients_raw.filter((kind) => !isQuarantinedDetailSourceKind(kind))
      : [],
    active_ingredients_raw: surfacedActiveIngredientsRaw
      ? fieldSources.active_ingredients_raw.filter((kind) => !isQuarantinedDetailSourceKind(kind))
      : [],
    how_to_use_raw: surfacedHowToUseRaw
      ? fieldSources.how_to_use_raw.filter((kind) => !isQuarantinedDetailSourceKind(kind))
      : [],
    faq_items:
      surfacedFaqItems.length > 0
        ? normalizePdpSourceKinds(surfacedFaqItems.map((item) => item.source_kind))
        : [],
  };
  const descriptionSummary = classifyPdpFieldQuality(
    surfacedDescriptionRaw ? surfacedFieldSources.description_raw : fieldSources.description_raw,
  );
  const detailsSummary = classifyPdpFieldQuality(
    surfacedDetailsSections.length > 0
      ? surfacedFieldSources.details_sections
      : quarantinedDetailsSections.length > 0
        ? quarantinedDetailsSections.map((section) => section.source_kind)
        : fieldSources.details_sections,
  );
  const ingredientsSummary = classifyPdpFieldQuality(
    surfacedIngredientsRaw ? surfacedFieldSources.ingredients_raw : fieldSources.ingredients_raw,
  );
  const activeIngredientsSummary = classifyPdpFieldQuality(
    surfacedActiveIngredientsRaw
      ? surfacedFieldSources.active_ingredients_raw
      : fieldSources.active_ingredients_raw,
  );
  const howToUseSummary = classifyPdpFieldQuality(
    surfacedHowToUseRaw ? surfacedFieldSources.how_to_use_raw : fieldSources.how_to_use_raw,
  );
  const faqSummary = classifyPdpFieldQuality(
    surfacedFaqItems.length > 0
      ? surfacedFieldSources.faq_items
      : quarantinedFaqItems.length > 0
        ? quarantinedFaqItems.map((item) => item.source_kind)
        : fieldSources.faq_items,
  );
  const quarantinedFields: ExtractedProduct["quarantined_pdp_fields"] = {};
  if (descriptionRaw && descriptionSummary.source_quality_status === "quarantined") {
    quarantinedFields.description_raw = descriptionRaw;
  }
  if (quarantinedDetailsSections.length > 0 || detailsSummary.source_quality_status === "quarantined") {
    quarantinedFields.details_sections =
      quarantinedDetailsSections.length > 0 ? quarantinedDetailsSections : detailsSections;
  }
  if (ingredientsRaw && ingredientsSummary.source_quality_status === "quarantined") {
    quarantinedFields.ingredients_raw = ingredientsRaw;
  }
  if (activeIngredientsRaw && activeIngredientsSummary.source_quality_status === "quarantined") {
    quarantinedFields.active_ingredients_raw = activeIngredientsRaw;
  }
  if (howToUseRaw && howToUseSummary.source_quality_status === "quarantined") {
    quarantinedFields.how_to_use_raw = howToUseRaw;
  }
  if (quarantinedFaqItems.length > 0 || faqSummary.source_quality_status === "quarantined") {
    quarantinedFields.faq_items = quarantinedFaqItems.length > 0 ? quarantinedFaqItems : faqItems;
  }

  return {
    ...(surfacedDescriptionRaw ? { description_raw: surfacedDescriptionRaw } : {}),
    ...(surfacedDetailsSections.length > 0 ? { details_sections: surfacedDetailsSections } : {}),
    ...(surfacedIngredientsRaw ? { ingredients_raw: surfacedIngredientsRaw } : {}),
    ...(surfacedActiveIngredientsRaw ? { active_ingredients_raw: surfacedActiveIngredientsRaw } : {}),
    ...(surfacedHowToUseRaw ? { how_to_use_raw: surfacedHowToUseRaw } : {}),
    ...(surfacedFaqItems.length > 0 ? { faq_items: surfacedFaqItems } : {}),
    field_capture_status: {
      description_raw: surfacedDescriptionRaw ? "present" : "missing",
      details_sections: surfacedDetailsSections.length > 0 ? "present" : "missing",
      ingredients_raw: surfacedIngredientsRaw ? "present" : "missing",
      active_ingredients_raw: surfacedActiveIngredientsRaw ? "present" : "missing",
      how_to_use_raw: surfacedHowToUseRaw ? "present" : "missing",
      faq_items: surfacedFaqItems.length > 0 ? "present" : "missing",
    } as const,
    field_sources: {
      description_raw: surfacedFieldSources.description_raw,
      details_sections: surfacedFieldSources.details_sections,
      ingredients_raw: surfacedFieldSources.ingredients_raw,
      active_ingredients_raw: surfacedFieldSources.active_ingredients_raw,
      how_to_use_raw: surfacedFieldSources.how_to_use_raw,
      faq_items: surfacedFieldSources.faq_items,
    },
    field_quality_summary: {
      description_raw: descriptionSummary,
      details_sections: detailsSummary,
      ingredients_raw: ingredientsSummary,
      active_ingredients_raw: activeIngredientsSummary,
      how_to_use_raw: howToUseSummary,
      faq_items: faqSummary,
    },
    ...(Object.keys(quarantinedFields).length > 0 ? { quarantined_pdp_fields: quarantinedFields } : {}),
  };
}

const PDP_COMPLETENESS_ACCESSORY_RE =
  /\b(brush|sponge|puff|applicator|sharpener|tweezer|curler|scissors|comb|mirror|case|bag|pouch|holder|spatula|tool|tools|gua sha|roller|loofah|headband|scrunchie|scarf|hat|cap|tote|clip|clips|pin|pins|keychain|key chain|tray|lash curler|refill case)\b/i;
const PDP_COMPLETENESS_BUNDLE_RE =
  /\b(build your own|bundle|set|kit|duo|trio|coffret|vault|calendar|advent calendar|mini set|travel set|starter set|value set|gift set|combo|show look|look set|collection set|collection kit|collection bundle)\b/i;
const PDP_COMPLETENESS_WEAK_BUNDLE_RE =
  /\b(routine|program|programme|regimen|protocol)\b/i;
const PDP_COMPLETENESS_LOOK_BUNDLE_RE =
  /\b(?:kylie'?s|vacay|vogue|on-the-go|inspired)\b.*(?:\blook\b|\bglam\b)/i;
const PDP_COMPLETENESS_FRAGRANCE_RE =
  /\b(fragrance|perfume|parfum|eau de|edt|edp|cologne|body mist|pen spray|scent)\b/i;
const PDP_COMPLETENESS_SKINCARE_RE =
  /\b(skincare|skin care|cleanser|toner|essence|serum|ampoule|moisturi[sz]er|cream|lotion|balm|mask|scrub|body scrub|peel|exfoliant|treatment|oil|patch|patchs|patches|eye patch|eye patches|hydrogel patch|pimple patch|spot stickers?|blemish stickers?|acne stickers?|healing dots?|acne|blemish|hydrocolloid|sunscreen|spf|face mist|facial mist|hydrating mist|retinol|vitamin c|niacinamide|aha|bha|acid|salicylic|benzoyl|azelaic|ceramide|hyaluronic)\b/i;
const PDP_COMPLETENESS_MAKEUP_RE =
  /\b(makeup|foundation|concealer|mascara|lipstick|lip gloss|lip glaze|lip oil|lip liner|lip luminizer|lip kit|luminizer|blush|bronzer|powder|highlighter|eyeshadow|eyeliner|brow|primer|setting spray|skin tint|tint|shade|palette)\b/i;
const PDP_COMPLETENESS_FORMULA_PAIR_RE =
  /\b(foundation|concealer|mascara|lipstick|liquid lipstick|lip gloss|high gloss|gloss drip|lip glaze|lip oil|lip liner|lip luminizer|lip kit|butter balm|tinted butter balm|luminizer|blush|bronzer|powder|highlighter|eyeshadow|eye shadow|eyeliner|brow|primer|setting spray|skin tint|tint|palette|nail lacquer)\b.*(?:\s[&+]\s|\s+and\s+|\s+plus\s+).*\b(foundation|concealer|mascara|lipstick|liquid lipstick|lip gloss|high gloss|gloss drip|lip glaze|lip oil|lip liner|lip luminizer|lip kit|butter balm|tinted butter balm|luminizer|blush|bronzer|powder|highlighter|eyeshadow|eye shadow|eyeliner|brow|primer|setting spray|skin tint|tint|palette|nail lacquer)\b/i;
const PDP_COMPLETENESS_HAIR_RE =
  /\b(haircare|hair care|shampoo|conditioner|scalp|leave-in|styling|curl|detangler)\b/i;
const PDP_COMPLETENESS_MIN_OVERVIEW_CHARS = 80;
const BUNDLE_COMPONENT_HEADING_RE =
  /\b(included|includes|inside|contents|what'?s inside|in the (?:set|kit|bundle)|set contains|kit contains)\b/i;
const BUNDLE_INCLUDE_TEXT_RE =
  /\b(?:this\s+)?(?:set|kit|bundle|collection|calendar|combo)?\s*(?:includes?|contains?|comes with|features)\b/i;
const BUNDLE_COMPONENT_NOISE_RE =
  /\b(?:free shipping|limited edition|add to cart|shop now|complete routine|gift box|packaging|full size value|value of|worth|savings?)\b/i;

function buildPdpCompletenessIdentityText(product: ExtractedProduct): string {
  return [
    product.title,
    product.url,
    ...(product.details_sections || []).flatMap((section) => {
      const heading = normalizeDetailSectionHeading(section.heading);
      return heading === "Details" ? [] : [section.heading, section.body];
    }),
    product.description_raw,
    product.how_to_use_raw,
    product.ingredients_raw,
    product.active_ingredients_raw,
    ...(product.variants || []).flatMap((variant) => [
      variant.option_name,
      variant.option_value,
      variant.sku,
      variant.url,
    ]),
  ]
    .map((value) => cleanText(value))
    .filter(Boolean)
    .join("\n");
}

function buildPdpCompletenessTitleText(product: ExtractedProduct): string {
  return [
    product.title,
    product.url,
    ...(product.variants || []).flatMap((variant) => [variant.option_name, variant.option_value, variant.sku, variant.url]),
  ]
    .map((value) => cleanText(value))
    .filter(Boolean)
    .join("\n");
}

function inferPdpCompletenessRequirements(product: ExtractedProduct) {
  const text = buildPdpCompletenessIdentityText(product);
  const titleText = buildPdpCompletenessTitleText(product);
  const accessory = PDP_COMPLETENESS_ACCESSORY_RE.test(titleText);
  const bundleSignals =
    PDP_COMPLETENESS_BUNDLE_RE.test(text) ||
    PDP_COMPLETENESS_WEAK_BUNDLE_RE.test(titleText) ||
    (!accessory && PDP_COMPLETENESS_FORMULA_PAIR_RE.test(text)) ||
    PDP_COMPLETENESS_LOOK_BUNDLE_RE.test(text);
  const bundle = !accessory && bundleSignals;
  const skincare = PDP_COMPLETENESS_SKINCARE_RE.test(text);
  const makeup = PDP_COMPLETENESS_MAKEUP_RE.test(text);
  const hair = PDP_COMPLETENESS_HAIR_RE.test(text);
  const fragrance =
    PDP_COMPLETENESS_FRAGRANCE_RE.test(titleText) ||
    (!skincare && !makeup && !hair && PDP_COMPLETENESS_FRAGRANCE_RE.test(text));
  const formula = !accessory && !bundle && (skincare || makeup || hair || fragrance);
  const needsRoutineUse = !accessory && !bundle && !fragrance && (skincare || hair);
  const needsIngredients = formula && !fragrance;
  return {
    accessory,
    bundle,
    fragrance,
    formula,
    needsRoutineUse,
    needsIngredients,
  };
}

export function classifyExtractedProductKind(product: ExtractedProduct): ExtractedProductKind {
  const requirements = inferPdpCompletenessRequirements(product);
  if (requirements.bundle) return "bundle";
  if (requirements.accessory) return "accessory";
  if (requirements.fragrance) return "fragrance";
  if (requirements.formula) return "single_formula";
  return "general_merchandise";
}

function normalizeBundleComponentCandidate(raw: string): ExtractedBundleComponent | null {
  const rawText = cleanText(raw);
  if (!rawText || rawText.length < 3 || rawText.length > 120) return null;
  if (BUNDLE_COMPONENT_NOISE_RE.test(rawText)) return null;
  if (/^(?:and|or|with|plus|includes?|contains?|bundle|set|kit|duo|trio|routine|vault)$/i.test(rawText)) return null;

  const quantityMatch = rawText.match(
    /^(?:(\d+\s*x|\d+-piece|one|two|three|four|five|six|seven|eight|nine|ten|full-size|mini|travel-size|travel)\s+)?(.+)$/i,
  );
  const quantity = cleanText(quantityMatch?.[1]);
  const name = cleanText(
    (quantityMatch?.[2] || rawText)
      .replace(/^(?:a|an|the)\s+/i, "")
      .replace(/\b(?:bundle|set|kit|duo|trio|routine|vault)\b$/i, "")
      .replace(/[.。]+$/g, ""),
  );
  if (!name || name.length < 3 || name.length > 100) return null;
  if (/^(?:full size|mini|deluxe|travel size)$/i.test(name)) return null;

  return {
    name,
    ...(quantity ? { quantity } : {}),
    source_kind: "bundle_component_candidate",
    raw_text: rawText,
  };
}

function parseBundleComponentCandidatesFromTitle(title: string): ExtractedBundleComponent[] {
  const rawTitle = cleanText(title);
  if (!rawTitle || !PDP_COMPLETENESS_BUNDLE_RE.test(rawTitle)) return [];

  const titleWithoutVariant = rawTitle.replace(/\s+[—–-]\s+(?:light|light medium|medium|medium deep|deep|dry skin edition|oily skin edition)$/i, "");
  const hasExplicitComponentSyntax =
    /^build your own\b/i.test(titleWithoutVariant) ||
    titleWithoutVariant.includes(":") ||
    /(?:\s[+&]\s|[,;]|\s+(?:and|plus)\s+)/i.test(titleWithoutVariant);
  if (!hasExplicitComponentSyntax) return [];

  const componentText = cleanText(
    (titleWithoutVariant.includes(":") ? titleWithoutVariant.split(":").slice(1).join(":") : titleWithoutVariant)
      .replace(/^build your own\s+/i, "")
      .replace(/\b(?:\d+-piece|full-size|mini|travel-size|travel)\b/gi, "")
      .replace(/\b(?:bundle|set|kit|duo|trio|routine|program|programme|regimen|protocol|coffret|vault)\b/gi, "")
      .replace(/\s+/g, " "),
  );
  if (!componentText || componentText.toLowerCase() === rawTitle.toLowerCase()) return [];

  const candidates = componentText
    .replace(/\s+(?:and|plus)\s+/gi, " + ")
    .split(/(?:\n|[•;,]|\s+[+&]\s+)/)
    .map(normalizeBundleComponentCandidate)
    .filter((item): item is ExtractedBundleComponent => Boolean(item))
    .map((component) => ({
      ...component,
      source_kind: "bundle_component_title_candidate",
    }));

  return dedupeBy(candidates, (item) => item.name.toLowerCase()).slice(0, 12);
}

function parseBundleComponentCandidatesFromText(text: string): ExtractedBundleComponent[] {
  const normalized = cleanText(
    text
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/(?:li|p|div|h\d)>/gi, "\n")
      .replace(/<[^>]+>/g, " "),
  );
  if (!normalized) return [];

  const includeMatch = normalized.match(BUNDLE_INCLUDE_TEXT_RE);
  const componentText = includeMatch
    ? normalized.slice(includeMatch.index! + includeMatch[0].length).replace(/^[:\s-]+/, "")
    : normalized;
  if (!componentText || componentText.length > 1_200) return [];

  const candidates = componentText
    .replace(/\s+(?:and|plus)\s+/gi, ", ")
    .split(/(?:\n|[•;]|,\s+)/)
    .map(normalizeBundleComponentCandidate)
    .filter((item): item is ExtractedBundleComponent => Boolean(item));

  return dedupeBy(candidates, (item) => item.name.toLowerCase()).slice(0, 12);
}

export function extractBundleComponents(product: ExtractedProduct): ExtractedBundleComponent[] {
  if (classifyExtractedProductKind(product) !== "bundle") return [];

  const sectionCandidates = (Array.isArray(product.details_sections) ? product.details_sections : [])
    .filter((section) => BUNDLE_COMPONENT_HEADING_RE.test(cleanText(section.heading)))
    .flatMap((section) =>
      parseBundleComponentCandidatesFromText(section.body).map((component) => ({
        ...component,
        source_kind: section.source_kind || component.source_kind,
      })),
    );
  if (sectionCandidates.length > 0) {
    return dedupeBy(sectionCandidates, (item) => item.name.toLowerCase()).slice(0, 12);
  }

  const titleCandidates = parseBundleComponentCandidatesFromTitle(product.title);
  if (titleCandidates.length > 0) return titleCandidates;

  const descriptionCandidates = parseBundleComponentCandidatesFromText(product.description_raw || "");
  if (descriptionCandidates.length > 0) {
    return descriptionCandidates.map((component) => ({
      ...component,
      source_kind: "bundle_component_description_candidate",
    }));
  }

  return [];
}

function withProductPdpProfile(product: ExtractedProduct): ExtractedProduct {
  const productKind = classifyExtractedProductKind(product);
  const bundleComponents = productKind === "bundle" ? extractBundleComponents(product) : [];
  return {
    ...product,
    product_kind: productKind,
    ...(bundleComponents.length > 0 ? { bundle_components: bundleComponents } : {}),
  };
}

export type MissingPdpFieldReason = "overview" | "how_to_use" | "ingredients";

export function getMissingPdpFieldReasons(product: ExtractedProduct): MissingPdpFieldReason[] {
  const detailsSections = Array.isArray(product?.details_sections)
    ? product.details_sections.filter((section) => !isTaxonomyOnlyDetailSection(section))
    : [];
  const descriptionRaw = cleanText(product?.description_raw);
  const hasOverview = detailsSections.length > 0 || descriptionRaw.length >= PDP_COMPLETENESS_MIN_OVERVIEW_CHARS;
  const hasHowToUse = Boolean(cleanText(product?.how_to_use_raw));
  const hasIngredients = Boolean(cleanText(product?.ingredients_raw) || cleanText(product?.active_ingredients_raw));
  const requirements = inferPdpCompletenessRequirements(product);
  const reasons: MissingPdpFieldReason[] = [];

  if (!hasOverview) reasons.push("overview");
  if (requirements.needsRoutineUse && !hasHowToUse) reasons.push("how_to_use");
  if (requirements.needsIngredients && !hasIngredients) reasons.push("ingredients");
  return reasons;
}

export function productHasMissingPdpFields(product: ExtractedProduct) {
  return getMissingPdpFieldReasons(product).length > 0;
}

type ShopifyDirectPdpThinReason =
  | "structured_sections"
  | "faq"
  | "content_images"
  | "gallery_depth"
  | "variant_gallery_depth";

type ShopifyDirectPdpHtmlSignals = {
  hasFaqHeading: boolean;
  hasHowToUseHeading: boolean;
  hasIngredientsHeading: boolean;
  hasClinicalHeading: boolean;
  candidateImageUrls: string[];
};

const SHOPIFY_HTML_IMAGE_URL_RE =
  /(?:https?:)?\/\/cdn\.shopify\.com\/s\/files\/[^"'()\s>]+\.(?:png|jpe?g|webp|gif|avif)(?:\?[^"'()\s>]*)?/gi;
const SHOPIFY_HTML_NON_PRODUCT_IMAGE_RE =
  /\b(?:logo|icon|badge|sprite|payment|klarna|affirm|afterpay|font|gotham|visa|mastercard|amex|paypal|facebook|instagram|tiktok|youtube|pinterest)\b/i;

function extractShopifyDirectPdpHtmlImageUrls(
  html: string | undefined,
  baseUrl: string,
  product: ExtractedProduct,
): string[] {
  if (!html) return [];
  const rawUrls = Array.from(html.matchAll(SHOPIFY_HTML_IMAGE_URL_RE))
    .map((match) => cleanText(match[0]))
    .filter(Boolean);
  const normalized = dedupeStringList(
    resolveStructuredImageUrls(
      baseUrl,
      rawUrls.map((url) => (url.startsWith("//") ? `https:${url}` : url)),
    ),
  ).filter((url) => !SHOPIFY_HTML_NON_PRODUCT_IMAGE_RE.test(url));
  if (normalized.length === 0) return [];

  const skuTokens = dedupeStringList(
    [product.variant_skus || [], (product.variants || []).map((variant) => variant.sku)]
      .flat()
      .map((value) => extractSkuImageToken(value))
      .filter(Boolean) as string[],
  );
  const scopedBySku = skuTokens.length
    ? normalized.filter((url) => {
        const token = extractSkuImageToken(url);
        return token ? skuTokens.includes(token) : false;
      })
    : [];
  if (scopedBySku.length > 0) return scopedBySku;

  const handle = extractShopifyProductHandle(product.url, baseUrl);
  const handleTokens = cleanText(handle)
    .toLowerCase()
    .split(/[^a-z0-9]+/i)
    .map((token) => token.trim())
    .filter((token) => token.length >= 4);
  const scopedByHandle = handleTokens.length
    ? normalized.filter((url) => {
        const lower = url.toLowerCase();
        return handleTokens.filter((token) => lower.includes(token)).length >= Math.min(2, handleTokens.length);
      })
    : [];
  return scopedByHandle.length > 0 ? scopedByHandle : normalized;
}

function inspectShopifyDirectPdpHtmlSignals(
  html: string | undefined,
  baseUrl: string,
  product: ExtractedProduct,
): ShopifyDirectPdpHtmlSignals {
  const text = cleanText(
    String(html || "")
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " "),
  );
  return {
    hasFaqHeading: /\b(?:frequently asked questions|routine faq|faqs?|questions & answers|questions and answers)\b/i.test(text),
    hasHowToUseHeading: /\b(?:how to use|how to apply|directions)\b/i.test(text),
    hasIngredientsHeading: /\b(?:ingredients|full ingredients|key ingredients)\b/i.test(text),
    hasClinicalHeading: /\b(?:clinical results|clinically proven|proven effective|results)\b/i.test(text),
    candidateImageUrls: extractShopifyDirectPdpHtmlImageUrls(html, baseUrl, product),
  };
}

function getShopifyDirectPdpThinReasons(
  product: ExtractedProduct,
  htmlSignals: ShopifyDirectPdpHtmlSignals,
): ShopifyDirectPdpThinReason[] {
  const reasons: ShopifyDirectPdpThinReason[] = [];
  const detailsSections = Array.isArray(product.details_sections)
    ? product.details_sections.filter((section) => !isTaxonomyOnlyDetailSection(section))
    : [];
  const faqItems = Array.isArray(product.faq_items) ? product.faq_items : [];
  const contentImageCount = Array.isArray(product.content_image_urls) ? product.content_image_urls.length : 0;
  const productImageCount = Array.isArray(product.image_urls) ? product.image_urls.length : 0;
  const maxVariantImageCount = Math.max(0, ...(product.variants || []).map((variant) => variant.image_urls.length));
  const htmlImageCount = htmlSignals.candidateImageUrls.length;
  const htmlShowsStructuredContent =
    htmlSignals.hasFaqHeading ||
    htmlSignals.hasHowToUseHeading ||
    htmlSignals.hasIngredientsHeading ||
    htmlSignals.hasClinicalHeading;

  if (detailsSections.length === 0 && htmlShowsStructuredContent) reasons.push("structured_sections");
  if (faqItems.length === 0 && htmlSignals.hasFaqHeading) reasons.push("faq");
  if (contentImageCount === 0 && htmlImageCount >= Math.max(3, productImageCount + 2)) reasons.push("content_images");
  if (productImageCount <= 1 && htmlImageCount >= 3) reasons.push("gallery_depth");
  if (maxVariantImageCount <= 1 && htmlImageCount >= 3) reasons.push("variant_gallery_depth");

  return dedupeStringList(reasons) as ShopifyDirectPdpThinReason[];
}

export type ImageVisionPdpFields = {
  descriptionRaw?: string;
  detailsSections?: ExtractedProductDetailSection[];
  ingredientsRaw?: string;
  activeIngredientsRaw?: string;
  howToUseRaw?: string;
  contentImageUrls?: string[];
};

export type ShopifyImageVisionClient = (params: {
  brand: string;
  seedUrl: string;
  baseUrl: string;
  product: ExtractedProduct;
  imageUrls: string[];
  missingReasons: MissingPdpFieldReason[];
}) => Promise<ImageVisionPdpFields | null>;

function isImageVisionDisabled() {
  return /^(?:0|false|no|off)$/i.test(String(process.env.CATALOG_IMAGE_VISION_ENRICHMENT || "").trim());
}

function getImageVisionApiKey() {
  return (
    process.env.CATALOG_IMAGE_VISION_API_KEY ||
    process.env.PIVOTA_GEMINI_API_KEY ||
    process.env.AURORA_SKIN_GEMINI_API_KEY ||
    process.env.AURORA_RECO_GEMINI_API_KEY ||
    process.env.GEMINI_API_KEY ||
    process.env.GOOGLE_GENERATIVE_AI_API_KEY ||
    process.env.GOOGLE_API_KEY ||
    ""
  ).trim();
}

function getImageVisionModel() {
  return (process.env.CATALOG_IMAGE_VISION_MODEL || process.env.GEMINI_VISION_MODEL || process.env.GEMINI_MODEL || "gemini-2.5-flash").trim();
}

function getImageVisionCandidateUrls(product: ExtractedProduct) {
  const maxImages = clampIntShared(
    process.env.CATALOG_IMAGE_VISION_MAX_IMAGES,
    DEFAULT_IMAGE_VISION_MAX_IMAGES,
    1,
    12,
  );
  return dedupeStringList([
    product.image_url,
    ...product.image_urls,
    ...product.variants.flatMap((variant) => [variant.image_url, ...variant.image_urls]),
  ])
    .filter((url) => /^https?:\/\//i.test(url))
    .filter((url) => !INVALID_IMAGE_URL_RE.test(url))
    .slice(0, maxImages);
}

export function shouldAttemptShopifyImageVisionEnrichment(product: ExtractedProduct) {
  if (isImageVisionDisabled()) return false;
  const kind = product.product_kind || classifyExtractedProductKind(product);
  if (kind === "accessory" || kind === "general_merchandise") return false;
  const candidateUrls = getImageVisionCandidateUrls(product);
  if (candidateUrls.length < 2) return false;
  return getMissingPdpFieldReasons(product).length > 0;
}

function normalizeImageVisionText(value: unknown, product: ExtractedProduct, minChars = 12) {
  const normalized = cleanText(typeof value === "string" ? value : undefined);
  if (!normalized || normalized.length < minChars) return "";
  const lower = normalized.toLowerCase();
  const title = cleanText(product.title).toLowerCase();
  if (lower === title) return "";
  if (/^(?:product type|primer|serum|moisturi[sz]er|toner|cleanser|sunscreen|foundation|concealer)$/i.test(normalized)) {
    return "";
  }
  if (/\b(?:not visible|not readable|cannot determine|unable to determine|no text visible)\b/i.test(normalized)) return "";
  return normalized;
}

function parseImageVisionJson(text: string) {
  const cleaned = cleanText(text)
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "");
  try {
    return JSON.parse(cleaned) as Record<string, unknown>;
  } catch {
    const matched = cleaned.match(/\{[\s\S]*\}/);
    if (!matched) return null;
    try {
      return JSON.parse(matched[0]) as Record<string, unknown>;
    } catch {
      return null;
    }
  }
}

function stripImageVisionListMarker(value: string) {
  return cleanText(value)
    .replace(/^(?:[*•·-]\s*)+/, "")
    .trim();
}

function normalizeImageVisionIngredientList(value: string) {
  const withoutMarker = stripImageVisionListMarker(value);
  const extracted = stripImageVisionListMarker(
    stripIngredientPackageDisclaimer(extractLikelyFullIngredientListText(withoutMarker)) || "",
  );
  if (extracted) return extracted;
  return looksLikeFullIngredientListText(withoutMarker) ? withoutMarker : "";
}

export function normalizeImageVisionFields(raw: Record<string, unknown>, product: ExtractedProduct, imageUrls: string[]): ImageVisionPdpFields | null {
  const descriptionRaw = normalizeImageVisionText(raw.description_raw ?? raw.descriptionRaw ?? raw.overview, product, 40);
  const rawSections = Array.isArray(raw.details_sections)
    ? raw.details_sections
    : Array.isArray(raw.detailsSections)
      ? raw.detailsSections
      : Array.isArray(raw.sections)
        ? raw.sections
        : [];
  const detailsSections = dedupeDetailSections(
    rawSections.flatMap((item): ExtractedProductDetailSection[] => {
      if (!item || typeof item !== "object") return [];
      const record = item as Record<string, unknown>;
      const heading = normalizeImageVisionText(record.heading ?? record.title ?? record.name, product, 3);
      const body = normalizeImageVisionText(record.body ?? record.text ?? record.content, product, 18);
      if (!heading || !body) return [];
      return [
        {
          heading,
          body,
          source_kind: "product_image_vision",
        },
      ];
    }),
  );
  const rawIngredients = normalizeImageVisionText(raw.ingredients_raw ?? raw.ingredientsRaw ?? raw.ingredients, product, 24);
  const ingredientsRaw = normalizeImageVisionIngredientList(rawIngredients);
  const activeIngredientsRaw = normalizeImageVisionText(
    raw.active_ingredients_raw ?? raw.activeIngredientsRaw ?? raw.active_ingredients,
    product,
    8,
  );
  const howToUseCandidate = normalizeImageVisionText(raw.how_to_use_raw ?? raw.howToUseRaw ?? raw.how_to_use, product, 18);
  const howToUseRaw = looksLikeHowToUseInstructionText(howToUseCandidate) ? howToUseCandidate : "";

  const fields: ImageVisionPdpFields = {
    ...(descriptionRaw ? { descriptionRaw } : {}),
    ...(detailsSections.length > 0 ? { detailsSections } : {}),
    ...(ingredientsRaw ? { ingredientsRaw } : {}),
    ...(activeIngredientsRaw ? { activeIngredientsRaw } : {}),
    ...(howToUseRaw ? { howToUseRaw } : {}),
    contentImageUrls: imageUrls,
  };
  return hasDisplayableImageVisionFields(fields) ? fields : null;
}

function hasDisplayableImageVisionFields(fields: ImageVisionPdpFields | null | undefined) {
  if (!fields) return false;
  const meaningfulSections = (fields.detailsSections || []).filter((section) => !isTaxonomyOnlyDetailSection(section));
  return Boolean(
    normalizeImageVisionText(fields.descriptionRaw, { title: "", url: "", image_url: "", image_urls: [], variant_skus: [], variants: [] }, 40) ||
      meaningfulSections.some((section) => cleanText(section.heading) && cleanText(section.body).length >= 18) ||
      cleanText(fields.ingredientsRaw) ||
      cleanText(fields.activeIngredientsRaw) ||
      cleanText(fields.howToUseRaw),
  );
}

async function fetchImageVisionInlinePart(url: string) {
  const timeoutMs = clampIntShared(
    process.env.CATALOG_IMAGE_VISION_IMAGE_FETCH_TIMEOUT_MS,
    DEFAULT_FETCH_TIMEOUT_MS,
    2_000,
    60_000,
  );
  const maxBytes = clampIntShared(
    process.env.CATALOG_IMAGE_VISION_MAX_IMAGE_BYTES,
    DEFAULT_IMAGE_VISION_MAX_IMAGE_BYTES,
    100_000,
    20_000_000,
  );
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        accept: "image/avif,image/webp,image/png,image/jpeg,image/*;q=0.9,*/*;q=0.1",
        "user-agent": process.env.PUPPETEER_USER_AGENT || DEFAULT_BROWSERISH_USER_AGENT,
      },
    });
    if (!response.ok) return null;
    const mimeType = (response.headers.get("content-type") || "").split(";")[0]?.trim().toLowerCase() || "";
    if (!mimeType.startsWith("image/")) return null;
    const bytes = Buffer.from(await response.arrayBuffer());
    if (bytes.byteLength === 0 || bytes.byteLength > maxBytes) return null;
    return {
      sourceUrl: url,
      part: {
        inlineData: {
          mimeType,
          data: bytes.toString("base64"),
        },
      },
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function extractShopifyImageVisionPdpFields(params: Parameters<ShopifyImageVisionClient>[0]) {
  const apiKey = getImageVisionApiKey();
  if (!apiKey) return null;

  const imageParts: Array<{ sourceUrl: string; part: { inlineData: { mimeType: string; data: string } } }> = [];
  for (const imageUrl of params.imageUrls) {
    const imagePart = await fetchImageVisionInlinePart(imageUrl);
    if (imagePart) imageParts.push(imagePart);
  }
  if (imageParts.length === 0) return null;

  const prompt = [
    "Extract only merchant-visible product detail text from these PDP/product images.",
    "Do not infer from the product title, category, brand, or general beauty knowledge.",
    "Do not create marketing copy, summaries, recommendations, or fallback values.",
    "If a field is not clearly readable in the images, return an empty string or empty array for that field.",
    "Return JSON only with keys: description_raw, details_sections, how_to_use_raw, ingredients_raw, active_ingredients_raw.",
    "details_sections must be an array of {heading, body}. Keep sections concise and evidence-backed.",
    `Product title for identity check only: ${params.product.title}`,
    `Source PDP: ${params.seedUrl}`,
  ].join("\n");

  const model = getImageVisionModel();
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      contents: [
        {
          role: "user",
          parts: [
            { text: prompt },
            ...imageParts.map((item) => item.part),
          ],
        },
      ],
      generationConfig: {
        temperature: 0,
        responseMimeType: "application/json",
      },
    }),
  });
  if (!response.ok) return null;
  const payload = (await response.json()) as {
    candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
  };
  const text = payload.candidates?.flatMap((candidate) => candidate.content?.parts || []).map((part) => part.text || "").join("\n");
  const parsed = parseImageVisionJson(text || "");
  if (!parsed) return null;
  return normalizeImageVisionFields(parsed, params.product, imageParts.map((item) => item.sourceUrl));
}

export function mergeShopifyDirectPdpImageVisionFields(
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  fields: ImageVisionPdpFields | null | undefined,
): Omit<ExtractResponse, "generated_at" | "logs"> {
  if (!response.products[0] || !hasDisplayableImageVisionFields(fields)) return response;

  const mergedProducts = response.products.map((product, idx) => {
    if (idx !== 0) return product;
    const baseSections = (Array.isArray(product.details_sections) ? product.details_sections : []).filter(
      (section) => !isTaxonomyOnlyDetailSection(section),
    );
    const visionSections = dedupeDetailSections(
      (fields?.detailsSections || []).map((section) => ({
        ...section,
        source_kind: "product_image_vision",
      })),
    );
    const mergedProduct: ExtractedProduct = {
      ...product,
      image_urls: [...product.image_urls],
      content_image_urls: dedupeStringList([...(product.content_image_urls || []), ...(fields?.contentImageUrls || [])]),
      variant_skus: [...product.variant_skus],
      variants: product.variants.map((variant) => ({
        ...variant,
        image_urls: [...variant.image_urls],
      })),
    };
    const useImageVisionDescription =
      cleanText(product.description_raw).length < PDP_COMPLETENESS_MIN_OVERVIEW_CHARS && Boolean(fields?.descriptionRaw);
    const useImageVisionIngredients = !product.ingredients_raw && Boolean(fields?.ingredientsRaw);
    const useImageVisionActiveIngredients = !product.active_ingredients_raw && Boolean(fields?.activeIngredientsRaw);
    const useImageVisionHowToUse = !product.how_to_use_raw && Boolean(fields?.howToUseRaw);
    Object.assign(
      mergedProduct,
      buildProductPdpFields({
        descriptionRaw:
          useImageVisionDescription ? fields?.descriptionRaw : product.description_raw,
        detailsSections: dedupeDetailSections([...baseSections, ...visionSections]),
        ingredientsRaw: useImageVisionIngredients ? fields?.ingredientsRaw : product.ingredients_raw,
        activeIngredientsRaw: useImageVisionActiveIngredients
          ? fields?.activeIngredientsRaw
          : product.active_ingredients_raw,
        howToUseRaw: useImageVisionHowToUse ? fields?.howToUseRaw : product.how_to_use_raw,
        faqItems: product.faq_items,
        fieldSources: {
          description_raw: [
            ...(product.field_sources?.description_raw || []),
            useImageVisionDescription ? "product_image_vision" : "",
          ],
          details_sections: [
            ...(product.field_sources?.details_sections || []),
            ...(visionSections.length > 0 ? ["product_image_vision"] : []),
          ],
          ingredients_raw: [
            ...(product.field_sources?.ingredients_raw || []),
            useImageVisionIngredients ? "product_image_vision" : "",
          ],
          active_ingredients_raw: [
            ...(product.field_sources?.active_ingredients_raw || []),
            useImageVisionActiveIngredients ? "product_image_vision" : "",
          ],
          how_to_use_raw: [
            ...(product.field_sources?.how_to_use_raw || []),
            useImageVisionHowToUse ? "product_image_vision" : "",
          ],
          faq_items: product.field_sources?.faq_items || [],
        },
      }),
    );
    return withProductPdpProfile(mergedProduct);
  });

  const { variants, adCopyById } = flattenVariants({
    brand: response.brand,
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

async function enrichShopifyDirectPdpWithImageVision(params: {
  brand: string;
  baseUrl: string;
  seedUrl: string;
  response: Omit<ExtractResponse, "generated_at" | "logs">;
  missingReasons: MissingPdpFieldReason[];
  log: Logger;
  imageVisionClient?: ShopifyImageVisionClient;
}) {
  const product = params.response.products[0];
  if (!product || !shouldAttemptShopifyImageVisionEnrichment(product)) return params.response;
  const imageUrls = getImageVisionCandidateUrls(product);
  if (!params.imageVisionClient && !getImageVisionApiKey()) {
    params.log(
      "warn",
      `Image-only Shopify PDP content gap; vision enrichment unavailable because Gemini API key is not configured: ${params.seedUrl}`,
    );
    return params.response;
  }

  const timeoutMs = clampIntShared(
    process.env.CATALOG_IMAGE_VISION_TIMEOUT_MS,
    DEFAULT_IMAGE_VISION_TIMEOUT_MS,
    5_000,
    180_000,
  );
  const client = params.imageVisionClient || extractShopifyImageVisionPdpFields;
  try {
    const fields = await withTimeoutShared(
      client({
        brand: params.brand,
        seedUrl: params.seedUrl,
        baseUrl: params.baseUrl,
        product,
        imageUrls,
        missingReasons: params.missingReasons,
      }),
      timeoutMs,
      "Shopify direct PDP image vision enrichment",
    );
    if (!hasDisplayableImageVisionFields(fields)) {
      params.log("warn", `Image-only Shopify PDP vision enrichment produced no displayable fields: ${params.seedUrl}`);
      return params.response;
    }
    const merged = mergeShopifyDirectPdpImageVisionFields(params.response, fields);
    params.log("success", `Recovered Shopify PDP content via product image vision: ${params.seedUrl}`);
    return merged;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error || "unknown_error");
    params.log("warn", `Image-only Shopify PDP vision enrichment failed; leaving PDP fields missing: ${msg}`);
    return params.response;
  }
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

function dedupeBy<T>(values: T[], keyFn: (value: T) => string) {
  const out: T[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const key = keyFn(value);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(value);
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

const SHOPIFY_IMAGE_CONTENT_HINT_FRAGMENTS = [
  "why_we_love",
  "why_it_works",
  "how_to",
  "how-to",
  "how_to_layer",
  "claim",
  "claims",
  "ingredient",
  "ingredients",
  "faq",
  "routine",
  "compare",
] as const;

const SHOPIFY_IMAGE_GENERIC_VISUAL_FRAGMENTS = [
  "packshot",
  "swatch",
  "hero",
  "model",
  "texture",
  "holding",
  "before_after",
  "before-after",
  "size_range",
  "whitebg",
] as const;

const SHOPIFY_IMAGE_GENERIC_SUPPORT_TOKENS = new Set([
  "claim",
  "claims",
  "ingredient",
  "ingredients",
  "how",
  "layer",
  "love",
  "works",
  "before",
  "after",
  "size",
  "range",
  "routine",
  "compare",
  "whitebg",
  "serum",
  "cream",
  "cleanser",
  "moisturizer",
  "moisturiser",
]);

const SHOPIFY_IMAGE_SIZE_SUFFIX_RE = /_(?:\d{2,5}x\d{0,5}(?:_crop_center)?|pico|icon|thumb|small|compact|medium|large|grande|master)$/i;

function tokenizeImageHints(values: Array<string | undefined | null>) {
  const tokens = new Set<string>();
  for (const value of values) {
    const decoded = safeDecodeURIComponent(String(value || "").toLowerCase());
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
  const haystack = safeDecodeURIComponent(url.toLowerCase());
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
      const queryWidth = Number(parsed.searchParams.get("width") || parsed.searchParams.get("w") || parsed.searchParams.get("sw") || 0);
      if (Number.isFinite(queryWidth) && queryWidth > 0) return queryWidth;
      const filename = parsed.pathname.split("/").pop() || "";
      const stem = filename.replace(/\.[a-z0-9]+$/i, "");
      const sizeMatch = stem.match(/_(\d{2,5})x\d{0,5}(?:_crop_center)?$/i);
      if (sizeMatch) {
        const pathWidth = Number(sizeMatch[1] || 0);
        return Number.isFinite(pathWidth) ? pathWidth : 0;
      }
      return 0;
    } catch {
      return 0;
    }
  };

  return readWidth(candidateUrl) >= readWidth(existingUrl) ? candidateUrl : existingUrl;
}

function canonicalizeShopifyImageUrl(url: string) {
  try {
    const parsed = new URL(url);
    parsed.searchParams.delete("width");
    parsed.searchParams.delete("w");
    parsed.searchParams.delete("sw");
    parsed.searchParams.delete("height");
    parsed.searchParams.delete("h");
    parsed.searchParams.delete("sh");

    const lastSlash = parsed.pathname.lastIndexOf("/");
    const dirname = lastSlash >= 0 ? parsed.pathname.slice(0, lastSlash + 1) : "";
    const filename = lastSlash >= 0 ? parsed.pathname.slice(lastSlash + 1) : parsed.pathname;
    const extIndex = filename.lastIndexOf(".");
    if (extIndex > 0) {
      const stem = filename.slice(0, extIndex);
      const ext = filename.slice(extIndex);
      let normalizedStem = stem;
      while (SHOPIFY_IMAGE_SIZE_SUFFIX_RE.test(normalizedStem)) {
        normalizedStem = normalizedStem.replace(SHOPIFY_IMAGE_SIZE_SUFFIX_RE, "");
      }
      let normalizedPath = `${dirname}${normalizedStem}${ext}`;
      const filesIndex = normalizedPath.lastIndexOf("/files/");
      const productsIndex = normalizedPath.lastIndexOf("/products/");
      if (filesIndex >= 0) {
        normalizedPath = normalizedPath.slice(filesIndex);
      } else if (productsIndex >= 0) {
        normalizedPath = normalizedPath.slice(productsIndex);
      }
      parsed.pathname = normalizedPath;
    }

    if (parsed.hostname.includes("shopify.com") || parsed.pathname.startsWith("/files/") || parsed.pathname.startsWith("/products/")) {
      parsed.protocol = "https:";
      parsed.hostname = "shopify-cdn.invalid";
      parsed.port = "";
    }

    return parsed.toString();
  } catch {
    return url;
  }
}

function getProductImageHintTokens(product: { title: string; url: string }) {
  const hintValues = [product.title];
  try {
    const parsed = new URL(product.url);
    hintValues.push(parsed.pathname, parsed.search);
  } catch {
    hintValues.push(product.url);
  }
  return tokenizeImageHints(hintValues);
}

function dedupeShopifyImageUrls(urls: string[]) {
  const bestByCanonical = new Map<string, string>();
  for (const candidate of urls) {
    const normalized = cleanText(candidate);
    if (!normalized) continue;
    const canonical = canonicalizeShopifyImageUrl(normalized);
    bestByCanonical.set(canonical, preferredImageVariant(bestByCanonical.get(canonical), normalized));
  }
  return Array.from(bestByCanonical.values());
}

function shouldRejectShopifyCrossProductImage(product: { title: string; url: string }, imageUrl: string) {
  const normalized = safeDecodeURIComponent(imageUrl.toLowerCase());
  const productTokens = getProductImageHintTokens(product);
  if (imageUrlMatchScore(imageUrl, productTokens) > 0) return false;

  const contentLike = SHOPIFY_IMAGE_CONTENT_HINT_FRAGMENTS.some((fragment) => normalized.includes(fragment));
  if (!contentLike) return false;

  if (SHOPIFY_IMAGE_GENERIC_VISUAL_FRAGMENTS.some((fragment) => normalized.includes(fragment))) return false;

  const informativeTokens = tokenizeImageHints([imageUrl]).filter((token) => !SHOPIFY_IMAGE_GENERIC_SUPPORT_TOKENS.has(token));
  if (informativeTokens.length === 0) return false;
  return true;
}

function filterShopifyProductImageUrls(product: { title: string; url: string }, urls: string[]) {
  const cleaned = dedupeShopifyImageUrls(urls);
  const filtered = cleaned.filter((candidate) => !shouldRejectShopifyCrossProductImage(product, candidate));
  return filtered.length > 0 ? filtered : cleaned;
}

function selectRelevantFallbackImageUrls(product: { title: string; url: string }, candidates: string[]) {
  const hintTokens = getProductImageHintTokens(product);
  if (hintTokens.length === 0) return [];

  const bestByCanonical = new Map<string, { url: string; score: number }>();
  for (const candidate of candidates) {
    const score = imageUrlMatchScore(candidate, hintTokens);
    if (score <= 0) continue;

    try {
      const canonical = canonicalizeShopifyImageUrl(candidate);
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

function isIdentityLikeVariantValue(value: string, sku: string) {
  const normalizedValue = cleanText(value).toLowerCase();
  const normalizedSku = cleanText(sku).toLowerCase();
  if (!normalizedValue) return true;
  if (normalizedSku && normalizedValue === normalizedSku) return true;
  if (/^(?:default|default title|title|variant|offer)$/i.test(normalizedValue)) return true;
  const compact = normalizedValue.replace(/[\s-]+/g, "");
  if (/^\d{8,14}$/.test(compact)) return true;
  return /^[a-z]{0,4}\d{6,}[a-z0-9-]*$/i.test(normalizedValue) && normalizedValue.length >= 8 && !/\s/.test(normalizedValue);
}

function finalizeExtractedVariants(variants: ExtractedVariant[]) {
  const prepared = (Array.isArray(variants) ? variants : []).map((variant) => {
    const hiddenFromSelector = isIdentityLikeVariantValue(variant.option_value, variant.sku);
    return {
      ...variant,
      source_origin: variant.source_origin || ("shopify_json" as const),
      source_quality_status: hiddenFromSelector ? ("quarantined" as const) : (variant.source_quality_status || "high"),
      hidden_from_selector: hiddenFromSelector,
      ad_copy: cleanText(variant.ad_copy),
    };
  });
  return prepared;
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
      if (cleanText(variant.ad_copy)) {
        adCopyById[variant.id] = variant.ad_copy;
      }
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

type ShopifySearchSuggestResponse = {
  resources?: {
    results?: {
      products?: ShopifySearchSuggestProduct[];
    };
  };
};

type ShopifySearchSuggestProduct = {
  title?: string;
  handle?: string;
  url?: string;
  available?: boolean;
};

type ShopifyProduct = {
  id: number;
  title: string;
  handle: string;
  type?: string;
  tags?: string[];
  description?: string;
  body_html?: string;
  content?: string;
  variants: ShopifyVariant[];
  options?: Array<{ name?: string }>;
  images?: Array<string | ShopifyImage>;
  featured_image?: string | ShopifyImage | null;
};

type ShopifyVariant = {
  id: number;
  sku?: string | null;
  title?: string;
  public_title?: string | null;
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
  context: { headers?: Record<string, string>; cookies?: Record<string, string> },
): Promise<ExtractedVariant["currency"] | null> {
  for (const candidate of urlCandidates) {
    const url = String(candidate || "").trim();
    if (!url) continue;
    const outcome = await fetchTextTracked(url, context, diagnostics);
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

function isGenericSingleShopifyVariant(product: ShopifyProduct, variant: ShopifyVariant): boolean {
  if (isDefaultShopifyVariant(variant)) return true;
  const normalizedTitle = normalizeProductIdentityText(product.title);
  if (!normalizedTitle) return false;
  const fields = [variant.title, variant.option1, variant.option2, variant.option3, variant.public_title]
    .map((value) => normalizeProductIdentityText(value || undefined))
    .filter(Boolean);
  return fields.length > 0 && fields.every((value) => value === normalizedTitle);
}

const SHOPIFY_SEARCH_STOP_TOKENS = new Set([
  "a",
  "an",
  "and",
  "by",
  "for",
  "in",
  "of",
  "the",
  "to",
  "with",
]);

function normalizeShopifySearchText(value: unknown): string {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/['’]/g, "")
    .replace(/&/g, " and ")
    .replace(/\b(?:new|limited edition)\b/g, " ")
    .replace(/\b\d+(?:\.\d+)?\s*(?:ml|oz|g|fl oz)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function shopifySearchTokens(value: unknown): string[] {
  return normalizeShopifySearchText(value)
    .split(" ")
    .filter((token) => token.length > 1 && !SHOPIFY_SEARCH_STOP_TOKENS.has(token));
}

function scoreShopifySearchSuggestion(candidate: ShopifySearchSuggestProduct, expectedTitle: string): number {
  const expectedNorm = normalizeShopifySearchText(expectedTitle);
  const candidateNorm = normalizeShopifySearchText(candidate.title);
  if (!expectedNorm || !candidateNorm) return 0;
  if (candidateNorm === expectedNorm) return 1;
  if (candidateNorm.includes(expectedNorm) || expectedNorm.includes(candidateNorm)) return 0.94;

  const expectedTokens = new Set(shopifySearchTokens(expectedTitle));
  const candidateTokens = new Set(shopifySearchTokens(candidate.title));
  if (expectedTokens.size === 0 || candidateTokens.size === 0) return 0;
  const overlap = Array.from(expectedTokens).filter((token) => candidateTokens.has(token)).length;
  const recall = overlap / expectedTokens.size;
  const precision = overlap / candidateTokens.size;
  return Math.min(recall, precision);
}

function extractShopifySuggestHandle(candidate: ShopifySearchSuggestProduct, baseUrl: string): string | undefined {
  const directHandle = String(candidate.handle || "").trim();
  if (directHandle) return directHandle;
  const candidateUrl = String(candidate.url || "").trim();
  return extractShopifyProductHandle(candidateUrl, baseUrl);
}

async function recoverShopifyDirectProductViaSearch(params: {
  baseUrl: string;
  productTitle?: string;
  context: FetchContext;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
}): Promise<ShopifyProduct | null> {
  const productTitle = cleanText(params.productTitle);
  if (!productTitle || productTitle.length < 3) return null;

  const searchUrl = `${params.baseUrl}/search/suggest.json?q=${encodeURIComponent(productTitle)}&resources[type]=product&resources[limit]=8`;
  params.log("info", `Searching Shopify products by title for stale direct PDP: ${productTitle}`);
  const suggestions = await fetchJsonTracked<ShopifySearchSuggestResponse>(searchUrl, params.context, params.diagnostics);
  const products = suggestions.data?.resources?.results?.products || [];
  if (products.length === 0) return null;

  const ranked = products
    .map((candidate) => ({
      candidate,
      score: scoreShopifySearchSuggestion(candidate, productTitle),
      handle: extractShopifySuggestHandle(candidate, params.baseUrl),
    }))
    .filter((entry) => entry.handle && entry.score >= 0.82)
    .sort((left, right) => right.score - left.score);

  const best = ranked[0];
  if (!best?.handle) {
    params.log("warn", `Shopify title search did not find a safe product match for: ${productTitle}`);
    return null;
  }

  const recoveredUrl = `${params.baseUrl}/products/${best.handle}.js`;
  params.log(
    "success",
    `Recovered stale Shopify PDP handle via title search: ${best.handle} (score=${best.score.toFixed(2)})`,
  );
  const recoveredProduct = await fetchJsonTracked<ShopifyProduct>(recoveredUrl, params.context, params.diagnostics);
  if (recoveredProduct.data && typeof recoveredProduct.data.id === "number") return recoveredProduct.data;
  return null;
}

async function recoverShopifyDirectProductViaExactFeedHandle(params: {
  baseUrl: string;
  directHandle: string;
  context: FetchContext;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
}): Promise<ShopifyProduct | null> {
  const expectedHandle = cleanText(params.directHandle).toLowerCase();
  if (!expectedHandle) return null;

  const maxPages = clampIntShared(process.env.SHOPIFY_MAX_PAGES, 20, 1, 200);
  params.log("info", `Searching Shopify products feed for exact direct PDP handle: ${expectedHandle}`);
  for (let page = 1; page <= maxPages; page += 1) {
    const url = `${params.baseUrl}/products.json?limit=250&page=${page}`;
    const batch = await fetchJsonTracked<ShopifyProductsResponse>(url, params.context, params.diagnostics);
    const products = batch.data?.products;
    if (!Array.isArray(products) || products.length === 0) break;
    const matched = products.find((product) => cleanText(product.handle).toLowerCase() === expectedHandle);
    if (matched && typeof matched.id === "number") {
      params.log("success", `Recovered Shopify direct PDP via exact products.json handle: ${expectedHandle}`);
      return matched;
    }
    if (products.length < 250) break;
  }
  params.log("warn", `Shopify products feed did not contain exact direct PDP handle: ${expectedHandle}`);
  return null;
}

async function tryExtractShopify(params: {
  brand: string;
  domain: string;
  baseUrl: string;
  marketId: ExtractInput["market"];
  seedUrl?: string;
  productTitle?: string;
  collectionHandle?: string;
  maxProducts: number;
  offset: number;
  limit: number;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
  browserRunner?: BrowserTaskRunner;
}): Promise<Omit<ExtractResponse, "generated_at" | "logs"> | null> {
  const log = params.log;
  const directHandles = buildShopifyDirectHandleCandidates(params.seedUrl, params.baseUrl, params.productTitle);
  const currencyHintUrls = dedupeStringList([params.seedUrl, params.baseUrl]);
  const marketProfile = getMarketProfile(normalizeMarketId(params.marketId));
  const shopifyContext = {
    headers: marketProfile.headers,
    cookies: marketProfile.cookies,
  };

  if (directHandles.length > 0) {
    for (const [index, directHandle] of directHandles.entries()) {
      const directUrl = `${params.baseUrl}/products/${directHandle}.js`;
      log("info", `Checking Shopify direct product feed: ${directUrl}`);
      const directProduct = await fetchJsonTracked<ShopifyProduct>(directUrl, shopifyContext, params.diagnostics!);
      if (!directProduct.data || typeof directProduct.data.id !== "number") continue;
      log(
        "success",
        index === 0
          ? `Shopify direct product detected for handle: ${directHandle}`
          : `Recovered Shopify direct product via canonical handle: ${directHandle}`,
      );
      setDiscoveryStrategy(params.diagnostics!, "shopify_json");
      const currencyHint = await fetchShopifyCurrencyHint(
        dedupeStringList([...currencyHintUrls, `${params.baseUrl}/products/${directHandle}`]),
        params.diagnostics!,
        shopifyContext,
      );
      const response = buildShopifyResponse({
        ...params,
        currencyHint,
        products: [directProduct.data],
        platformLabel: index === 0 ? "Shopify (Direct PDP)" : "Shopify (Direct PDP Canonical Repair)",
      });
      return enrichDirectShopifyPdpResponse({
        brand: params.brand,
        baseUrl: params.baseUrl,
        seedUrl: `${params.baseUrl}/products/${directHandle}`,
        response,
        diagnostics: params.diagnostics,
        log,
        context: shopifyContext,
      });
    }
    const directHandle = directHandles[0]!;
    const feedRecoveredProduct = await recoverShopifyDirectProductViaExactFeedHandle({
      baseUrl: params.baseUrl,
      directHandle,
      context: shopifyContext,
      diagnostics: params.diagnostics,
      log,
    });
    if (feedRecoveredProduct) {
      setDiscoveryStrategy(params.diagnostics!, "shopify_json");
      const currencyHint = await fetchShopifyCurrencyHint(currencyHintUrls, params.diagnostics!, shopifyContext);
      const response = buildShopifyResponse({
        ...params,
        currencyHint,
        products: [feedRecoveredProduct],
        platformLabel: "Shopify (Direct PDP Feed Repair)",
      });
      return enrichDirectShopifyPdpResponse({
        brand: params.brand,
        baseUrl: params.baseUrl,
        seedUrl: `${params.baseUrl}/products/${feedRecoveredProduct.handle}`,
        response,
        diagnostics: params.diagnostics,
        log,
        context: shopifyContext,
      });
    }
    const directSeedStatus = await classifyMissingShopifyDirectSeed({
      seedUrl: params.seedUrl,
      baseUrl: params.baseUrl,
      context: shopifyContext,
      diagnostics: params.diagnostics,
    });
      if (directSeedStatus === "not_found" || directSeedStatus === "non_product_redirect") {
        const recoveredProduct = await recoverShopifyDirectProductViaSearch({
          baseUrl: params.baseUrl,
        productTitle: params.productTitle,
        context: shopifyContext,
        diagnostics: params.diagnostics,
        log,
      });
      if (recoveredProduct) {
        setDiscoveryStrategy(params.diagnostics!, "shopify_json");
        const currencyHint = await fetchShopifyCurrencyHint(currencyHintUrls, params.diagnostics!, shopifyContext);
        const response = buildShopifyResponse({
          ...params,
          currencyHint,
          products: [recoveredProduct],
          platformLabel: "Shopify (Direct PDP Search Repair)",
        });
        return enrichDirectShopifyPdpResponse({
          brand: params.brand,
          baseUrl: params.baseUrl,
          seedUrl: `${params.baseUrl}/products/${recoveredProduct.handle}`,
          response,
          diagnostics: params.diagnostics,
          log,
          context: shopifyContext,
          });
        }
        const browserRecoveredProduct = await recoverShopifyDirectProductViaBrowser({
          brand: params.brand,
          baseUrl: params.baseUrl,
          seedUrl: params.seedUrl,
          directHandle,
          productTitle: params.productTitle,
          context: shopifyContext,
          diagnostics: params.diagnostics,
          log,
          browserRunner: params.browserRunner,
        });
        if (browserRecoveredProduct) {
          setDiscoveryStrategy(params.diagnostics!, "managed_browser");
          return buildDirectRecoveredShopifyBrowserResponse({
            ...params,
            platformLabel: "Shopify (Direct PDP Browser Recovery)",
            product: browserRecoveredProduct,
          });
        }
        log(
          "warn",
          `Shopify direct product feed not found for handle: ${directHandle}; seed status=${directSeedStatus}. Skipping generic rediscovery.`,
      );
      setDiscoveryStrategy(params.diagnostics!, "shopify_json");
      if (!params.diagnostics.failure_category) {
        setFailureCategory(params.diagnostics, "no_product_urls");
      }
      return buildEmptyShopifyDirectPdpResponse(params, "Shopify (Direct PDP)");
    }

    log("warn", `Shopify direct product feed not found for handle: ${directHandle}. Falling back to direct page discovery.`);
  return null;
}

function buildDirectRecoveredShopifyBrowserResponse(params: {
  brand: string;
  domain: string;
  offset: number;
  limit: number;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  platformLabel: string;
  product: ExtractedProduct;
}): Omit<ExtractResponse, "generated_at" | "logs"> {
  const products = [withProductPdpProfile(params.product)];
  const { variants, adCopyById } = flattenVariants({
    brand: params.brand,
    products,
    simulated: false,
  });
  return {
    brand: params.brand,
    domain: params.domain,
    mode: "puppeteer" as const,
    platform: params.platformLabel,
    products,
    variants,
    pricing: computePricingStats(variants),
    ad_copy: { by_variant_id: adCopyById },
    pagination: {
      offset: params.offset,
      limit: params.limit,
      next_offset: null,
      has_more: false,
      discovered_urls: 1,
    },
    diagnostics: params.diagnostics,
  };
}

  const probeUrl = params.collectionHandle
    ? `${params.baseUrl}/collections/${params.collectionHandle}/products.json?limit=1`
    : `${params.baseUrl}/products.json?limit=1`;

  log("info", `Checking Shopify feed: ${probeUrl}`);
  const probe = await fetchJsonTracked<ShopifyProductsResponse>(probeUrl, shopifyContext, params.diagnostics!);
  if (!probe.data || !Array.isArray(probe.data.products)) {
    log("warn", "Shopify feed not found.");
    return null;
  }

  log("success", "Shopify feed detected.");
  setDiscoveryStrategy(params.diagnostics!, "shopify_json");
  const currencyHint = await fetchShopifyCurrencyHint(currencyHintUrls, params.diagnostics!, shopifyContext);

  const allProducts: ShopifyProduct[] = [];
  const maxPages = clampIntShared(process.env.SHOPIFY_MAX_PAGES, 20, 1, 200);
  const feedPrefix = params.collectionHandle ? `/collections/${params.collectionHandle}` : "";

  for (let page = 1; page <= maxPages; page++) {
    const url = `${params.baseUrl}${feedPrefix}/products.json?limit=250&page=${page}`;
    const batch = await fetchJsonTracked<ShopifyProductsResponse>(url, shopifyContext, params.diagnostics!);
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

export async function enrichDirectShopifyPdpResponse(params: {
  brand: string;
  baseUrl: string;
  seedUrl?: string;
  response: Omit<ExtractResponse, "generated_at" | "logs">;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
  context?: FetchContext;
  browserRunner?: typeof runBrowserTaskWithFallback<ExtractedProduct | null>;
  imageVisionClient?: ShopifyImageVisionClient;
}): Promise<Omit<ExtractResponse, "generated_at" | "logs">> {
  const product = params.response.products[0];
  if (!params.seedUrl || !product || params.response.products.length !== 1) return params.response;
  let response = params.response;
  let pageHtml: string | undefined;
  let pageHtmlFetched = false;

  const fetchSeedPageHtml = async () => {
    if (pageHtmlFetched) return pageHtml;
    pageHtmlFetched = true;
    try {
      const pageOutcome = await fetchTextTracked(params.seedUrl!, withBrowserishHtmlHeaders(params.context || {}), params.diagnostics);
      if (isNonProductRedirectForRequestedPdp(params.seedUrl!, pageOutcome.finalUrl, params.baseUrl)) {
        params.log(
          "warn",
          `Discarding Shopify direct PDP HTML after non-product redirect: ${params.seedUrl} -> ${pageOutcome.finalUrl}`,
        );
        pageHtml = undefined;
        return pageHtml;
      }
      pageHtml = pageOutcome.body || undefined;
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error || "unknown_error");
      params.log("warn", `Shopify direct PDP HTML fetch failed; continuing without embedded PDP recovery: ${msg}`);
    }
    return pageHtml;
  };

  const faqMissing = !Array.isArray(product.faq_items) || product.faq_items.length === 0;
  const reviewSummaryPreviewMissing =
    !Array.isArray(product.review_summary?.preview_items) || product.review_summary.preview_items.length === 0;
  const reviewSummaryAggregateMissing =
    !(Number(product.review_summary?.review_count || 0) > 0 && Number(product.review_summary?.rating || 0) > 0);
  const reviewSummaryMissing = !product.review_summary || reviewSummaryPreviewMissing || reviewSummaryAggregateMissing;
  if (faqMissing || reviewSummaryMissing) {
    try {
      const html = await fetchSeedPageHtml();
      const okendoMetafieldJson = extractOkendoMetafieldJsonFromHtml(html);
      const faqItems = faqMissing ? await fetchOkendoFaqItemsFromMetafieldJson(okendoMetafieldJson, params.seedUrl) : [];
      const reviewSummary = reviewSummaryMissing
        ? await fetchOkendoReviewSummaryFromMetafieldJson(okendoMetafieldJson, params.seedUrl)
        : null;
      if (faqItems.length > 0) {
        response = mergeShopifyDirectPdpFaqFallback(response, faqItems);
        params.log("success", `Recovered ${faqItems.length} Shopify PDP FAQ items via Okendo questions: ${params.seedUrl}`);
      }
      if (reviewSummary) {
        response = mergeShopifyDirectPdpReviewSummaryFallback(response, reviewSummary);
        const previewCount = Array.isArray(reviewSummary.preview_items) ? reviewSummary.preview_items.length : 0;
        params.log(
          "success",
          previewCount > 0
            ? `Recovered ${previewCount} Shopify PDP merchant review previews via Okendo reviews: ${params.seedUrl}`
            : `Recovered Shopify PDP merchant review aggregate via Okendo reviews: ${params.seedUrl}`,
        );
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error || "unknown_error");
      params.log("warn", `Shopify direct PDP Okendo enrichment failed; continuing without FAQ/review recovery: ${msg}`);
    }
  }

  const productBeforeEmbeddedMerge = response.products[0];
  if (productBeforeEmbeddedMerge) {
    const missingBefore = getMissingPdpFieldReasons(productBeforeEmbeddedMerge);
    const productMissingImagesBefore = productBeforeEmbeddedMerge.image_urls.length === 0;
    const variantMissingImagesBefore = productBeforeEmbeddedMerge.variants.some((variant) => variant.image_urls.length === 0);
    if (missingBefore.length > 0 || productMissingImagesBefore || variantMissingImagesBefore) {
      const html = await fetchSeedPageHtml();
      if (html) {
        const merged = mergeShopifyDirectPdpEmbeddedProductJson(response, html);
        const productAfter = merged.products[0];
        const missingAfter = productAfter ? getMissingPdpFieldReasons(productAfter) : missingBefore;
        const beforeSignal = [
          productBeforeEmbeddedMerge.image_urls.length,
          productBeforeEmbeddedMerge.details_sections?.length || 0,
          productBeforeEmbeddedMerge.description_raw ? 1 : 0,
          missingBefore.join(","),
        ].join("|");
        const afterSignal = productAfter
          ? [
              productAfter.image_urls.length,
              productAfter.details_sections?.length || 0,
              productAfter.description_raw ? 1 : 0,
              missingAfter.join(","),
            ].join("|")
          : beforeSignal;
        response = merged;
        if (afterSignal !== beforeSignal) {
          params.log("success", `Recovered Shopify PDP fields via embedded product-json: ${params.seedUrl}`);
        }
      }
    }
  }

  const productBeforeOfficialHtmlMerge = response.products[0];
  if (productBeforeOfficialHtmlMerge) {
    const missingBefore = getMissingPdpFieldReasons(productBeforeOfficialHtmlMerge);
    if (missingBefore.includes("ingredients") || missingBefore.includes("how_to_use")) {
      const html = await fetchSeedPageHtml();
      if (html) {
        const htmlPdpFields = extractShopifyDirectPdpHtmlPdpFields(html);
        if (htmlPdpFields.ingredientsRaw || htmlPdpFields.activeIngredientsRaw || htmlPdpFields.howToUseRaw) {
          const mergedPdpFields = buildProductPdpFields({
            descriptionRaw: productBeforeOfficialHtmlMerge.description_raw,
            detailsSections: [
              ...((Array.isArray(productBeforeOfficialHtmlMerge.details_sections)
                ? productBeforeOfficialHtmlMerge.details_sections
                : []) || []),
              ...htmlPdpFields.detailsSections,
            ],
            ingredientsRaw: productBeforeOfficialHtmlMerge.ingredients_raw || htmlPdpFields.ingredientsRaw,
            activeIngredientsRaw:
              productBeforeOfficialHtmlMerge.active_ingredients_raw || htmlPdpFields.activeIngredientsRaw,
            howToUseRaw: productBeforeOfficialHtmlMerge.how_to_use_raw || htmlPdpFields.howToUseRaw,
            faqItems: productBeforeOfficialHtmlMerge.faq_items,
            fieldSources: {
              description_raw: productBeforeOfficialHtmlMerge.field_sources?.description_raw || [],
              details_sections: [
                ...(productBeforeOfficialHtmlMerge.field_sources?.details_sections || []),
                ...(htmlPdpFields.detailsSections.length > 0 ? ["shopify_direct_pdp_html_labeled_sections"] : []),
              ],
              ingredients_raw: [
                ...(productBeforeOfficialHtmlMerge.field_sources?.ingredients_raw || []),
                ...(htmlPdpFields.ingredientsRaw ? ["shopify_direct_pdp_html_labeled_ingredients"] : []),
              ],
              active_ingredients_raw: [
                ...(productBeforeOfficialHtmlMerge.field_sources?.active_ingredients_raw || []),
                ...(htmlPdpFields.activeIngredientsRaw ? ["shopify_direct_pdp_html_labeled_active_ingredients"] : []),
              ],
              how_to_use_raw: [
                ...(productBeforeOfficialHtmlMerge.field_sources?.how_to_use_raw || []),
                ...(htmlPdpFields.howToUseRaw ? ["shopify_direct_pdp_html_labeled_how_to_use"] : []),
              ],
              faq_items: productBeforeOfficialHtmlMerge.field_sources?.faq_items || [],
            },
          });
          const products = response.products.map((item, index) =>
            index === 0
              ? {
                  ...item,
                  ...mergedPdpFields,
                }
              : item,
          );
          response = {
            ...response,
            products,
          };
          params.log("success", `Recovered Shopify PDP official HTML fields: ${params.seedUrl}`);
        }
      }
    }
  }

  response = applyReviewedVariantOverride(response, params.seedUrl, params.log);

  const currentProduct = response.products[0];
  if (!currentProduct) return response;
  const productMissingImages = currentProduct.image_urls.length === 0;
  const variantMissingImages = currentProduct.variants.some((variant) => variant.image_urls.length === 0);
  const missingPdpFieldReasons = getMissingPdpFieldReasons(currentProduct);
  const pageHtmlForSignals =
    missingPdpFieldReasons.length > 0 ||
    currentProduct.image_urls.length <= 1 ||
    currentProduct.variants.every((variant) => variant.image_urls.length <= 1) ||
    !Array.isArray(currentProduct.details_sections) ||
    currentProduct.details_sections.length === 0 ||
    !Array.isArray(currentProduct.faq_items) ||
    currentProduct.faq_items.length === 0 ||
    !Array.isArray(currentProduct.content_image_urls) ||
    currentProduct.content_image_urls.length === 0
      ? await fetchSeedPageHtml()
      : undefined;
  const htmlSignals = inspectShopifyDirectPdpHtmlSignals(pageHtmlForSignals, params.baseUrl, currentProduct);
  const thinPdpReasons = getShopifyDirectPdpThinReasons(currentProduct, htmlSignals);
  const detailsSections = Array.isArray(currentProduct.details_sections) ? currentProduct.details_sections : [];
  const onlyTaxonomyDetails =
    detailsSections.length > 0 && detailsSections.every((section) => isTaxonomyOnlyDetailSection(section));
  const candidateImageUrls = getImageVisionCandidateUrls(currentProduct);
  const canAttemptImageOnlyRecovery =
    shouldAttemptShopifyImageVisionEnrichment(currentProduct) &&
    candidateImageUrls.length >= 3 &&
    (detailsSections.length === 0 || onlyTaxonomyDetails);
  const tryImageVisionEnrichment = async (fallbackResponse: Omit<ExtractResponse, "generated_at" | "logs">) => {
    const productForVision = fallbackResponse.products[0];
    return enrichShopifyDirectPdpWithImageVision({
      brand: params.brand,
      baseUrl: params.baseUrl,
      seedUrl: params.seedUrl!,
      response: fallbackResponse,
      missingReasons: productForVision ? getMissingPdpFieldReasons(productForVision) : missingPdpFieldReasons,
      log: params.log,
      imageVisionClient: params.imageVisionClient,
    });
  };
  const enrichmentReasons = [
    productMissingImages ? "product_images" : "",
    variantMissingImages ? "variant_images" : "",
    ...thinPdpReasons.map((reason) => `pdp_${reason}`),
  ].filter(Boolean);
  if (enrichmentReasons.length === 0) {
    if (canAttemptImageOnlyRecovery) {
      params.log(
        "info",
        `Shopify direct PDP requires image-only enrichment for missing structured PDP fields (product_kind=${currentProduct.product_kind || classifyExtractedProductKind(currentProduct)}): ${params.seedUrl}`,
      );
      return tryImageVisionEnrichment(response);
    }
    return response;
  }

  params.log(
    "info",
    `Shopify direct PDP requires browser enrichment for ${enrichmentReasons.join(", ")} (product_kind=${currentProduct.product_kind || classifyExtractedProductKind(currentProduct)}): ${params.seedUrl}`,
  );

  const navigationTimeoutMs = clampIntShared(
    process.env.PUPPETEER_DIRECT_PDP_ENRICH_NAV_TIMEOUT_MS,
    Math.max(DEFAULT_NAV_TIMEOUT_MS, 15_000),
    5_000,
    120_000,
  );
  const scrapeTimeoutMs = clampIntShared(process.env.PUPPETEER_SCRAPE_TIMEOUT_MS, DEFAULT_SCRAPE_TIMEOUT_MS, 10_000, 300_000);

  const browserRunner = params.browserRunner || runBrowserTaskWithFallback;
  let browserRun: Awaited<ReturnType<typeof runBrowserTaskWithFallback<ExtractedProduct | null>>>;
  try {
    browserRun = await browserRunner(
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
            context: params.context || {},
          }),
          scrapeTimeoutMs,
          "Shopify direct PDP image enrichment",
        ),
      { diagnostics: params.diagnostics, log: params.log },
    );
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error || "unknown_error");
    params.log("warn", `Browser enrichment failed for Shopify PDP; trying image-only PDP enrichment before returning direct feed response: ${msg}`);
    return tryImageVisionEnrichment(response);
  }

  if (!browserRun.result) {
    params.log("warn", `Browser enrichment did not recover images for Shopify PDP; trying image-only PDP enrichment: ${params.seedUrl}`);
    return tryImageVisionEnrichment(response);
  }

  if (!isShopifyDirectPdpFallbackUsable(currentProduct, browserRun.result)) {
    params.log(
      "warn",
      `Discarding Shopify PDP browser enrichment because fallback identity did not match direct feed product; trying image-only PDP enrichment: ${params.seedUrl}`,
    );
    return tryImageVisionEnrichment(response);
  }

  const merged = mergeShopifyDirectPdpFallback(params.brand, response, browserRun.result, {
    preservePdpFieldSourceKinds: true,
  });
  if ((merged.products[0]?.image_urls.length || 0) > (product.image_urls.length || 0)) {
    params.log(
      "success",
      `Recovered ${merged.products[0]?.image_urls.length || 0} Shopify PDP images via browser enrichment: ${params.seedUrl}`,
    );
  }
  if (merged.products[0] && shouldAttemptShopifyImageVisionEnrichment(merged.products[0])) {
    return tryImageVisionEnrichment(merged);
  }
  return merged;
}

async function recoverShopifyDirectProductViaBrowser(params: {
  brand: string;
  baseUrl: string;
  seedUrl?: string;
  directHandle: string;
  productTitle?: string;
  context: FetchContext;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
  log: Logger;
  browserRunner?: BrowserTaskRunner;
}): Promise<ExtractedProduct | null> {
  if (!params.seedUrl) return null;

  const navigationTimeoutMs = clampIntShared(
    process.env.PUPPETEER_DIRECT_PDP_ENRICH_NAV_TIMEOUT_MS,
    Math.max(DEFAULT_NAV_TIMEOUT_MS, 15_000),
    5_000,
    120_000,
  );
  const scrapeTimeoutMs = clampIntShared(process.env.PUPPETEER_SCRAPE_TIMEOUT_MS, DEFAULT_SCRAPE_TIMEOUT_MS, 10_000, 300_000);

  try {
    const browserRunner = params.browserRunner || runBrowserTaskWithFallback;
    const browserRun = await browserRunner(
      async (browser) =>
        withTimeoutShared(
          scrapeProductPage({
            browser,
            url: params.seedUrl!,
            baseUrl: params.baseUrl,
            navigationTimeoutMs,
            verbose: false,
            log: params.log,
            diagnostics: params.diagnostics,
            context: params.context,
          }),
          scrapeTimeoutMs,
          "Shopify direct PDP browser recovery",
        ),
      { diagnostics: params.diagnostics, log: params.log },
    );
    const recovered = browserRun.result;
    if (!recovered) return null;
    if (!isUsableRecoveredShopifyDirectBrowserProduct(recovered, params.productTitle, params.directHandle, params.seedUrl, params.baseUrl)) {
      params.log("warn", `Discarding Shopify direct PDP browser recovery because recovered page identity did not match: ${params.seedUrl}`);
      return null;
    }
    params.log("success", `Recovered Shopify direct PDP via browser scrape: ${params.seedUrl}`);
    return recovered;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error || "unknown_error");
    params.log("warn", `Shopify direct PDP browser recovery failed; continuing without browser recovery: ${msg}`);
    return null;
  }
}

function extractShopifyProductHandle(seedUrl: string | undefined, baseUrl: string): string | undefined {
  if (!seedUrl) return undefined;
  try {
    const parsed = new URL(seedUrl, baseUrl);
    const match = parsed.pathname.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products?\/([^/?#]+)/i);
    return match?.[1] ? decodeURIComponent(match[1]) : undefined;
  } catch {
    return undefined;
  }
}

const SHOPIFY_DUPLICATE_COPY_HANDLE_SUFFIX_RE = /-copy(?:-\d+)?$/i;
const SHOPIFY_DUPLICATE_COUNTER_HANDLE_SUFFIX_RE = /-(\d{1,2})$/;

function stripShopifyDuplicateHandleSuffix(handle: string, productTitle?: string): string | null {
  const normalized = cleanText(handle).toLowerCase();
  if (!normalized) return null;

  const withoutCopySuffix = normalized.replace(SHOPIFY_DUPLICATE_COPY_HANDLE_SUFFIX_RE, "");
  if (withoutCopySuffix && withoutCopySuffix !== normalized) return withoutCopySuffix;

  const counterMatch = normalized.match(SHOPIFY_DUPLICATE_COUNTER_HANDLE_SUFFIX_RE);
  if (!counterMatch) return null;
  const baseHandle = normalized.slice(0, -counterMatch[0].length);
  if (!baseHandle) return null;

  const titleTokens = new Set(shopifySearchTokens(productTitle));
  const handleTokens = new Set(shopifySearchTokens(baseHandle.replace(/-/g, " ")));
  if (titleTokens.size === 0 || handleTokens.size === 0) return baseHandle;
  const overlap = Array.from(handleTokens).filter((token) => titleTokens.has(token)).length;
  return overlap >= Math.max(2, Math.ceil(handleTokens.size * 0.5)) ? baseHandle : null;
}

function buildShopifyDirectHandleCandidates(
  seedUrl: string | undefined,
  baseUrl: string,
  productTitle?: string,
): string[] {
  const directHandle = extractShopifyProductHandle(seedUrl, baseUrl);
  if (!directHandle) return [];
  const canonicalHandle = stripShopifyDuplicateHandleSuffix(directHandle, productTitle);
  return dedupeStringList([directHandle, canonicalHandle]);
}

export function isNonProductRedirectForRequestedPdp(requestedUrl: string, finalUrl: string, baseUrl: string): boolean {
  return isLikelyProductUrlShared(requestedUrl, baseUrl) && !isLikelyProductUrlShared(finalUrl, baseUrl);
}

function isUsableRecoveredShopifyDirectBrowserProduct(
  recovered: ExtractedProduct,
  requestedTitle: string | undefined,
  directHandle: string,
  seedUrl: string,
  baseUrl: string,
): boolean {
  if (!cleanText(recovered.title)) return false;
  const recoveredUrl = cleanText(recovered.url || seedUrl);
  if (!isLikelyProductUrlShared(recoveredUrl, baseUrl)) return false;
  if (recovered.variants.length === 0 && recovered.image_urls.length === 0 && !recovered.description_raw) return false;

  const recoveredTitle = normalizeProductIdentityText(recovered.title);
  const expectedTitle = normalizeProductIdentityText(requestedTitle);
  if (expectedTitle) {
    if (recoveredTitle === expectedTitle || recoveredTitle.includes(expectedTitle) || expectedTitle.includes(recoveredTitle)) {
      return true;
    }
    const recoveredTokens = productIdentityTokens(recovered.title);
    const expectedTokens = productIdentityTokens(requestedTitle);
    let overlap = 0;
    for (const token of recoveredTokens) {
      if (expectedTokens.has(token)) overlap += 1;
    }
    if (overlap >= Math.min(2, expectedTokens.size || 0)) return true;
  }

  const handleTokens = new Set(
    directHandle
      .toLowerCase()
      .split(/[^a-z0-9]+/i)
      .map((token) => token.trim())
      .filter((token) => token.length > 2),
  );
  const recoveredTokens = productIdentityTokens(recovered.title);
  let handleOverlap = 0;
  for (const token of recoveredTokens) {
    if (handleTokens.has(token)) handleOverlap += 1;
  }
  return handleOverlap >= Math.min(2, handleTokens.size || 0);
}

function normalizeProductIdentityText(value: string | undefined): string {
  return cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function productIdentityTokens(value: string | undefined): Set<string> {
  const stopwords = new Set([
    "the",
    "and",
    "for",
    "with",
    "skin",
    "care",
    "beauty",
    "skincare",
    "makeup",
  ]);
  return new Set(
    normalizeProductIdentityText(value)
      .split(" ")
      .filter((token) => token.length > 2 && !stopwords.has(token)),
  );
}

function extractProductSkus(product: ExtractedProduct): Set<string> {
  return new Set(
    [
      ...(product.variant_skus || []),
      ...(product.variants || []).flatMap((variant) => [variant.sku, variant.id]),
    ]
      .map((value) => normalizeProductIdentityText(value))
      .filter(Boolean),
  );
}

export function isShopifyDirectPdpFallbackUsable(primaryProduct: ExtractedProduct, fallbackProduct: ExtractedProduct): boolean {
  const primarySkus = extractProductSkus(primaryProduct);
  const fallbackSkus = extractProductSkus(fallbackProduct);
  for (const sku of fallbackSkus) {
    if (primarySkus.has(sku)) return true;
  }

  const primaryTitle = normalizeProductIdentityText(primaryProduct.title);
  const fallbackTitle = normalizeProductIdentityText(fallbackProduct.title);
  if (!primaryTitle || !fallbackTitle) return false;
  if (primaryTitle === fallbackTitle || primaryTitle.includes(fallbackTitle) || fallbackTitle.includes(primaryTitle)) {
    return true;
  }

  const primaryTokens = productIdentityTokens(primaryProduct.title);
  const fallbackTokens = productIdentityTokens(fallbackProduct.title);
  const minimumComparableTokens = Math.min(primaryTokens.size, fallbackTokens.size);
  if (minimumComparableTokens === 0) return false;
  const overlap = [...fallbackTokens].filter((token) => primaryTokens.has(token)).length;
  return overlap >= Math.min(2, minimumComparableTokens) && overlap / minimumComparableTokens >= 0.6;
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
      const isSingleDefault =
        (p.variants || []).length === 1 && isGenericSingleShopifyVariant(p, p.variants[0]!);
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
      Boolean(titleSplit) &&
      (product.variants || []).length === 1 &&
      isGenericSingleShopifyVariant(product, product.variants[0]!);
    const officialText = product.body_html || product.description || product.content;
    const singleDefaultSizeEvidence =
      !treatAsPseudoVariant &&
      (product.variants || []).length === 1 &&
      isGenericSingleShopifyVariant(product, product.variants[0]!)
        ? extractProductSizeEvidence(product.title, product.handle, productUrl, officialText, ...productImageUrls)
        : { optionValue: "" };

    const canonicalProductTitle = treatAsPseudoVariant ? titleSplit!.baseTitle : product.title;
    const optionName = treatAsPseudoVariant
      ? "Variant"
      : singleDefaultSizeEvidence.optionValue
        ? "Size"
      : product.options?.map((o) => o.name).filter((n): n is string => Boolean(n && n.trim())).join(" / ") || "Variant";
    const officialTextSource = product.body_html
      ? "shopify_body_html"
      : product.description
        ? "shopify_description"
        : product.content
          ? "shopify_content"
          : "";
    const currency = params.currencyHint || "USD";
    const bodyHtmlPdpFields = extractShopifyBodyHtmlPdpFields(officialText);
    const tagDetailSections = extractPayloadTagDetailSections([
      {
        tags: [
          ...(Array.isArray(product.tags) ? product.tags : []),
          ...(typeof product.type === "string" && product.type.trim() ? [`Product Type:${product.type}`] : []),
        ],
      },
    ]);
    const directDetailsSections = dedupeDetailSections([
      ...bodyHtmlPdpFields.detailsSections,
      ...tagDetailSections,
    ]);
    const productPdpFields = buildProductPdpFields({
      descriptionRaw: officialText,
      detailsSections: directDetailsSections,
      ingredientsRaw: bodyHtmlPdpFields.ingredientsRaw,
      activeIngredientsRaw: bodyHtmlPdpFields.activeIngredientsRaw,
      howToUseRaw: bodyHtmlPdpFields.howToUseRaw,
      faqItems: [],
      fieldSources: {
        description_raw: officialTextSource ? [officialTextSource] : [],
        details_sections: [
          ...(bodyHtmlPdpFields.detailsSections.length > 0 ? ["shopify_body_html_labeled_sections"] : []),
          ...(tagDetailSections.length > 0 ? ["shopify_product_tags"] : []),
        ],
        ingredients_raw: bodyHtmlPdpFields.ingredientsRaw ? ["shopify_body_html_labeled_ingredients"] : [],
        active_ingredients_raw: bodyHtmlPdpFields.activeIngredientsRaw ? ["shopify_body_html_labeled_active_ingredients"] : [],
        how_to_use_raw: bodyHtmlPdpFields.howToUseRaw ? ["shopify_body_html_labeled_how_to_use"] : [],
        faq_items: [],
      },
    });

    const extractedVariants: ExtractedVariant[] = finalizeExtractedVariants((product.variants || []).map((v) => {
      const optionValue = treatAsPseudoVariant
        ? titleSplit!.variantLabel
        : singleDefaultSizeEvidence.optionValue
          ? singleDefaultSizeEvidence.optionValue
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
    }));

    const existing: ExtractedProduct =
      extractedByTitle.get(canonicalProductTitle) ||
      {
        title: canonicalProductTitle,
        url: productUrl,
        image_url: productImageUrls[0] || "",
        image_urls: productImageUrls,
        ...(singleDefaultSizeEvidence.optionValue ? { volume: singleDefaultSizeEvidence.optionValue } : {}),
        ...(singleDefaultSizeEvidence.alternateOptionValue
          ? { product_volume: singleDefaultSizeEvidence.alternateOptionValue }
          : {}),
        ...(singleDefaultSizeEvidence.detailLabel ? { size_detail_label: singleDefaultSizeEvidence.detailLabel } : {}),
        variant_skus: [],
        variants: [],
        ...productPdpFields,
      };

    const seenVariants = new Set(existing.variants.map((variant) => `${variant.id}|${variant.sku}|${variant.url}`));
    for (const variant of extractedVariants) {
      const key = `${variant.id}|${variant.sku}|${variant.url}`;
      if (seenVariants.has(key)) continue;
      seenVariants.add(key);
      existing.variants.push(variant);
    }
    existing.image_urls = dedupeShopifyImageUrls([
      ...existing.image_urls,
      ...productImageUrls,
      ...extractedVariants.flatMap((variant) => variant.image_urls),
    ]);
    existing.image_url = existing.image_urls[0] || existing.image_url || "";
    existing.variant_skus = dedupeStringList([...existing.variant_skus, ...extractedVariants.map((variant) => variant.sku)]);
    if (singleDefaultSizeEvidence.optionValue && !existing.volume) {
      existing.volume = singleDefaultSizeEvidence.optionValue;
    }
    if (singleDefaultSizeEvidence.alternateOptionValue && !existing.product_volume) {
      existing.product_volume = singleDefaultSizeEvidence.alternateOptionValue;
    }
    if (singleDefaultSizeEvidence.detailLabel && !existing.size_detail_label) {
      existing.size_detail_label = singleDefaultSizeEvidence.detailLabel;
    }
    const mergedPdpFields = buildProductPdpFields({
      descriptionRaw: existing.description_raw || productPdpFields.description_raw,
      detailsSections:
        (Array.isArray(existing.details_sections) && existing.details_sections.length > 0)
          ? existing.details_sections
          : productPdpFields.details_sections,
      ingredientsRaw: existing.ingredients_raw || productPdpFields.ingredients_raw,
      activeIngredientsRaw: existing.active_ingredients_raw || productPdpFields.active_ingredients_raw,
      howToUseRaw: existing.how_to_use_raw || productPdpFields.how_to_use_raw,
      faqItems:
        (Array.isArray(existing.faq_items) && existing.faq_items.length > 0)
          ? existing.faq_items
          : productPdpFields.faq_items,
      fieldSources: {
        description_raw: [
          ...(existing.field_sources?.description_raw || []),
          ...(productPdpFields.field_sources?.description_raw || []),
        ],
        details_sections: [
          ...(existing.field_sources?.details_sections || []),
          ...(productPdpFields.field_sources?.details_sections || []),
        ],
        ingredients_raw: [
          ...(existing.field_sources?.ingredients_raw || []),
          ...(productPdpFields.field_sources?.ingredients_raw || []),
        ],
        active_ingredients_raw: [
          ...(existing.field_sources?.active_ingredients_raw || []),
          ...(productPdpFields.field_sources?.active_ingredients_raw || []),
        ],
        how_to_use_raw: [
          ...(existing.field_sources?.how_to_use_raw || []),
          ...(productPdpFields.field_sources?.how_to_use_raw || []),
        ],
        faq_items: [
          ...(existing.field_sources?.faq_items || []),
          ...(productPdpFields.field_sources?.faq_items || []),
        ],
      },
    });
    Object.assign(existing, mergedPdpFields);
    extractedByTitle.set(canonicalProductTitle, existing);
  }

  const extractedProducts = Array.from(extractedByTitle.values()).map(withProductPdpProfile);

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

type MissingShopifyDirectSeedStatus = "product_page" | "not_found" | "non_product_redirect" | "unknown";

async function classifyMissingShopifyDirectSeed(params: {
  seedUrl?: string;
  baseUrl: string;
  context: FetchContext;
  diagnostics: NonNullable<ExtractResponse["diagnostics"]>;
}): Promise<MissingShopifyDirectSeedStatus> {
  if (!params.seedUrl) return "unknown";

  const seed = await fetchTextTracked(params.seedUrl, params.context, params.diagnostics);
  const requestedSeedUrl = canonicalizeUrlShared(params.seedUrl, params.baseUrl);
  const resolvedSeedUrl = canonicalizeUrlShared(seed.finalUrl || params.seedUrl, params.baseUrl);

  if (!seed.ok && !seed.blockedBy) return "not_found";
  if (requestedSeedUrl !== resolvedSeedUrl && isUnsafeSeedLocaleRedirect(requestedSeedUrl, resolvedSeedUrl, params.baseUrl)) {
    return "non_product_redirect";
  }
  if (requestedSeedUrl !== resolvedSeedUrl && looksLikeKnownNonProductUrl(resolvedSeedUrl, params.baseUrl)) {
    return "non_product_redirect";
  }
  if (seed.body && looksLikeProductPageHtml(seed.body)) return "product_page";
  return "unknown";
}

function buildEmptyShopifyDirectPdpResponse(
  params: {
    brand: string;
    domain: string;
    offset: number;
    limit: number;
    diagnostics: ExtractResponse["diagnostics"];
  },
  platformLabel: string,
): Omit<ExtractResponse, "generated_at" | "logs"> {
  return {
    brand: params.brand,
    domain: params.domain,
    mode: "puppeteer" as const,
    platform: platformLabel,
    products: [],
    variants: [],
    pricing: { currency: "USD", min: 0, max: 0, avg: 0 },
    ad_copy: { by_variant_id: {} },
    pagination: {
      offset: params.offset,
      limit: params.limit,
      next_offset: null,
      has_more: false,
      discovered_urls: 0,
    },
    diagnostics: params.diagnostics,
  };
}

function mergeShopifyDirectPdpFaqFallback(
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  faqItems: ExtractedProductFaqItem[],
): Omit<ExtractResponse, "generated_at" | "logs"> {
  if (!response.products[0] || faqItems.length === 0) return response;

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

    Object.assign(
      mergedProduct,
      buildProductPdpFields({
        descriptionRaw: product.description_raw,
        detailsSections: product.details_sections,
        ingredientsRaw: product.ingredients_raw,
        activeIngredientsRaw: product.active_ingredients_raw,
        howToUseRaw: product.how_to_use_raw,
        faqItems: dedupeFaqItems([...(product.faq_items || []), ...faqItems]),
        fieldSources: {
          description_raw: product.field_sources?.description_raw || [],
          details_sections: product.field_sources?.details_sections || [],
          ingredients_raw: product.field_sources?.ingredients_raw || [],
          active_ingredients_raw: product.field_sources?.active_ingredients_raw || [],
          how_to_use_raw: product.field_sources?.how_to_use_raw || [],
          faq_items: [
            ...(product.field_sources?.faq_items || []),
            ...faqItems.map((item) => item.source_kind || "okendo_questions_api"),
          ],
        },
      }),
    );

    return withProductPdpProfile(mergedProduct);
  });

  return {
    ...response,
    products: mergedProducts,
  };
}

function mergeShopifyDirectPdpReviewSummaryFallback(
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  reviewSummary: ExtractedProductReviewSummary | null | undefined,
): Omit<ExtractResponse, "generated_at" | "logs"> {
  if (!response.products[0] || !reviewSummary) return response;

  const mergedProducts = response.products.map((product, idx) => {
    if (idx !== 0) return product;
    return withProductPdpProfile({
      ...product,
      image_urls: [...product.image_urls],
      variant_skus: [...product.variant_skus],
      variants: product.variants.map((variant) => ({
        ...variant,
        image_urls: [...variant.image_urls],
      })),
      review_summary: mergeOkendoReviewSummary(product.review_summary, reviewSummary),
    });
  });

  return {
    ...response,
    products: mergedProducts,
  };
}

function mergeShopifyDirectPdpEmbeddedProductJson(
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  html: string | undefined,
): Omit<ExtractResponse, "generated_at" | "logs"> {
  if (!response.products[0] || !html) return response;

  const embeddedScripts = extractShopifyProductJsonAttributeScriptsFromHtml(html);
  const htmlVolumeText = extractShopifyProductVolumeTextFromHtml(html);
  if (embeddedScripts.length === 0 && !htmlVolumeText) return response;
  const embeddedFields: ReturnType<typeof extractShopifyEmbeddedProductPayloadPdpFields> =
    embeddedScripts.length > 0
      ? extractShopifyEmbeddedProductPayloadPdpFields(embeddedScripts)
      : {
          descriptionRaw: undefined,
          detailsSections: [],
          ingredientsRaw: undefined,
          activeIngredientsRaw: undefined,
          howToUseRaw: undefined,
          imageUrls: [],
        };
  const mergedImages = resolveStructuredImageUrls(response.domain, embeddedFields.imageUrls);

  const mergedProducts = response.products.map((product, idx) => {
    if (idx !== 0) return product;

    const mergedProduct: ExtractedProduct = {
      ...product,
      image_urls: dedupeStringList([...product.image_urls, ...mergedImages]),
      variant_skus: [...product.variant_skus],
      variants: product.variants.map((variant) => {
        const variantImages = variant.image_urls.length > 0 ? variant.image_urls : mergedImages;
        return {
          ...variant,
          image_urls: dedupeStringList(variantImages),
          image_url: variant.image_url || variantImages[0] || "",
        };
      }),
    };
    mergedProduct.image_url = mergedProduct.image_urls[0] || product.image_url || "";
    const htmlSizeEvidence =
      mergedProduct.variants.length === 1 &&
      isGenericOfferOptionValue(mergedProduct.variants[0]?.option_value, mergedProduct.title)
        ? extractProductSizeEvidence(
            htmlVolumeText,
            html,
            mergedProduct.description_raw,
            ...(Array.isArray(mergedProduct.details_sections)
              ? mergedProduct.details_sections.flatMap((section) => [section.heading, section.body])
              : []),
            mergedProduct.url,
          )
        : { optionValue: "" };
    const htmlVolumeOptionValue =
      mergedProduct.variants.length === 1 && isGenericOfferOptionValue(mergedProduct.variants[0]?.option_value, mergedProduct.title)
        ? htmlSizeEvidence.optionValue
        : "";
    if (htmlVolumeOptionValue && mergedProduct.variants[0]) {
      mergedProduct.variants[0] = {
        ...mergedProduct.variants[0],
        option_name: "Size",
        option_value: htmlVolumeOptionValue,
        hidden_from_selector: false,
      };
    }
    if (htmlSizeEvidence.optionValue && !mergedProduct.volume) {
      mergedProduct.volume = htmlSizeEvidence.optionValue;
    }
    if (htmlSizeEvidence.alternateOptionValue && !mergedProduct.product_volume) {
      mergedProduct.product_volume = htmlSizeEvidence.alternateOptionValue;
    }
    if (htmlSizeEvidence.detailLabel && !mergedProduct.size_detail_label) {
      mergedProduct.size_detail_label = htmlSizeEvidence.detailLabel;
    }

    Object.assign(
      mergedProduct,
      buildProductPdpFields({
        descriptionRaw: product.description_raw || embeddedFields.descriptionRaw,
        detailsSections: dedupeDetailSections([
          ...(product.details_sections || []),
          ...(embeddedFields.detailsSections || []),
        ]),
        ingredientsRaw: product.ingredients_raw || embeddedFields.ingredientsRaw,
        activeIngredientsRaw: product.active_ingredients_raw || embeddedFields.activeIngredientsRaw,
        howToUseRaw: product.how_to_use_raw || embeddedFields.howToUseRaw,
        faqItems: product.faq_items || [],
        fieldSources: {
          description_raw: [
            ...(product.field_sources?.description_raw || []),
            embeddedFields.descriptionRaw ? "embedded_product_json" : "",
          ],
          details_sections: [
            ...(product.field_sources?.details_sections || []),
            ...(embeddedFields.detailsSections || []).map((section) => section.source_kind),
          ],
          ingredients_raw: [
            ...(product.field_sources?.ingredients_raw || []),
            embeddedFields.ingredientsRaw ? "embedded_product_json" : "",
          ],
          active_ingredients_raw: [
            ...(product.field_sources?.active_ingredients_raw || []),
            embeddedFields.activeIngredientsRaw ? "embedded_product_json" : "",
          ],
          how_to_use_raw: [
            ...(product.field_sources?.how_to_use_raw || []),
            embeddedFields.howToUseRaw ? "embedded_product_json" : "",
          ],
          faq_items: product.field_sources?.faq_items || [],
        },
      }),
    );

    return withProductPdpProfile(mergedProduct);
  });

  return {
    ...response,
    products: mergedProducts,
  };
}

const FALLBACK_DESCRIPTION_SITE_BOILERPLATE_RE =
  /\b(?:shop (?:top selling|best selling)|official (?:site|store)|skincare quiz|skin(?:care)? routine quiz|find your perfect|free shipping|join (?:our )?(?:mailing list|newsletter)|healthy,?\s+glowing skin packed with high performing ingredients)\b/i;

const FALLBACK_DESCRIPTION_TITLE_STOPWORDS = new Set([
  "the",
  "and",
  "with",
  "for",
  "from",
  "into",
  "your",
  "skin",
  "care",
  "skincare",
  "beauty",
  "product",
  "default",
  "title",
]);

function getProductSpecificTitleTokens(title: string) {
  return cleanText(title)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length >= 4 && !FALLBACK_DESCRIPTION_TITLE_STOPWORDS.has(token))
    .slice(0, 8);
}

function isUsableShopifyFallbackDescription(product: ExtractedProduct, rawDescription: string | undefined) {
  const description = cleanText(rawDescription);
  if (description.length < PDP_COMPLETENESS_MIN_OVERVIEW_CHARS) return false;
  if (isPdpContentNoiseText(description)) return false;
  if (FALLBACK_DESCRIPTION_SITE_BOILERPLATE_RE.test(description)) return false;
  const descriptionLower = description.toLowerCase();
  const titleTokens = getProductSpecificTitleTokens(product.title);
  if (titleTokens.length === 0) return true;
  return titleTokens.some((token) => descriptionLower.includes(token));
}

export function mergeShopifyDirectPdpFallback(
  brand: string,
  response: Omit<ExtractResponse, "generated_at" | "logs">,
  fallbackProduct: ExtractedProduct,
  options?: {
    preservePdpFieldSourceKinds?: boolean;
  },
): Omit<ExtractResponse, "generated_at" | "logs"> {
  if (!response.products[0]) return response;
  const preservePdpFieldSourceKinds = Boolean(options?.preservePdpFieldSourceKinds);
  const mapFallbackSection = (section: ExtractedProductDetailSection) => ({
    ...section,
    source_kind: preservePdpFieldSourceKinds ? cleanText(section.source_kind) || "unknown" : tagFallbackSourceKind(section.source_kind),
  });
  const mapFallbackFaqItem = (item: ExtractedProductFaqItem) => ({
    ...item,
    source_kind: preservePdpFieldSourceKinds ? cleanText(item.source_kind) || "unknown" : tagFallbackSourceKind(item.source_kind),
  });
  const mergeFallbackFieldSources = (sourceKinds: string[] | undefined, fallbackLabel: string) =>
    preservePdpFieldSourceKinds ? normalizePdpSourceKinds(sourceKinds).filter(Boolean) : fallbackFieldSourceKinds(sourceKinds, fallbackLabel);

  const mergedProducts = response.products.map((product, idx) => {
    if (idx !== 0) return product;

    const mergedProduct: ExtractedProduct = {
      ...product,
      image_urls: [...product.image_urls],
      content_image_urls: dedupeStringList([
        ...((product.content_image_urls || []) as string[]),
        ...((fallbackProduct.content_image_urls || []) as string[]),
      ]),
      variant_skus: [...product.variant_skus],
      variants: product.variants.map((variant) => ({
        ...variant,
        image_urls: [...variant.image_urls],
      })),
    };
    const useFallbackDescription = Boolean(
      !product.description_raw &&
        isUsableShopifyFallbackDescription(product, fallbackProduct.description_raw),
    );
    const useFallbackIngredients = !product.ingredients_raw && Boolean(fallbackProduct.ingredients_raw);
    const useFallbackActiveIngredients =
      !product.active_ingredients_raw && Boolean(fallbackProduct.active_ingredients_raw);
    const useFallbackHowToUse = !product.how_to_use_raw && Boolean(fallbackProduct.how_to_use_raw);
    const useFallbackFaqItems =
      (!Array.isArray(product.faq_items) || product.faq_items.length === 0) &&
      Array.isArray(fallbackProduct.faq_items) &&
      fallbackProduct.faq_items.length > 0;
    Object.assign(
      mergedProduct,
      buildProductPdpFields({
        descriptionRaw: useFallbackDescription ? fallbackProduct.description_raw : product.description_raw,
        detailsSections: dedupeDetailSections([
          ...((Array.isArray(product.details_sections) ? product.details_sections : []) || []),
          ...((Array.isArray(fallbackProduct.details_sections) ? fallbackProduct.details_sections : []) || []).map(mapFallbackSection),
        ]),
        ingredientsRaw: useFallbackIngredients ? fallbackProduct.ingredients_raw : product.ingredients_raw,
        activeIngredientsRaw:
          useFallbackActiveIngredients ? fallbackProduct.active_ingredients_raw : product.active_ingredients_raw,
        howToUseRaw: useFallbackHowToUse ? fallbackProduct.how_to_use_raw : product.how_to_use_raw,
        faqItems: useFallbackFaqItems
          ? (fallbackProduct.faq_items || []).map(mapFallbackFaqItem)
          : product.faq_items,
        fieldSources: {
          description_raw: [
            ...(product.field_sources?.description_raw || []),
            ...(useFallbackDescription
              ? mergeFallbackFieldSources(fallbackProduct.field_sources?.description_raw || [], "description_raw")
              : []),
          ],
          details_sections: [
            ...(product.field_sources?.details_sections || []),
            ...mergeFallbackFieldSources(fallbackProduct.field_sources?.details_sections || [], "details_sections"),
          ],
          ingredients_raw: [
            ...(product.field_sources?.ingredients_raw || []),
            ...(useFallbackIngredients
              ? mergeFallbackFieldSources(fallbackProduct.field_sources?.ingredients_raw || [], "ingredients_raw")
              : []),
          ],
          active_ingredients_raw: [
            ...(product.field_sources?.active_ingredients_raw || []),
            ...(useFallbackActiveIngredients
              ? mergeFallbackFieldSources(
                  fallbackProduct.field_sources?.active_ingredients_raw || [],
                  "active_ingredients_raw",
                )
              : []),
          ],
          how_to_use_raw: [
            ...(product.field_sources?.how_to_use_raw || []),
            ...(useFallbackHowToUse
              ? mergeFallbackFieldSources(fallbackProduct.field_sources?.how_to_use_raw || [], "how_to_use_raw")
              : []),
          ],
          faq_items: [
            ...(product.field_sources?.faq_items || []),
            ...(useFallbackFaqItems
              ? mergeFallbackFieldSources(fallbackProduct.field_sources?.faq_items || [], "faq_items")
              : []),
          ],
        },
      }),
    );

    const rawFallbackProductImages = dedupeShopifyImageUrls([
      ...fallbackProduct.image_urls,
      fallbackProduct.image_url,
      ...fallbackProduct.variants.flatMap((variant) => variant.image_urls),
      ...fallbackProduct.variants.map((variant) => variant.image_url),
    ]);
    const fallbackProductImages = preservePdpFieldSourceKinds
      ? dedupeShopifyImageUrls(rawFallbackProductImages)
      : selectRelevantFallbackImageUrls(
          {
            title: mergedProduct.title,
            url: mergedProduct.url,
          },
          rawFallbackProductImages,
        );

    if (fallbackProductImages.length === 0) return withProductPdpProfile(mergedProduct);

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
      const directVariantOptionGeneric = isGenericOfferOptionValue(variant.option_value, mergedProduct.title);
      const fallbackVariantOptionGeneric = isGenericOfferOptionValue(
        matchedFallback?.option_value,
        mergedProduct.title,
      );
      const useFallbackVariantDisplay = Boolean(
        matchedFallback && directVariantOptionGeneric && !fallbackVariantOptionGeneric,
      );
      const rawVariantFallbackImages = dedupeShopifyImageUrls([
        ...(matchedFallback?.image_urls || []),
        matchedFallback?.image_url,
        ...fallbackProductImages,
      ]);
      const relevantVariantFallbackImages = preservePdpFieldSourceKinds
        ? rawVariantFallbackImages
        : selectRelevantFallbackImageUrls(
            {
              title: [mergedProduct.title, variant.option_name, variant.option_value].filter(Boolean).join(" "),
              url: variant.url || mergedProduct.url,
            },
            rawVariantFallbackImages,
          );

      const mergedVariantImages = dedupeShopifyImageUrls([
        ...variant.image_urls,
        variant.image_url,
        ...relevantVariantFallbackImages,
      ]);

      return {
        ...variant,
        ...(useFallbackVariantDisplay
          ? {
              option_name: matchedFallback?.option_name || variant.option_name,
              option_value: matchedFallback?.option_value || variant.option_value,
            }
          : {}),
        image_urls: mergedVariantImages,
        image_url: mergedVariantImages[0] || variant.image_url || mergedProduct.image_url,
      };
    });

    mergedProduct.image_urls = dedupeShopifyImageUrls([
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

    return withProductPdpProfile(mergedProduct);
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
  return filterShopifyProductImageUrls(
    {
      title: product.title,
      url: `${baseUrl}/products/${product.handle}`,
    },
    resolveStructuredImageUrls(baseUrl, [product.featured_image, product.images]),
  );
}

function resolveShopifyVariantImageUrls(baseUrl: string, product: ShopifyProduct, variant: ShopifyVariant) {
  const images = product.images || [];
  const matchedImages = images
    .filter((image) => typeof image === "object" && image !== null && (image.variant_ids || []).includes(variant.id));

  return dedupeShopifyImageUrls([
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

async function fetchTextWithFinalUrl(url: string): Promise<{ status: number | null; body: string | null; finalUrl: string }> {
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
    if (!res.ok) {
      return {
        status: res.status,
        body: null,
        finalUrl: res.url || url,
      };
    }
    return {
      status: res.status,
      body: await res.text(),
      finalUrl: res.url || url,
    };
  } catch {
    return {
      status: null,
      body: null,
      finalUrl: url,
    };
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
    const seed = await fetchTextWithFinalUrl(params.seedUrl);
    if (
      isKnownCrossProductResolutionMismatch({
        sourceUrl: params.seedUrl,
        extractedUrl: seed.finalUrl,
      })
    ) {
      params.log("warn", `Seed URL resolved to incompatible product page; skipping seed-page discovery: ${params.seedUrl} -> ${seed.finalUrl}`);
      return { sitemapUrl: undefined, productUrls: [] as string[] };
    } else if (seed.body) {
      const seedHtml = seed.body;
      const seedUrls = extractProductUrlsFromHtml(seedHtml, params.baseUrl);
      if (
        seedUrls.length > 0 &&
        isKnownCrossProductResolutionMismatch({
          sourceUrl: params.seedUrl,
          extractedUrl: seedUrls[0],
        })
      ) {
        params.log(
          "warn",
          `Seed page surfaced an incompatible PDP candidate; skipping seed-page discovery: ${params.seedUrl} -> ${seedUrls[0]}`,
        );
        return { sitemapUrl: undefined, productUrls: [] as string[] };
      } else
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

const LOCALE_PATH_SEGMENT_RE = /^[a-z]{2}(?:-[a-z]{2})?$/i;

function getLocalePathSegment(pathname: string): string | null {
  const segment = pathname.split("/").filter(Boolean)[0] || "";
  return LOCALE_PATH_SEGMENT_RE.test(segment) ? segment.toLowerCase() : null;
}

function normalizePageUrlSignal(rawUrl: string | undefined, baseUrl: string) {
  if (!rawUrl) return null;

  try {
    const canonical = canonicalizeUrlShared(toAbsoluteUrlShared(baseUrl, rawUrl), baseUrl);
    const parsed = new URL(canonical);
    return {
      canonical,
      origin: parsed.origin,
      pathname: parsed.pathname,
      locale: getLocalePathSegment(parsed.pathname),
    };
  } catch {
    return null;
  }
}

function listJsonLdObjectUrls(baseUrl: string, obj: Record<string, unknown>): string[] {
  const offerUrls = normalizeJsonLdOffers(obj.offers)
    .map((offer) => (typeof offer.url === "string" ? offer.url : ""))
    .filter(Boolean);

  return dedupeStringList([
    typeof obj.url === "string" ? obj.url : "",
    typeof obj["@id"] === "string" ? String(obj["@id"]) : "",
    ...offerUrls,
  ]).map((url) => canonicalizeUrlShared(toAbsoluteUrlShared(baseUrl, url), baseUrl));
}

function scoreJsonLdObjectForPage(params: {
  object: Record<string, unknown>;
  pageSignals: Array<ReturnType<typeof normalizePageUrlSignal>>;
  baseUrl: string;
}): number {
  const objectUrls = listJsonLdObjectUrls(params.baseUrl, params.object);
  if (objectUrls.length === 0) return 0;

  let score = 0;
  for (const objectUrl of objectUrls) {
    try {
      const parsed = new URL(objectUrl);
      const objectLocale = getLocalePathSegment(parsed.pathname);

      for (const signal of params.pageSignals) {
        if (!signal) continue;

        if (objectUrl === signal.canonical) score += 120;
        if (parsed.origin === signal.origin && parsed.pathname === signal.pathname) score += 90;
        if (signal.locale && objectLocale && signal.locale === objectLocale) score += 20;
        if (signal.locale && objectLocale && signal.locale !== objectLocale) score -= 25;
      }
    } catch {
      // ignore malformed structured URLs
    }
  }

  return score;
}

export function pickBestJsonLdObjectForPage(params: {
  candidates: Array<Record<string, unknown>>;
  pageUrl: string;
  canonicalUrl?: string;
  baseUrl: string;
}): Record<string, unknown> | null {
  const pageSignals = dedupeStringList([params.canonicalUrl, params.pageUrl])
    .map((url) => normalizePageUrlSignal(url, params.baseUrl))
    .filter(Boolean);

  if (params.candidates.length === 0) return null;

  let best: { object: Record<string, unknown>; score: number } | null = null;
  for (const candidate of params.candidates) {
    const score = scoreJsonLdObjectForPage({
      object: candidate,
      pageSignals,
      baseUrl: params.baseUrl,
    });

    if (!best || score > best.score) best = { object: candidate, score };
  }

  return best?.object || params.candidates[0] || null;
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

function safeDecodeURIComponent(value: string | undefined) {
  const raw = String(value || "");
  if (!raw) return "";
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
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

function injectBaseHref(html: string, pageUrl: string): string {
  if (!html || /<base\b/i.test(html)) return html;
  const baseTag = `<base href="${pageUrl.replace(/"/g, "&quot;")}">`;

  if (/<head\b[^>]*>/i.test(html)) {
    return html.replace(/<head\b([^>]*)>/i, `<head$1>${baseTag}`);
  }

  if (/<html\b[^>]*>/i.test(html)) {
    return html.replace(/<html\b([^>]*)>/i, `<html$1><head>${baseTag}</head>`);
  }

  return `<head>${baseTag}</head>${html}`;
}

async function fetchHtmlViaNativeRequest(
  url: string,
  diagnostics: ExtractResponse["diagnostics"],
  context: FetchContext,
): Promise<{ status: number | null; body: string | null; finalUrl: string }> {
  const outcome = await fetchTextTracked(url, withBrowserishHtmlHeaders(context), diagnostics!);

  return {
    status: outcome.status,
    body: outcome.body,
    finalUrl: outcome.finalUrl,
  };
}

function withBrowserishHtmlHeaders(context: FetchContext = {}): FetchContext {
  const inheritedHeaders = Object.fromEntries(
    Object.entries(context.headers || {}).filter(
      ([key]) => !["user-agent", "accept-language"].includes(key.toLowerCase()),
    ),
  );
  return {
    ...context,
    headers: {
      ...inheritedHeaders,
      "accept-language": "en-US,en;q=0.9",
      "user-agent": process.env.PUPPETEER_USER_AGENT || DEFAULT_BROWSERISH_USER_AGENT,
    },
  };
}

export async function extractPageSignals(page: Page): Promise<ScrapedPageSignals> {
  const scraped = (await page.evaluate(() => {
    const documentBase = document.baseURI || location.href;
    const title =
      document.querySelector("h1")?.textContent?.trim() ||
      document.querySelector('meta[property="og:title"]')?.getAttribute("content")?.trim() ||
      document.title ||
      "";

    const canonical =
      (document.querySelector('link[rel="canonical"]') as HTMLLinkElement | null)?.href ||
      document.querySelector('meta[property="og:url"]')?.getAttribute("content") ||
      documentBase;

    const metaDescription =
      document.querySelector('meta[name="description"]')?.getAttribute("content")?.trim() ||
      document.querySelector('meta[property="og:description"]')?.getAttribute("content")?.trim() ||
      "";

    const normalizeSectionText = (raw: string) =>
      raw
        .replace(/\r\n/g, "\n")
        .replace(/[ \t]+/g, " ")
        .replace(/\n[ \t]+/g, "\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim();

    const decodeHtmlText = (raw: string) => {
      const container = document.createElement("div");
      container.innerHTML = raw;
      return container.textContent?.trim() || "";
    };

    const richTextJsonToText = (value: unknown): string => {
      const walk = (node: unknown): string[] => {
        if (!node || typeof node !== "object") return [];
        const record = node as Record<string, unknown>;
        const nodeType = typeof record.type === "string" ? record.type : "";
        const textValue = typeof record.value === "string" ? record.value : "";
        const children = Array.isArray(record.children) ? record.children : [];
        const childText = children.flatMap((child) => walk(child)).filter(Boolean);

        if (nodeType === "text") {
          const text = normalizeSectionText(textValue);
          return text ? [text] : [];
        }
        if (nodeType === "list-item") {
          const text = normalizeSectionText(childText.join(" ").trim());
          return text ? [`- ${text}`] : [];
        }
        if (nodeType === "paragraph" || nodeType === "heading" || nodeType === "root" || nodeType === "list") {
          const text = normalizeSectionText(childText.join(nodeType === "list" ? "\n" : " ").trim());
          return text ? [text] : [];
        }

        const text = normalizeSectionText([textValue, ...childText].join(" ").trim());
        return text ? [text] : [];
      };

      return normalizeSectionText(walk(value).join("\n\n"));
    };

    const readSectionContainerText = (root: Element | null | undefined): string | undefined => {
      if (!root) return undefined;
      const candidates = [
        ".markdown",
        ".metafield-rich_text_field",
        ".rte",
        ".wysiwyg",
        "[class*='rich_text']",
        "[class*='rich-text']",
        ".accordion__content",
        ".accordion__content-container",
        ".accordion-content",
        ".accordion-content-wrap",
        ".accordion-content-wrap-inner",
        ".vc_tta-panel-body",
        ".wpb_wrapper",
        ".wpb_text_column",
        ".woocommerce-product-details__short-description",
      ];
      for (const selector of candidates) {
        const node = root.querySelector(selector);
        const text = normalizeSectionText((node as HTMLElement | null)?.innerText || node?.textContent || "");
        if (text) return text;
      }
      const text = normalizeSectionText((root as HTMLElement).innerText || root.textContent || "");
      return text || undefined;
    };

    const rawCustomMetafields =
      (window as any)?.corner?.sessionData?.product?.customMetafields ||
      (window as any)?.corner?.sessionData?.customMetafields ||
      null;
    const customMetafieldHowToText = richTextJsonToText(rawCustomMetafields?.how_to_use_1_);
    const customMetafieldTab1Text = richTextJsonToText(rawCustomMetafields?.product_info_tab_1_body);
    const customMetafieldTab2Text = richTextJsonToText(rawCustomMetafields?.product_info_tab_2_body);
    const customMetafieldFullIngredientsText = richTextJsonToText(rawCustomMetafields?.product_info_tab_3_full_ingredients);
    const customMetafieldKeyIngredientsText = richTextJsonToText(rawCustomMetafields?.product_info_tab_3_key_ingredients);

    const productDetailsText = (() => {
      const hiddenOverview = document.getElementById("overview-about-text");
      const hiddenRaw = hiddenOverview?.getAttribute("value")?.trim() || "";
      if (hiddenRaw) {
        try {
          const decoded = decodeURIComponent(hiddenRaw);
          const text = normalizeSectionText(decodeHtmlText(decoded));
          if (text) return text;
        } catch {
          const text = normalizeSectionText(decodeHtmlText(hiddenRaw));
          if (text) return text;
        }
      }

      const moreAbout = document.querySelector(".more-about-product-content");
      if (moreAbout instanceof HTMLElement) {
        const text = normalizeSectionText(moreAbout.innerText || moreAbout.textContent || "");
        if (text) return text;
      }

      const wooShortDescription = Array.from(
        document.querySelectorAll(
          [
            ".woocommerce-product-details__short-description",
            ".summary .woocommerce-product-details__short-description",
            ".entry-summary .woocommerce-product-details__short-description",
          ].join(", "),
        ),
      ) as HTMLElement[];
      for (const node of wooShortDescription.slice(0, 4)) {
        const text = normalizeSectionText(node.innerText || node.textContent || "");
        if (text) return text;
      }

      const summaryHeading = Array.from(document.querySelectorAll("h2, h3, h4")).find((node) =>
        /^summary$/i.test(normalizeSectionText(node.textContent || "")),
      );
      if (summaryHeading) {
        const bodyParts: string[] = [];
        let cursor = summaryHeading.nextElementSibling;
        let guard = 0;
        while (cursor && guard < 4) {
          if (/^H[2-4]$/i.test(cursor.tagName)) break;
          const text = normalizeSectionText((cursor as HTMLElement).innerText || cursor.textContent || "");
          if (text) bodyParts.push(text);
          cursor = cursor.nextElementSibling;
          guard += 1;
        }
        const summaryText = normalizeSectionText(bodyParts.join("\n\n"));
        if (summaryText) return summaryText;
      }

      return "";
    })();
    const productVolumeText = (() => {
      const volumeNode = document.querySelector(".product__volume, .product-volume, [data-product-volume]");
      const text = normalizeSectionText((volumeNode as HTMLElement | null)?.innerText || volumeNode?.textContent || "");
      return text || undefined;
    })();

    const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
      .map((s) => s.textContent || "")
      .filter(Boolean);
    const okendoMetafieldJson = (() => {
      const scripts = Array.from(document.querySelectorAll("script[data-oke-metafield-data]"));
      for (const script of scripts.reverse()) {
        const raw = script.textContent?.trim() || "";
        if (!raw) continue;
        try {
          const parsed = JSON.parse(raw) as Record<string, unknown>;
          if (typeof parsed.productId === "string" && parsed.productId.trim()) {
            return raw;
          }
        } catch {
          // ignore non-product metadata payloads
        }
      }
      return undefined;
    })();
    const embeddedProductScripts = Array.from(document.querySelectorAll("script"))
      .map((script) => script.textContent || "")
      .filter((text) =>
        /window\.reelUp_productJSON\s*=|_RSConfig\.product\s*=|window\.corner\.sessionData\.product\s*=|corner\.sessionData\.product\s*=|sgGlobalVars\.currentProduct\s*=|window\.theme\.product\s*=|theme\.product\s*=/i.test(
          text,
        ),
      )
      .slice(0, 8);

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
            const absolute = new URL(candidate, documentBase).toString();
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
      const out: DomVariantMeta[] = [];
      const bySku = new Map<string, DomVariantMeta>();

      const mergeMeta = (meta: DomVariantMeta) => {
        if (!meta.sku) return;
        const existing = bySku.get(meta.sku);
        const merged = existing
          ? {
              ...existing,
              ...Object.fromEntries(
                Object.entries(meta).filter(([, value]) => {
                  if (Array.isArray(value)) return value.length > 0;
                  return value != null && value !== "";
                }),
              ),
              image_urls:
                existing.image_urls || meta.image_urls
                  ? Array.from(new Set([...(existing.image_urls || []), ...(meta.image_urls || [])]))
                  : undefined,
            }
          : meta;
        bySku.set(meta.sku, merged);
      };

      const pageSku =
        (document.querySelector(".product-detail[data-pid]") as HTMLElement | null)?.getAttribute("data-pid")?.trim() ||
        (document.querySelector("[itemprop='sku']") as HTMLElement | null)?.textContent?.trim() ||
        "";

      const normalizeDomOptionName = (raw: string) => {
        const normalized = raw.trim().toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
        if (!normalized) return "";
        if (["size", "sizes", "volume", "capacity"].includes(normalized)) return "Size";
        if (["color", "colour"].includes(normalized)) return "Color";
        if (["shade", "tone", "hue"].includes(normalized)) return "Shade";
        if (["scent", "fragrance", "flavor", "flavour"].includes(normalized)) return "Scent";
        return normalized.replace(/\b\w/g, (char) => char.toUpperCase());
      };

      const normalizeDomOptionValue = (rawText: string, rawAttr: string) => {
        const text = normalizeSectionText(rawText).replace(/\s+/g, " ").trim();
        if (text && !/^select(?:ed)?$/i.test(text)) return text;
        const attr = rawAttr.trim();
        const sizeMatch = attr.match(/(\d+(?:\.\d+)?)\s*(ml|m l|g|kg|oz|fl oz|l|lb|lbs|mm|cm)\b/i);
        if (sizeMatch) return `${sizeMatch[1]}${String(sizeMatch[2] || "").toLowerCase().replace(/\s+/g, "")}`;
        return attr.replace(/^(?:na|size|variant)[-_:\s]*/i, "").trim();
      };

      const embeddedSkusEl = document.querySelector("[data-product-skus-value]") as HTMLElement | null;
      const raw = embeddedSkusEl?.getAttribute("data-product-skus-value") || "";
      if (raw) {
        const textarea = document.createElement("textarea");
        textarea.innerHTML = raw;
        const decoded = textarea.value;

        try {
          const parsed = JSON.parse(decoded) as unknown;
          if (Array.isArray(parsed)) {
            parsed
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
                const multiShade =
                  typeof obj.multi_shade_description === "string" ? obj.multi_shade_description.trim() : "";

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
              .filter((variant) => Boolean(variant.sku))
              .forEach(mergeMeta);
          }
        } catch {
          // ignore malformed embedded variant metadata
        }
      }

      const attributeGroups = Array.from(
        document.querySelectorAll(
          ".product-variation[data-attr], .attribute-values[data-attr], [data-attr].product_tile-attributes_value",
        ),
      ) as HTMLElement[];
      for (const group of attributeGroups.slice(0, 24)) {
        const optionName = normalizeDomOptionName(group.getAttribute("data-attr") || "");
        if (!optionName) continue;
        const candidates = Array.from(
          group.querySelectorAll("button[data-attr-value], a[data-attr-value], [role='button'][data-attr-value]"),
        ) as HTMLElement[];
        const selected =
          candidates.filter((node) => {
            const className = node.className || "";
            const ariaPressed = (node.getAttribute("aria-pressed") || "").toLowerCase();
            return /\b(selected|is-selected|active)\b/i.test(String(className)) || ariaPressed === "true";
          }) || [];
        const scopedCandidates = selected.length > 0 ? selected : candidates.length === 1 ? candidates : [];
        for (const node of scopedCandidates.slice(0, 12)) {
          const rawAttrValue = node.getAttribute("data-attr-value") || "";
          const valueNode = node.querySelector(
            ".size-value, .swatch-value, .attribute-value_text, .attribute-value_name, [data-attr-value]",
          ) as HTMLElement | null;
          const optionValue = normalizeDomOptionValue(
            valueNode?.textContent || node.textContent || "",
            valueNode?.getAttribute("data-attr-value") || rawAttrValue,
          );
          if (!optionValue) continue;
          const sku =
            node.getAttribute("data-pid")?.trim() ||
            group.closest("[data-pid]")?.getAttribute("data-pid")?.trim() ||
            pageSku;
          if (!sku) continue;
          mergeMeta({
            sku,
            option_name: optionName,
            option_value: optionValue,
          });
        }
      }

      for (const meta of bySku.values()) out.push(meta);
      return out;
    })();

    let howToUseContent = document.getElementById("accordion-toggle-How to Use");
    let ingredientsContent = document.getElementById("accordion-toggle-Ingredients and Safety");
    const howToUseAccordion = ((patterns: RegExp[]) => {
      const controls = Array.from(
        document.querySelectorAll(
          "button, summary, .accordion__toggle, .accordion-title, .acc__btn, .module-accordion .item .trigger, .module-accordion .item .text, .vc_tta-panel-title a, .vc_tta-panel-heading a",
        ),
      ) as HTMLElement[];
      for (const control of controls.slice(0, 120)) {
        const label = normalizeSectionText(
          control.getAttribute("title") || control.getAttribute("aria-label") || control.textContent || "",
        );
        if (!label || !patterns.some((pattern) => pattern.test(label))) continue;
        const targetId = control.getAttribute("aria-controls") || "";
        const target = targetId ? document.getElementById(targetId) : null;
        const accordionItem =
          control.closest(".accordion__item") ||
          control.closest("accordion-wrap") ||
          control.closest(".pv-extra-details__accordion") ||
          control.closest(".module-accordion .item") ||
          control.closest(".vc_tta-panel") ||
          control.closest(".acc") ||
          control.parentElement;
        const content =
          target ||
          accordionItem?.querySelector?.(
            ".accordion__content, .accordion__content-container, .accordion-content, .accordion-content-wrap, .accordion-content-wrap-inner, .vc_tta-panel-body, .wpb_wrapper, .wpb_text_column, .details, .inner-text, .wysiwyg, .faq-answer, .faq__answer",
          ) ||
          control.nextElementSibling;
        const text = readSectionContainerText(content as Element | null) || readSectionContainerText(accordionItem);
        if (!text) continue;
        return {
          container: accordionItem || content || control.parentElement || control,
          text,
        };
      }
      return { container: null, text: undefined };
    })([/^how\s*to$/i, /\bhow to use\b/i, /\bhow to layer\b/i, /\busage\b/i, /\bdirections?\b/i]);
    const ingredientsAccordion = ((patterns: RegExp[]) => {
      const controls = Array.from(
        document.querySelectorAll(
          "button, summary, .accordion__toggle, .accordion-title, .acc__btn, .module-accordion .item .trigger, .module-accordion .item .text, .vc_tta-panel-title a, .vc_tta-panel-heading a",
        ),
      ) as HTMLElement[];
      for (const control of controls.slice(0, 120)) {
        const label = normalizeSectionText(
          control.getAttribute("title") || control.getAttribute("aria-label") || control.textContent || "",
        );
        if (!label || !patterns.some((pattern) => pattern.test(label))) continue;
        const targetId = control.getAttribute("aria-controls") || "";
        const target = targetId ? document.getElementById(targetId) : null;
        const accordionItem =
          control.closest(".accordion__item") ||
          control.closest("accordion-wrap") ||
          control.closest(".pv-extra-details__accordion") ||
          control.closest(".module-accordion .item") ||
          control.closest(".vc_tta-panel") ||
          control.closest(".acc") ||
          control.parentElement;
        const content =
          target ||
          accordionItem?.querySelector?.(
            ".accordion__content, .accordion__content-container, .accordion-content, .accordion-content-wrap, .accordion-content-wrap-inner, .vc_tta-panel-body, .wpb_wrapper, .wpb_text_column, .details, .inner-text, .wysiwyg, .faq-answer, .faq__answer",
          ) ||
          control.nextElementSibling;
        const text = readSectionContainerText(content as Element | null) || readSectionContainerText(accordionItem);
        if (!text) continue;
        return {
          container: accordionItem || content || control.parentElement || control,
          text,
        };
      }
      return { container: null, text: undefined };
    })([/\bkey ingredients?\b/i, /\bingredients?(?: and safety)?\b/i]);
    const faqAccordion = ((patterns: RegExp[]) => {
      const controls = Array.from(
        document.querySelectorAll(
          "button, summary, .accordion__toggle, .accordion-title, .acc__btn, .module-accordion .item .trigger, .module-accordion .item .text, .vc_tta-panel-title a, .vc_tta-panel-heading a",
        ),
      ) as HTMLElement[];
      for (const control of controls.slice(0, 120)) {
        const label = normalizeSectionText(
          control.getAttribute("title") || control.getAttribute("aria-label") || control.textContent || "",
        );
        if (!label || !patterns.some((pattern) => pattern.test(label))) continue;
        const targetId = control.getAttribute("aria-controls") || "";
        const target = targetId ? document.getElementById(targetId) : null;
        const accordionItem =
          control.closest(".accordion__item") ||
          control.closest("accordion-wrap") ||
          control.closest(".pv-extra-details__accordion") ||
          control.closest(".module-accordion .item") ||
          control.closest(".vc_tta-panel") ||
          control.closest(".acc") ||
          control.parentElement;
        const content =
          target ||
          accordionItem?.querySelector?.(
            ".accordion__content, .accordion__content-container, .accordion-content, .accordion-content-wrap, .accordion-content-wrap-inner, .vc_tta-panel-body, .wpb_wrapper, .wpb_text_column, .details, .inner-text, .wysiwyg, .faq-answer, .faq__answer",
          ) ||
          control.nextElementSibling;
        const text = readSectionContainerText(content as Element | null) || readSectionContainerText(accordionItem);
        if (!text) continue;
        return {
          container: accordionItem || content || control.parentElement || control,
          text,
        };
      }
      return { container: null, text: undefined };
    })([/\b(?:faqs?|frequently asked questions?)\b/i]);

    if (!howToUseContent || !ingredientsContent) {
      const buttons = Array.from(document.querySelectorAll("button[aria-controls]")) as HTMLButtonElement[];
      for (const button of buttons) {
        const titleText = (button.getAttribute("title") || button.textContent || "").trim().toLowerCase();
        if (!titleText) continue;

        const targetId = button.getAttribute("aria-controls") || "";
        if (!targetId) continue;

        if (!howToUseContent && (titleText === "how to use" || titleText === "usage")) {
          howToUseContent = document.getElementById(targetId);
        } else if (
          !ingredientsContent &&
          (titleText === "ingredients" || titleText === "ingredients and safety" || titleText === "ingredients & safety")
        ) {
          ingredientsContent = document.getElementById(targetId);
        }

        if (howToUseContent && ingredientsContent) break;
      }
    }

    const howToUseText =
      readSectionContainerText(howToUseContent) || howToUseAccordion.text || customMetafieldHowToText || undefined;
    const ingredientsAccordionText =
      readSectionContainerText(ingredientsContent) || ingredientsAccordion.text || customMetafieldFullIngredientsText || undefined;
    const ingredientsDisclaimerText =
      ingredientsContent?.querySelector(".product-details-accordions-ingredients-disclaimer")?.textContent?.trim() || undefined;
    const ingredientFlyoutText = (() => {
      const nodes = Array.from(
        document.querySelectorAll(
          ".ingredients-flyout-content, [data-original-ingredients], .product-ingredients-modal__content, .product-ingredients-modal .modal__inner.product-ingredients, .modal.product-ingredients-modal",
        ),
      ) as HTMLElement[];
      for (const node of nodes.slice(0, 8)) {
        const attrRaw = node.getAttribute("data-original-ingredients") || "";
        const attrText = attrRaw ? normalizeSectionText(decodeHtmlText(attrRaw)) : "";
        const visibleText = normalizeSectionText(node.innerText || node.textContent || "");
        const combined = normalizeSectionText([attrText, visibleText].filter(Boolean).join("\n\n"));
        if (!combined) continue;
        if (
          /\bactive ingredients?\b/i.test(combined) ||
          /\binactive ingredients?\b/i.test(combined) ||
          /\b(?:full ingredients?|ingredients?)\b/i.test(combined)
        ) {
          return combined;
        }
      }
      return undefined;
    })();
    const ingredientsMarkdownText = ingredientFlyoutText || ingredientsAccordionText || undefined;
    const keyIngredientsText = (() => {
      const titleNodes = Array.from(document.querySelectorAll(".title, h2, h3, h4, h5, span, div")) as HTMLElement[];
      for (const node of titleNodes.slice(0, 80)) {
        const heading = normalizeSectionText(node.textContent || "");
        if (!/^key ingredients$/i.test(heading)) continue;
        const wrapper = node.closest(".content") || node.parentElement || node.closest("section") || node.closest("div");
        const listNode =
          wrapper?.querySelector?.(".list") ||
          node.nextElementSibling ||
          wrapper?.querySelector?.("[class*='list']");
        const body = normalizeSectionText((listNode as HTMLElement | null)?.innerText || listNode?.textContent || "");
        if (body) return body;
      }
      return undefined;
    })();
    const faqItems = (() => {
      const items: Array<{
        question: string;
        answer: string;
        source_kind: string;
        source_url?: string;
        source_title?: string;
      }> = [];
      const seen = new Set<string>();

      const normalizeQuestion = (value: string) =>
        normalizeSectionText(value)
          .replace(/^(?:q(?:uestion)?\s*[:/-]\s*)/i, "")
          .trim();
      const normalizeAnswer = (value: string) =>
        normalizeSectionText(value)
          .replace(/^(?:a(?:nswer)?\s*[:/-]\s*)/i, "")
          .trim();
      const looksLikeQuestion = (value: string) => {
        const normalized = normalizeQuestion(value);
        if (!normalized) return false;
        return (
          /[?？]$/.test(normalized) ||
          /^(?:can|is|are|do|does|did|will|would|should|could|where|when|why|how|what|who|which)\b/i.test(normalized)
        );
      };
      const pushFaqItem = (questionRaw: string, answerRaw: string, sourceKind: string, sourceTitle = "FAQ") => {
        const question = normalizeQuestion(questionRaw);
        const answer = normalizeAnswer(answerRaw);
        if (!question || !answer || !looksLikeQuestion(question)) return;
        const key = `${question.toLowerCase()}|${answer.toLowerCase()}|${sourceKind.toLowerCase()}`;
        if (seen.has(key)) return;
        seen.add(key);
        items.push({
          question,
          answer,
          source_kind: sourceKind,
          source_url: location.href,
          source_title: sourceTitle,
        });
      };
      const resolveControlledBody = (control: HTMLElement) => {
        const targetId = control.getAttribute("aria-controls") || "";
        if (targetId) {
          const target = document.getElementById(targetId);
          const targetText = normalizeSectionText((target as HTMLElement | null)?.innerText || target?.textContent || "");
          if (targetText) return targetText;
        }

        if (control.tagName.toLowerCase() === "summary") {
          const details = control.closest("details");
          if (details) {
            const bodyParts = Array.from(details.children)
              .filter((child) => child !== control)
              .map((child) => normalizeSectionText((child as HTMLElement).innerText || child.textContent || ""))
              .filter(Boolean);
            if (bodyParts.length > 0) return bodyParts.join("\n\n");
          }
        }

        const wrapper =
          control.closest(".accordion__item") ||
          control.closest("accordion-wrap") ||
          control.closest(".pv-extra-details__accordion") ||
          control.closest(".acc") ||
          control.parentElement;
        const content =
          wrapper?.querySelector?.(
            ".accordion__content, .accordion__content-container, .wysiwyg, .accordion-content-wrap-inner, .accordion-content-wrap, .acc__menu, .pv-extra-details__accordion-body, .faq-answer, .faq__answer",
          ) || control.nextElementSibling;
        return normalizeSectionText((content as HTMLElement | null)?.innerText || content?.textContent || "");
      };
      const collectFaqItemsFromRoot = (root: ParentNode | null | undefined, sourceKind: string, sourceTitle = "FAQ") => {
        if (!root) return;
        const controls = Array.from(
          root.querySelectorAll(
            "button[aria-controls], [role='tab'][aria-controls], button.accordion-title, .acc__btn, details > summary",
          ),
        ) as HTMLElement[];
        for (const control of controls.slice(0, 32)) {
          const question = normalizeQuestion(
            control.getAttribute("title") || control.getAttribute("aria-label") || control.textContent || "",
          );
          if (!looksLikeQuestion(question)) continue;
          const answer = resolveControlledBody(control);
          pushFaqItem(question, answer, sourceKind, sourceTitle);
        }

        const questionNodes = Array.from(root.querySelectorAll("[data-faq-question], .faq-question, .faq__question, h3, h4, h5")) as HTMLElement[];
        for (const node of questionNodes.slice(0, 24)) {
          const question = normalizeQuestion(node.getAttribute("data-faq-question") || node.textContent || "");
          if (!looksLikeQuestion(question)) continue;
          const answerNode =
            node.nextElementSibling ||
            node.parentElement?.querySelector?.("[data-faq-answer], .faq-answer, .faq__answer, p, div");
          const answer = normalizeAnswer((answerNode as HTMLElement | null)?.innerText || answerNode?.textContent || "");
          pushFaqItem(question, answer, sourceKind, sourceTitle);
        }
      };

      const faqContainers = new Set<ParentNode>();
      const faqHeadingNodes = Array.from(
        document.querySelectorAll("h2, h3, h4, h5, button[aria-controls], summary, .accordion-title, .acc__btn"),
      ) as HTMLElement[];
      for (const node of faqHeadingNodes.slice(0, 80)) {
        const heading = normalizeSectionText(node.textContent || "");
        if (!/\b(?:faq|frequently asked questions?|q\s*&\s*a|questions?)\b/i.test(heading)) continue;
        const targetId = node.getAttribute("aria-controls") || "";
        const target = targetId ? document.getElementById(targetId) : null;
        const container =
          target ||
          node.closest(".accordion__item") ||
          node.closest("section, article, details") ||
          node.parentElement ||
          node;
        faqContainers.add(container);
      }

      if (faqContainers.size > 0) {
        for (const container of Array.from(faqContainers)) {
          collectFaqItemsFromRoot(container, "faq_section", "FAQ");
        }
      }

      if (items.length === 0) {
        collectFaqItemsFromRoot(document, "accordion_question_answer", "FAQ");
      }

      if (items.length === 0 && /drjart\.com$/i.test(location.hostname)) {
        const root = document.querySelector("main") || document.body;
        const lines = normalizeSectionText((root as HTMLElement).innerText || root.textContent || "")
          .split("\n")
          .map((line) => normalizeSectionText(line))
          .filter(Boolean);
        const faqStart = lines.findIndex((line) =>
          /\b(?:routine faq|frequently asked questions?|faqs?)\b/i.test(line),
        );
        if (faqStart >= 0) {
          let currentQuestion = "";
          let answerLines: string[] = [];
          const flush = () => {
            if (!currentQuestion || answerLines.length === 0) return;
            pushFaqItem(currentQuestion, answerLines.join(" "), "faq_linear_text", "FAQ");
          };
          for (const line of lines.slice(faqStart + 1)) {
            if (/^(?:shop|social|need help\?|need help|dr\.?\s*jart\b|back to top|top)$/i.test(line)) break;
            if (/^(?:\d+\.\s+|q:\s*)/i.test(line) || /\?$/.test(line)) {
              flush();
              currentQuestion = line.replace(/^\d+\.\s+/, "").replace(/^q:\s*/i, "");
              answerLines = [];
              continue;
            }
            if (currentQuestion) answerLines.push(line.replace(/^a:\s*/i, ""));
          }
          flush();
        }
      }

      return items;
    })();
    const faqHtmlSnippets = Array.from(
      document.querySelectorAll(
        ".modal__content, .pv-extra-details__section-description, .pv-extra-details__accordion-body, .accordion__item, .accordion__content, .accordion__content-container",
      ),
    )
      .map((node) => (node as HTMLElement).innerHTML || "")
      .filter((html) => /\b(?:faqs?|frequently asked questions?|q\s*&\s*a)\b/i.test(html))
      .concat(
        faqAccordion.container instanceof HTMLElement && faqAccordion.container.innerHTML
          ? [faqAccordion.container.innerHTML]
          : [],
      )
      .filter(Boolean)
      .slice(0, 24);
    const renderedReviewSummary = (() => {
      const normalizeReviewText = (value: string) =>
        normalizeSectionText(value)
          .replace(/\s+/g, " ")
          .replace(/\bSame page link\.?/gi, " ")
          .trim();
      const nodes = Array.from(document.querySelectorAll("[data-bv-show='rating_summary']")) as HTMLElement[];
      for (const node of nodes.slice(0, 6)) {
        const clone = node.cloneNode(true) as HTMLElement;
        clone.querySelectorAll("style, script, noscript, meta, svg, path, polygon").forEach((child) => child.remove());
        const text = normalizeReviewText(clone.innerText || clone.textContent || "");
        const ariaLabels = Array.from(node.querySelectorAll("[aria-label]"))
          .map((child) => normalizeReviewText((child as HTMLElement).getAttribute("aria-label") || ""))
          .filter((label) => /(?:out of 5 stars|reviews?)/i.test(label));
        if (!text && ariaLabels.length === 0) continue;
        return {
          text,
          ...(ariaLabels.length > 0 ? { aria_labels: ariaLabels } : {}),
        };
      }
      return undefined;
    })();
    const detailsSections = (() => {
      const sections: ExtractedProductDetailSection[] = [];
      const seen = new Set<string>();
      const looksRelevantHeading = (heading: string) =>
        /\b(description|overview|details?|benefits?|how to(?:\s+(?:use|apply))?|usage|suggested usage|application|tutorial|pro tip|eye look|everyday eye|ingredients?|active ingredients?|inci|about|what(?:'|’)s in it\??|faq|frequently asked questions?|q\s*&\s*a|questions?|clinical(?:\s+results?)?|consumer study results?|results?|hydration|hydrates?|sebum|oil[-\s]*moisture|moisture|absorbs?|pores?|texture|finish|layer)\b/i.test(
          heading,
        );
      const isNoiseHeadingOrBody = (heading: string, body = "") =>
        /^(?:privacy overview|privacy settings|cookie settings|manage consent|consent preferences?)$/i.test(
          normalizeSectionText(heading),
        ) ||
        /^leave feedback(?: about this)?(?: cancel reply)?$/i.test(
          normalizeSectionText(heading),
        ) ||
        /\b(?:accept all|privacy policy privacy settings|some tracking technologies|strictly necessary cookies?)\b/i.test(
          normalizeSectionText(`${heading}\n${body}`),
        );
      const shouldSkipSectionNode = (node: Element | null | undefined) =>
        Boolean(
          node?.closest(
            [
              "header",
              "nav",
              "footer",
              ".header__dropdown",
              ".drawer__inner",
              ".predictive-search",
              "[class*='comparison']",
              "[id*='comparison']",
              ".cky-consent-container",
              ".cky-modal",
              ".cli-modal",
              ".cookie-notice",
              ".privacy-preferences",
              ".ot-sdk-container",
              "[id*='onetrust']",
              "[class*='onetrust']",
              "[id*='cookie']",
              "[class*='cookie']",
            ].join(", "),
          ),
        );
      const sectionImageInvalidUrlRe =
        /(placeholder\.svg|\/favicon|\/apple-touch-icon|\/logo(?:[._/-]|$)|\/sprite(?:[._/-]|$)|tracking|teads\.tv|\/MenuBanner\/|\/Library-Sites-|data:image\/svg)/i;
      const normalizeSectionImageCandidates = (raw: string | null | undefined) => {
        const trimmed = typeof raw === "string" ? raw.trim() : "";
        if (!trimmed) return [];
        return trimmed
          .split(",")
          .map((part) => part.trim().split(/\s+/)[0] || "")
          .filter(Boolean);
      };
      const collectSectionMediaUrls = (roots: Array<Element | null | undefined>) => {
        const seenMedia = new Set<string>();
        const out: string[] = [];
        const pushMediaUrl = (raw: string | null | undefined) => {
          for (const candidate of normalizeSectionImageCandidates(raw)) {
            try {
              const absolute = new URL(candidate, documentBase).toString();
              if (!/^https?:\/\//i.test(absolute)) continue;
              if (sectionImageInvalidUrlRe.test(absolute)) continue;
              if (seenMedia.has(absolute)) continue;
              seenMedia.add(absolute);
              out.push(absolute);
            } catch {
              // ignore invalid image candidates
            }
          }
        };

        for (const root of roots) {
          if (!(root instanceof Element) || shouldSkipSectionNode(root)) continue;
          const imageNodes = [
            ...(root instanceof HTMLImageElement ? [root] : []),
            ...Array.from(root.querySelectorAll("img")),
          ] as HTMLImageElement[];
          for (const imageNode of imageNodes) {
            if (shouldSkipSectionNode(imageNode)) continue;
            const width = Number(imageNode.getAttribute("width") || imageNode.naturalWidth || 0);
            const height = Number(imageNode.getAttribute("height") || imageNode.naturalHeight || 0);
            if (width > 0 && height > 0 && width < 72 && height < 72) continue;
            pushMediaUrl(imageNode.currentSrc || "");
            pushMediaUrl(imageNode.getAttribute("data-src"));
            pushMediaUrl(imageNode.getAttribute("srcset"));
            pushMediaUrl(imageNode.getAttribute("src"));
            if (out.length >= 8) return out;
          }
        }
        return out;
      };
      const pushSection = (
        headingRaw: string,
        bodyRaw: string,
        sourceKind: string,
        mediaRoots: Array<Element | null | undefined> = [],
      ) => {
        const heading = normalizeSectionText(headingRaw);
        const body = normalizeSectionText(bodyRaw);
        if (!heading || !body || !looksRelevantHeading(heading)) return;
        if (isNoiseHeadingOrBody(heading, body)) return;
        const key = `${heading.toLowerCase()}|${body.toLowerCase()}|${sourceKind.toLowerCase()}`;
        if (seen.has(key)) return;
        seen.add(key);
        const mediaUrls = collectSectionMediaUrls(mediaRoots);
        sections.push({
          heading,
          body,
          source_kind: sourceKind,
          ...(mediaUrls.length > 0 ? { media_urls: mediaUrls } : {}),
        });
      };
      const pushStructuredProseSection = (
        headingRaw: string,
        bodyRaw: string,
        sourceKind: string,
        mediaRoots: Array<Element | null | undefined> = [],
      ) => {
        const heading = normalizeSectionText(headingRaw);
        const body = normalizeSectionText(bodyRaw);
        if (!heading || !body) return;
        if (isNoiseHeadingOrBody(heading, body)) return;
        const key = `${heading.toLowerCase()}|${body.toLowerCase()}|${sourceKind.toLowerCase()}`;
        if (seen.has(key)) return;
        seen.add(key);
        const mediaUrls = collectSectionMediaUrls(mediaRoots);
        sections.push({
          heading,
          body,
          source_kind: sourceKind,
          ...(mediaUrls.length > 0 ? { media_urls: mediaUrls } : {}),
        });
      };
      const extractStructuredProseSection = (container: Element | null | undefined, sourceKind: string) => {
        if (!(container instanceof HTMLElement) || shouldSkipSectionNode(container)) return;
        const headingNode = container.querySelector(
          ".section-title, h1, h2, h3, h4, h5, h6, p.h1, p.h2, p.h3, p.h4, p.h5, p.h6, p strong, strong",
        ) as HTMLElement | null;
        const heading = normalizeSectionText(headingNode?.innerText || headingNode?.textContent || "");
        if (!heading) return;

        const paragraphParts = Array.from(container.querySelectorAll("p, li"))
          .map((node) => normalizeSectionText((node as HTMLElement).innerText || node.textContent || ""))
          .filter(Boolean)
          .filter((text) => text !== heading);
        let body = normalizeSectionText(paragraphParts.join("\n\n"));
        if (body.startsWith(heading)) {
          body = normalizeSectionText(body.slice(heading.length));
        }
        if (!body) {
          const fullText = normalizeSectionText(container.innerText || container.textContent || "");
          body = fullText.startsWith(heading)
            ? normalizeSectionText(fullText.slice(heading.length))
            : fullText.replace(heading, "").trim();
        }
        if (!body) return;
        pushStructuredProseSection(heading, body, sourceKind, [
          container.closest("image-with-text, .image-with-text, .section-stack") || container.parentElement || container,
        ]);
      };

      if (productDetailsText) {
        pushSection("Details", productDetailsText, "page_product_details");
      }
      if (howToUseText) {
        pushSection("How to Use", howToUseText, "accordion_how_to_use", [howToUseContent, howToUseAccordion.container]);
      }
      if (ingredientsMarkdownText) {
        pushSection("Ingredients", ingredientsMarkdownText, "accordion_ingredients", [
          ingredientsContent,
          ingredientsAccordion.container,
        ]);
      }
      if (!ingredientsMarkdownText && ingredientFlyoutText) {
        pushSection("Ingredients", ingredientFlyoutText, "ingredients_flyout");
      }
      if (ingredientsDisclaimerText) {
        pushSection("Ingredients Disclaimer", ingredientsDisclaimerText, "accordion_ingredients_disclaimer");
      }
      if (keyIngredientsText) {
        pushSection("Key Ingredients", keyIngredientsText, "page_key_ingredients");
      }
      if (customMetafieldTab1Text) {
        pushSection("Benefits", customMetafieldTab1Text, "custom_metafield_tab_1");
      }
      if (customMetafieldTab2Text) {
        pushSection("Details", customMetafieldTab2Text, "custom_metafield_tab_2");
      }
      if (customMetafieldKeyIngredientsText) {
        pushSection("Key Ingredients", customMetafieldKeyIngredientsText, "custom_metafield_key_ingredients");
      }
      if (customMetafieldFullIngredientsText) {
        pushSection("Ingredients", customMetafieldFullIngredientsText, "custom_metafield_full_ingredients");
      }
      if (customMetafieldHowToText) {
        pushSection("How to Use", customMetafieldHowToText, "custom_metafield_how_to_use");
      }

      const productContentRoots = Array.from(
        document.querySelectorAll(
          [
            ".figma-html-wrapper",
            ".custom-figma-block",
            ".figma-tab2-wrapper",
            ".tab2-container",
            ".tab3-html-wrapper",
            ".tab3-container",
            ".tab-panel",
            ".left-section-routine",
            ".routine-content",
            ".woocommerce-product-details__short-description",
            ".woocommerce-Tabs-panel",
            ".woocommerce-tabs",
            ".wc-tab",
            ".wpb-content-wrapper",
            ".vc_tta-container",
            ".vc_tta-panels",
            ".vc_tta-panel",
            "[id*='__new_custom_pdp']",
            "[id*='section_custom_content']",
            ".product__description",
          ].join(", "),
        ),
      ) as HTMLElement[];
      for (const root of productContentRoots.filter((node) => !shouldSkipSectionNode(node)).slice(0, 24)) {
        const headings = Array.from(root.querySelectorAll("h2, h3, h4")) as HTMLElement[];
        for (const headingNode of headings.filter((node) => looksRelevantHeading(node.textContent || "")).slice(0, 16)) {
          if (shouldSkipSectionNode(headingNode)) continue;
          const heading = headingNode.textContent || "";
          const bodyParts: string[] = [];
          const mediaRoots: Element[] = [];
          let cursor = headingNode.nextElementSibling;
          let guard = 0;
          while (cursor && guard < 6) {
            if (/^H[2-4]$/i.test(cursor.tagName)) break;
            if (!shouldSkipSectionNode(cursor)) {
              const text = (cursor as HTMLElement).innerText || cursor.textContent || "";
              if (normalizeSectionText(text)) {
                bodyParts.push(text);
                mediaRoots.push(cursor);
              }
            }
            cursor = cursor.nextElementSibling;
            guard += 1;
          }

          if (bodyParts.length === 0) {
            const parent = headingNode.parentElement;
            const parentText = normalizeSectionText(parent?.innerText || parent?.textContent || "");
            const headingText = normalizeSectionText(heading);
            const body = parentText.replace(headingText, "").trim();
            if (body) {
              bodyParts.push(body);
              if (parent) mediaRoots.push(parent);
            }
          }

          if (bodyParts.length > 0) {
            pushSection(heading, bodyParts.join("\n\n"), "pdp_content_heading", mediaRoots);
          }
        }
      }

      const controls = Array.from(
        document.querySelectorAll("button[aria-controls], [role='tab'][aria-controls]"),
      ) as HTMLElement[];
      for (const control of controls.filter((node) => looksRelevantHeading(node.textContent || "")).slice(0, 24)) {
        if (shouldSkipSectionNode(control)) continue;
        const heading =
          control.getAttribute("title") ||
          control.getAttribute("aria-label") ||
          control.textContent ||
          "";
        const targetId = control.getAttribute("aria-controls") || "";
        if (!targetId) continue;
        const target = document.getElementById(targetId);
        if (!target) continue;
        const body = (target as HTMLElement).innerText || target.textContent || "";
        pushSection(heading, body, "accordion_control", [target, control.parentElement]);
      }

      const accordionButtons = Array.from(
        document.querySelectorAll(
          "button.accordion-title, .accordion__toggle, .acc__btn, .module-accordion .item .trigger, .module-accordion .item .text, .vc_tta-panel-title a, .vc_tta-panel-heading a",
        ),
      ) as HTMLElement[];
      for (const button of accordionButtons.filter((node) => looksRelevantHeading(node.textContent || "")).slice(0, 24)) {
        if (shouldSkipSectionNode(button)) continue;
        const heading =
          button.getAttribute("title") ||
          button.getAttribute("aria-label") ||
          button.textContent ||
          "";
        let body = "";
        const targetId = button.getAttribute("aria-controls") || "";
        if (targetId) {
          const target = document.getElementById(targetId);
          body = (target as HTMLElement | null)?.innerText || target?.textContent || "";
        }
        if (!normalizeSectionText(body)) {
          const wrapper =
            button.closest("accordion-wrap") ||
            button.closest(".pv-extra-details__accordion") ||
            button.closest(".module-accordion .item") ||
            button.closest(".vc_tta-panel") ||
            button.closest(".acc") ||
            button.parentElement;
          const content =
            wrapper?.querySelector?.(
              ".accordion-content-wrap-inner, .accordion-content-wrap, .acc__menu, .pv-extra-details__accordion-body, .vc_tta-panel-body, .wpb_wrapper, .wpb_text_column, .details, .inner-text",
            ) ||
            button.nextElementSibling;
          body = (content as HTMLElement | null)?.innerText || content?.textContent || "";
        }
        pushSection(heading, body, "accordion_button", [
          targetId ? document.getElementById(targetId) : null,
          button.closest("accordion-wrap") ||
            button.closest(".pv-extra-details__accordion") ||
            button.closest(".module-accordion .item") ||
            button.closest(".vc_tta-panel") ||
            button.closest(".acc") ||
            button.parentElement,
          button.nextElementSibling,
        ]);
      }

      const detailSummaries = Array.from(document.querySelectorAll("details > summary")) as HTMLElement[];
      for (const summary of detailSummaries.filter((node) => looksRelevantHeading(node.textContent || "")).slice(0, 24)) {
        if (shouldSkipSectionNode(summary)) continue;
        const heading =
          summary.getAttribute("title") ||
          summary.getAttribute("aria-label") ||
          summary.textContent ||
          "";
        const details = summary.closest("details");
        let body = "";
        if (details) {
          const bodyParts = Array.from(details.children)
            .filter((child) => child !== summary)
            .map((child) => (child as HTMLElement).innerText || child.textContent || "")
            .filter((text) => normalizeSectionText(text));
          body = bodyParts.join("\n\n");
        }
        if (!normalizeSectionText(body)) {
          const content = summary.nextElementSibling;
          body = (content as HTMLElement | null)?.innerText || content?.textContent || "";
        }
        pushSection(heading, body, "details_summary", [details, summary.nextElementSibling]);
      }

      const sectionStackProseNodes = Array.from(
        document.querySelectorAll(".section-stack .prose, image-with-text .prose, .image-with-text .prose"),
      ) as HTMLElement[];
      for (const proseNode of sectionStackProseNodes.slice(0, 32)) {
        const sourceKind =
          proseNode.closest("image-with-text, .image-with-text") ? "page_image_with_text_prose" : "page_section_stack_prose";
        extractStructuredProseSection(proseNode, sourceKind);
      }

      const productModalNodes = Array.from(document.querySelectorAll("product-modal")) as HTMLElement[];
      for (const productModal of productModalNodes.slice(0, 16)) {
        const heading =
          productModal.querySelector("[data-popup-open], .radio__legend__link")?.textContent ||
          productModal.querySelector("h1, h2, h3")?.textContent ||
          "";
        const bodyNode = productModal.querySelector(
          "dialog .product-modal__content, dialog, .product-modal__content, [class*='modal__content']",
        );
        const body = (bodyNode as HTMLElement | null)?.innerText || bodyNode?.textContent || "";
        pushSection(heading, body, "product_modal_content", [bodyNode, productModal]);
      }

      const modalNodes = Array.from(
        document.querySelectorAll("aside.modal, .modal.js-modal, dialog.product-modal, [role='dialog']"),
      );
      for (const modal of modalNodes.slice(0, 16)) {
        const heading =
          modal.querySelector(".modal__header h1, .modal__header h2, .modal__header h3, h1, h2, h3")?.textContent || "";
        const bodyNode = modal.querySelector(".modal__content, .modal-content, [class*='modal__content']");
        const body = (bodyNode as HTMLElement | null)?.innerText || bodyNode?.textContent || "";
        pushSection(heading, body, "modal_content", [bodyNode, modal]);
      }

      const headingNodes = Array.from(document.querySelectorAll("h2, h3, h4"));
      for (const headingNode of headingNodes.filter((node) => looksRelevantHeading(node.textContent || "")).slice(0, 24)) {
        if (shouldSkipSectionNode(headingNode)) continue;
        const heading = headingNode.textContent || "";
        const bodyParts: string[] = [];
        const mediaRoots: Element[] = [];
        let cursor = headingNode.nextElementSibling;
        let guard = 0;
        while (cursor && guard < 4) {
          if (/^H[2-4]$/i.test(cursor.tagName)) break;
          const text = (cursor as HTMLElement).innerText || cursor.textContent || "";
          if (normalizeSectionText(text)) {
            bodyParts.push(text);
            mediaRoots.push(cursor);
          }
          cursor = cursor.nextElementSibling;
          guard += 1;
        }
        if (bodyParts.length > 0) {
          pushSection(heading, bodyParts.join("\n\n"), "heading_sibling", mediaRoots);
        }
      }

      if (/drjart\.com$/i.test(location.hostname)) {
        const root = (document.querySelector("main") as HTMLElement | null) || document.body;
        const linearLines = normalizeSectionText(root.innerText || root.textContent || "")
          .split("\n")
          .map((line) => normalizeSectionText(line))
          .filter(Boolean);
        const narrativeImages = collectSectionMediaUrls(
          Array.from(root.querySelectorAll("img")).filter((node) => {
            if (!(node instanceof HTMLImageElement)) return false;
            const rect = node.getBoundingClientRect();
            const width = Number(node.getAttribute("width") || node.naturalWidth || rect.width || 0);
            const height = Number(node.getAttribute("height") || node.naturalHeight || rect.height || 0);
            return rect.top > 1000 && width >= 160 && height >= 160;
          }),
        );
        const uniquePreserveOrder = (values: string[]) => {
          const out: string[] = [];
          const seenValues = new Set<string>();
          for (const value of values) {
            const normalized = normalizeSectionText(value);
            if (!normalized) continue;
            const key = normalized.toLowerCase();
            if (seenValues.has(key)) continue;
            seenValues.add(key);
            out.push(normalized);
          }
          return out;
        };
        const clinicalNarrativeImages = narrativeImages.filter((url) =>
          /(?:testingresults|claims|clinical|claim|results|desktop_ba|module_03|_test\/|cryosorbet_model)/i.test(url),
        );
        const regimenNarrativeImages = narrativeImages.filter((url) =>
          /(?:regimen[_-]?step|routine|module_01|texture|howto|how-to|step)/i.test(url),
        );
        const concernNarrativeImages = narrativeImages.filter((url) => /Concern_Icon/i.test(url));
        const treatmentNarrativeImages = narrativeImages.filter((url) => /Treatment_Icon/i.test(url));
        const resultNarrativeImages = narrativeImages.filter((url) => /Result_Icon/i.test(url));
        const pushLinearSection = (
          headingRaw: string,
          bodyLines: string[],
          sourceKind: string,
          mediaUrls: string[] = [],
        ) => {
          const heading = normalizeSectionText(headingRaw);
          const body = normalizeSectionText(uniquePreserveOrder(bodyLines).join("\n"));
          if (!heading || !body || !looksRelevantHeading(heading)) return;
          const key = `${heading.toLowerCase()}|${body.toLowerCase()}|${sourceKind.toLowerCase()}`;
          if (seen.has(key)) return;
          seen.add(key);
          sections.push({
            heading,
            body,
            source_kind: sourceKind,
            ...(mediaUrls.length > 0 ? { media_urls: mediaUrls } : {}),
          });
        };

        const productDetailsIndices = linearLines.reduce<number[]>((acc, line, index) => {
          if (/^product details$/i.test(line)) acc.push(index);
          return acc;
        }, []);
        const labelDrivenDetailsIndex = linearLines.findIndex((line) =>
          /^(?:clinical results:?|sensory results:?|results:?|what makes it unique:?|what(?:'|’)s in it:?|free from:?)$/i.test(line),
        );
        const firstDetailsIndex = productDetailsIndices[0] ?? labelDrivenDetailsIndex;
        const detailStopIndex = linearLines.findIndex(
          (line, index) =>
            index > firstDetailsIndex &&
            (/^\$\d/.test(line) ||
              /^qty:?$/i.test(line) ||
              /^\d+\s*mL\b/i.test(line) ||
              /^how to use$/i.test(line) ||
              /^ingredients$/i.test(line) ||
              /\b(?:routine faq|frequently asked questions?|faqs?)\b/i.test(line) ||
              /^proven\. effective\.?$/i.test(line)),
        );
        if (firstDetailsIndex >= 0) {
          const detailLines = linearLines.slice(
            firstDetailsIndex,
            detailStopIndex > firstDetailsIndex ? detailStopIndex : Math.min(linearLines.length, firstDetailsIndex + 48),
          );
          const captureLabelBlock = (heading: string, labelRe: RegExp, nextLabelRes: RegExp[]) => {
            const startIndex = detailLines.findIndex((line) => labelRe.test(line));
            if (startIndex < 0) return;
            const endIndex = detailLines.findIndex(
              (line, index) => index > startIndex && nextLabelRes.some((pattern) => pattern.test(line)),
            );
            const bodyLines = detailLines
              .slice(startIndex + 1, endIndex > startIndex ? endIndex : undefined)
              .filter((line) => !labelRe.test(line));
            pushLinearSection(heading, bodyLines, "drjart_linear_details");
          };
          captureLabelBlock(
            "Clinical Results",
            /^(?:clinical results:?|results:?)$/i,
            [/^sensory results:?$/i, /^what makes it unique:?$/i, /^what(?:'|’)s in it:?$/i, /^free from:?$/i],
          );
          captureLabelBlock(
            "Sensory Results",
            /^sensory results:?$/i,
            [/^what makes it unique:?$/i, /^what(?:'|’)s in it:?$/i, /^free from:?$/i],
          );
          captureLabelBlock("Benefits", /^what makes it unique:?$/i, [/^what(?:'|’)s in it:?$/i, /^free from:?$/i]);
          captureLabelBlock("Key Ingredients", /^what(?:'|’)s in it:?$/i, [/^free from:?$/i]);
          captureLabelBlock("Free Of", /^free from:?$/i, []);
        }

        const faqStart = linearLines.findIndex((line, index) => index > firstDetailsIndex && /\b(?:routine faq|frequently asked questions?|faqs?)\b/i.test(line));
        const storyStartIndex = linearLines.findIndex(
          (line, index) =>
            index > firstDetailsIndex &&
            (/^proven\. effective\.?$/i.test(line) ||
              /^rapid results you can see\.?$/i.test(line) ||
              /^how to use your /i.test(line) ||
              /^korean skincare routine/i.test(line) ||
              /^concern$/i.test(line) ||
              /^treatment$/i.test(line) ||
              /^result$/i.test(line)),
        );
        const storyLines = storyStartIndex >= 0
          ? linearLines.slice(storyStartIndex, faqStart > storyStartIndex ? faqStart : linearLines.length)
          : [];
        if (storyLines.length > 0) {
          const storySectionDefs = [
            {
              match: /^proven\. effective\.?$/i,
              heading: "Clinical Results",
              resolveMedia: () => clinicalNarrativeImages.slice(0, 2),
            },
            {
              match: /^rapid results you can see\.?$/i,
              heading: "Clinical Results",
              resolveMedia: () => clinicalNarrativeImages.slice(0, 2),
            },
            {
              match: /^how to use your /i,
              heading: "How to Use",
              resolveMedia: () => regimenNarrativeImages.slice(0, 2),
            },
            {
              match: /^korean skincare routine/i,
              heading: "How to Use",
              resolveMedia: () => regimenNarrativeImages.slice(0, 3),
            },
            {
              match: /^concern$/i,
              heading: "Best For",
              resolveMedia: () => concernNarrativeImages.slice(0, 1),
            },
            {
              match: /^treatment$/i,
              heading: "Key Ingredients",
              resolveMedia: () => treatmentNarrativeImages.slice(0, 1),
            },
            {
              match: /^result$/i,
              heading: "Benefits",
              resolveMedia: () => resultNarrativeImages.slice(0, 1),
            },
          ] as const;
          for (let index = 0; index < storyLines.length; index += 1) {
            const def = storySectionDefs.find((entry) => entry.match.test(storyLines[index] || ""));
            if (!def) continue;
            const bodyLines: string[] = [];
            let cursor = index + 1;
            while (cursor < storyLines.length) {
              const nextLine = storyLines[cursor] || "";
              if (storySectionDefs.some((entry) => entry.match.test(nextLine))) break;
              if (/^(?:more moisture\. more strength\. now bounce\.?|we love a good skincare post\.?)$/i.test(nextLine)) break;
              bodyLines.push(nextLine);
              cursor += 1;
            }
            const mediaUrls = def.resolveMedia();
            pushLinearSection(def.heading, bodyLines, "drjart_linear_story", mediaUrls);
            index = cursor - 1;
          }
        }
      }

      return sections;
    })();
    const activeIngredientsText =
      detailsSections.find((section) => /\bactive ingredients?\b/i.test(section.heading))?.body || undefined;

    return {
      title,
      canonical,
      metaDescription,
      priceTexts,
      imageCandidates,
      scripts,
      embeddedProductScripts,
      domVariants,
      productVolumeText,
      productDetailsText,
      howToUseText,
      ingredientsMarkdownText,
      ingredientsDisclaimerText,
      activeIngredientsText,
      detailsSections,
      faqItems,
      faqHtmlSnippets,
      okendoMetafieldJson,
      renderedReviewSummary,
    };
  })) as RawScrapedPageSignals;
  const { renderedReviewSummary: rawRenderedReviewSummary, ...restScraped } = scraped;
  const parsedRenderedReviewSummary = parseRenderedBazaarvoiceReviewSummary(rawRenderedReviewSummary);
  return {
    ...restScraped,
    detailsSections: normalizeStructuredProseDetailSections(restScraped.detailsSections),
    ...(parsedRenderedReviewSummary ? { renderedReviewSummary: parsedRenderedReviewSummary } : {}),
  };
}

function extractSkuImageToken(value: string | undefined) {
  const normalized = cleanText(value);
  if (!normalized) return "";
  const match = normalized.match(/sku_([A-Z0-9]{4,12})_/i) || normalized.match(/\b([A-Z0-9]{4,12})\b/i);
  return match?.[1]?.toUpperCase() || "";
}

function buildSkuScopedImageUrlMap(imageUrls: string[]) {
  const out = new Map<string, string[]>();
  for (const imageUrl of imageUrls) {
    const normalized = cleanText(imageUrl);
    if (!normalized || /PosterImage_videos|\/videos\//i.test(normalized)) continue;
    const token = extractSkuImageToken(normalized);
    if (!token) continue;
    const existing = out.get(token) || [];
    if (!existing.includes(normalized)) existing.push(normalized);
    out.set(token, existing);
  }
  return out;
}

function parseComparablePrice(raw: unknown) {
  const normalized = normalizePrice(raw);
  const parsed = Number.parseFloat(String(normalized).replace(/[^0-9.]+/g, ""));
  return Number.isFinite(parsed) ? Number(parsed.toFixed(2)) : null;
}

const PRODUCT_IMAGE_NOISE_RE =
  /(placeholder|favicon|apple-touch-icon|brands?-logo|logo(?:[._/-]|$)|sprite(?:[._/-]|$)|tracking|teads\.tv|menubanner|library-sites|navbar|email-signup|popup)/i;

function isCleanProductImageAssetUrl(rawUrl: string, baseUrl: string): boolean {
  const value = cleanText(rawUrl);
  if (!value) return false;
  if (isLikelyProductUrlShared(value, baseUrl)) return false;
  try {
    const parsed = new URL(value, baseUrl);
    if (!/^https?:$/i.test(parsed.protocol)) return false;
    const pathAndSearch = parsed.pathname + parsed.search;
    if (PRODUCT_IMAGE_NOISE_RE.test(pathAndSearch)) return false;
    return /\.(?:png|jpe?g|webp|gif|avif)(?:$|[?#])/i.test(pathAndSearch);
  } catch {
    return false;
  }
}

function productImageDedupeKey(rawUrl: string, baseUrl: string): string {
  try {
    const parsed = new URL(rawUrl, baseUrl);
    for (const param of ["sw", "sh", "sm", "w", "h", "width", "height"]) {
      parsed.searchParams.delete(param);
    }
    const search = Array.from(parsed.searchParams.entries())
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key}=${value}`)
      .join("&");
    return `${parsed.origin}${parsed.pathname}${search ? `?${search}` : ""}`;
  } catch {
    return rawUrl;
  }
}

function filterCleanProductImageAssetUrls(baseUrl: string, urls: Array<string | undefined>, limit = 12) {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const url of urls) {
    const value = cleanText(url);
    if (!value || !isCleanProductImageAssetUrl(value, baseUrl)) continue;
    const absolute = new URL(value, baseUrl).toString();
    const dedupeKey = productImageDedupeKey(absolute, baseUrl);
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    out.push(absolute);
    if (out.length >= limit) break;
  }
  return out;
}

export function buildProductFromPageSignals(params: {
  extracted: ScrapedPageSignals;
  pageLooksLikeProduct: boolean;
  sourceUrl: string;
  baseUrl: string;
  verbose: boolean;
  log: Logger;
}): ExtractedProduct | null {
  const { extracted } = params;
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

  const productObj = pickBestJsonLdObjectForPage({
    candidates: objects.filter((o) => isType(o, "Product")),
    pageUrl: params.sourceUrl,
    canonicalUrl: extracted.canonical,
    baseUrl: params.baseUrl,
  });
  const productGroupObj = pickBestJsonLdObjectForPage({
    candidates: objects.filter((o) => isType(o, "ProductGroup")),
    pageUrl: params.sourceUrl,
    canonicalUrl: extracted.canonical,
    baseUrl: params.baseUrl,
  });
  const variantProducts = normalizeJsonLdObjects(productGroupObj?.hasVariant).filter((o) => isType(o, "Product"));
  const primaryProductObj =
    productObj ||
    pickBestJsonLdObjectForPage({
      candidates: variantProducts,
      pageUrl: params.sourceUrl,
      canonicalUrl: extracted.canonical,
      baseUrl: params.baseUrl,
    }) ||
    productGroupObj ||
    null;

  if (!productObj && params.verbose) {
    params.log("warn", "> No JSON-LD Product schema found. Falling back to title/meta/price extraction.");
  }
  if (!primaryProductObj && !params.pageLooksLikeProduct) {
    if (params.verbose) {
      params.log("warn", `> Skipping non-product candidate: ${params.sourceUrl}`);
    }
    return null;
  }

  const productTitle = (
    typeof productGroupObj?.name === "string" ? productGroupObj.name : typeof primaryProductObj?.name === "string" ? primaryProductObj.name : extracted.title
  ).trim() || extracted.title;
  const productUrl = canonicalizeUrlShared(
    toAbsoluteUrlShared(
      params.baseUrl,
      extracted.canonical || (typeof primaryProductObj?.url === "string" ? primaryProductObj.url : params.sourceUrl),
    ),
    params.baseUrl,
  );
  if (
    isKnownCrossProductResolutionMismatch({
      sourceUrl: params.sourceUrl,
      extractedUrl: productUrl,
      extractedTitle: productTitle,
    })
  ) {
    if (params.verbose) {
      params.log(
        "warn",
        `> Rejecting cross-product resolution mismatch for seed URL: ${params.sourceUrl} -> ${productUrl} (${productTitle})`,
      );
    }
    return null;
  }

  const imageRaw = primaryProductObj?.image ?? productGroupObj?.image;
  const embeddedShopifyPayloadFields = extractShopifyEmbeddedProductPayloadPdpFields(extracted.embeddedProductScripts);
  const productImageUrls = filterCleanProductImageAssetUrls(params.baseUrl, [
    ...resolveStructuredImageUrls(params.baseUrl, [
      imageRaw,
      productGroupObj?.image,
      embeddedShopifyPayloadFields.imageUrls,
      extracted.imageCandidates,
    ]),
    ...variantProducts.flatMap((variantProduct) => resolveStructuredImageUrls(params.baseUrl, variantProduct.image)),
  ]);
  const imageUrl = productImageUrls[0] || "";

  const officialText = choosePreferredProductOverview({
    structured:
      (typeof primaryProductObj?.description === "string" ? primaryProductObj.description : undefined) ||
      (typeof productGroupObj?.description === "string" ? productGroupObj.description : undefined) ||
      embeddedShopifyPayloadFields.descriptionRaw,
    detailed: typeof extracted.productDetailsText === "string" ? extracted.productDetailsText : undefined,
    meta: extracted.metaDescription,
  });
  const officialTextPdpFields = extractShopifyBodyHtmlPdpFields(officialText);

  const offersRaw = primaryProductObj?.offers;
  const offers = normalizeJsonLdOffers(offersRaw);

  const domMetaBySku = new Map<string, DomVariantMeta>();
  for (const meta of extracted.domVariants || []) {
    if (!meta.sku) continue;
    domMetaBySku.set(meta.sku, meta);
  }

  const cleanedExtractedDetailsSections = (extracted.detailsSections || []).map((section) => {
    const normalizedHeading = normalizeDetailSectionHeading(section.heading);
    return normalizedHeading === "How to Use"
      ? {
          ...section,
          body: stripInlineFaqText(section.body),
        }
      : section;
  }).filter((section) => {
    const heading = cleanText(section.heading);
    const body = cleanText(section.body);
    const productDetailsBody = cleanText(extracted.productDetailsText);
    if (!heading || !body) return false;
    if (/\(tab expanded\)/i.test(heading)) return false;
    if (/\bloading questions\b/i.test(body)) return false;
    if (section.source_kind === "heading_sibling" && /^\$\d+(?:\.\d{2})?$/.test(body)) return false;
    if (section.source_kind === "heading_sibling" && /\bmore\b$/i.test(body)) return false;
    if (section.source_kind === "accordion_button" && productDetailsBody && body === productDetailsBody && heading !== "Details") {
      return false;
    }
    return true;
  });
  const inlineFaqItems = (extracted.faqHtmlSnippets || []).flatMap((html) =>
    extractInlineFaqItemsFromHtml(html, {
      sourceKind: "inline_html_faq",
      sourceUrl: productUrl,
      sourceTitle: "FAQ",
    }),
  );
  const combinedFaqItems = filterUsefulFaqItems([...(extracted.faqItems || []), ...inlineFaqItems]);

  const mergedDetailsSections = dedupeDetailSections([
    ...cleanedExtractedDetailsSections,
    ...embeddedShopifyPayloadFields.detailsSections,
    ...officialTextPdpFields.detailsSections,
  ]);
  const howToUseText =
    stripInlineFaqText(typeof extracted.howToUseText === "string" ? extracted.howToUseText.trim() : "") ||
    embeddedShopifyPayloadFields.howToUseRaw ||
    officialTextPdpFields.howToUseRaw ||
    undefined;
  const rawIngredientsMarkdownText =
    (typeof extracted.ingredientsMarkdownText === "string" ? extracted.ingredientsMarkdownText.trim() : "") ||
    embeddedShopifyPayloadFields.ingredientsRaw ||
    officialTextPdpFields.ingredientsRaw ||
    undefined;
  const urlProductSlug = (() => {
    try {
      return decodeURIComponent(new URL(productUrl).pathname.split("/").filter(Boolean).pop() || "");
    } catch {
      return "";
    }
  })();
  const titleVariant = splitTitleIntoBaseAndVariant(productTitle)?.variantLabel || "";
  const scopedIngredientsMarkdownText =
    extractVariantScopedIngredientListText(rawIngredientsMarkdownText, [
      titleVariant,
      productTitle,
      extracted.title,
      urlProductSlug,
    ]) || rawIngredientsMarkdownText;
  const ingredientsMarkdownText =
    extractLikelyFullIngredientListText(scopedIngredientsMarkdownText) || scopedIngredientsMarkdownText || undefined;
  const ingredientsDisclaimerText =
    typeof extracted.ingredientsDisclaimerText === "string" ? extracted.ingredientsDisclaimerText.trim() : undefined;
  const activeIngredientsText =
    (typeof extracted.activeIngredientsText === "string" ? extracted.activeIngredientsText.trim() : "") ||
    embeddedShopifyPayloadFields.activeIngredientsRaw ||
    officialTextPdpFields.activeIngredientsRaw ||
    undefined;
  const derivedPdpBodies = deriveProductPdpModuleBodies({
    ingredientsMarkdownText,
    activeIngredientsText,
    howToUseText,
    detailsSections: mergedDetailsSections,
  });
  const productPdpFields = buildProductPdpFields({
    descriptionRaw: officialText || extracted.productDetailsText || extracted.metaDescription,
    detailsSections: mergedDetailsSections,
    ingredientsRaw: derivedPdpBodies.ingredientsRaw,
    activeIngredientsRaw: derivedPdpBodies.activeIngredientsRaw,
    howToUseRaw: derivedPdpBodies.howToUseRaw,
    faqItems: combinedFaqItems,
    fieldSources: {
      description_raw: [
        officialText ? "structured_overview" : "",
        !officialText && embeddedShopifyPayloadFields.descriptionRaw ? "embedded_shopify_product_payload" : "",
        !officialText && extracted.productDetailsText ? "page_product_details" : "",
        !officialText && !extracted.productDetailsText && extracted.metaDescription ? "meta_description" : "",
      ],
      details_sections: mergedDetailsSections.map((section) => section.source_kind),
      ingredients_raw: [
        typeof extracted.ingredientsMarkdownText === "string" &&
        !!extractLikelyFullIngredientListText(extracted.ingredientsMarkdownText)
          ? "page_ingredients_section"
          : "",
        !extracted.ingredientsMarkdownText && embeddedShopifyPayloadFields.ingredientsRaw
          ? "embedded_shopify_product_payload"
          : "",
        !extracted.ingredientsMarkdownText && officialTextPdpFields.ingredientsRaw
          ? "structured_overview_labeled_ingredients"
          : "",
        !(typeof extracted.ingredientsMarkdownText === "string" &&
          !!extractLikelyFullIngredientListText(extracted.ingredientsMarkdownText)) &&
        !embeddedShopifyPayloadFields.ingredientsRaw &&
        !officialTextPdpFields.ingredientsRaw &&
        derivedPdpBodies.ingredientsRaw
          ? "details_section_ingredients"
          : "",
      ],
      active_ingredients_raw: [
        extracted.activeIngredientsText ? "page_active_ingredients_section" : "",
        !extracted.activeIngredientsText && embeddedShopifyPayloadFields.activeIngredientsRaw
          ? "embedded_shopify_product_payload"
          : "",
        !extracted.activeIngredientsText && officialTextPdpFields.activeIngredientsRaw
          ? "structured_overview_labeled_active_ingredients"
          : "",
        !activeIngredientsText && derivedPdpBodies.activeIngredientsRaw
          ? "details_section_active_ingredients"
          : "",
      ],
      how_to_use_raw: [
        extracted.howToUseText ? "page_how_to_use_section" : "",
        !extracted.howToUseText && embeddedShopifyPayloadFields.howToUseRaw
          ? "embedded_shopify_product_payload"
          : "",
        !extracted.howToUseText && officialTextPdpFields.howToUseRaw
          ? "structured_overview_labeled_how_to_use"
          : "",
        !howToUseText && derivedPdpBodies.howToUseRaw
          ? "details_section_how_to_use"
          : "",
      ],
      faq_items: [
        combinedFaqItems.some((item) => item.source_kind !== "inline_html_faq") ? "page_faq_section" : "",
        combinedFaqItems.some((item) => item.source_kind === "inline_html_faq") ? "inline_html_faq" : "",
      ],
    },
  });
  const productSizeEvidence = extractProductSizeEvidence(
    typeof primaryProductObj?.size === "string" ? primaryProductObj.size : undefined,
    typeof productGroupObj?.size === "string" ? productGroupObj.size : undefined,
    variantProducts.length === 1 && typeof variantProducts[0]?.size === "string"
      ? variantProducts[0].size
      : undefined,
    extracted.productVolumeText,
    extracted.productDetailsText,
    ...mergedDetailsSections.flatMap((section) => [section.heading, section.body]),
    productTitle,
    productUrl,
    ...productImageUrls,
  );
  const productSizeOptionValue = productSizeEvidence.optionValue;
  const skuScopedProductImages = buildSkuScopedImageUrlMap(productImageUrls);

  let variants: ExtractedVariant[] = finalizeExtractedVariants(
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
          const variantSpecificImageUrls = filterCleanProductImageAssetUrls(params.baseUrl, [
            ...resolveStructuredImageUrls(params.baseUrl, [variantImageRaw, variantOffer?.image]),
            ...resolveStructuredImageUrls(params.baseUrl, [domMeta?.image_urls, domMeta?.image_url]),
          ]);
          const variantImageTokens = dedupeStringList(
            [extractSkuImageToken(sku), ...variantSpecificImageUrls.map((url) => extractSkuImageToken(url))]
              .filter(Boolean) as string[],
          );
          const skuScopedVariantImages = dedupeStringList(
            variantImageTokens.flatMap((token) => skuScopedProductImages.get(token) || []),
          );
          const variantImageUrls = filterCleanProductImageAssetUrls(params.baseUrl,
            variantSpecificImageUrls.length > 1
              ? variantSpecificImageUrls
              : variantSpecificImageUrls.length > 0 && skuScopedVariantImages.length > 0
                ? [...variantSpecificImageUrls, ...skuScopedVariantImages]
                : skuScopedVariantImages.length > 0
                  ? skuScopedVariantImages
                  : variantSpecificImageUrls.length > 0
                    ? variantSpecificImageUrls
                    : productImageUrls,
          );
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
              domMeta?.price ??
              extracted.priceTexts[idx] ??
              extracted.priceTexts[0],
          );
          const stock = stockFromAvailability(offer.availability);
          const optionValueFromOffer =
            (typeof offer.name === "string" ? offer.name.trim() : "") ||
            (typeof offer.description === "string" ? offer.description.trim() : "");
          const offerImageRaw = offer.image;
          const offerSpecificImageUrls = filterCleanProductImageAssetUrls(params.baseUrl, [
            ...resolveStructuredImageUrls(params.baseUrl, [offerImageRaw, domMeta?.image_urls, domMeta?.image_url]),
          ]);
          const offerImageTokens = dedupeStringList(
            [extractSkuImageToken(sku), ...offerSpecificImageUrls.map((url) => extractSkuImageToken(url))]
              .filter(Boolean) as string[],
          );
          const skuScopedOfferImages = dedupeStringList(
            offerImageTokens.flatMap((token) => skuScopedProductImages.get(token) || []),
          );
          const offerFallbackImageUrls = filterCleanProductImageAssetUrls(params.baseUrl, [
            ...resolveStructuredImageUrls(params.baseUrl, [imageRaw, extracted.imageCandidates]),
            ...productImageUrls,
          ]);
          const offerImageUrls = filterCleanProductImageAssetUrls(params.baseUrl,
            offerSpecificImageUrls.length > 1
              ? offerSpecificImageUrls
              : offerSpecificImageUrls.length > 0 && skuScopedOfferImages.length > 0
                ? [...offerSpecificImageUrls, ...skuScopedOfferImages]
                : skuScopedOfferImages.length > 0
                  ? skuScopedOfferImages
                  : offerSpecificImageUrls.length > 0
                    ? offerSpecificImageUrls
                    : offerFallbackImageUrls,
          );
          const displayableDomOptionValue =
            domMeta?.option_value && !isGenericOfferOptionValue(domMeta.option_value, productTitle)
              ? domMeta.option_value
              : "";
          const sizeOptionValue = displayableDomOptionValue
            ? ""
            : extractProductSizeOptionValue(
                domMeta?.option_value,
                optionValueFromOffer,
                productSizeOptionValue,
                productTitle,
                productUrl,
                typeof offer.url === "string" ? offer.url : "",
                ...offerImageUrls,
              );
          const displayableOfferOptionValue = isGenericOfferOptionValue(optionValueFromOffer, productTitle)
            ? ""
            : optionValueFromOffer;

          const optionValue = displayableDomOptionValue || sizeOptionValue || displayableOfferOptionValue || sku;
          const optionName =
            (displayableDomOptionValue ? domMeta?.option_name : undefined) || (sizeOptionValue ? "Size" : "Offer");

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
            option_name: productSizeOptionValue ? "Size" : "Offer",
            option_value: productSizeOptionValue || "Default",
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
            ad_copy: generateMockAdCopy(
              productTitle,
              productSizeOptionValue || "Default",
              normalizePrice(extracted.priceTexts[0]),
            ),
          },
        ],
  );

  if (variants.length > 1) {
    const pagePrice = parseComparablePrice(extracted.priceTexts[0]);
    const priceMatchedIndices =
      pagePrice == null
        ? []
        : variants
            .map((variant, index) => ({ index, price: parseComparablePrice(variant.price) }))
            .filter((entry) => entry.price != null && Math.abs((entry.price as number) - pagePrice) < 0.001)
            .map((entry) => entry.index);
    const densestVariantIndex = variants.reduce((bestIndex, variant, index, all) => {
      const bestImages = Array.isArray(all[bestIndex]?.image_urls) ? all[bestIndex].image_urls.length : 0;
      const currentImages = Array.isArray(variant.image_urls) ? variant.image_urls.length : 0;
      return currentImages > bestImages ? index : bestIndex;
    }, 0);
    const selectedVariantIndex =
      priceMatchedIndices.length === 1
        ? priceMatchedIndices[0]
        : densestVariantIndex;
    if (selectedVariantIndex > 0 && selectedVariantIndex < variants.length) {
      variants = [variants[selectedVariantIndex], ...variants.filter((_, index) => index !== selectedVariantIndex)];
    }
  }

  const primaryVariantImageUrls = Array.isArray(variants[0]?.image_urls) ? variants[0].image_urls.filter(Boolean) : [];
  const finalProductImageUrls =
    primaryVariantImageUrls.length > 1
      ? filterCleanProductImageAssetUrls(params.baseUrl, primaryVariantImageUrls)
      : filterCleanProductImageAssetUrls(params.baseUrl, [
          ...productImageUrls,
          ...variants.flatMap((variant) => variant.image_urls),
          ...variants.map((variant) => variant.image_url),
        ]);
  const finalProductImageUrl = finalProductImageUrls[0] || imageUrl;
  const contentImageUrls = dedupeStringList(
    mergedDetailsSections
      .flatMap((section) => (Array.isArray(section.media_urls) ? section.media_urls : []))
      .filter((url) => !finalProductImageUrls.includes(url)),
  );

  if (params.verbose) {
    if (productObj) {
      params.log("data", "> Found JSON-LD 'Product' Schema");
    } else if (productGroupObj) {
      params.log("data", "> Found JSON-LD 'ProductGroup' Schema");
    }
    params.log("success", `> Extracted ${variants.length} offers/variants`);
  }

  return withProductPdpProfile({
    title: productTitle,
    url: productUrl,
    image_url: finalProductImageUrl,
    image_urls: finalProductImageUrls,
    ...(productSizeEvidence.optionValue ? { volume: productSizeEvidence.optionValue } : {}),
    ...(productSizeEvidence.alternateOptionValue
      ? { product_volume: productSizeEvidence.alternateOptionValue }
      : {}),
    ...(productSizeEvidence.detailLabel ? { size_detail_label: productSizeEvidence.detailLabel } : {}),
    ...(contentImageUrls.length > 0 ? { content_image_urls: contentImageUrls } : {}),
    ...(extracted.renderedReviewSummary ? { review_summary: extracted.renderedReviewSummary } : {}),
    variant_skus: dedupeStringList(variants.map((variant) => variant.sku)),
    variants,
    ...productPdpFields,
  });
}

function scoreScrapedProductCompleteness(product: ExtractedProduct | null | undefined): number {
  if (!product) return -1_000;
  let score = 0;
  if (cleanText(product.title)) score += 2;
  if (cleanText(product.description_raw).length >= PDP_COMPLETENESS_MIN_OVERVIEW_CHARS) score += 3;
  score += Math.min(Array.isArray(product.details_sections) ? product.details_sections.length : 0, 8) * 3;
  if (cleanText(product.how_to_use_raw)) score += 5;
  if (cleanText(product.ingredients_raw)) score += 6;
  if (cleanText(product.active_ingredients_raw)) score += 4;
  score += Math.min(Array.isArray(product.faq_items) ? product.faq_items.length : 0, 4);
  if (Number(product.review_summary?.review_count || 0) > 0 && Number(product.review_summary?.rating || 0) > 0) score += 5;
  score += Math.min(Array.isArray(product.review_summary?.preview_items) ? product.review_summary!.preview_items!.length : 0, 3);
  score += Math.min(Array.isArray(product.image_urls) ? product.image_urls.length : 0, 8);
  score -= getMissingPdpFieldReasons(product).length * 6;
  return score;
}

function hasCleanPositivePrefetchedOfferPrice(raw: unknown): boolean {
  const value = cleanText(String(raw || ""));
  if (!value) return false;
  const matches = value.match(/(?:[$€£¥₩]\s*)?\d+(?:[.,]\d{1,2})?/g) || [];
  if (matches.length !== 1) return false;
  const parsed = Number.parseFloat(matches[0]!.replace(/[^0-9.]+/g, ""));
  return Number.isFinite(parsed) && parsed > 0;
}

function isLikelyPrefetchedDirectPdpUrl(rawUrl: string, baseUrl: string): boolean {
  if (isLikelyProductUrlShared(rawUrl, baseUrl)) return true;
  try {
    const parsed = new URL(rawUrl, baseUrl);
    const base = new URL(baseUrl);
    const normalizeHost = (host: string) => host.toLowerCase().replace(/^www\./, "");
    if (normalizeHost(parsed.host) !== normalizeHost(base.host)) return false;
    const segments = parsed.pathname.toLowerCase().split("/").filter(Boolean);
    if (segments.length !== 2 || segments[0] !== "shop") return false;
    const slug = segments[1] || "";
    if (!/[a-z0-9]+-[a-z0-9-]+/.test(slug)) return false;
    return /\b(?:serum|shampoo|conditioner|cream|cleanser|balm|mask|oil|toner|moisturizer|sunscreen|spf|treatment|lotion|soap|wash|gel|patch)\b/i.test(
      slug.replace(/-/g, " "),
    );
  } catch {
    return false;
  }
}

export function isUsablePrefetchedProductAfterBotChallenge(product: ExtractedProduct | null | undefined, sourceUrl: string, baseUrl: string): boolean {
  if (!product) return false;
  const title = cleanText(product.title);
  if (!title) return false;

  const productUrl = cleanText(product.url || sourceUrl);
  if (!isLikelyPrefetchedDirectPdpUrl(productUrl, baseUrl)) return false;

  const imageUrls = dedupeStringList([
    product.image_url,
    ...(Array.isArray(product.image_urls) ? product.image_urls : []),
    ...(Array.isArray(product.variants) ? product.variants.flatMap((variant) => [variant.image_url, ...(variant.image_urls || [])]) : []),
  ]);
  if (imageUrls.length === 0) return false;
  if (imageUrls.length > 24) return false;
  if (!imageUrls.every((imageUrl) => isCleanProductImageAssetUrl(imageUrl, baseUrl))) return false;

  const hasPositiveOffer = (product.variants || []).some((variant) => hasCleanPositivePrefetchedOfferPrice(variant.price));
  if (!hasPositiveOffer) return false;

  const hasProductContext =
    cleanText(product.description_raw).length >= PDP_COMPLETENESS_MIN_OVERVIEW_CHARS ||
    (Array.isArray(product.details_sections) && product.details_sections.some((section) => cleanText(section.body).length >= 20));
  return hasProductContext;
}

async function scrapeProductPage(params: {
  browser: Browser;
  url: string;
  baseUrl: string;
  context: FetchContext;
  diagnostics: ExtractResponse["diagnostics"];
  navigationTimeoutMs: number;
  verbose: boolean;
  log: Logger;
}): Promise<ExtractedProduct | null> {
  const page = await params.browser.newPage();
  let prefetchRequestHandler: ((request: HTTPRequest) => void) | null = null;
  let prefetchedProductCandidate: ExtractedProduct | null = null;
  await page.evaluateOnNewDocument(() => {
    if (typeof (globalThis as any).__name !== "function") {
      (globalThis as any).__name = <T>(value: T) => value;
    }
  });
  const ensureBrowserEvalHelpers = async () => {
    await page
      .evaluate(() => {
        if (typeof (globalThis as any).__name !== "function") {
          (globalThis as any).__name = <T>(value: T) => value;
        }
      })
      .catch(() => undefined);
  };
  await ensureBrowserEvalHelpers();

  const expandRelevantPdpModules = async () => {
    await page.evaluate(() => {
      const relevantHeadingRe =
        /\b(product details|details?|benefits?|how to (?:use|apply)|ingredients?(?:\s*&\s*|\s+and\s+)safety|ingredients?|active ingredients?|inci|what(?:'|’)s in it\??|faq|frequently asked questions?|q\s*&\s*a|questions?|clinical(?:\s+results?)?|results?|eye look|everyday eye|application|tutorial|pro tip)\b/i;

      const summaries = Array.from(document.querySelectorAll("details > summary")) as HTMLElement[];
      for (const summary of summaries.filter((node) => relevantHeadingRe.test(node.textContent || "")).slice(0, 24)) {
        const heading =
          summary.getAttribute("title") ||
          summary.getAttribute("aria-label") ||
          summary.textContent ||
          "";
        if (!relevantHeadingRe.test(heading)) continue;
        const details = summary.closest("details");
        if (details && !details.hasAttribute("open")) details.setAttribute("open", "");
      }

      const controls = Array.from(
        document.querySelectorAll(
          "button[aria-controls], [role='tab'][aria-controls], button.accordion-title, .accordion__toggle, .acc__btn, .vc_tta-panel-title a, .vc_tta-panel-heading a",
        ),
      ) as HTMLElement[];
      for (const control of controls.filter((node) => relevantHeadingRe.test(node.textContent || "")).slice(0, 24)) {
        const heading =
          control.getAttribute("title") ||
          control.getAttribute("aria-label") ||
          control.textContent ||
          "";
        if (!relevantHeadingRe.test(heading)) continue;
        const expanded = (control.getAttribute("aria-expanded") || "").toLowerCase();
        if (expanded === "true") continue;
        control.click();
      }

      const ingredientModalTriggers = Array.from(
        document.querySelectorAll(
          ".product-ingredients__modal-trigger, [data-modal-handle='productIngredients'], [data-modal-handle=\"productIngredients\"]",
        ),
      ) as HTMLElement[];
      for (const trigger of ingredientModalTriggers.slice(0, 2)) {
        trigger.click();
      }
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
  };

  const waitForRenderedReviewSummary = async () => {
    await page
      .waitForFunction(
        () => {
          const roots = Array.from(document.querySelectorAll("[data-bv-show='rating_summary']")) as HTMLElement[];
          return roots.some((node) => {
            const clone = node.cloneNode(true) as HTMLElement;
            clone.querySelectorAll("style, script, noscript, meta, svg, path, polygon").forEach((child) => child.remove());
            const text = (clone.innerText || clone.textContent || "").replace(/\s+/g, " ").trim();
            const ariaLabels = Array.from(node.querySelectorAll("[aria-label]"))
              .map((child) => ((child as HTMLElement).getAttribute("aria-label") || "").replace(/\s+/g, " ").trim())
              .filter(Boolean);
            const corpus = [text, ...ariaLabels].join(" | ");
            return /(?:\d+(?:\.\d+)?)\s*(?:out of 5 stars|read)\D+\d[\d,]*\s*reviews?/i.test(corpus);
          });
        },
        { timeout: 5000, polling: 250 },
      )
      .catch(() => undefined);
  };

  const enablePrefetchRequestBlocking = async () => {
    if (prefetchRequestHandler) return;
    await page.setRequestInterception(true);
    prefetchRequestHandler = (request: HTTPRequest) => {
      if (request.isNavigationRequest() && request.frame() === page.mainFrame()) {
        void request.continue().catch(() => undefined);
        return;
      }
      void request.abort().catch(() => undefined);
    };
    page.on("request", prefetchRequestHandler);
  };

  const disablePrefetchRequestBlocking = async () => {
    if (!prefetchRequestHandler) return;
    page.off("request", prefetchRequestHandler);
    prefetchRequestHandler = null;
    await page.setRequestInterception(false).catch(() => undefined);
  };

  try {
    if (params.verbose) params.log("info", `Scraping: ${params.url}`);
    await preparePage(page, {
      baseUrl: params.baseUrl,
      context: params.context,
      navigationTimeoutMs: params.navigationTimeoutMs,
    });
    const prefetched = await fetchHtmlViaNativeRequest(params.url, params.diagnostics!, params.context);
    const prefetchedNonProductRedirect = isNonProductRedirectForRequestedPdp(params.url, prefetched.finalUrl, params.baseUrl);
    const prefetchedUnsafeLocale =
      prefetched.finalUrl !== params.url && isUnsafeSeedLocaleRedirect(params.url, prefetched.finalUrl, params.baseUrl);
    if (prefetchedNonProductRedirect) {
      params.log("warn", `Discarding prefetched PDP HTML after non-product redirect: ${params.url} -> ${prefetched.finalUrl}`);
    }
    if (prefetchedUnsafeLocale) {
      params.log("warn", `Discarding prefetched PDP HTML after incompatible locale redirect: ${params.url} -> ${prefetched.finalUrl}`);
    }
    if (prefetched.body && !prefetchedUnsafeLocale && !prefetchedNonProductRedirect) {
      await enablePrefetchRequestBlocking();
      await page.setContent(injectBaseHref(prefetched.body, params.url), { waitUntil: "domcontentloaded" });
      await ensureBrowserEvalHelpers();
      const prefetchedExtracted = await enrichExtractedFaqItemsWithOkendoQuestions(
        {
          ...(await extractPageSignals(page)),
          okendoMetafieldJson: extractOkendoMetafieldJsonFromHtml(prefetched.body),
        },
        params.url,
      );
      await disablePrefetchRequestBlocking();
      const prefetchedLooksLikeProduct =
        looksLikeProductPageHtml(prefetched.body) ||
        (isLikelyProductUrlShared(params.url, params.baseUrl) &&
          Boolean(cleanText(prefetchedExtracted.title)) &&
          (
            prefetchedExtracted.priceTexts.length > 0 ||
            prefetchedExtracted.detailsSections.length > 0 ||
            prefetchedExtracted.imageCandidates.length > 0
          ));
      const prefetchedProduct = buildProductFromPageSignals({
        extracted: prefetchedExtracted,
        pageLooksLikeProduct: prefetchedLooksLikeProduct,
        sourceUrl: params.url,
        baseUrl: params.baseUrl,
        verbose: params.verbose,
        log: params.log,
      });
      if (prefetchedProduct && !productHasMissingPdpFields(prefetchedProduct)) return prefetchedProduct;
      prefetchedProductCandidate = prefetchedProduct;
    }

    const visit = await gotoPageOrThrow(page, {
      url: params.url,
      baseUrl: params.baseUrl,
      context: params.context,
      diagnostics: params.diagnostics!,
    });
    if (isNonProductRedirectForRequestedPdp(params.url, visit.url, params.baseUrl)) {
      params.log("warn", `Discarding browser PDP scrape after non-product redirect: ${params.url} -> ${visit.url}`);
      if (prefetchedProductCandidate) return prefetchedProductCandidate;
      throw new BotChallengeError("unknown", visit.url, "Direct PDP redirected to non-product page");
    }
    if (visit.url !== params.url && isUnsafeSeedLocaleRedirect(params.url, visit.url, params.baseUrl)) {
      params.log("warn", `Discarding browser PDP scrape after incompatible locale redirect: ${params.url} -> ${visit.url}`);
      return null;
    }
    await ensureBrowserEvalHelpers();

    await expandRelevantPdpModules();
    await waitForRenderedReviewSummary();

    const extracted = await enrichExtractedFaqItemsWithOkendoQuestions(
      {
        ...(await extractPageSignals(page)),
        okendoMetafieldJson: extractOkendoMetafieldJsonFromHtml(visit.content),
      },
      params.url,
    );
    const liveLooksLikeProduct =
      looksLikeProductPageHtml(visit.content) ||
      (isLikelyProductUrlShared(params.url, params.baseUrl) &&
        Boolean(cleanText(extracted.title)) &&
        (
          extracted.priceTexts.length > 0 ||
          extracted.detailsSections.length > 0 ||
          extracted.imageCandidates.length > 0
        ));

    const liveProduct = buildProductFromPageSignals({
      extracted,
      pageLooksLikeProduct: liveLooksLikeProduct,
      sourceUrl: params.url,
      baseUrl: params.baseUrl,
      verbose: params.verbose,
      log: params.log,
    });
    if (prefetchedProductCandidate && scoreScrapedProductCompleteness(prefetchedProductCandidate) >= scoreScrapedProductCompleteness(liveProduct)) {
      return prefetchedProductCandidate;
    }
    return liveProduct;
  } catch (err) {
    if (err instanceof BotChallengeError) {
      if (isUsablePrefetchedProductAfterBotChallenge(prefetchedProductCandidate, params.url, params.baseUrl)) {
        params.log(
          "warn",
          `Browser PDP challenge detected after usable prefetched product extraction; preserving prefetched product: ${params.url}`,
        );
        return prefetchedProductCandidate;
      }
      throw err;
    }
    const message = err instanceof Error ? err.message : String(err);
    if (isUsablePrefetchedProductAfterBotChallenge(prefetchedProductCandidate, params.url, params.baseUrl)) {
      params.log(
        "warn",
        `Browser PDP scrape failed after usable prefetched product extraction; preserving prefetched product: ${params.url} (${message})`,
      );
      return prefetchedProductCandidate;
    }
    params.log("warn", `Failed to scrape ${params.url}: ${message}`);
    return null;
  } finally {
    await disablePrefetchRequestBlocking();
    await page.close().catch(() => undefined);
  }
}
