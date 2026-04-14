import { createHash } from "crypto";
import http from "http";
import https from "https";
import { type Browser, type Page } from "puppeteer";

import type {
  ExtractInput,
  ExtractResponse,
  ExtractedProduct,
  ExtractedProductDetailSection,
  ExtractedProductFaqItem,
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
  scoreProductCandidateUrl,
  setDiscoveryStrategy,
  setFailureCategory,
  toAbsoluteUrl as toAbsoluteUrlShared,
  withTimeout as withTimeoutShared,
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
        context: browserContext,
        diagnostics,
        selectorRootDetected: resolved.selectorRootDetected && !resolved.storefrontResolved,
        log,
      });
      const batchCandidates = chooseDiscoveryBatchCandidates({
        productUrls: discovered.productUrls,
        offset: batchOffset,
        limit: batchLimit,
        reserve: discoveryReserve,
        seedUrl: target.seedUrl,
        baseUrl,
      });

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

      const htmlPrefetched = await withTimeoutShared(
        mapWithConcurrencyShared(batchCandidates, concurrency, async (url, idx) => {
          const verbose = idx < 3;
          return scrapeProductPageViaHtml({
            url,
            baseUrl,
            diagnostics,
            verbose,
            log,
          });
        }),
        scrapeTimeoutMs,
        "Native HTML product scraping",
      );
      const htmlProducts = htmlPrefetched.filter((product): product is ExtractedProduct => Boolean(product));
      const htmlProductsAreComplete = canReturnHtmlProductsWithoutBrowser({
        products: htmlProducts,
        candidateCount: batchCandidates.length,
      });
      if (htmlProductsAreComplete) {
        const products = htmlProducts.slice(0, batchLimit);
        const { variants, adCopyById } = flattenVariants({
          brand: input.brand,
          products,
          simulated: false,
        });
        const nextOffset = batchOffset + batchLimit;
        const reachedDiscoveryCap = discovered.productUrls.length >= discoveryLimit && discoveryLimit < maxProductsTotal;
        const hasMore =
          nextOffset < maxProductsTotal && (nextOffset < discovered.productUrls.length || reachedDiscoveryCap);
        const pricing = computePricingStats(variants);
        log("success", `Extraction Complete. ${variants.length} variants processed successfully.`);

        return {
          brand: input.brand,
          domain: target.domain,
          generated_at: generatedAt,
          mode: "puppeteer",
          platform: "Generic Website",
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
      }

      let browserRun: Awaited<ReturnType<typeof runBrowserTaskWithFallback<Array<ExtractedProduct | null>>>> | null = null;
      try {
        browserRun = await runBrowserTaskWithFallback(
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
      } catch (error) {
        if (htmlProducts.length > 0) {
          const msg = error instanceof Error ? error.message : String(error || "unknown_error");
          log("warn", `Browser scraping failed; returning native HTML results: ${msg}`);
          const products = htmlProducts.slice(0, batchLimit);
          const { variants, adCopyById } = flattenVariants({
            brand: input.brand,
            products,
            simulated: false,
          });
          const nextOffset = batchOffset + batchLimit;
          const reachedDiscoveryCap = discovered.productUrls.length >= discoveryLimit && discoveryLimit < maxProductsTotal;
          const hasMore =
            nextOffset < maxProductsTotal && (nextOffset < discovered.productUrls.length || reachedDiscoveryCap);
          const pricing = computePricingStats(variants);
          log("success", `Extraction Complete. ${variants.length} variants processed successfully.`);

          return {
            brand: input.brand,
            domain: target.domain,
            generated_at: generatedAt,
            mode: "puppeteer",
            platform: "Generic Website",
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
        }
        throw error;
      }

      const browserProducts = browserRun.result.filter((product): product is ExtractedProduct => Boolean(product));
      const products = (browserProducts.length > 0 ? browserProducts : htmlProducts).slice(0, batchLimit);
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
  contentImageCandidates: string[];
  scripts: string[];
  domVariants: DomVariantMeta[];
  productDetailsText: string;
  howToUseText?: string;
  ingredientsMarkdownText?: string;
  ingredientsDisclaimerText?: string;
  activeIngredientsText?: string;
  detailsSections: ExtractedProductDetailSection[];
  appDataRaw?: string;
  faqItems: ExtractedProductFaqItem[];
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
    const sharedOpening =
      structured.length >= 40 &&
      detailed.length >= 40 &&
      structuredLower.slice(0, 40) === detailedLower.slice(0, 40);
    const structuredLineCount = structured.split("\n").map((line) => cleanText(line)).filter(Boolean).length;
    const detailedLineCount = detailed.split("\n").map((line) => cleanText(line)).filter(Boolean).length;
    const detailedKeepsStructure =
      sharedOpening &&
      detailedLineCount > structuredLineCount &&
      detailed.length >= Math.max(80, Math.round(structured.length * 0.75));

    if (startsWithStructured || detailedKeepsStructure || (materiallyLonger && looksLikeExpandedOverview)) {
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

function safeDecodeURIComponent(text: string) {
  try {
    return decodeURIComponent(text);
  } catch {
    return text;
  }
}

function decodeHtmlEntities(text: string) {
  return text.replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (match, entity: string) => {
    const normalized = String(entity || "").toLowerCase();
    if (normalized.startsWith("#x")) {
      const code = Number.parseInt(normalized.slice(2), 16);
      return Number.isFinite(code) ? String.fromCodePoint(code) : match;
    }
    if (normalized.startsWith("#")) {
      const code = Number.parseInt(normalized.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : match;
    }

    switch (normalized) {
      case "amp":
        return "&";
      case "lt":
        return "<";
      case "gt":
        return ">";
      case "quot":
        return '"';
      case "apos":
      case "#39":
        return "'";
      case "nbsp":
        return " ";
      case "ndash":
        return "–";
      case "mdash":
        return "—";
      case "hellip":
        return "…";
      case "copy":
        return "©";
      case "reg":
        return "®";
      case "trade":
        return "™";
      default:
        return match;
    }
  });
}

function cleanHtmlText(html: string | undefined) {
  if (!html) return "";
  return cleanText(
    decodeHtmlEntities(
      html
        .replace(/<!--[\s\S]*?-->/g, " ")
        .replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1>/gi, " ")
        .replace(/<li\b[^>]*>/gi, "\n- ")
        .replace(/<\/li\s*>/gi, "\n")
        .replace(/<\/(?:ul|ol|p|div|section|article|details|summary|h[1-6])\s*>/gi, "\n")
        .replace(/<br\s*\/?>/gi, "\n"),
    ),
  );
}

function extractHtmlAttribute(tag: string, attribute: string) {
  const quotedMatch = tag.match(new RegExp(`${attribute}\\s*=\\s*(['"])([\\s\\S]*?)\\1`, "i"));
  if (quotedMatch?.[2]) return decodeHtmlEntities(quotedMatch[2].trim());
  const bareMatch = tag.match(new RegExp(`${attribute}\\s*=\\s*([^\\s>]+)`, "i"));
  return bareMatch?.[1] ? decodeHtmlEntities(bareMatch[1].trim()) : "";
}

function extractMetaTagContent(
  html: string,
  params: { name?: string; property?: string; itemprop?: string },
) {
  const metaTags = html.match(/<meta\b[^>]*>/gi) || [];
  for (const tag of metaTags) {
    const name = extractHtmlAttribute(tag, "name").toLowerCase();
    const property = extractHtmlAttribute(tag, "property").toLowerCase();
    const itemprop = extractHtmlAttribute(tag, "itemprop").toLowerCase();

    if (
      (params.name && name === params.name.toLowerCase()) ||
      (params.property && property === params.property.toLowerCase()) ||
      (params.itemprop && itemprop === params.itemprop.toLowerCase())
    ) {
      return cleanHtmlText(extractHtmlAttribute(tag, "content"));
    }
  }
  return "";
}

function pushRelevantHtmlSection(
  sections: ExtractedProductDetailSection[],
  headingRaw: string,
  bodyRaw: string,
  sourceKind: string,
) {
  let heading = cleanHtmlText(headingRaw);
  if (/^how to$/i.test(heading)) heading = "How to Use";
  const body = cleanHtmlText(bodyRaw);
  if (!heading || !body) return;
  if (heading.length > 180) return;
  if (
    body.length > 12_000 &&
    /\b(?:window\.|document\.|function\s+\w+|productVariants|shopify-osm|klarna|matchmymakeup|addEventListener)\b/i.test(
      body,
    )
  ) {
    return;
  }
  if (
    !/\b(details?|benefits?|how to (?:use|apply)|usage(?: details)?|suggested usage|directions?|ingredients?|active ingredients?|key ingredients?|inci|description|beauty tips|formula|about|what(?:'|’)s in it\??|faq|frequently asked questions?|q\s*&\s*a|questions?|clinical(?:\s+results?)?|results?|hydration|hydrates?|sebum|oil[-\s]*moisture|moisture|absorbs?|pores?|texture|finish|coverage|shades?|spf|skin)\b/i.test(
      heading,
    )
  ) {
    return;
  }

  sections.push({
    heading,
    body,
    source_kind: sourceKind,
  });
}

function extractStrongLedHtmlSections(html: string, sourceKind: string) {
  const sections: ExtractedProductDetailSection[] = [];
  const paragraphMatches = Array.from(html.matchAll(/<p\b[^>]*>([\s\S]*?)<\/p>/gi));
  let currentHeading = "";
  let currentBodyParts: string[] = [];

  const flush = () => {
    if (!currentHeading || currentBodyParts.length === 0) {
      currentHeading = "";
      currentBodyParts = [];
      return;
    }

    pushRelevantHtmlSection(sections, currentHeading, currentBodyParts.join("\n\n"), sourceKind);
    currentHeading = "";
    currentBodyParts = [];
  };

  for (const match of paragraphMatches) {
    const paragraphHtml = match[1] || "";
    const strongLed = paragraphHtml.match(/^\s*<strong\b[^>]*>([\s\S]*?)<\/strong>\s*(?:<br\s*\/?>)?\s*([\s\S]*)$/i);
    const heading = cleanHtmlText(strongLed?.[1] || "");
    const bodyAfterHeading = cleanHtmlText(strongLed?.[2] || "");
    const looksLikeSectionHeading =
      Boolean(heading) &&
      heading.length <= 140 &&
      !/[.!?]\s+\S/.test(heading);

    if (looksLikeSectionHeading) {
      flush();
      currentHeading = heading;
      if (bodyAfterHeading) currentBodyParts.push(bodyAfterHeading);
      continue;
    }

    const body = cleanHtmlText(paragraphHtml);
    if (body && currentHeading) currentBodyParts.push(body);
  }

  flush();
  return dedupeDetailSections(sections);
}

function stripProductTabActionHtml(html: string) {
  return html.replace(/<div\b[^>]*class=["'][^"']*\btoggle-ellipsis__actions\b[\s\S]*$/i, "");
}

function extractHeadingParagraphPairSections(html: string, sourceKind: string) {
  const sections: ExtractedProductDetailSection[] = [];
  for (const containerMatch of html.matchAll(
    /<div\b[^>]*class=["'][^"']*\bfigma-text\b[^"']*["'][^>]*>([\s\S]*?)(?=<\/div>\s*<div\b[^>]*class=["'][^"']*\bfigma-image\b|<\/div>\s*<\/div>\s*<\/div>)/gi,
  )) {
    const containerHtml = containerMatch[1] || "";
    for (const pairMatch of containerHtml.matchAll(
      /<h([1-6])\b[^>]*>([\s\S]*?)<\/h\1>\s*<p\b[^>]*>([\s\S]*?)<\/p>/gi,
    )) {
      pushRelevantHtmlSection(sections, pairMatch[2] || "", pairMatch[3] || "", sourceKind);
    }
  }
  return dedupeDetailSections(sections);
}

function extractHeroDescriptionSections(html: string) {
  const sections: ExtractedProductDetailSection[] = [];
  for (const match of html.matchAll(
    /<h([1-6])\b[^>]*class=["'][^"']*\bhero__title\b[^"']*["'][^>]*>([\s\S]*?)<\/h\1>[\s\S]{0,5000}?<p\b[^>]*class=["'][^"']*\bhero__description\b[^"']*["'][^>]*>([\s\S]*?)<\/p>/gi,
  )) {
    pushRelevantHtmlSection(sections, match[2] || "", match[3] || "", "hero_description_html");
  }
  return dedupeDetailSections(sections);
}

function extractHtmlProductTabSections(html: string) {
  const headingsByTab = new Map<string, string>();
  for (const match of html.matchAll(
    /<li\b[^>]*\bdata-tab=["']([^"']+)["'][^>]*>[\s\S]*?<span\b[^>]*>([\s\S]*?)<\/span>[\s\S]*?<\/li>/gi,
  )) {
    const tabId = cleanText(match[1]);
    const heading = cleanHtmlText(match[2] || "");
    if (tabId && heading) headingsByTab.set(tabId, heading);
  }

  const contentMatches = Array.from(
    html.matchAll(/<div\b[^>]*class=["'][^"']*\btab-content\b[^"']*\btab-content-([^"'\s]+)\b[^"']*["'][^>]*>/gi),
  );
  if (headingsByTab.size === 0 || contentMatches.length === 0) return [];

  const sections: ExtractedProductDetailSection[] = [];
  for (let idx = 0; idx < contentMatches.length; idx += 1) {
    const match = contentMatches[idx]!;
    const tabId = cleanText(match[1]);
    const heading = headingsByTab.get(tabId);
    if (!heading) continue;

    const start = match.index || 0;
    const nextContentStart = contentMatches[idx + 1]?.index;
    const nextProductBlockStart = html.slice(start + match[0].length).search(/<div\b[^>]*class=["'][^"']*\bproduct__block\b/);
    const fallbackEnd = nextProductBlockStart >= 0 ? start + match[0].length + nextProductBlockStart : html.length;
    const end = typeof nextContentStart === "number" ? nextContentStart : fallbackEnd;
    const sectionHtml = stripProductTabActionHtml(html.slice(start, end));

    const strongLedSections = /^description$/i.test(heading)
      ? extractStrongLedHtmlSections(sectionHtml, "product_tab_description")
      : [];
    if (strongLedSections.length > 0) {
      sections.push(...strongLedSections);
      continue;
    }

    pushRelevantHtmlSection(sections, heading, sectionHtml, "product_tab_html");
  }

  return dedupeDetailSections(sections);
}

function extractHtmlDetailSections(html: string) {
  const sections: ExtractedProductDetailSection[] = [];

  sections.push(...extractHtmlProductTabSections(html));
  sections.push(...extractHeadingParagraphPairSections(html, "custom_heading_paragraph_html"));
  sections.push(...extractHeroDescriptionSections(html));

  for (const match of html.matchAll(/<details\b[^>]*>([\s\S]*?)<\/details>/gi)) {
    const block = match[1] || "";
    const summaryMatch = block.match(/<summary\b[^>]*>([\s\S]*?)<\/summary>/i);
    if (!summaryMatch) continue;
    const heading = cleanHtmlText(summaryMatch[1] || "");
    if (looksLikeFaqQuestion(heading) && !/\bhow(?:\s*|-)?to(?:\s+(?:use|apply))?\b/i.test(heading)) continue;
    const bodyHtml = block.replace(summaryMatch[0], "");
    pushRelevantHtmlSection(sections, heading, bodyHtml, "details_summary");
  }

  for (const match of html.matchAll(
    /<button\b[^>]*(?:aria-label|title)=["']([^"']+)["'][^>]*>[\s\S]*?<\/button>[\s\S]{0,12000}?<div\b[^>]*class=["'][^"']*accordion__content[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi,
  )) {
    pushRelevantHtmlSection(sections, match[1] || "", match[2] || "", "accordion_button_html");
  }

  for (const match of html.matchAll(
    /<(?:div|p)\b[^>]*class=["'][^"']*\btitle\b[^"']*["'][^>]*>\s*(Ingredients|How to Use|Usage Details|Directions?|Benefits?|Description)\s*<\/(?:div|p)>[\s\S]{0,12000}?<(?:div|p)\b[^>]*class=["'][^"']*(ingredients-flyout-content|product-flyout-directions-list|description)[^"']*["'][^>]*>([\s\S]*?)<\/(?:div|p)>/gi,
  )) {
    const heading = match[1] || "";
    const sourceClass = (match[2] || "").toLowerCase();
    const sourceKind =
      sourceClass.includes("ingredients")
        ? "title_flyout_ingredients"
        : sourceClass.includes("directions")
          ? "title_flyout_how_to_use"
          : "title_flyout_description";
    pushRelevantHtmlSection(sections, heading, match[3] || "", sourceKind);
  }

  for (const match of html.matchAll(
    /<(?:p|div)\b[^>]*class=["'][^"']*ingredients-flyout-content[^"']*["'][^>]*>/gi,
  )) {
    const attrText = cleanHtmlText(decodeHtmlEntities(extractHtmlAttribute(match[0] || "", "data-original-ingredients")));
    if (!attrText) continue;
    pushRelevantHtmlSection(sections, "Ingredients", attrText, "title_flyout_ingredients_attr");
  }

  for (const match of html.matchAll(
    /<(?:div|span|p)\b[^>]*class=["'][^"']*\btitle\b[^"']*["'][^>]*>\s*Key ingredients\s*<\/(?:div|span|p)>[\s\S]{0,4000}?<(?:div|p)\b[^>]*class=["'][^"']*\blist\b[^"']*["'][^>]*>([\s\S]*?)<\/(?:div|p)>/gi,
  )) {
    pushRelevantHtmlSection(sections, "Key Ingredients", match[1] || "", "page_key_ingredients_html");
  }

  for (const match of html.matchAll(
    /<div\b[^>]*class=["'][^"']*product-accordion[^"']*["'][^>]*>[\s\S]*?<div\b[^>]*class=["'][^"']*product-accordion-header[^"']*["'][^>]*>[\s\S]*?<h[1-6]\b[^>]*>([\s\S]*?)<\/h[1-6]>[\s\S]*?<\/div>[\s\S]*?<div\b[^>]*class=["'][^"']*accordion-panel[^"']*["'][^>]*>([\s\S]*?)<\/div>[\s\S]*?<\/div>/gi,
  )) {
    pushRelevantHtmlSection(sections, match[1] || "", match[2] || "", "product_accordion_html");
  }

  for (const match of html.matchAll(
    /<div\b[^>]*class=["'][^"']*description-container[^"']*["'][^>]*>[\s\S]*?<h[1-6]\b[^>]*class=["'][^"']*description-header[^"']*["'][^>]*>([\s\S]*?)<\/h[1-6]>[\s\S]*?<div\b[^>]*class=["'][^"']*description-body[^"']*["'][^>]*>([\s\S]*?)<\/div>[\s\S]*?<\/div>/gi,
  )) {
    pushRelevantHtmlSection(sections, match[1] || "", match[2] || "", "description_header_html");
  }

  const keyIngredientEntries = Array.from(
    html.matchAll(
      /<div\b[^>]*class=["'][^"']*child-ingredient[^"']*["'][^>]*>[\s\S]*?<div\b[^>]*class=["'][^"']*name[^"']*["'][^>]*>([\s\S]*?)<\/div>[\s\S]*?<div\b[^>]*class=["'][^"']*description[^"']*["'][^>]*>([\s\S]*?)<\/div>[\s\S]*?<\/div>/gi,
    ),
  )
    .map((match) => {
      const name = cleanHtmlText(match[1] || "");
      const description = cleanHtmlText(match[2] || "");
      if (!name && !description) return "";
      return [name, description].filter(Boolean).join(": ");
    })
    .filter(Boolean);
  if (keyIngredientEntries.length > 0) {
    pushRelevantHtmlSection(
      sections,
      "Key Ingredients",
      keyIngredientEntries.join("\n\n"),
      "key_ingredients_html",
    );
  }

  const descriptionBlock =
    html.match(
      /<div\b[^>]*class=["'][^"']*product-info-description[^"']*["'][^>]*>[\s\S]*?<div\b[^>]*class=["'][^"']*description[^"']*["'][^>]*>([\s\S]*?)<\/div>/i,
    )?.[1] || "";
  if (descriptionBlock) {
    pushRelevantHtmlSection(sections, "Description", descriptionBlock, "page_product_description");
  }

  for (const match of html.matchAll(
    /<product-modal\b[^>]*>[\s\S]*?<a\b[^>]*data-popup-open[^>]*>([\s\S]*?)<\/a>[\s\S]*?<dialog\b[^>]*class=["'][^"']*\bproduct-modal\b[^"']*["'][^>]*>[\s\S]*?<div\b[^>]*class=["'][^"']*\bproduct-modal__content\b[^"']*["'][^>]*>([\s\S]*?)<\/div>[\s\S]*?<\/dialog>[\s\S]*?<\/product-modal>/gi,
  )) {
    pushRelevantHtmlSection(sections, match[1] || "", match[2] || "", "product_modal_html");
  }

  return dedupeDetailSections(sections);
}

function looksLikeFaqQuestion(value: string | undefined) {
  const normalized = normalizeFaqQuestion(value);
  if (!normalized) return false;
  return (
    /[?？]$/.test(normalized) ||
    /^(?:can|is|are|do|does|did|will|would|should|could|where|when|why|how|what|who|which)\b/i.test(normalized)
  );
}

function extractHtmlFaqItems(html: string, sourceUrl: string) {
  const items: ExtractedProductFaqItem[] = [];
  const seen = new Set<string>();
  const push = (questionRaw: string, answerRaw: string, sourceKind: string, sourceTitle = "FAQ") => {
    const question = normalizeFaqQuestion(questionRaw).replace(/^\d{1,2}[.)]\s*/, "");
    const answer = normalizeFaqAnswer(answerRaw);
    if (!looksLikeFaqQuestion(question) || !answer) return;
    const key = `${question.toLowerCase()}|${answer.toLowerCase()}|${sourceKind.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    items.push({
      question,
      answer,
      source_kind: sourceKind,
      source_url: sourceUrl,
      source_title: sourceTitle,
    });
  };

  const tabHeadingsById = new Map<string, string>();
  for (const match of html.matchAll(
    /<li\b[^>]*\bdata-tab=["']([^"']+)["'][^>]*>[\s\S]*?<span\b[^>]*>([\s\S]*?)<\/span>[\s\S]*?<\/li>/gi,
  )) {
    const tabId = cleanText(match[1]);
    const heading = cleanHtmlText(match[2] || "");
    if (tabId && /\b(?:faq|frequently asked questions?|q\s*&\s*a|questions?)\b/i.test(heading)) {
      tabHeadingsById.set(tabId, heading || "FAQ");
    }
  }

  const contentMatches = Array.from(
    html.matchAll(/<div\b[^>]*class=["'][^"']*\btab-content\b[^"']*\btab-content-([^"'\s]+)\b[^"']*["'][^>]*>/gi),
  );
  for (let idx = 0; idx < contentMatches.length; idx += 1) {
    const match = contentMatches[idx]!;
    const tabId = cleanText(match[1]);
    const heading = tabHeadingsById.get(tabId);
    if (!heading) continue;

    const start = match.index || 0;
    const nextContentStart = contentMatches[idx + 1]?.index;
    const nextProductBlockStart = html.slice(start + match[0].length).search(/<div\b[^>]*class=["'][^"']*\bproduct__block\b/);
    const fallbackEnd = nextProductBlockStart >= 0 ? start + match[0].length + nextProductBlockStart : html.length;
    const end = typeof nextContentStart === "number" ? nextContentStart : fallbackEnd;
    const sectionHtml = stripProductTabActionHtml(html.slice(start, end));
    const strongMatches = Array.from(sectionHtml.matchAll(/<strong\b[^>]*>([\s\S]*?)<\/strong>/gi));

    for (let qIdx = 0; qIdx < strongMatches.length; qIdx += 1) {
      const strongMatch = strongMatches[qIdx]!;
      const question = cleanHtmlText(strongMatch[1] || "");
      if (!looksLikeFaqQuestion(question)) continue;
      const answerStart = (strongMatch.index || 0) + strongMatch[0].length;
      const answerEnd = strongMatches[qIdx + 1]?.index ?? sectionHtml.length;
      push(question, sectionHtml.slice(answerStart, answerEnd), "product_tab_faq", heading);
    }
  }

  for (const match of html.matchAll(/<details\b[^>]*>([\s\S]*?)<\/details>/gi)) {
    const block = match[1] || "";
    const summaryMatch = block.match(/<summary\b[^>]*>([\s\S]*?)<\/summary>/i);
    if (!summaryMatch) continue;
    const question = cleanHtmlText(summaryMatch[1] || "");
    if (/\bhow(?:\s*|-)?to(?:\s+(?:use|apply))?\b/i.test(question)) continue;
    if (!looksLikeFaqQuestion(question)) continue;
    const bodyHtml = block.replace(summaryMatch[0], "");
    push(question, bodyHtml, "merchant_faq", "FAQ");
  }

  return dedupeFaqItems(items);
}

function extractHtmlImageCandidates(html: string, baseUrl: string) {
  const out: string[] = [];
  const seen = new Set<string>();
  const push = (raw: string | undefined) => {
    const trimmed = typeof raw === "string" ? raw.trim() : "";
    if (out.length >= 24) return;
    if (!trimmed) return;
    const candidates = trimmed
      .split(",")
      .map((part) => part.trim().split(/\s+/)[0] || "")
      .filter(Boolean);
    for (const candidate of candidates) {
      const normalized = normalizeImageUrlCandidate(baseUrl, decodeHtmlEntities(candidate));
      if (!normalized || seen.has(normalized)) continue;
      seen.add(normalized);
      out.push(normalized);
    }
  };

  const metaTags = html.match(/<meta\b[^>]*>/gi) || [];
  for (const tag of metaTags) {
    const property = extractHtmlAttribute(tag, "property").toLowerCase();
    const name = extractHtmlAttribute(tag, "name").toLowerCase();
    const itemprop = extractHtmlAttribute(tag, "itemprop").toLowerCase();
    if (
      property === "og:image" ||
      name === "twitter:image" ||
      itemprop === "image" ||
      property === "product:image"
    ) {
      push(extractHtmlAttribute(tag, "content"));
    }
  }

  for (const tag of html.match(/<img\b[^>]*>/gi) || []) {
    push(extractHtmlAttribute(tag, "zoom-src"));
    push(extractHtmlAttribute(tag, "data-zoom-src"));
    push(extractHtmlAttribute(tag, "data-zoom-image"));
    push(extractHtmlAttribute(tag, "data-large-image"));
    push(extractHtmlAttribute(tag, "data-src"));
    push(extractHtmlAttribute(tag, "srcset"));
    push(extractHtmlAttribute(tag, "src"));
    if (out.length >= 12) break;
  }

  return out;
}

function extractHtmlContentImageCandidates(html: string, baseUrl: string) {
  const out: string[] = [];
  const seen = new Set<string>();
  const push = (raw: string | undefined) => {
    const trimmed = typeof raw === "string" ? raw.trim() : "";
    if (!trimmed) return;
    const candidates = trimmed
      .split(",")
      .map((part) => part.trim().split(/\s+/)[0] || "")
      .filter(Boolean);
    for (const candidate of candidates) {
      const normalized = normalizeImageUrlCandidate(baseUrl, decodeHtmlEntities(candidate));
      const dedupeKey = normalized ? imageDedupeKey(normalized) : "";
      if (!normalized || seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      out.push(normalized);
      if (out.length >= 24) return;
    }
  };

  const pushTagImageSources = (tag: string) => {
    push(extractHtmlAttribute(tag, "data-src"));
    push(extractHtmlAttribute(tag, "data-srcset"));
    push(extractHtmlAttribute(tag, "data-zoom-src"));
    push(extractHtmlAttribute(tag, "data-zoom-image"));
    push(extractHtmlAttribute(tag, "data-large-image"));
    push(extractHtmlAttribute(tag, "srcset"));
    push(extractHtmlAttribute(tag, "src"));
  };

  const pushCssImageSources = (block: string) => {
    for (const match of block.matchAll(/url\(\s*(['"]?)([^'")]+)\1\s*\)/gi)) {
      push(match[2] || "");
      if (out.length >= 24) return;
    }
  };

  const contentContainerRe =
    /<(section|div|article)\b[^>]*(?:class|id)=["'][^"']*(?:figma-image|qq-content-stack|figma-html-wrapper|custom-figma-block|new_custom_pdp|section_custom_content|product__content|product__description|hero__media|hero__image|hero__content|image__hero__frame|image__hero__scale|brick__slider|brick__section|brick__block__image|tab2-container|tab3-container)[^"']*["'][^>]*>[\s\S]{0,18000}?<\/\1>/gi;
  for (const match of html.matchAll(contentContainerRe)) {
    const block = match[0] || "";
    for (const tag of block.match(/<(?:img|source)\b[^>]*>/gi) || []) {
      pushTagImageSources(tag);
      if (out.length >= 24) return out;
    }
    pushCssImageSources(block);
    if (out.length >= 24) return out;
  }

  return out;
}

function extractJsonLdScriptsFromHtml(html: string) {
  return Array.from(html.matchAll(/<script\b[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi))
    .map((match) => decodeHtmlEntities((match[1] || "").trim()))
    .filter(Boolean);
}

export function extractProductFromHtmlSnapshot(params: {
  html: string;
  url: string;
  baseUrl: string;
  verbose?: boolean;
  log?: Logger;
}): ExtractedProduct | null {
  const html = params.html || "";
  if (!html) return null;

  const title =
    cleanHtmlText(html.match(/<h1\b[^>]*>([\s\S]*?)<\/h1>/i)?.[1]) ||
    extractMetaTagContent(html, { property: "og:title" }) ||
    cleanHtmlText(html.match(/<title\b[^>]*>([\s\S]*?)<\/title>/i)?.[1]);

  const canonical =
    canonicalizeUrlShared(
      toAbsoluteUrlShared(
        params.baseUrl,
        extractHtmlAttribute(html.match(/<link\b[^>]*rel=["']canonical["'][^>]*>/i)?.[0] || "", "href") ||
          extractMetaTagContent(html, { property: "og:url" }) ||
          params.url,
      ),
      params.baseUrl,
    );

  const metaDescription =
    extractMetaTagContent(html, { name: "description" }) ||
    extractMetaTagContent(html, { property: "og:description" });

  const muradIngredientsBody = matchHtmlSnippet(html, /<p[^>]+id=["']ingredients-content["'][^>]*>([\s\S]*?)<\/p>/i);
  const muradIngredientsRaw = extractMuradIngredientListText(muradIngredientsBody);
  const muradHowToRaw = extractMuradHowToSectionText(html);
  const detailsSections = dedupeDetailSections(
    [
      ...extractHtmlDetailSections(html),
      muradIngredientsRaw
        ? {
            heading: "Full List of Ingredients",
            body: muradIngredientsRaw,
            source_kind: "html_snapshot_murad_ingredients_content",
          }
        : null,
      muradHowToRaw
        ? {
            heading: "How-To",
            body: muradHowToRaw,
            source_kind: "html_snapshot_murad_how_to_section",
          }
        : null,
    ].filter((section): section is ExtractedProductDetailSection => Boolean(section)),
  );
  const faqItems = extractHtmlFaqItems(html, params.url);
  const ingredientsSection =
    detailsSections.find((section) => {
      const heading = cleanText(section?.heading);
      return /\b(ingredients?|inci)\b/i.test(heading) && !/\b(?:active|key|hero) ingredients?\b/i.test(heading);
    }) ||
    firstMatchingSectionBody(detailsSections, [/\bwhat(?:'|’)s in it\??\b/i]);
  const howToUseSection =
    firstMatchingSectionBody(detailsSections, [/\bhow(?:\s*|-)?to(?:\s+(?:use|apply))?\b/i]) ||
    firstMatchingSectionBody(detailsSections, [/\b(usage(?: details)?|suggested usage|directions?|beauty tips)\b/i]);
  const activeIngredientsSection = firstMatchingSectionBody(detailsSections, [/\b(?:active|key|hero) ingredients?\b/i]);
  const productTabDescriptionBody = cleanText(
    detailsSections
      .filter((section) => cleanText(section.source_kind) === "product_tab_description")
      .map((section) => [cleanText(section.heading), normalizedSectionBody(section)].filter(Boolean).join("\n"))
      .join("\n\n"),
  );
  const customPdpDescriptionBody = cleanText(
    detailsSections
      .filter((section) => cleanText(section.source_kind) === "custom_heading_paragraph_html")
      .map((section) => [cleanText(section.heading), normalizedSectionBody(section)].filter(Boolean).join("\n"))
      .join("\n\n"),
  );
  const descriptionSection =
    firstMatchingSectionBody(detailsSections, [/\bdescription\b/i]) ||
    firstMatchingSectionBody(detailsSections, [/\b(details?|benefits?|about)\b/i]) ||
    (productTabDescriptionBody
      ? {
          heading: "Description",
          body: productTabDescriptionBody,
          source_kind: "product_tab_description",
        }
      : customPdpDescriptionBody
        ? {
            heading: "Description",
            body: customPdpDescriptionBody,
            source_kind: "custom_heading_paragraph_html",
          }
      : null);

  const extracted: ScrapedPageSignals = {
    title,
    canonical,
    metaDescription,
    priceTexts: dedupeStringList(
      [
        extractMetaTagContent(html, { property: "og:price:amount" }),
        extractMetaTagContent(html, { property: "product:price:amount" }),
        ...Array.from(html.matchAll(/\$\s*\d+(?:\.\d{2})?/g)).slice(0, 6).map((match) => match[0] || ""),
      ].map((value) => cleanHtmlText(value)),
    ),
    imageCandidates: extractHtmlImageCandidates(html, params.baseUrl),
    contentImageCandidates: extractHtmlContentImageCandidates(html, params.baseUrl),
    scripts: extractJsonLdScriptsFromHtml(html),
    domVariants: [],
    productDetailsText: descriptionSection?.body || "",
    howToUseText: howToUseSection?.body,
    ingredientsMarkdownText: ingredientsSection?.body,
    ingredientsDisclaimerText: undefined,
    activeIngredientsText: activeIngredientsSection?.body,
    detailsSections,
    faqItems,
  };

  const pageLooksLikeProduct =
    looksLikeProductPageHtml(html) ||
    (isLikelyProductUrlShared(params.url, params.baseUrl) &&
      Boolean(cleanText(extracted.title)) &&
      (extracted.priceTexts.length > 0 || extracted.detailsSections.length > 0 || extracted.imageCandidates.length > 0));

  return buildProductFromPageSignals({
    extracted,
    pageLooksLikeProduct,
    sourceUrl: params.url,
    baseUrl: params.baseUrl,
    verbose: Boolean(params.verbose),
    log: params.log || (() => undefined),
  });
}

function renderPaulasChoiceRichText(value: unknown, joiner = "\n\n"): string {
  if (typeof value === "string") return value;
  if (!value) return "";

  if (Array.isArray(value)) {
    return cleanText(value.map((item) => renderPaulasChoiceRichText(item, "")).filter(Boolean).join(joiner));
  }

  if (typeof value !== "object") return "";

  const record = value as Record<string, unknown>;

  if (record.break) return "\n";

  if (Array.isArray(record.items)) {
    const items = record.items
      .map((item) => cleanText(renderPaulasChoiceRichText(item, "")))
      .filter(Boolean);
    return items.join("\n");
  }

  if ("text" in record) {
    return renderPaulasChoiceRichText(record.text, "");
  }

  return "";
}

export function extractPaulasChoiceAppDataPdpFields(appDataRaw?: string) {
  const normalizedRaw = typeof appDataRaw === "string" ? appDataRaw.trim() : "";
  if (!normalizedRaw) return null;

  try {
    const parsed = JSON.parse(normalizedRaw) as Record<string, unknown>;
    const common = (parsed.common && typeof parsed.common === "object") ? (parsed.common as Record<string, unknown>) : {};
    const commonStrings =
      common.strings && typeof common.strings === "object" ? (common.strings as Record<string, unknown>) : {};
    const page = (parsed.page && typeof parsed.page === "object") ? (parsed.page as Record<string, unknown>) : {};
    const pageStrings =
      page.strings && typeof page.strings === "object" ? (page.strings as Record<string, unknown>) : {};
    const strings = {
      ...commonStrings,
      ...pageStrings,
    };
    const product = page.product && typeof page.product === "object" ? (page.product as Record<string, unknown>) : {};

    const sectionHeading = (key: string, fallback: string) =>
      cleanText(typeof strings[key] === "string" ? (strings[key] as string) : undefined) || fallback;

    const whatIsItText =
      cleanText(typeof product.whyIsItDifferent === "string" ? (product.whyIsItDifferent as string) : undefined) ||
      renderPaulasChoiceRichText(page.whatIsIt);
    const benefitsText = cleanText(typeof product.whatDoesItDo === "string" ? (product.whatDoesItDo as string) : undefined);
    const howToUseText =
      cleanText(typeof product.howToUse === "string" ? (product.howToUse as string) : undefined) ||
      renderPaulasChoiceRichText(page.howToUseContent);
    const keyIngredientsText = cleanText(
      typeof product.keyIngredients === "string" ? (product.keyIngredients as string) : undefined,
    );
    const researchText = renderPaulasChoiceRichText(page.research);

    const ingredientsData = Array.isArray(page.ingredientsData) ? page.ingredientsData : [];
    const allIngredientsText = cleanText(
      ingredientsData
        .map((entry) => {
          if (!entry || typeof entry !== "object") return "";
          const record = entry as Record<string, unknown>;
          return typeof record.name === "string" ? record.name : "";
        })
        .filter(Boolean)
        .join(", "),
    );

    const detailsSections = dedupeDetailSections([
      {
        heading: sectionHeading("whyIsItDifferent", "What is it"),
        body: whatIsItText,
        source_kind: "paulaschoice_appdata_what_is_it",
      },
      {
        heading: sectionHeading("whatDoesItDo", "Benefits"),
        body: benefitsText,
        source_kind: "paulaschoice_appdata_benefits",
      },
      {
        heading: sectionHeading("keyIngredients", "Key Ingredients"),
        body: keyIngredientsText,
        source_kind: "paulaschoice_appdata_key_ingredients",
      },
      {
        heading: sectionHeading("allIngredients", "All Ingredients"),
        body: allIngredientsText,
        source_kind: "paulaschoice_appdata_all_ingredients",
      },
      {
        heading: sectionHeading("howToUse", "How to use"),
        body: howToUseText,
        source_kind: "paulaschoice_appdata_how_to_use",
      },
      {
        heading: sectionHeading("research", "Research"),
        body: researchText,
        source_kind: "paulaschoice_appdata_research",
      },
    ]);

    const descriptionRaw = cleanText([whatIsItText, benefitsText].filter(Boolean).join("\n\n")) || undefined;

    return {
      descriptionRaw,
      detailsSections,
      ingredientsRaw: allIngredientsText || undefined,
      activeIngredientsRaw: keyIngredientsText || undefined,
      howToUseRaw: howToUseText || undefined,
    };
  } catch {
    return null;
  }
}

function dedupeDetailSections(sections: ExtractedProductDetailSection[]) {
  const out: ExtractedProductDetailSection[] = [];
  const seen = new Set<string>();
  for (const section of Array.isArray(sections) ? sections : []) {
    const heading = normalizeDetailSectionHeading(section?.heading);
    const body = cleanText(section?.body);
    const sourceKind = cleanText(section?.source_kind) || "unknown";
    if (!heading || !body) continue;
    const key = `${heading.toLowerCase()}|${body.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      heading,
      body,
      source_kind: sourceKind,
    });
  }
  return out;
}

function escapeRegex(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizedSectionBody(section: ExtractedProductDetailSection | null | undefined) {
  const heading = cleanText(section?.heading);
  const body = cleanText(section?.body);
  if (!body) return "";
  if (!heading) return body;
  const stripped = cleanText(body.replace(new RegExp(`^${escapeRegex(heading)}(?:\\s*[:\\-–—]?\\s*)?`, "i"), ""));
  return stripped || body;
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
  if (/^(?:how to apply|directions?|usage|how[-\s]*to)$/i.test(heading)) return "How to Use";
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

function uniqueFieldSources(values: Array<string | undefined | null>) {
  return dedupeStringList(values.map((value) => cleanText(typeof value === "string" ? value : undefined)));
}

function firstMatchingSectionBody(
  sections: ExtractedProductDetailSection[],
  patterns: RegExp[],
): ExtractedProductDetailSection | null {
  let bestMatch: ExtractedProductDetailSection | null = null;
  let bestScore = -1;
  for (const section of Array.isArray(sections) ? sections : []) {
    const heading = cleanText(section?.heading);
    if (!heading) continue;
    if (!patterns.some((pattern) => pattern.test(heading))) continue;
    const normalizedBody = normalizedSectionBody(section);
    const lineCount = normalizedBody.split("\n").map((line) => cleanText(line)).filter(Boolean).length;
    const score = normalizedBody.length + lineCount * 20;
    if (score <= bestScore) continue;
    bestScore = score;
    bestMatch = {
      heading: cleanText(section.heading),
      body: normalizedBody || cleanText(section.body),
      source_kind: cleanText(section.source_kind) || "unknown",
    };
  }
  return bestMatch;
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
    ? new RegExp(`(?:${escapedLabels})\\s*:\\s*([\\s\\S]+?)(?=(?:${escapedStopLabels})\\s*:|$)`, "i")
    : new RegExp(`(?:${escapedLabels})\\s*:\\s*([\\s\\S]+)$`, "i");
  const match = normalized.match(pattern);
  return cleanText(match?.[1]);
}

export function looksLikeFullIngredientListText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return false;
  const commaCount = (normalized.match(/,/g) || []).length;
  const dashSeparatedCount = (normalized.match(/\s-\s/g) || []).length;
  const proseSignal =
    /\b(?:nourishes?|provides?|helps?|boosts?|refines?|hydrates?|absorbs?|soothes?|calms?|brightens?|moisturiz(?:es?|ing)|balances?)\b/i.test(
      normalized,
    );
  return (
    /\b(active ingredients?|inactive ingredients?|full ingredients?|ingredient list|inci|composition)\s*:/i.test(normalized) ||
    /\b(?:water\/aqua|aqua\/water|aqua\/water\/eau)\b/i.test(normalized) ||
    /\bci\s*\d{5}\b/i.test(normalized) ||
    commaCount >= 4 ||
    dashSeparatedCount >= 4 ||
    (commaCount >= 2 && !proseSignal && normalized.length < 500)
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
  const ingredientSections = detailsSections.filter((section) => {
    const heading = cleanText(section?.heading);
    return /\b(ingredients?|ingredient list|inci|composition|what(?:'|’)s in it\??|formula)\b/i.test(heading);
  });
  const fullIngredientSection =
    ingredientSections.find((section) => looksLikeFullIngredientListText(section.body)) ||
    firstMatchingSectionBody(detailsSections, [/\bwhat(?:'|’)s in it\??\b/i]) ||
    firstMatchingSectionBody(detailsSections, [/\bformula\b/i]);
  const ingredientSection =
    fullIngredientSection ||
    ingredientSections.find((section) => !/\b(?:active|key|hero) ingredients?\b/i.test(cleanText(section?.heading))) ||
    ingredientSections[0];
  const ingredientSectionBody = cleanText(ingredientSection?.body);
  const explicitIngredients = cleanText(params.ingredientsMarkdownText);
  const explicitFullIngredients = looksLikeFullIngredientListText(explicitIngredients)
    ? stripIngredientPackageDisclaimer(explicitIngredients)
    : "";
  const activeIngredientSection =
    firstMatchingSectionBody(detailsSections, [/\bactive ingredients?\b/i]) ||
    (!explicitIngredients ? firstMatchingSectionBody(detailsSections, [/\b(?:key|hero) ingredients?\b/i]) : null);
  const explicitActiveIngredients = cleanText(params.activeIngredientsText);
  const sectionActiveIngredients = cleanText(activeIngredientSection?.body);
  const activeIngredients =
    (sectionActiveIngredients.length > explicitActiveIngredients.length ? sectionActiveIngredients : explicitActiveIngredients) ||
    undefined;
  const ingredientsRaw =
    explicitFullIngredients ||
    (looksLikeFullIngredientListText(ingredientSectionBody)
      ? stripIngredientPackageDisclaimer(ingredientSectionBody)
      : "") ||
    undefined;
  const ingredientSummaryBody = !explicitFullIngredients && explicitIngredients ? explicitIngredients : ingredientSectionBody;
  const labeledActiveIngredients =
    extractDelimitedLabeledSectionText(
      ingredientsRaw || ingredientSummaryBody,
      ["Active Ingredients", "Active Ingredient"],
      ["Inactive Ingredients", "Ingredient List", "Ingredients"],
    ) || undefined;
  const activeIngredientsRaw =
    ((labeledActiveIngredients && labeledActiveIngredients.length >= (activeIngredients || "").length
      ? labeledActiveIngredients
      : activeIngredients) ||
      labeledActiveIngredients) ||
    (!ingredientsRaw && ingredientSummaryBody ? ingredientSummaryBody : "") ||
    undefined;
  const howToUseRaw =
    cleanText(params.howToUseText) ||
    firstMatchingSectionBody(detailsSections, [/\bhow(?:\s*|-)?to(?:\s+(?:use|apply))?\b/i, /\busage instructions?\b/i])?.body ||
    undefined;

  return {
    ingredientsRaw,
    activeIngredientsRaw,
    howToUseRaw,
  };
}

function matchHtmlSnippet(html: string, pattern: RegExp) {
  const match = html.match(pattern);
  return cleanText(match?.[1]);
}

function matchMetaContent(html: string, attribute: string, value: string) {
  const escapedAttribute = attribute.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const escapedValue = value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return cleanText(
    html.match(
      new RegExp(
        `<meta[^>]+${escapedAttribute}=["']${escapedValue}["'][^>]+content=["']([^"']+)["'][^>]*>`,
        "i",
      ),
    )?.[1],
  );
}

function extractHtmlAccordionSections(params: {
  html: string;
  startPattern: RegExp;
  headingPattern: RegExp;
  bodyPattern: RegExp;
  sourceKind: string;
}) {
  const starts = Array.from(params.html.matchAll(params.startPattern))
    .map((match) => match.index ?? -1)
    .filter((index) => index >= 0);
  if (starts.length === 0) return [] as ExtractedProductDetailSection[];

  const sections: ExtractedProductDetailSection[] = [];
  for (let i = 0; i < starts.length; i += 1) {
    const start = starts[i]!;
    const end = i + 1 < starts.length ? starts[i + 1]! : params.html.length;
    const block = params.html.slice(start, end);
    const heading = matchHtmlSnippet(block, params.headingPattern);
    const body = matchHtmlSnippet(block, params.bodyPattern);
    if (!heading || !body) continue;
    sections.push({
      heading,
      body,
      source_kind: params.sourceKind,
    });
  }

  return sections;
}

function extractMuradIngredientListText(text: string | undefined) {
  const normalized = cleanText(text);
  if (!normalized) return "";
  const truncated = normalized.match(
    /^([\s\S]*?)(?=\bFormulated Without:|\bWhile Murad(?:®)? strives to keep ingredient lists on this website as accurate as possible\b|$)/i,
  )?.[1];
  return cleanText(truncated || normalized);
}

function extractMuradHowToSectionText(html: string) {
  const sectionHtml = html.match(
    /<section[^>]+class=["'][^"']*shopify-section how-to[^"']*["'][^>]*>([\s\S]*?)<\/section>/i,
  )?.[1];
  if (!sectionHtml) return "";

  const sectionTitle = matchHtmlSnippet(sectionHtml, /<h2[^>]*>([\s\S]*?)<\/h2>/i);
  const stepBodies = dedupeStringList(
    Array.from(
      sectionHtml.matchAll(/<div[^>]+class=["'][^"']*metafield-rich_text_field[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi),
    ).map((match) => cleanHtmlText(match[1])),
  );

  return cleanText([sectionTitle, ...stepBodies].filter(Boolean).join("\n"));
}

export function extractStaticHtmlPdpFallbackProduct(params: {
  brand: string;
  url: string;
  html: string;
}): ExtractedProduct | null {
  const title =
    matchMetaContent(params.html, "property", "og:title") ||
    matchMetaContent(params.html, "name", "twitter:title") ||
    matchHtmlSnippet(params.html, /<title[^>]*>([\s\S]*?)<\/title>/i);
  const metaDescription =
    matchMetaContent(params.html, "name", "description") ||
    matchMetaContent(params.html, "property", "og:description");
  const heroImage =
    matchMetaContent(params.html, "property", "og:image") ||
    matchMetaContent(params.html, "name", "twitter:image");
  const detailsBody =
    matchHtmlSnippet(
      params.html,
      /<div[^>]+class=["'][^"']*product__content[^"']*["'][^>]*>([\s\S]*?)<modal\b/i,
    ) ||
    matchHtmlSnippet(
      params.html,
      /<div[^>]+class=["'][^"']*product__content[^"']*["'][^>]*>([\s\S]*?)<\/div>\s*<\/section>/i,
    );
  const ingredientsBody =
    matchHtmlSnippet(
      params.html,
      /<modal[^>]+handle=["']productIngredients["'][\s\S]*?<div[^>]+class=["'][^"']*product-ingredients-modal__content[^"']*["'][^>]*>([\s\S]*?)<\/div>/i,
    ) ||
    matchHtmlSnippet(
      params.html,
      /<modal[^>]+handle=["']productIngredients["'][\s\S]*?<div[^>]+class=["'][^"']*modal__content[^"']*["'][^>]*>([\s\S]*?)<\/div>/i,
    );
  const muradIngredientsBody = matchHtmlSnippet(
    params.html,
    /<p[^>]+id=["']ingredients-content["'][^>]*>([\s\S]*?)<\/p>/i,
  );
  const muradIngredientsRaw = extractMuradIngredientListText(muradIngredientsBody);
  const muradHowToRaw = extractMuradHowToSectionText(params.html);
  const accordionSections = extractHtmlAccordionSections({
    html: params.html,
    startPattern: /<div[^>]+class=["'][^"']*product-infos__accordion__category\b[^"']*["'][^>]*>/gi,
    headingPattern:
      /<button[^>]+class=["'][^"']*product-infos__accordion__category__title[^"']*["'][^>]*>([\s\S]*?)<\/button>/i,
    bodyPattern: /<div[^>]+class=["'][^"']*collapse\b[^"']*["'][^>]*>([\s\S]*)$/i,
    sourceKind: "html_snapshot_product_infos_accordion",
  });

  const detailsSections = dedupeDetailSections(
    [
      detailsBody
        ? {
            heading: "Details",
            body: detailsBody,
            source_kind: "html_snapshot_product_content",
          }
        : null,
      ingredientsBody
        ? {
            heading: "Full Ingredients",
            body: ingredientsBody,
            source_kind: "html_snapshot_product_ingredients_modal",
          }
        : null,
      muradIngredientsBody
        ? {
            heading: "Full List of Ingredients",
            body: muradIngredientsBody,
            source_kind: "html_snapshot_murad_ingredients_content",
          }
        : null,
      muradHowToRaw
        ? {
            heading: "How-To",
            body: muradHowToRaw,
            source_kind: "html_snapshot_murad_how_to_section",
          }
        : null,
      ...accordionSections,
    ].filter((section): section is ExtractedProductDetailSection => Boolean(section)),
  );

  const derivedBodies = deriveProductPdpModuleBodies({
    detailsSections,
  });
  const pdpFields = buildProductPdpFields({
    descriptionRaw: detailsBody || metaDescription,
    detailsSections,
    ingredientsRaw: muradIngredientsRaw || derivedBodies.ingredientsRaw,
    activeIngredientsRaw: derivedBodies.activeIngredientsRaw,
    howToUseRaw: muradHowToRaw || derivedBodies.howToUseRaw,
    fieldSources: {
      description_raw: [detailsBody ? "html_snapshot_product_content" : metaDescription ? "meta_description" : ""],
      details_sections: detailsSections.map((section) => section.source_kind),
      ingredients_raw: [
        muradIngredientsRaw
          ? "html_snapshot_murad_ingredients_content"
          : derivedBodies.ingredientsRaw
            ? "html_snapshot_product_ingredients_modal"
            : "",
      ],
      active_ingredients_raw: [derivedBodies.activeIngredientsRaw ? "html_snapshot_product_ingredients_modal" : ""],
      how_to_use_raw: [
        muradHowToRaw
          ? "html_snapshot_murad_how_to_section"
          : derivedBodies.howToUseRaw
            ? "html_snapshot_product_content"
            : "",
      ],
    },
  });

  if (
    !title &&
    !heroImage &&
    !pdpFields.description_raw &&
    !pdpFields.ingredients_raw &&
    !pdpFields.active_ingredients_raw &&
    !pdpFields.how_to_use_raw &&
    (pdpFields.details_sections?.length || 0) === 0
  ) {
    return null;
  }

  return {
    title: title || params.brand,
    url: params.url,
    image_url: heroImage || "",
    image_urls: heroImage ? [heroImage] : [],
    variant_skus: [],
    variants: [],
    ...pdpFields,
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
  const howToUseRaw = cleanText(params.howToUseRaw);
  const faqItems = dedupeFaqItems(params.faqItems || []);

  return {
    ...(descriptionRaw ? { description_raw: descriptionRaw } : {}),
    ...(detailsSections.length > 0 ? { details_sections: detailsSections } : {}),
    ...(ingredientsRaw ? { ingredients_raw: ingredientsRaw } : {}),
    ...(activeIngredientsRaw ? { active_ingredients_raw: activeIngredientsRaw } : {}),
    ...(howToUseRaw ? { how_to_use_raw: howToUseRaw } : {}),
    ...(faqItems.length > 0 ? { faq_items: faqItems } : {}),
    field_capture_status: {
      description_raw: descriptionRaw ? "present" : "missing",
      details_sections: detailsSections.length > 0 ? "present" : "missing",
      ingredients_raw: ingredientsRaw ? "present" : "missing",
      active_ingredients_raw: activeIngredientsRaw ? "present" : "missing",
      how_to_use_raw: howToUseRaw ? "present" : "missing",
      faq_items: faqItems.length > 0 ? "present" : "missing",
    } as const,
    field_sources: {
      description_raw: uniqueFieldSources(params.fieldSources?.description_raw || []),
      details_sections: uniqueFieldSources(params.fieldSources?.details_sections || []),
      ingredients_raw: uniqueFieldSources(params.fieldSources?.ingredients_raw || []),
      active_ingredients_raw: uniqueFieldSources(params.fieldSources?.active_ingredients_raw || []),
      how_to_use_raw: uniqueFieldSources(params.fieldSources?.how_to_use_raw || []),
      faq_items: uniqueFieldSources(params.fieldSources?.faq_items || []),
    },
  };
}

function productHasMissingPdpFields(product: ExtractedProduct) {
  const detailsSections = Array.isArray(product?.details_sections) ? product.details_sections : [];
  const faqItems = Array.isArray(product?.faq_items) ? product.faq_items : [];
  const descriptionMissing = !cleanText(product?.description_raw);
  const moduleMissing =
    detailsSections.length === 0 &&
    !cleanText(product?.ingredients_raw) &&
    !cleanText(product?.active_ingredients_raw) &&
    !cleanText(product?.how_to_use_raw) &&
    faqItems.length === 0;
  return descriptionMissing || moduleMissing;
}

function countRecoveredPdpFields(product: ExtractedProduct | null | undefined) {
  if (!product) return 0;

  return (
    Number(Boolean(cleanText(product.description_raw))) +
    Number(Boolean(cleanText(product.ingredients_raw) || cleanText(product.active_ingredients_raw))) +
    Number(Boolean(cleanText(product.how_to_use_raw))) +
    Number((product.details_sections || []).length > 0)
  );
}

function chooseMergedDescriptionRaw(existing?: string, fallback?: string) {
  return (
    choosePreferredProductOverview({
      structured: existing,
      detailed: fallback,
      meta: existing,
    }) ||
    cleanText(existing) ||
    cleanText(fallback) ||
    undefined
  );
}

export function canReturnHtmlProductsWithoutBrowser(params: {
  products: ExtractedProduct[];
  candidateCount: number;
}) {
  const products = Array.isArray(params.products) ? params.products : [];
  if (products.length === 0 || products.length !== params.candidateCount) return false;
  return products.every(
    (product) =>
      product.image_urls.length > 0 &&
      product.variants.every((variant) => variant.image_urls.length > 0) &&
      !productHasMissingPdpFields(product),
  );
}

export function resolveDirectPdpEnrichmentUrl(params: {
  seedUrl?: string;
  productUrl?: string;
  baseUrl: string;
}): string | null {
  const canonicalProductUrl = cleanText(params.productUrl);
  if (canonicalProductUrl && isLikelyProductUrlShared(canonicalProductUrl, params.baseUrl)) {
    return canonicalProductUrl;
  }

  const seedUrl = cleanText(params.seedUrl);
  if (seedUrl && isLikelyProductUrlShared(seedUrl, params.baseUrl)) {
    return seedUrl;
  }

  return canonicalProductUrl || seedUrl || null;
}

export function chooseDiscoveryBatchCandidates(params: {
  productUrls: string[];
  offset: number;
  limit: number;
  reserve: number;
  seedUrl?: string;
  baseUrl: string;
}): string[] {
  const requestedWindowEnd = params.offset + params.limit + params.reserve;
  const directSeedCandidate = Boolean(
    params.seedUrl && scoreProductCandidateUrl(params.seedUrl, params.baseUrl) >= 4,
  );
  if (!directSeedCandidate) {
    return params.productUrls.slice(params.offset, requestedWindowEnd);
  }

  const seedCandidateLimit = clampIntShared(
    process.env.PUPPETEER_SEED_DISCOVERY_CANDIDATE_LIMIT,
    4,
    1,
    20,
  );
  return params.productUrls.slice(params.offset, Math.min(requestedWindowEnd, params.offset + seedCandidateLimit));
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

const SHOPIFY_FEED_NON_PRODUCT_TITLE_RE =
  /\b(?:welcome gift|surprise gift|free gift|gift with purchase|gwp|gift card|e-?gift card|sample(?:s)?|deluxe sample|complimentary sample|complimentary deluxe sample)\b/i;
const SHOPIFY_FEED_NON_PRODUCT_HANDLE_RE =
  /(?:^|[-_/])(?:welcome-gift|surprise-gift|free-gift|gift-with-purchase|gwp|gift-card|e-gift-card|sample|samples|deluxe-sample|complimentary-sample|complimentary-deluxe-sample)(?:[-_/]|$)/i;
const SHOPIFY_FEED_GIFT_TOKEN_RE = /(?:^|[-_/ ])gift(?:$|[-_/ ])/i;
const SHOPIFY_FEED_PROMO_CONTEXT_RE = /\b(?:rewards store|gift with purchase|free gift|complimentary|welcome|surprise)\b/i;
const SHOPIFY_FEED_ZERO_PRICE_TRAVEL_RE = /\b(?:travel|trial|mini)\b/i;

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

export function isNonProductShopifyFeedProduct(product: ShopifyProduct): boolean {
  const title = safeDecodeURIComponent(String(product.title || "").trim().toLowerCase());
  const handle = safeDecodeURIComponent(String(product.handle || "").trim().toLowerCase());
  const body = cleanText(product.body_html || "").toLowerCase();
  const variantPrices = Array.isArray(product.variants)
    ? product.variants
        .map((variant) => Number.parseFloat(normalizeShopifyPrice(variant.price, "USD")))
        .filter((value) => Number.isFinite(value))
    : [];
  const allVariantsAreZeroPriced = variantPrices.length > 0 && variantPrices.every((value) => value === 0);
  if (SHOPIFY_FEED_NON_PRODUCT_TITLE_RE.test(title)) return true;
  if (SHOPIFY_FEED_NON_PRODUCT_HANDLE_RE.test(handle)) return true;
  if (
    SHOPIFY_FEED_GIFT_TOKEN_RE.test(`${title} ${handle}`) &&
    (allVariantsAreZeroPriced || SHOPIFY_FEED_PROMO_CONTEXT_RE.test(`${title} ${handle} ${body}`))
  ) {
    return true;
  }
  if (allVariantsAreZeroPriced && SHOPIFY_FEED_ZERO_PRICE_TRAVEL_RE.test(`${title} ${handle}`)) {
    return true;
  }
  if (
    body &&
    /\b(?:gift with purchase|free gift|complimentary sample|deluxe sample|while supplies last)\b/i.test(body) &&
    /\b(?:gift|sample|samples)\b/i.test(`${title} ${handle}`)
  ) {
    return true;
  }
  return false;
}

export function filterShopifyCatalogProducts(products: ShopifyProduct[]): ShopifyProduct[] {
  const filtered = products.filter((product) => !isNonProductShopifyFeedProduct(product));
  return filtered.length > 0 ? filtered : products;
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
  marketId: ExtractInput["market"];
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
  const marketProfile = getMarketProfile(normalizeMarketId(params.marketId));
  const shopifyContext = {
    headers: marketProfile.headers,
    cookies: marketProfile.cookies,
  };

  if (directHandle) {
    const directUrl = `${params.baseUrl}/products/${directHandle}.js`;
    log("info", `Checking Shopify direct product feed: ${directUrl}`);
    const directProduct = await fetchJsonTracked<ShopifyProduct>(directUrl, shopifyContext, params.diagnostics!);
    if (directProduct.data && typeof directProduct.data.id === "number") {
      log("success", `Shopify direct product detected for handle: ${directHandle}`);
      setDiscoveryStrategy(params.diagnostics!, "shopify_json");
      const currencyHint = await fetchShopifyCurrencyHint(currencyHintUrls, params.diagnostics!, shopifyContext);
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
    log("warn", `Shopify direct product feed not found for handle: ${directHandle}. Falling back to direct page discovery.`);
    return null;
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

  const filteredProducts = filterShopifyCatalogProducts(allProducts);
  const removedCount = allProducts.length - filteredProducts.length;
  if (removedCount > 0) {
    log("warn", `Filtered ${removedCount} non-product Shopify feed rows before catalog response assembly.`);
  }
  const limitedProducts = filteredProducts.slice(0, params.maxProducts);
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
  browserRunner?: typeof runBrowserTaskWithFallback<ExtractedProduct | null>;
  htmlFetcher?: typeof fetchHtmlViaNativeRequest;
}): Promise<Omit<ExtractResponse, "generated_at" | "logs">> {
  const product = params.response.products[0];
  if (!params.seedUrl || !product || params.response.products.length !== 1) return params.response;

  const productMissingImages = product.image_urls.length === 0;
  const variantMissingImages = product.variants.some((variant) => variant.image_urls.length === 0);
  const productMissingPdpFields = productHasMissingPdpFields(product);

  const enrichmentUrl = resolveDirectPdpEnrichmentUrl({
    seedUrl: params.seedUrl,
    productUrl: product.url,
    baseUrl: params.baseUrl,
  });
  if (!enrichmentUrl) return params.response;

  let responseWithDirectPdpSignals = params.response;
  const htmlFetcher = params.htmlFetcher || fetchHtmlViaNativeRequest;
  const htmlSnapshot = await htmlFetcher(enrichmentUrl, params.diagnostics);
  if (htmlSnapshot.body) {
    const htmlProduct = extractProductFromHtmlSnapshot({
      html: htmlSnapshot.body,
      url: htmlSnapshot.finalUrl || enrichmentUrl,
      baseUrl: params.baseUrl,
      log: params.log,
    });
    if (htmlProduct) {
      const htmlMerged = mergeShopifyDirectPdpSignals(params.brand, params.response, htmlProduct);
      const htmlMergedProduct = htmlMerged.products[0];
      const htmlRecoveredMorePdpFields = countRecoveredPdpFields(htmlMergedProduct) > countRecoveredPdpFields(product);
      const htmlRecoveredMoreImages =
        (htmlMergedProduct?.image_urls.length || 0) > product.image_urls.length ||
        Boolean(
          htmlMergedProduct?.variants.some((variant, idx) => {
            const originalVariant = product.variants[idx];
            return (variant.image_urls.length || 0) > (originalVariant?.image_urls.length || 0);
          }),
        );
      if (htmlRecoveredMorePdpFields || htmlRecoveredMoreImages) {
        responseWithDirectPdpSignals = htmlMerged;
      }
      const htmlStillMissingImages =
        (htmlMergedProduct?.image_urls.length || 0) === 0 ||
        Boolean(htmlMergedProduct?.variants.some((variant) => variant.image_urls.length === 0));
      const htmlStillMissingPdpFields = htmlMergedProduct ? productHasMissingPdpFields(htmlMergedProduct) : true;

      if (!htmlStillMissingImages && !htmlStillMissingPdpFields) {
        params.log("success", `Merged Shopify PDP page signals from native HTML: ${enrichmentUrl}`);
        return htmlMerged;
      }

      if (!productMissingImages && !variantMissingImages && !htmlStillMissingPdpFields) {
        params.log("success", `Merged Shopify PDP modules from native HTML: ${enrichmentUrl}`);
        return htmlMerged;
      }
    }
  }

  const directPdpProduct = responseWithDirectPdpSignals.products[0];
  const directPdpStillMissingImages =
    (directPdpProduct?.image_urls.length || 0) === 0 ||
    Boolean(directPdpProduct?.variants.some((variant) => variant.image_urls.length === 0));
  const directPdpStillMissingPdpFields = directPdpProduct ? productHasMissingPdpFields(directPdpProduct) : true;
  if (!directPdpStillMissingImages && !directPdpStillMissingPdpFields) return responseWithDirectPdpSignals;
  if (!productMissingImages && !variantMissingImages && !productMissingPdpFields) return responseWithDirectPdpSignals;

  params.log(
    "info",
    `Shopify direct PDP returned incomplete image/PDP fields. Attempting browser enrichment: ${enrichmentUrl}`,
  );

  try {
    const htmlSnapshot = await fetchTextTracked(params.seedUrl, {}, params.diagnostics);
    const secondaryHtmlProduct =
      htmlSnapshot.ok && htmlSnapshot.body
        ? extractProductFromHtmlSnapshot({
            html: htmlSnapshot.body,
            url: params.seedUrl,
            baseUrl: params.baseUrl,
            log: params.log,
          })
        : null;
    if (secondaryHtmlProduct) {
      responseWithDirectPdpSignals = mergeShopifyDirectPdpSignals(params.brand, responseWithDirectPdpSignals, secondaryHtmlProduct);
      const mergedProduct = responseWithDirectPdpSignals.products[0];
      const recoveredFields =
        Number(Boolean(mergedProduct?.description_raw)) +
        Number(Boolean(mergedProduct?.ingredients_raw || mergedProduct?.active_ingredients_raw)) +
        Number(Boolean(mergedProduct?.how_to_use_raw)) +
        Number(((mergedProduct?.details_sections || []).length > 0));
      if (recoveredFields > 0) {
        params.log(
          "info",
          `Merged Shopify PDP fields from secondary native HTML snapshot before browser enrichment: ${params.seedUrl}`,
        );
      }
      if (mergedProduct && !productHasMissingPdpFields(mergedProduct)) return responseWithDirectPdpSignals;
    }
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error || "unknown_error");
    params.log("warn", `Secondary native HTML snapshot enrichment failed for Shopify PDP: ${msg}`);
  }

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
            url: enrichmentUrl,
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
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error || "unknown_error");
    params.log("warn", `Browser enrichment failed for Shopify PDP; returning best available response: ${msg}`);
    return responseWithDirectPdpSignals;
  }

  if (!browserRun.result) {
    params.log("warn", `Browser enrichment did not recover images for Shopify PDP: ${params.seedUrl}`);
    return responseWithDirectPdpSignals;
  }

  const merged = mergeShopifyDirectPdpSignals(params.brand, responseWithDirectPdpSignals, browserRun.result);
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
    const match = parsed.pathname.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products?\/([^/?#]+)/i);
    return match?.[1] ? safeDecodeURIComponent(match[1]) : null;
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
    const officialDetailsSections = extractStrongLedHtmlSections(officialText || "", "shopify_body_html_section");
    const derivedOfficialPdpBodies = deriveProductPdpModuleBodies({
      detailsSections: officialDetailsSections,
    });
    const productPdpFields = buildProductPdpFields({
      descriptionRaw: officialText,
      detailsSections: officialDetailsSections,
      ingredientsRaw:
        extractLabeledSectionText(officialText, ["Ingredients and Safety", "Ingredients"]) ||
        derivedOfficialPdpBodies.ingredientsRaw,
      activeIngredientsRaw: extractLabeledSectionText(officialText, ["Active Ingredients", "Active Ingredient"]),
      howToUseRaw: extractLabeledSectionText(officialText, ["How to Use"]) || derivedOfficialPdpBodies.howToUseRaw,
      faqItems: [],
      fieldSources: {
        description_raw: officialText ? ["shopify_body_html"] : [],
        details_sections: officialDetailsSections.map((section) => section.source_kind),
        ingredients_raw: officialText ? ["shopify_body_html_labeled_ingredients"] : [],
        active_ingredients_raw: officialText ? ["shopify_body_html_labeled_active_ingredients"] : [],
        how_to_use_raw: officialText ? ["shopify_body_html_labeled_how_to_use"] : [],
        faq_items: [],
      },
    });

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

    const existing: ExtractedProduct =
      extractedByTitle.get(canonicalProductTitle) ||
      {
        title: canonicalProductTitle,
        url: productUrl,
        image_url: productImageUrls[0] || "",
        image_urls: productImageUrls,
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
    const mergedPdpFields = buildProductPdpFields({
      descriptionRaw: chooseMergedDescriptionRaw(existing.description_raw, productPdpFields.description_raw),
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

export function mergeShopifyDirectPdpSignals(
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
      content_image_urls: [...(product.content_image_urls || [])],
      variant_skus: [...product.variant_skus],
      variants: product.variants.map((variant) => ({
        ...variant,
        image_urls: [...variant.image_urls],
      })),
    };
    Object.assign(
      mergedProduct,
      buildProductPdpFields({
        descriptionRaw: chooseMergedDescriptionRaw(product.description_raw, fallbackProduct.description_raw),
        detailsSections: dedupeDetailSections([
          ...(Array.isArray(product.details_sections) ? product.details_sections : []),
          ...(Array.isArray(fallbackProduct.details_sections) ? fallbackProduct.details_sections : []),
        ]),
        ingredientsRaw: product.ingredients_raw || fallbackProduct.ingredients_raw,
        activeIngredientsRaw: product.active_ingredients_raw || fallbackProduct.active_ingredients_raw,
        howToUseRaw: product.how_to_use_raw || fallbackProduct.how_to_use_raw,
        faqItems:
          (Array.isArray(product.faq_items) && product.faq_items.length > 0)
            ? product.faq_items
            : fallbackProduct.faq_items,
        fieldSources: {
          description_raw: [
            ...(product.field_sources?.description_raw || []),
            ...(fallbackProduct.field_sources?.description_raw || []),
          ],
          details_sections: [
            ...(product.field_sources?.details_sections || []),
            ...(fallbackProduct.field_sources?.details_sections || []),
          ],
          ingredients_raw: [
            ...(product.field_sources?.ingredients_raw || []),
            ...(fallbackProduct.field_sources?.ingredients_raw || []),
          ],
          active_ingredients_raw: [
            ...(product.field_sources?.active_ingredients_raw || []),
            ...(fallbackProduct.field_sources?.active_ingredients_raw || []),
          ],
          how_to_use_raw: [
            ...(product.field_sources?.how_to_use_raw || []),
            ...(fallbackProduct.field_sources?.how_to_use_raw || []),
          ],
          faq_items: [
            ...(product.field_sources?.faq_items || []),
            ...(fallbackProduct.field_sources?.faq_items || []),
          ],
        },
      }),
    );

    const rawFallbackProductImages = dedupeStringList([
      ...fallbackProduct.image_urls,
      fallbackProduct.image_url,
      ...fallbackProduct.variants.flatMap((variant) => variant.image_urls),
      ...fallbackProduct.variants.map((variant) => variant.image_url),
    ]);
    const fallbackContentImages = dedupeStringList(fallbackProduct.content_image_urls || []);
    const fallbackProductImages = selectRelevantFallbackImageUrls(
      {
        title: mergedProduct.title,
        url: mergedProduct.url,
      },
      rawFallbackProductImages,
    );
    const mergedContentImages = dedupeStringList([
      ...(mergedProduct.content_image_urls || []),
      ...fallbackContentImages,
    ]);
    if (mergedContentImages.length > 0) mergedProduct.content_image_urls = mergedContentImages;

    if (fallbackProductImages.length === 0 && mergedContentImages.length === 0) return mergedProduct;

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
          ...mergedContentImages,
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
      ...mergedContentImages,
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

export const mergeShopifyDirectPdpFallback = mergeShopifyDirectPdpSignals;

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
  const result = await fetchTextWithFinalUrl(url);
  return result.ok ? result.body : null;
}

async function fetchTextWithFinalUrl(url: string): Promise<{ ok: boolean; body: string | null; finalUrl: string }> {
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
    const finalUrl = res.url || url;
    if (!res.ok) return { ok: false, body: null, finalUrl };
    return { ok: true, body: await res.text(), finalUrl };
  } catch {
    return { ok: false, body: null, finalUrl: url };
  } finally {
    clearTimeout(timer);
  }
}

function tokenizePathSegment(value: string): string[] {
  return value
    .toLowerCase()
    .split(/[^a-z0-9]+/i)
    .map((token) => token.trim())
    .filter(Boolean)
    .filter((token) => token.length > 1)
    .filter((token) => !["product", "products", "collection", "collections"].includes(token));
}

function scoreSeedUrlAffinity(candidateUrl: string, seedUrl: string, baseUrl: string): number {
  const candidate = parseHttpUrl(candidateUrl, baseUrl);
  const seed = parseHttpUrl(seedUrl, baseUrl);
  if (!candidate || !seed) return Number.NEGATIVE_INFINITY;

  const candidateSegment = candidate.pathname.split("/").filter(Boolean).pop() || "";
  const seedSegment = seed.pathname.split("/").filter(Boolean).pop() || "";
  const candidateTokens = tokenizePathSegment(candidateSegment);
  const seedTokens = tokenizePathSegment(seedSegment);
  if (candidateTokens.length === 0 || seedTokens.length === 0) return 0;

  const candidateSet = new Set(candidateTokens);
  const seedSet = new Set(seedTokens);
  let shared = 0;
  for (const token of seedSet) {
    if (candidateSet.has(token)) shared += 1;
  }

  const unionSize = new Set([...candidateSet, ...seedSet]).size || 1;
  let score = shared / unionSize;
  if (candidateSegment === seedSegment) score += 1;
  if (candidate.pathname.toLowerCase() === seed.pathname.toLowerCase()) score += 1;
  return score;
}

function rankProductUrlsForSeed(productUrls: string[], seedUrl: string, baseUrl: string): string[] {
  return [...productUrls].sort((a, b) => {
    const affinityDiff = scoreSeedUrlAffinity(b, seedUrl, baseUrl) - scoreSeedUrlAffinity(a, seedUrl, baseUrl);
    if (affinityDiff !== 0) return affinityDiff;

    const scoreDiff = scoreProductCandidateUrl(b, baseUrl) - scoreProductCandidateUrl(a, baseUrl);
    if (scoreDiff !== 0) return scoreDiff;

    return a.localeCompare(b);
  });
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

export async function discoverProductUrls(params: { baseUrl: string; maxProducts: number; seedUrl?: string; log: Logger }) {
  if (params.seedUrl) {
    params.log("info", `GET ${params.seedUrl}`);
    const seed = await fetchTextWithFinalUrl(params.seedUrl);
    const seedHtml = seed.body;
    if (seedHtml) {
      const requestedSeedUrl = canonicalizeUrlShared(params.seedUrl, params.baseUrl);
      const resolvedSeedUrl = canonicalizeUrlShared(seed.finalUrl || params.seedUrl, params.baseUrl);
      const resolvedSeedLooksProductLike = scoreProductCandidateUrl(resolvedSeedUrl, params.baseUrl) >= 4;
      if (resolvedSeedLooksProductLike && looksLikeProductPageHtml(seedHtml)) {
        params.log("success", "Seed page resolved directly to a PDP.");
        return { sitemapUrl: undefined, productUrls: [resolvedSeedUrl] };
      }

      const seedUrls = (!seed.ok || resolvedSeedLooksProductLike
        ? []
        : rankProductUrlsForSeed(extractProductUrlsFromHtml(seedHtml, params.baseUrl), requestedSeedUrl, params.baseUrl)
      ).slice(0, params.maxProducts);
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
  /(placeholder\.svg|\/favicon|\/apple-touch-icon|\/[^/?#]*logo[^/?#]*\.(?:png|jpe?g|webp|svg)(?:[?#]|$)|\/logo(?:[._/-]|$)|\/sprite(?:[._/-]|$)|\/track(?:ing)?[._/-]|tracking|teads\.tv)/i;

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

function imageDedupeKey(value: string) {
  try {
    const parsed = new URL(value);
    return `${parsed.origin}${parsed.pathname}`.toLowerCase();
  } catch {
    return value.toLowerCase();
  }
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

type NativeHtmlFetchResult = { status: number | null; body: string | null; finalUrl: string };

export async function fetchHtmlViaNativeRequest(
  url: string,
  diagnostics: ExtractResponse["diagnostics"],
  redirectCount = 0,
): Promise<NativeHtmlFetchResult> {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result: NativeHtmlFetchResult) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    try {
      const parsed = new URL(url);
      const request = (parsed.protocol === "https:" ? https : http).request(
        parsed,
        {
          method: "GET",
          headers: {
            accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
          },
        },
        (response) => {
          const status = response.statusCode ?? null;
          diagnostics?.http_trace.push({ url, status });

          if (
            status &&
            [301, 302, 303, 307, 308].includes(status) &&
            redirectCount < 5
          ) {
            const location = response.headers.location;
            if (location) {
              const nextUrl = new URL(location, url).toString();
              response.resume();
              void fetchHtmlViaNativeRequest(nextUrl, diagnostics, redirectCount + 1).then(finish);
              return;
            }
          }

          let body = "";
          response.setEncoding("utf8");
          response.on("data", (chunk) => {
            body += chunk;
          });
          response.on("end", () => {
            finish({ status, body, finalUrl: url });
          });
        },
      );

      request.setTimeout(DEFAULT_FETCH_TIMEOUT_MS, () => {
        request.destroy(new Error(`Native HTML request timed out after ${DEFAULT_FETCH_TIMEOUT_MS}ms`));
      });
      request.on("error", () => {
        finish({ status: null, body: null, finalUrl: url });
      });
      request.end();
    } catch {
      finish({ status: null, body: null, finalUrl: url });
    }
  });
}

export async function extractPageSignals(page: Page): Promise<ScrapedPageSignals> {
  return page.evaluate(async () => {
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

    const looksLikeIngredientModalText = (raw: string) =>
      /\bingredients?\b/i.test(raw) || /[A-Za-z][^,\n]{1,40},\s*[A-Za-z][^,\n]{1,40},\s*[A-Za-z][^,\n]{1,40}/.test(raw);

    const fetchGuerlainIngredientModalText = async (rawUrl: string) => {
      const modalUrl = rawUrl.trim();
      if (!modalUrl) return "";

      try {
        const response = await fetch(new URL(modalUrl, documentBase).toString(), {
          credentials: "include",
        });
        if (!response.ok) return "";

        const html = await response.text();
        if (!html.trim()) return "";

        const parsed = new DOMParser().parseFromString(html, "text/html");
        const selectors = [
          ".modal__content",
          ".modal-content",
          ".modal-body",
          ".product-ingredients",
          ".ingredients",
          "[class*='ingredient']",
          "body",
        ];

        for (const selector of selectors) {
          const nodes = Array.from(parsed.querySelectorAll(selector));
          for (const node of nodes.slice(0, 8)) {
            const text = normalizeSectionText((node as HTMLElement).innerText || node.textContent || "");
            if (text && looksLikeIngredientModalText(text)) return text;
          }
        }
      } catch {
        // ignore modal fetch failures
      }

      return "";
    };

    const readGuerlainIngredientModalDomText = () => {
      const selectors = [
        "#ingredientsModal .modal__content",
        "#ingredientsModal .modal-content",
        "#ingredientsModal .modal-body",
        "[id*='ingredientsModal'] .modal__content",
        "[id*='ingredientsModal'] .modal-content",
        "[id*='ingredientsModal'] .modal-body",
        ".modal.show .modal__content",
        ".modal.show .modal-content",
        ".modal.show .modal-body",
        ".modal.in .modal__content",
        ".modal.in .modal-content",
        ".modal.in .modal-body",
      ];

      for (const selector of selectors) {
        const nodes = Array.from(document.querySelectorAll(selector));
        for (const node of nodes.slice(0, 8)) {
          const text = normalizeSectionText((node as HTMLElement).innerText || node.textContent || "");
          if (text && looksLikeIngredientModalText(text)) return text;
        }
      }

      return "";
    };

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
        /(placeholder\.svg|\/favicon|\/apple-touch-icon|\/[^/?#]*logo[^/?#]*\.(?:png|jpe?g|webp|svg)(?:[?#]|$)|\/logo(?:[._/-]|$)|\/sprite(?:[._/-]|$)|\/track(?:ing)?[._/-]|tracking|teads\.tv|\/MenuBanner\/|\/Library-Sites-)/i;
      const seen = new Set<string>();
      const out: string[] = [];
      const dedupeKey = (value: string) => {
        try {
          const parsed = new URL(value);
          return `${parsed.origin}${parsed.pathname}`.toLowerCase();
        } catch {
          return value.toLowerCase();
        }
      };

      const push = (raw: string | null | undefined) => {
        const trimmed = typeof raw === "string" ? raw.trim() : "";
        if (out.length >= 8) return;
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
            const key = dedupeKey(absolute);
            if (seen.has(key)) continue;
            seen.add(key);
            out.push(absolute);
            if (out.length >= 8) return;
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
    const contentImageCandidates = (() => {
      const selectors = [
        ".figma-image img",
        ".figma-image source",
        ".qq-content-stack img",
        ".qq-content-stack source",
        ".figma-html-wrapper img",
        ".figma-html-wrapper source",
        ".custom-figma-block img",
        ".custom-figma-block source",
        ".hero__media img",
        ".hero__media source",
        ".hero__image img",
        ".hero__image source",
        ".image__hero__frame img",
        ".image__hero__frame source",
        ".image__hero__scale img",
        ".image__hero__scale source",
        ".brick__slider img",
        ".brick__slider source",
        ".brick__section img",
        ".brick__section source",
        ".brick__block__image img",
        ".brick__block__image source",
        "[id*='__new_custom_pdp'] img",
        "[id*='__new_custom_pdp'] source",
        "[id*='section_custom_content'] img",
        "[id*='section_custom_content'] source",
        "[class*='product__content'] img",
        "[class*='product__content'] source",
        ".product__description img",
        ".product__description source",
      ];
      const invalidUrlRe =
        /(placeholder\.svg|\/favicon|\/apple-touch-icon|\/[^/?#]*logo[^/?#]*\.(?:png|jpe?g|webp|svg)(?:[?#]|$)|\/logo(?:[._/-]|$)|\/sprite(?:[._/-]|$)|\/track(?:ing)?[._/-]|tracking|teads\.tv|\/MenuBanner\/|\/Library-Sites-)/i;
      const seen = new Set<string>();
      const out: string[] = [];
      const dedupeKey = (value: string) => {
        try {
          const parsed = new URL(value);
          return `${parsed.origin}${parsed.pathname}`.toLowerCase();
        } catch {
          return value.toLowerCase();
        }
      };

      const push = (raw: string | null | undefined) => {
        const trimmed = typeof raw === "string" ? raw.trim() : "";
        if (out.length >= 24) return;
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
            const key = dedupeKey(absolute);
            if (seen.has(key)) continue;
            seen.add(key);
            out.push(absolute);
            if (out.length >= 24) return;
          } catch {
            // ignore invalid image candidates
          }
        }
      };

      for (const selector of selectors) {
        const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 48);
        for (const node of nodes) {
          const el = node as HTMLElement;
          push(el.getAttribute("data-src"));
          push(el.getAttribute("data-srcset"));
          push(el.getAttribute("zoom-src"));
          push(el.getAttribute("data-zoom-src"));
          push(el.getAttribute("data-zoom-image"));
          push(el.getAttribute("data-large-image"));
          push(el.getAttribute("srcset"));
          push(el.getAttribute("src"));
          if (out.length >= 24) return out;
        }
      }

      return out;
    })();

    const domVariants = (() => {
      const el = document.querySelector("[data-product-skus-value]") as HTMLElement | null;
      const raw = el?.getAttribute("data-product-skus-value") || "";
      if (!raw) return [] as DomVariantMeta[];

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
          .filter((variant) => Boolean(variant.sku));
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

    let howToUseText = howToUseContent?.querySelector(".markdown")?.textContent?.trim() || undefined;
    let ingredientsMarkdownText = ingredientsContent?.querySelector(".markdown")?.textContent?.trim() || undefined;
    let ingredientsDisclaimerText =
      ingredientsContent?.querySelector(".product-details-accordions-ingredients-disclaimer")?.textContent?.trim() || undefined;

    if (!howToUseText || !ingredientsMarkdownText || !ingredientsDisclaimerText) {
      const productFormAccordions = Array.from(document.querySelectorAll(".product-form__accordion")) as HTMLElement[];
      for (const accordion of productFormAccordions) {
        const button = accordion.querySelector("button") as HTMLButtonElement | null;
        const headingText = normalizeSectionText(
          button?.getAttribute("aria-label") || button?.getAttribute("title") || button?.textContent || "",
        ).toLowerCase();
        if (!headingText) continue;

        const content =
          accordion.querySelector(".accordion__content, .accordion__content.rte, .product-form__accordion-details") ||
          button?.parentElement?.nextElementSibling ||
          button?.nextElementSibling;
        const bodyText = normalizeSectionText((content as HTMLElement | null)?.innerText || content?.textContent || "");
        if (!bodyText) continue;

        if (!howToUseText && /^how to(?: use)?$/i.test(headingText)) {
          howToUseText = bodyText;
        } else if (!ingredientsMarkdownText && /^ingredients(?: and safety| & safety)?$/i.test(headingText)) {
          ingredientsMarkdownText = bodyText;
        } else if (
          !ingredientsDisclaimerText &&
          /^ingredients disclaimer$/i.test(headingText)
        ) {
          ingredientsDisclaimerText = bodyText;
        }
      }
    }
    const ingredientFlyoutText = (() => {
      const nodes = Array.from(
        document.querySelectorAll(".ingredients-flyout-content, [data-original-ingredients]"),
      ) as HTMLElement[];
      for (const node of nodes.slice(0, 8)) {
        const attrRaw = node.getAttribute("data-original-ingredients") || "";
        const attrText = attrRaw ? normalizeSectionText(decodeHtmlText(attrRaw)) : "";
        const visibleText = normalizeSectionText(node.innerText || node.textContent || "");
        const combined = normalizeSectionText([attrText, visibleText].filter(Boolean).join("\n\n"));
        if (!combined) continue;
        if (/\bactive ingredients?\b/i.test(combined) || /\binactive ingredients?\b/i.test(combined)) {
          return combined;
        }
      }
      return undefined;
    })();
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
          control.closest("accordion-wrap") ||
          control.closest(".pv-extra-details__accordion") ||
          control.closest(".acc") ||
          control.parentElement;
        const content =
          wrapper?.querySelector?.(
            ".accordion-content-wrap-inner, .accordion-content-wrap, .acc__menu, .pv-extra-details__accordion-body, .faq-answer, .faq__answer",
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

      return items;
    })();
    const detailsSections = await (async () => {
      const sections: ExtractedProductDetailSection[] = [];
      const seen = new Set<string>();
      const looksRelevantHeading = (heading: string) =>
        /\b(details?|benefits?|how(?:\s*|-)?to(?:\s+(?:use|apply))?|usage instructions?|ingredients?|active ingredients?|key ingredients?|inci|composition|about|what(?:'|’)s in it\??|faq|frequently asked questions?|q\s*&\s*a|questions?|clinical(?:\s+results?)?|results?|hydration|hydrates?|sebum|oil[-\s]*moisture|moisture|absorbs?|pores?|texture|finish|layer)\b/i.test(
          heading,
        );
      const shouldSkipSectionNode = (node: Element | null | undefined) =>
        Boolean(
          node?.closest(
            "header, nav, footer, .header__dropdown, .drawer__inner, .predictive-search, [class*='comparison'], [id*='comparison']",
          ),
        );
      const pushSection = (headingRaw: string, bodyRaw: string, sourceKind: string) => {
        let heading = normalizeSectionText(headingRaw);
        if (/^how to$/i.test(heading)) heading = "How to Use";
        const body = normalizeSectionText(bodyRaw);
        if (!heading || !body || !looksRelevantHeading(heading)) return;
        const key = `${heading.toLowerCase()}|${body.toLowerCase()}|${sourceKind.toLowerCase()}`;
        if (seen.has(key)) return;
        seen.add(key);
        sections.push({
          heading,
          body,
          source_kind: sourceKind,
        });
      };

      if (productDetailsText) {
        pushSection("Details", productDetailsText, "page_product_details");
      }
      if (howToUseText) {
        pushSection("How to Use", howToUseText, "accordion_how_to_use");
      }
      if (ingredientsMarkdownText) {
        pushSection("Ingredients", ingredientsMarkdownText, "accordion_ingredients");
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

      const guerlainIngredientSections = Array.from(
        document.querySelectorAll("[data-section='ingredientsCarousel'] section.section-ingredient, section#ingredientsCarousel.section-ingredient"),
      ) as HTMLElement[];
      const ingredientModalUrls = new Set<string>();
      for (const section of guerlainIngredientSections.slice(0, 4)) {
        const introText = normalizeSectionText(
          section.querySelector(".section-header .section-description, .section-description")?.textContent || "",
        );
        const ingredientCards = Array.from(section.querySelectorAll(".GSA_ingredient")).map((item) => {
          const title = normalizeSectionText(item.querySelector(".GSA_ingredient_title")?.textContent || "");
          const body = normalizeSectionText(item.querySelector(".GSA_ingredient_description")?.textContent || "");
          if (title && body) return `${title}: ${body}`;
          return title || body;
        });
        const keyIngredientsBody = normalizeSectionText([introText, ...ingredientCards.filter(Boolean)].join("\n\n"));
        if (keyIngredientsBody) {
          pushSection("Key Ingredients", keyIngredientsBody, "guerlain_ingredients_carousel");
        }

        const modalUrl =
          section.querySelector("button[data-url-ingredient], [data-target='#ingredientsModal'][data-url-ingredient]")?.getAttribute(
            "data-url-ingredient",
          ) || "";
        if (modalUrl.trim()) ingredientModalUrls.add(modalUrl.trim());
      }

      const modalDomText = normalizeSectionText(readGuerlainIngredientModalDomText());
      if (modalDomText) {
        pushSection("Ingredients", modalDomText, "guerlain_ingredients_modal_dom");
      }

      for (const modalUrl of ingredientModalUrls) {
        const modalText = normalizeSectionText(await fetchGuerlainIngredientModalText(modalUrl));
        if (modalText) {
          pushSection("Ingredients", modalText, "guerlain_ingredients_modal");
        }
      }

      const productAccordions = Array.from(document.querySelectorAll(".product-accordion")) as HTMLElement[];
      for (const accordion of productAccordions.slice(0, 16)) {
        const heading =
          accordion.querySelector(".product-accordion-header h1, .product-accordion-header h2, .product-accordion-header h3, .product-accordion-header h4, .product-accordion-header h5, .product-accordion-header h6")
            ?.textContent ||
          accordion.querySelector(".product-accordion-header")?.textContent ||
          "";
        const body =
          accordion.querySelector(".accordion-panel, [accordion-body]")?.textContent ||
          "";
        pushSection(heading, body, "product_accordion");
      }

      const keyIngredientCards = Array.from(document.querySelectorAll(".product-key-ingredients .child-ingredient")) as HTMLElement[];
      if (keyIngredientCards.length > 0) {
        const body = keyIngredientCards
          .map((card) => {
            const name = card.querySelector(".name")?.textContent?.trim() || "";
            const description = card.querySelector(".description")?.textContent?.trim() || "";
            if (!name && !description) return "";
            return [name, description].filter(Boolean).join(": ");
          })
          .filter(Boolean)
          .join("\n\n");
        pushSection("Key Ingredients", body, "key_ingredients_section");
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
          let cursor = headingNode.nextElementSibling;
          let guard = 0;
          while (cursor && guard < 6) {
            if (/^H[2-4]$/i.test(cursor.tagName)) break;
            if (!shouldSkipSectionNode(cursor)) {
              const text = (cursor as HTMLElement).innerText || cursor.textContent || "";
              if (normalizeSectionText(text)) bodyParts.push(text);
            }
            cursor = cursor.nextElementSibling;
            guard += 1;
          }

          if (bodyParts.length === 0) {
            const parent = headingNode.parentElement;
            const parentText = normalizeSectionText(parent?.innerText || parent?.textContent || "");
            const headingText = normalizeSectionText(heading);
            const body = parentText.replace(headingText, "").trim();
            if (body) bodyParts.push(body);
          }

          if (bodyParts.length > 0) {
            pushSection(heading, bodyParts.join("\n\n"), "pdp_content_heading");
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
        pushSection(heading, body, "accordion_control");
      }

      const accordionButtons = Array.from(document.querySelectorAll("button.accordion-title, .acc__btn")) as HTMLElement[];
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
            button.parentElement ||
            button.closest(".pv-extra-details__accordion") ||
            button.closest(".acc");
          const content =
            wrapper?.querySelector?.(".accordion-content-wrap-inner, .accordion-content-wrap, .acc__menu, .pv-extra-details__accordion-body") ||
            button.nextElementSibling;
          body = (content as HTMLElement | null)?.innerText || content?.textContent || "";
        }
        pushSection(heading, body, "accordion_button");
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
        pushSection(heading, body, "details_summary");
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
        pushSection(heading, body, "product_modal_content");
      }

      const modalNodes = Array.from(
        document.querySelectorAll("aside.modal, .modal.js-modal, dialog.product-modal, [role='dialog']"),
      );
      for (const modal of modalNodes.slice(0, 16)) {
        const heading =
          modal.querySelector(".modal__header h1, .modal__header h2, .modal__header h3, h1, h2, h3")?.textContent || "";
        const bodyNode = modal.querySelector(".modal__content, .modal-content, [class*='modal__content']");
        const body = (bodyNode as HTMLElement | null)?.innerText || bodyNode?.textContent || "";
        pushSection(heading, body, "modal_content");
      }

      const headingNodes = Array.from(document.querySelectorAll("h2, h3, h4"));
      for (const headingNode of headingNodes.filter((node) => looksRelevantHeading(node.textContent || "")).slice(0, 24)) {
        if (shouldSkipSectionNode(headingNode)) continue;
        const heading = headingNode.textContent || "";
        const bodyParts: string[] = [];
        let cursor = headingNode.nextElementSibling;
        let guard = 0;
        while (cursor && guard < 4) {
          if (/^H[2-4]$/i.test(cursor.tagName)) break;
          const text = (cursor as HTMLElement).innerText || cursor.textContent || "";
          if (normalizeSectionText(text)) bodyParts.push(text);
          cursor = cursor.nextElementSibling;
          guard += 1;
        }
        if (bodyParts.length > 0) {
          pushSection(heading, bodyParts.join("\n\n"), "heading_sibling");
        }
      }

      return sections;
    })();
    const activeIngredientsText =
      detailsSections.find((section) => /\b(?:active|key|hero) ingredients?\b/i.test(section.heading))?.body || undefined;
    const appDataRaw = (document.getElementById("appData") as HTMLElement | null)?.getAttribute("data")?.trim() || undefined;

    return {
      title,
      canonical,
      metaDescription,
      priceTexts,
      imageCandidates,
      contentImageCandidates,
      scripts,
      domVariants,
      productDetailsText,
      howToUseText,
      ingredientsMarkdownText,
      ingredientsDisclaimerText,
      activeIngredientsText,
      detailsSections,
      appDataRaw,
      faqItems,
    };
  });
}

function buildProductFromPageSignals(params: {
  extracted: ScrapedPageSignals;
  pageLooksLikeProduct: boolean;
  sourceUrl: string;
  baseUrl: string;
  verbose: boolean;
  log: Logger;
}): ExtractedProduct | null {
  const { extracted } = params;
  const paulasChoiceAppData = extractPaulasChoiceAppDataPdpFields(extracted.appDataRaw);
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

  const imageRaw = primaryProductObj?.image ?? productGroupObj?.image;
  const contentImageUrls = dedupeStringList(
    resolveStructuredImageUrls(params.baseUrl, extracted.contentImageCandidates),
  );
  const productImageUrls = dedupeStringList([
    ...resolveStructuredImageUrls(params.baseUrl, [imageRaw, productGroupObj?.image, extracted.imageCandidates]),
    ...variantProducts.flatMap((variantProduct) => resolveStructuredImageUrls(params.baseUrl, variantProduct.image)),
    ...contentImageUrls,
  ]);
  const imageUrl = productImageUrls[0] || "";

  const structuredOverview =
    (typeof primaryProductObj?.description === "string" ? primaryProductObj.description : undefined) ||
    (typeof productGroupObj?.description === "string" ? productGroupObj.description : undefined);
  const mergedDetailedOverview =
    cleanText([extracted.productDetailsText, paulasChoiceAppData?.descriptionRaw].filter(Boolean).join("\n\n")) || undefined;

  const officialText = choosePreferredProductOverview({
    structured: structuredOverview,
    detailed: mergedDetailedOverview,
    meta: extracted.metaDescription,
  });

  const offersRaw = primaryProductObj?.offers;
  const offers = normalizeJsonLdOffers(offersRaw);

  const domMetaBySku = new Map<string, DomVariantMeta>();
  for (const meta of extracted.domVariants || []) {
    if (!meta.sku) continue;
    domMetaBySku.set(meta.sku, meta);
  }

  const faqItems = Array.isArray(extracted.faqItems) ? extracted.faqItems : [];
  const mergedDetailsSections = dedupeDetailSections([
    ...(Array.isArray(extracted.detailsSections) ? extracted.detailsSections : []),
    ...(paulasChoiceAppData?.detailsSections || []),
  ]);
  const howToUseText =
    (typeof extracted.howToUseText === "string" ? extracted.howToUseText.trim() : undefined) ||
    paulasChoiceAppData?.howToUseRaw;
  const ingredientsMarkdownText =
    (typeof extracted.ingredientsMarkdownText === "string" ? extracted.ingredientsMarkdownText.trim() : undefined) ||
    paulasChoiceAppData?.ingredientsRaw;
  const ingredientsDisclaimerText =
    typeof extracted.ingredientsDisclaimerText === "string" ? extracted.ingredientsDisclaimerText.trim() : undefined;
  const activeIngredientsText =
    (typeof extracted.activeIngredientsText === "string" ? extracted.activeIngredientsText.trim() : undefined) ||
    paulasChoiceAppData?.activeIngredientsRaw;
  const derivedPdpBodies = deriveProductPdpModuleBodies({
    ingredientsMarkdownText,
    activeIngredientsText,
    howToUseText,
    detailsSections: mergedDetailsSections,
  });
  const productPdpFields = buildProductPdpFields({
    descriptionRaw: officialText || mergedDetailedOverview || extracted.metaDescription,
    detailsSections: mergedDetailsSections,
    ingredientsRaw: derivedPdpBodies.ingredientsRaw,
    activeIngredientsRaw: derivedPdpBodies.activeIngredientsRaw,
    howToUseRaw: derivedPdpBodies.howToUseRaw,
    faqItems,
    fieldSources: {
      description_raw: [
        officialText && cleanText(officialText) === cleanText(structuredOverview) ? "structured_overview" : "",
        officialText &&
        cleanText(officialText) === cleanText(mergedDetailedOverview) &&
        Boolean(paulasChoiceAppData?.descriptionRaw)
          ? "paulaschoice_appdata_overview"
          : "",
        officialText &&
        cleanText(officialText) === cleanText(mergedDetailedOverview) &&
        !paulasChoiceAppData?.descriptionRaw &&
        extracted.productDetailsText
          ? "page_product_details"
          : "",
        !officialText && mergedDetailedOverview ? "page_product_details" : "",
        !officialText && !mergedDetailedOverview && extracted.metaDescription ? "meta_description" : "",
      ],
      details_sections: mergedDetailsSections.map((section) => section.source_kind),
      ingredients_raw: [
        ingredientsMarkdownText && looksLikeFullIngredientListText(ingredientsMarkdownText)
          ? cleanText(ingredientsMarkdownText) === cleanText(paulasChoiceAppData?.ingredientsRaw)
            ? "paulaschoice_appdata_all_ingredients"
            : "page_ingredients_section"
          : "",
        !ingredientsMarkdownText && derivedPdpBodies.ingredientsRaw
          ? "details_section_ingredients"
          : "",
      ],
      active_ingredients_raw: [
        activeIngredientsText
          ? cleanText(activeIngredientsText) === cleanText(paulasChoiceAppData?.activeIngredientsRaw)
            ? "paulaschoice_appdata_key_ingredients"
            : "page_active_ingredients_section"
          : "",
        !activeIngredientsText && derivedPdpBodies.activeIngredientsRaw
          ? "details_section_active_ingredients"
          : "",
      ],
      how_to_use_raw: [
        howToUseText
          ? cleanText(howToUseText) === cleanText(paulasChoiceAppData?.howToUseRaw)
            ? "paulaschoice_appdata_how_to_use"
            : "page_how_to_use_section"
          : "",
        !howToUseText && derivedPdpBodies.howToUseRaw
          ? "details_section_how_to_use"
          : "",
      ],
      faq_items: faqItems.length > 0 ? ["page_faq_section"] : [],
    },
  });

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
    ...(contentImageUrls.length > 0 ? { content_image_urls: contentImageUrls } : {}),
    variant_skus: dedupeStringList(variants.map((variant) => variant.sku)),
    variants,
    ...productPdpFields,
  };
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
  let prefetchedProduct: ExtractedProduct | null = null;

  const expandRelevantPdpModules = async () => {
    await page.evaluate(() => {
      const relevantHeadingRe =
        /\b(product details|details?|benefits?|how(?:\s*|-)?to(?:\s+(?:use|apply))?|usage instructions?|ingredients?(?:\s*&\s*|\s+and\s+)safety|ingredients?|active ingredients?|inci|composition|what(?:'|’)s in it\??|faq|frequently asked questions?|q\s*&\s*a|questions?|clinical(?:\s+results?)?|results?)\b/i;

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
        document.querySelectorAll("button[aria-controls], [role='tab'][aria-controls], button.accordion-title, .acc__btn"),
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

      const guerlainButtons = Array.from(
        document.querySelectorAll("button[data-url-ingredient][data-target='#ingredientsModal'], button.btn-ingredient[data-url-ingredient]"),
      ) as HTMLElement[];
      for (const button of guerlainButtons.slice(0, 4)) {
        button.click();
      }
    });
    await new Promise((resolve) => setTimeout(resolve, 150));

    await page
      .waitForSelector(
        "#ingredientsModal .modal__content, #ingredientsModal .modal-content, #ingredientsModal .modal-body, [id*='ingredientsModal'] .modal__content, [id*='ingredientsModal'] .modal-content, [id*='ingredientsModal'] .modal-body, .modal.show .modal__content, .modal.show .modal-content, .modal.show .modal-body, .modal.in .modal__content, .modal.in .modal-content, .modal.in .modal-body",
        { timeout: 1_500 },
      )
      .catch(() => undefined);
  };

  try {
    if (params.verbose) params.log("info", `Scraping: ${params.url}`);
    await preparePage(page, {
      baseUrl: params.baseUrl,
      context: params.context,
      navigationTimeoutMs: params.navigationTimeoutMs,
    });
    const prefetched = await fetchHtmlViaNativeRequest(params.url, params.diagnostics!);
    const prefetchedSourceUrl = prefetched.finalUrl || params.url;
    if (prefetched.body) {
      await page.setContent(injectBaseHref(prefetched.body, prefetchedSourceUrl), { waitUntil: "domcontentloaded" });
      const prefetchedExtracted = await extractPageSignals(page);
      const prefetchedLooksLikeProduct =
        looksLikeProductPageHtml(prefetched.body) ||
        (isLikelyProductUrlShared(prefetchedSourceUrl, params.baseUrl) &&
          Boolean(cleanText(prefetchedExtracted.title)) &&
          (
            prefetchedExtracted.priceTexts.length > 0 ||
            prefetchedExtracted.detailsSections.length > 0 ||
            prefetchedExtracted.imageCandidates.length > 0
          ));
      prefetchedProduct = buildProductFromPageSignals({
        extracted: prefetchedExtracted,
        pageLooksLikeProduct: prefetchedLooksLikeProduct,
        sourceUrl: prefetchedSourceUrl,
        baseUrl: params.baseUrl,
        verbose: params.verbose,
        log: params.log,
      });
      if (prefetchedProduct && !productHasMissingPdpFields(prefetchedProduct)) return prefetchedProduct;
    }

    const visit = await gotoPageOrThrow(page, {
      url: params.url,
      baseUrl: params.baseUrl,
      context: params.context,
      diagnostics: params.diagnostics!,
    });

    await expandRelevantPdpModules();

    const extracted = await extractPageSignals(page);
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

    const renderedHtml = await page.content().catch(() => "");
    const renderedHtmlProduct = renderedHtml
      ? extractProductFromHtmlSnapshot({
          html: renderedHtml,
          url: params.url,
          baseUrl: params.baseUrl,
          verbose: params.verbose,
          log: params.log,
        })
      : null;
    if (renderedHtmlProduct) {
      const liveRecoveredFields = countRecoveredPdpFields(liveProduct);
      const renderedRecoveredFields = countRecoveredPdpFields(renderedHtmlProduct);
      if (!liveProduct || renderedRecoveredFields > liveRecoveredFields) {
        params.log("info", `Recovered richer PDP fields from rendered browser HTML snapshot: ${params.url}`);
        return renderedHtmlProduct;
      }
    }

    return liveProduct;
  } catch (err) {
    if (err instanceof BotChallengeError) {
      throw err;
    }
    const message = err instanceof Error ? err.message : String(err);
    if (prefetchedProduct) {
      params.log("warn", `Returning native HTML snapshot fallback for ${params.url} after scrape failure: ${message}`);
      return prefetchedProduct;
    }
    params.log("warn", `Failed to scrape ${params.url}: ${message}`);
    return null;
  } finally {
    await page.close().catch(() => undefined);
  }
}

async function scrapeProductPageViaHtml(params: {
  url: string;
  baseUrl: string;
  diagnostics: ExtractResponse["diagnostics"];
  verbose: boolean;
  log: Logger;
}) {
  const snapshot = await fetchHtmlViaNativeRequest(params.url, params.diagnostics!);
  if (!snapshot.body) return null;

  return extractProductFromHtmlSnapshot({
    html: snapshot.body,
    url: snapshot.finalUrl || params.url,
    baseUrl: params.baseUrl,
    verbose: params.verbose,
    log: params.log,
  });
}
