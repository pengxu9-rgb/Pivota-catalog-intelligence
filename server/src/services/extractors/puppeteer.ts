import { createHash } from "crypto";
import puppeteer, { type Browser } from "puppeteer";

import type {
  ExtractInput,
  ExtractResponse,
  ExtractedProduct,
  ExtractedVariant,
  ExtractedVariantRow,
  Extractor,
  StockStatus,
} from "./types";

export class PuppeteerExtractor implements Extractor {
  async extract(input: ExtractInput): Promise<ExtractResponse> {
    const generatedAt = new Date().toISOString();

    const logs: ExtractResponse["logs"] = [];
    const log = (type: ExtractResponse["logs"][number]["type"], msg: string) => {
      logs.push({ at: new Date().toISOString(), type, msg });
    };

    const target = parseTarget(input.domain);
    const baseUrl = target.baseUrl;
    const maxProducts = clampInt(process.env.MAX_PRODUCTS, 50, 1, 5000);

    log("info", `Initializing Puppeteer extraction for: ${input.brand}`);
    log("info", `Target: ${baseUrl}`);
    if (target.seedUrl) log("info", `Seed URL: ${target.seedUrl}`);

    try {
      // 1) Fast path: Shopify JSON feed (no browser required).
      const shopify = await tryExtractShopify({
        brand: input.brand,
        domain: target.domain,
        baseUrl,
        collectionHandle: target.collectionHandle,
        maxProducts,
        log,
      });
      if (shopify) return { ...shopify, generated_at: generatedAt, logs };

      // 2) Generic path: sitemap discovery + JSON-LD parsing with Puppeteer.
      log("info", "Shopify feed not detected. Falling back to Sitemap + JSON-LD extraction.");
      const discovered = await discoverProductUrls({ baseUrl, maxProducts, seedUrl: target.seedUrl, log });

      if (discovered.productUrls.length === 0) {
        log("error", "No product URLs discovered (robots/sitemap).");
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
          logs,
        };
      }

      log("success", `Discovered ${discovered.productUrls.length} product URLs.`);

      const concurrency = clampInt(process.env.PUPPETEER_CONCURRENCY, 2, 1, 6);
      const navigationTimeoutMs = clampInt(process.env.PUPPETEER_NAV_TIMEOUT_MS, 30_000, 5_000, 120_000);

      const browser = await puppeteer.launch({
        headless: true,
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
        args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
      });

      try {
        const scrapedProducts = await mapWithConcurrency(discovered.productUrls, concurrency, async (url, idx) => {
          const verbose = idx < 3;
          return scrapeProductPage({ browser, url, baseUrl, navigationTimeoutMs, verbose, log });
        });

        const products = scrapedProducts.filter((p): p is ExtractedProduct => Boolean(p));
        const { variants, adCopyById } = flattenVariants({
          brand: input.brand,
          products,
          simulated: false,
        });

        const pricing = computePricingStats(variants);
        log("success", `Extraction Complete. ${variants.length} variants processed successfully.`);

        return {
          brand: input.brand,
          domain: target.domain,
          generated_at: generatedAt,
          mode: "puppeteer",
          platform: "JSON-LD / Sitemap",
          sitemap: discovered.sitemapUrl,
          products,
          variants,
          pricing,
          ad_copy: { by_variant_id: adCopyById },
          logs,
        };
      } finally {
        await browser.close();
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      log("error", `Puppeteer extraction failed: ${msg}`);
      return {
        brand: input.brand,
        domain: target.domain,
        generated_at: generatedAt,
        mode: "puppeteer",
        platform: "Error",
        products: [],
        variants: [],
        pricing: { currency: "USD", min: 0, max: 0, avg: 0 },
        ad_copy: { by_variant_id: {} },
        logs,
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
  price?: string;
  ingredients?: string;
};

function clampInt(value: string | undefined, fallback: number, min: number, max: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
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
  const overview = cleanText(params.overview) || `Experience the ultimate luxury with ${params.title}.`;
  const parts = [overview];

  const howToUse = cleanText(params.howToUse);
  if (howToUse) parts.push(`How to Use: ${howToUse}`);

  const ingredientsAndSafety = cleanText(params.ingredientsAndSafety);
  if (ingredientsAndSafety) parts.push(`Ingredients and Safety: ${ingredientsAndSafety}`);

  return parts.join("\n\n");
}

function generateMockAdCopy(title: string, variantValue: string, price: string) {
  const subject = pick(AD_SUBJECT_TEMPLATES).replace("{title}", title).replace("{variant}", variantValue);
  const caption = pick(AD_CAPTION_TEMPLATES).replace("{title}", title).replace("{variant}", variantValue);
  return `**Subject:** ${subject}\n\n**Instagram Caption:**\n${caption}\n\n**Price:** $${price}`;
}

function cleanText(text?: string) {
  if (!text) return "";
  const withNewlines = text
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

  if (nums.length === 0) return { currency: "USD" as const, min: 0, max: 0, avg: 0 };
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const avg = nums.reduce((a, b) => a + b, 0) / nums.length;
  return { currency: "USD" as const, min, max, avg: Number(avg.toFixed(2)) };
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
  images?: ShopifyImage[];
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
  featured_image?: ShopifyImage | null;
};

type ShopifyImage = {
  src?: string;
  variant_ids?: number[];
};

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
  collectionHandle?: string;
  maxProducts: number;
  log: Logger;
}): Promise<Omit<ExtractResponse, "generated_at" | "logs"> | null> {
  const log = params.log;

  const probeUrl = params.collectionHandle
    ? `${params.baseUrl}/collections/${params.collectionHandle}/products.json?limit=1`
    : `${params.baseUrl}/products.json?limit=1`;

  log("info", `Checking Shopify feed: ${probeUrl}`);
  const probe = await fetchJson<ShopifyProductsResponse>(probeUrl);
  if (!probe || !Array.isArray(probe.products)) {
    log("warn", "Shopify feed not found.");
    return null;
  }

  log("success", "Shopify feed detected.");

  const allProducts: ShopifyProduct[] = [];
  const maxPages = clampInt(process.env.SHOPIFY_MAX_PAGES, 20, 1, 200);
  const feedPrefix = params.collectionHandle ? `/collections/${params.collectionHandle}` : "";

  for (let page = 1; page <= maxPages; page++) {
    const url = `${params.baseUrl}${feedPrefix}/products.json?limit=250&page=${page}`;
    const batch = await fetchJson<ShopifyProductsResponse>(url);
    const products = batch?.products;
    if (!products || products.length === 0) break;
    allProducts.push(...products);
    if (products.length < 250) break;
  }

  const limitedProducts = allProducts.slice(0, params.maxProducts);
  log("data", `Loaded ${limitedProducts.length} products from Shopify feed.`);

  const variantDiscoverySetting = (process.env.SHOPIFY_VARIANT_DISCOVERY || "auto").toLowerCase().trim();
  const forceDiscoveryOff = ["0", "false", "no", "off", "none"].includes(variantDiscoverySetting);
  const forceDiscoveryOn = ["1", "true", "yes", "on", "title"].includes(variantDiscoverySetting);

  const discoveryCandidates = limitedProducts
    .map((p) => {
      const split = splitTitleIntoBaseAndVariant(p.title);
      const isSingleDefault = (p.variants || []).length === 1 && isDefaultShopifyVariant(p.variants[0]!);
      return Boolean(split && isSingleDefault);
    })
    .filter(Boolean).length;

  const discoveryRate = limitedProducts.length > 0 ? discoveryCandidates / limitedProducts.length : 0;
  const autoDiscoveryOn = discoveryRate >= 0.2;

  const enableTitleDiscovery = !forceDiscoveryOff && (forceDiscoveryOn || (variantDiscoverySetting === "auto" && autoDiscoveryOn));
  if (enableTitleDiscovery && discoveryCandidates > 0) {
    log(
      "info",
      `Variant discovery enabled (mode=${variantDiscoverySetting}). Candidates: ${discoveryCandidates}/${limitedProducts.length} (${Math.round(
        discoveryRate * 100,
      )}%).`,
    );
  } else {
    log(
      "info",
      `Variant discovery disabled (mode=${variantDiscoverySetting}). Candidates: ${discoveryCandidates}/${limitedProducts.length} (${Math.round(
        discoveryRate * 100,
      )}%).`,
    );
  }

  const extractedByTitle = new Map<string, ExtractedProduct>();

  for (const product of limitedProducts) {
    const productUrl = `${params.baseUrl}/products/${product.handle}`;
    const titleSplit = enableTitleDiscovery ? splitTitleIntoBaseAndVariant(product.title) : null;
    const treatAsPseudoVariant =
      Boolean(titleSplit) && (product.variants || []).length === 1 && isDefaultShopifyVariant(product.variants[0]!);

    const canonicalProductTitle = treatAsPseudoVariant ? titleSplit!.baseTitle : product.title;
    const optionName = treatAsPseudoVariant
      ? "Variant"
      : product.options?.map((o) => o.name).filter((n): n is string => Boolean(n && n.trim())).join(" / ") || "Variant";
    const officialText = product.body_html;

    const extractedVariants: ExtractedVariant[] = (product.variants || []).map((v) => {
      const optionValue = treatAsPseudoVariant
        ? titleSplit!.variantLabel
        : [v.option1, v.option2, v.option3].filter((x): x is string => Boolean(x && x.trim())).join(" / ") ||
          v.title?.trim() ||
          "Default";

      const sku = (v.sku || "").trim() || `SHOPIFY-${v.id}`;
      const price = (v.price || "0.00").trim();
      const stock = toStockStatus(v.available, v.inventory_quantity);
      const imageUrl = resolveShopifyVariantImageUrl(product, v) || "";
      const description = getMergedDescription({ title: canonicalProductTitle, overview: officialText });
      const adCopy = generateMockAdCopy(canonicalProductTitle, optionValue, price);

      return {
        id: String(v.id),
        sku,
        url: productUrl,
        option_name: optionName,
        option_value: optionValue,
        price,
        currency: "USD",
        stock,
        description,
        image_url: imageUrl,
        ad_copy: adCopy,
      };
    });

    const existing =
      extractedByTitle.get(canonicalProductTitle) ||
      ({
        title: canonicalProductTitle,
        url: productUrl,
        variants: [],
      } satisfies ExtractedProduct);

    existing.variants.push(...extractedVariants);
    extractedByTitle.set(canonicalProductTitle, existing);
  }

  const extractedProducts = Array.from(extractedByTitle.values());

  const { variants, adCopyById } = flattenVariants({
    brand: params.brand,
    products: extractedProducts,
    simulated: false,
  });

  const productCount = extractedProducts.length;
  const variantCount = variants.length;
  const avg = productCount > 0 ? (variantCount / productCount).toFixed(2) : "0.00";
  const multi = extractedProducts.filter((p) => p.variants.length > 1).length;
  log("data", `Summary: products=${productCount}, variants=${variantCount}, avg=${avg}, multi=${multi}`);

  const pricing = computePricingStats(variants);
  log("success", `Extraction Complete. ${variants.length} variants processed successfully.`);

  return {
    brand: params.brand,
    domain: params.domain,
    mode: "puppeteer",
    platform: params.collectionHandle ? `Shopify (Collection: ${params.collectionHandle})` : "Shopify",
    products: extractedProducts,
    variants,
    pricing,
    ad_copy: { by_variant_id: adCopyById },
  };
}

function resolveShopifyVariantImageUrl(product: ShopifyProduct, variant: ShopifyVariant): string | undefined {
  const direct = variant.featured_image?.src;
  if (direct) return direct;

  const images = product.images || [];
  const match = images.find((img) => (img.variant_ids || []).includes(variant.id));
  if (match?.src) return match.src;

  return images[0]?.src;
}

function toStockStatus(available?: boolean, inventoryQuantity?: number | null): StockStatus {
  if (available === false) return "Out of Stock";
  const qty = typeof inventoryQuantity === "number" ? inventoryQuantity : undefined;
  const lowStockThreshold = clampInt(process.env.LOW_STOCK_THRESHOLD, 10, 1, 9999);
  if (qty !== undefined && qty > 0 && qty <= lowStockThreshold) return "Low Stock";
  return "In Stock";
}

async function fetchJson<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: {
        accept: "application/json",
        "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
      },
    });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: {
        accept: "text/plain,text/html,application/xml;q=0.9,*/*;q=0.8",
        "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
      },
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

function extractProductUrlsFromHtml(html: string, baseUrl: string) {
  const urls = new Set<string>();
  for (const match of html.matchAll(/\/product\/[^"'?#\s<]+/gi)) {
    urls.add(toAbsoluteUrl(baseUrl, match[0]));
  }
  for (const match of html.matchAll(/\/products\/[^"'?#\s<]+/gi)) {
    urls.add(toAbsoluteUrl(baseUrl, match[0]));
  }
  return Array.from(urls);
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

  const maxSitemaps = clampInt(process.env.MAX_SITEMAPS, 10, 1, 100);

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
    }
  }

  const deduped = Array.from(new Set(pageUrls));

  const productLike = deduped.filter((u) => /\/product\/|\/products\//i.test(u));
  const selected = (productLike.length > 0 ? productLike : deduped)
    .slice(0, params.maxProducts)
    .filter((u) => u.startsWith("http"));

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

function stableId(input: string) {
  return createHash("sha1").update(input).digest("hex").slice(0, 12);
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
  navigationTimeoutMs: number;
  verbose: boolean;
  log: Logger;
}): Promise<ExtractedProduct | null> {
  const page = await params.browser.newPage();
  await page.setUserAgent(process.env.PUPPETEER_USER_AGENT || "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36");
  page.setDefaultNavigationTimeout(params.navigationTimeoutMs);

  try {
    if (params.verbose) params.log("info", `Scraping: ${params.url}`);

    await page.goto(params.url, { waitUntil: "domcontentloaded" });

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

      const domVariants = (() => {
        const el = document.querySelector("[data-product-skus-value]") as HTMLElement | null;
        const raw = el?.getAttribute("data-product-skus-value") || "";
        if (!raw) return [] as Array<{
          sku: string;
          option_name?: string;
          option_value?: string;
          url_path?: string;
          image_url?: string;
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
              const firstImage = images[0] as Record<string, unknown> | undefined;
              const imageUrl = typeof firstImage?.src === "string" ? firstImage.src.trim() : "";

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
                price: price || undefined,
                ingredients: ingredients || undefined,
              };
            })
            .filter((v) => Boolean(v.sku));
        } catch {
          return [];
        }
      })();

      const getAccordionContentByTitle = (value: string) => {
        const normalize = (s: string) => s.trim().toLowerCase();
        const buttons = Array.from(document.querySelectorAll("button[aria-controls]")) as HTMLButtonElement[];
        const btn = buttons.find((b) => normalize(b.getAttribute("title") || b.textContent || "") === normalize(value));
        const id = btn?.getAttribute("aria-controls") || "";
        if (!id) return null;
        return document.getElementById(id);
      };

      const howToUseContent =
        getAccordionContentByTitle("How to Use") ||
        getAccordionContentByTitle("How to use") ||
        document.getElementById("accordion-toggle-How to Use");

      const ingredientsContent =
        getAccordionContentByTitle("Ingredients and Safety") ||
        getAccordionContentByTitle("Ingredients & Safety") ||
        document.getElementById("accordion-toggle-Ingredients and Safety");

      const howToUseText = howToUseContent?.querySelector(".markdown")?.textContent?.trim() || undefined;
      const ingredientsMarkdownText = ingredientsContent?.querySelector(".markdown")?.textContent?.trim() || undefined;
      const ingredientsDisclaimerText =
        ingredientsContent?.querySelector(".product-details-accordions-ingredients-disclaimer")?.textContent?.trim() || undefined;

      return {
        title,
        canonical,
        scripts,
        domVariants,
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
    if (!productObj) {
      if (params.verbose) params.log("warn", "> No JSON-LD Product schema found.");
      return null;
    }

    const productTitle = (typeof productObj.name === "string" ? productObj.name : extracted.title).trim() || extracted.title;
    const productUrl = toAbsoluteUrl(params.baseUrl, typeof productObj.url === "string" ? productObj.url : extracted.canonical);

    const imageRaw = productObj.image;
    const imageUrl = (() => {
      if (typeof imageRaw === "string") return toAbsoluteUrl(params.baseUrl, imageRaw);
      if (Array.isArray(imageRaw) && typeof imageRaw[0] === "string") return toAbsoluteUrl(params.baseUrl, imageRaw[0]);
      if (imageRaw && typeof imageRaw === "object" && typeof (imageRaw as any).url === "string") {
        return toAbsoluteUrl(params.baseUrl, (imageRaw as any).url);
      }
      return "";
    })();

    const officialText = typeof productObj.description === "string" ? productObj.description : undefined;

    const offersRaw = productObj.offers;
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
      offers.length > 0
        ? offers.map((offer, idx) => {
            const skuRaw = typeof offer.sku === "string" ? offer.sku.trim() : "";
            const sku = skuRaw || `AUTO-${stableId(`${productUrl}|${idx}`)}`;

            const domMeta = domMetaBySku.get(sku);

            const offerUrl = (() => {
              if (domMeta?.url_path) return toAbsoluteUrl(params.baseUrl, domMeta.url_path);
              return toAbsoluteUrl(params.baseUrl, typeof offer.url === "string" ? offer.url : productUrl);
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
            const offerImageUrl = (() => {
              if (typeof offerImageRaw === "string" && offerImageRaw.trim()) return toAbsoluteUrl(params.baseUrl, offerImageRaw);
              if (Array.isArray(offerImageRaw) && typeof offerImageRaw[0] === "string") return toAbsoluteUrl(params.baseUrl, offerImageRaw[0]);
              if (domMeta?.image_url) return toAbsoluteUrl(params.baseUrl, domMeta.image_url);
              return imageUrl;
            })();

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
              price: "0.00",
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
              ad_copy: generateMockAdCopy(productTitle, "Default", "0.00"),
            },
          ];

    if (params.verbose) {
      params.log("data", "> Found JSON-LD 'Product' Schema");
      params.log("success", `> Extracted ${variants.length} offers/variants`);
    }

    return {
      title: productTitle,
      url: productUrl,
      variants,
    };
  } catch (err) {
    params.log("warn", `Failed to scrape ${params.url}`);
    return null;
  } finally {
    await page.close().catch(() => undefined);
  }
}
