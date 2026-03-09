import puppeteer, { type Browser, type HTTPResponse, type Page } from "puppeteer";

import type {
  BlockProvider,
  DiscoveryStrategy,
  ExtractionDiagnostics,
  FailureCategory,
  HttpTraceEntry,
  MarketId,
} from "./types";

const TRACKING_QUERY_PARAM_RE = /^(utm_|fbclid$|gclid$|mc_|_ga$|_gl$|ref$|source$)/i;
const STATIC_ASSET_EXT_RE =
  /\.(?:css|js|mjs|map|png|jpe?g|gif|webp|svg|ico|pdf|xml|txt|woff2?|ttf|eot|otf|mp3|wav|mp4|webm|zip|gz|tar|json)(?:$|[?#])/i;
const NEGATIVE_PATH_RE =
  /(?:^|\/)(?:collections?|collection|category|catalogsearch|search|cart|account|customer|blog|blogs|pages?|faq|privacy|terms|wishlist|gift(?:ing)?|store-locator|customer-service)(?:\/|$)/i;
const PRODUCT_SIGNAL_RE =
  /"@type"\s*:\s*(?:"Product"|\[[^\]]*"Product")|application\/ld\+json|add to cart|buy now|quick shop|price(?:currency)?|itemprop=["']price["']/i;
const PRICE_SIGNAL_RE = /[$€£¥]\s?\d|price(?:currency)?|sale price|from\s+[$€£¥]/i;
const CATEGORY_TEXT_RE = /\b(shop|bestsellers|skincare|haircare|bodycare|fragrance|makeup|collections?)\b/i;

const MARKET_KEYWORDS: Record<MarketId, string[]> = {
  US: ["usa", "united states", "us"],
  "EU-DE": ["germany", "deutschland", "de"],
  SG: ["singapore", "sg"],
  JP: ["japan", "jp", "日本"],
};

export type FetchContext = {
  headers?: Record<string, string>;
  cookies?: Record<string, string>;
  url_params?: Record<string, string>;
};

export type LoggerFn = (type: "info" | "success" | "warn" | "error" | "data", msg: string) => void;

export type ParseTargetResult = {
  domain: string;
  baseUrl: string;
  seedUrl?: string;
  collectionHandle?: string;
};

export type StorefrontResolution = {
  target: ParseTargetResult;
  selectorRootDetected: boolean;
  storefrontResolved: boolean;
};

export type DiscoveryResult = {
  sitemapUrl?: string;
  productUrls: string[];
  deadSitemapDetected: boolean;
  challengeDetected: boolean;
};

export type BrowserRuntimeMode = "local" | "managed";

export type BrowserDiscoveryResult = {
  productUrls: string[];
  categoryUrls: string[];
};

type FetchOutcome = {
  finalUrl: string;
  status: number | null;
  ok: boolean;
  body: string | null;
  contentType: string | null;
  blockedBy: BlockProvider;
};

type PageVisitResult = {
  url: string;
  status: number | null;
  content: string;
  title: string;
};

type BrowserTaskOptions = {
  diagnostics: ExtractionDiagnostics;
  log?: LoggerFn;
};

type AnchorCandidate = {
  href: string;
  text: string;
  contextText?: string;
  html?: string;
};

export class BotChallengeError extends Error {
  provider: BlockProvider;
  url: string;

  constructor(provider: BlockProvider, url: string, message = "Bot challenge detected") {
    super(message);
    this.name = "BotChallengeError";
    this.provider = provider;
    this.url = url;
  }
}

export function clampInt(value: string | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

export function clampOptionalInt(value: number | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

export async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
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

export async function mapWithConcurrency<T, R>(
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

export function normalizeMarketId(value: string | undefined): MarketId {
  const normalized = String(value || "US").trim().toUpperCase();
  if (normalized === "EU-DE" || normalized === "US" || normalized === "SG" || normalized === "JP") {
    return normalized;
  }
  return "US";
}

export function createDiagnostics(requestedDomain: string, resolvedBaseUrl: string): ExtractionDiagnostics {
  return {
    requested_domain: requestedDomain,
    resolved_base_url: resolvedBaseUrl,
    discovery_strategy: null,
    failure_category: null,
    block_provider: null,
    http_trace: [],
  };
}

export function appendHttpTrace(diagnostics: ExtractionDiagnostics, entry: HttpTraceEntry): void {
  diagnostics.http_trace.push(entry);
  if (diagnostics.http_trace.length > 25) {
    diagnostics.http_trace.shift();
  }
}

export function setDiscoveryStrategy(diagnostics: ExtractionDiagnostics, strategy: DiscoveryStrategy): void {
  diagnostics.discovery_strategy = strategy;
}

export function setFailureCategory(diagnostics: ExtractionDiagnostics, category: FailureCategory): void {
  diagnostics.failure_category = category;
}

export function parseTarget(raw: string): ParseTargetResult {
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

export function getCollectionHandle(pathname: string): string | undefined {
  const matched = pathname.match(/^\/collections\/([^/]+)/i);
  return matched?.[1];
}

export function withUrlParams(rawUrl: string, urlParams: Record<string, string> | undefined): string {
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

export function toAbsoluteUrl(baseUrl: string, href: string): string {
  try {
    return new URL(href, baseUrl).toString();
  } catch {
    return href;
  }
}

export function canonicalizeUrl(rawUrl: string, baseUrl?: string): string {
  try {
    const parsed = baseUrl ? new URL(rawUrl, baseUrl) : new URL(rawUrl);
    parsed.hash = "";

    const keys = Array.from(parsed.searchParams.keys());
    for (const key of keys) {
      if (TRACKING_QUERY_PARAM_RE.test(key)) parsed.searchParams.delete(key);
    }

    return parsed.toString();
  } catch {
    return rawUrl;
  }
}

function buildFetchHeaders(context: FetchContext, accept: string): Record<string, string> {
  const headers: Record<string, string> = {
    accept,
    "user-agent": process.env.PUPPETEER_USER_AGENT || "PivotaCatalogIntelligence/1.0",
    ...(context.headers || {}),
  };

  const cookieString = Object.entries(context.cookies || {})
    .map(([name, value]) => `${name}=${value}`)
    .join("; ");

  if (cookieString) headers.cookie = cookieString;
  return headers;
}

function toHeaderMap(headers: Headers | Record<string, string>): Record<string, string> {
  if (headers instanceof Headers) {
    const out: Record<string, string> = {};
    headers.forEach((value, key) => {
      out[key.toLowerCase()] = value;
    });
    return out;
  }

  return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]));
}

function textFromHtml(fragment: string): string {
  return fragment
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractPrimaryBrandToken(host: string): string {
  const parts = host
    .toLowerCase()
    .split(".")
    .map((part) => part.trim())
    .filter(Boolean)
    .filter((part) => part !== "www");

  const longest = parts.sort((a, b) => b.length - a.length)[0];
  return longest || host.toLowerCase();
}

function sameBrandFamily(a: string, b: string): boolean {
  return extractPrimaryBrandToken(a) === extractPrimaryBrandToken(b);
}

function pathDepth(pathname: string): number {
  return pathname.split("/").filter(Boolean).length;
}

function hyphenCount(value: string): number {
  return (value.match(/-/g) || []).length;
}

export function isStaticAssetUrl(rawUrl: string, baseUrl: string): boolean {
  const parsed = parseHttpUrl(rawUrl, baseUrl);
  if (!parsed) return true;
  return STATIC_ASSET_EXT_RE.test(parsed.pathname);
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

export function scoreProductCandidateUrl(rawUrl: string, baseUrl: string): number {
  const parsed = parseHttpUrl(rawUrl, baseUrl);
  if (!parsed) return Number.NEGATIVE_INFINITY;

  const baseHost = parseHttpUrl(baseUrl, baseUrl)?.host.toLowerCase();
  if (baseHost && parsed.host.toLowerCase() !== baseHost) return Number.NEGATIVE_INFINITY;
  if (isStaticAssetUrl(parsed.toString(), baseUrl)) return Number.NEGATIVE_INFINITY;

  const path = parsed.pathname.toLowerCase();
  if (path === "/" || path === "") return Number.NEGATIVE_INFINITY;
  if (NEGATIVE_PATH_RE.test(path)) return -4;

  let score = 0;
  if (/\/products?\//.test(path)) score += 6;
  if (/\/p\/[^/]+$/.test(path)) score += 6;
  if (/[-_]\d{4,}\.html$/.test(path) || /\.html$/.test(path)) score += 6;

  const depth = pathDepth(path);
  const lastSegment = path.split("/").filter(Boolean).pop() || "";
  if (depth === 1 && hyphenCount(lastSegment) >= 3) score += 4;
  if (depth <= 2 && /^the-[a-z0-9-]+$/i.test(lastSegment)) score += 4;
  if (depth <= 2 && hyphenCount(lastSegment) >= 2) score += 2;
  if (depth <= 2 && /serum|cream|cleanser|mask|oil|toner|moisturizer|sunscreen|spf|treatment|body|eye|lip|gel/i.test(lastSegment)) {
    score += 2;
  }

  return score;
}

export function isLikelyProductUrl(rawUrl: string, baseUrl: string): boolean {
  const mode = (process.env.PDP_DETECTION_MODE || "score").toLowerCase();
  if (mode !== "score") {
    const parsed = parseHttpUrl(rawUrl, baseUrl);
    if (!parsed) return false;
    const path = parsed.pathname.toLowerCase();
    if (path === "/" || path === "") return false;
    if (/\/products?\//.test(path)) return true;
    if (/[-_]\d{4,}\.html$/.test(path)) return true;
    if (/\/p\/[^/]+$/.test(path)) return true;
    return false;
  }

  return scoreProductCandidateUrl(rawUrl, baseUrl) >= 4;
}

function isLikelyCategoryUrl(rawUrl: string, baseUrl: string): boolean {
  const parsed = parseHttpUrl(rawUrl, baseUrl);
  if (!parsed) return false;
  if (isStaticAssetUrl(parsed.toString(), baseUrl)) return false;
  if (NEGATIVE_PATH_RE.test(parsed.pathname)) return false;
  if (isLikelyProductUrl(parsed.toString(), baseUrl)) return false;

  const depth = pathDepth(parsed.pathname);
  const lastSegment = parsed.pathname.split("/").filter(Boolean).pop() || "";
  if (depth === 0 || depth > 2) return false;
  if (hyphenCount(lastSegment) >= 3 && /^the-/i.test(lastSegment)) return false;
  return CATEGORY_TEXT_RE.test(lastSegment) || depth <= 2;
}

export function extractProductUrlsFromHtml(html: string, baseUrl: string): string[] {
  const ranked = new Map<string, number>();

  for (const candidate of extractAnchorCandidates(html, baseUrl)) {
    const absolute = canonicalizeUrl(candidate.href, baseUrl);
    const score = scoreProductCandidateUrl(absolute, baseUrl);
    if (score >= 4) ranked.set(absolute, Math.max(score, ranked.get(absolute) || 0));
  }

  if (ranked.size > 0) {
    return Array.from(ranked.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([url]) => url);
  }

  const fallbackPatterns = [
    /["'](\/product\/[^"'?#\s<]+)["']/gi,
    /["'](\/products\/[^"'?#\s<]+)["']/gi,
    /["'](https?:\/\/[^"'?#\s<]+)["']/gi,
  ];

  const urls = new Set<string>();
  for (const pattern of fallbackPatterns) {
    for (const match of html.matchAll(pattern)) {
      const candidate = match[1] || match[0];
      const absolute = canonicalizeUrl(toAbsoluteUrl(baseUrl, candidate), baseUrl);
      if (isLikelyProductUrl(absolute, baseUrl)) urls.add(absolute);
    }
  }

  return Array.from(urls);
}

function extractAnchorCandidates(html: string, baseUrl: string): AnchorCandidate[] {
  const out: AnchorCandidate[] = [];

  for (const match of html.matchAll(/<a\b[^>]*\bhref\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const rawHref = match[1]?.trim();
    if (!rawHref) continue;
    if (/^(#|mailto:|tel:|javascript:)/i.test(rawHref)) continue;
    out.push({
      href: toAbsoluteUrl(baseUrl, rawHref.replace(/&amp;/gi, "&")),
      text: textFromHtml(match[2] || ""),
      html: match[0] || "",
    });
  }

  for (const match of html.matchAll(/<option\b[^>]*value\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/option>/gi)) {
    const rawHref = match[1]?.trim();
    if (!rawHref || /^(#|javascript:)/i.test(rawHref)) continue;
    out.push({
      href: toAbsoluteUrl(baseUrl, rawHref.replace(/&amp;/gi, "&")),
      text: textFromHtml(match[2] || ""),
      html: match[0] || "",
    });
  }

  return out;
}

export function looksLikeProductPageHtml(html: string): boolean {
  const normalized = html.toLowerCase();
  let score = 0;
  if (/"@type"\s*:\s*(?:"product"|\[[^\]]*"product")/i.test(normalized)) score += 4;
  if (PRICE_SIGNAL_RE.test(normalized)) score += 2;
  if (/add to cart|buy now|quick shop|notify me/i.test(normalized)) score += 2;
  if (/meta[^>]+property=["']og:type["'][^>]+content=["']product["']/i.test(normalized)) score += 2;
  return score >= 4;
}

function scoreBrowserCandidate(candidate: AnchorCandidate, baseUrl: string): number {
  let score = scoreProductCandidateUrl(candidate.href, baseUrl);
  const text = `${candidate.text} ${candidate.contextText || ""}`.toLowerCase();
  if (PRICE_SIGNAL_RE.test(text)) score += 3;
  if (/add to cart|buy now|quick shop|learn more|shop now|details/i.test(text)) score += 2;
  if (/in stock|out of stock|shade|size/i.test(text)) score += 1;
  return score;
}

export function detectBlockProvider(params: {
  status: number | null;
  headers?: Headers | Record<string, string>;
  body?: string | null;
  url?: string;
  title?: string | null;
}): BlockProvider {
  const headers = toHeaderMap(params.headers || {});
  const title = (params.title || "").toLowerCase();
  const body = (params.body || "").toLowerCase();
  const joined = `${title}\n${body}`;

  if (headers["cf-mitigated"] || headers["cf-ray"] || /cloudflare|just a moment|performing security verification|ray id/.test(joined)) {
    return "cloudflare";
  }

  if (headers["server"]?.toLowerCase().includes("akamai") || /akamai|_abck|bm_sz/.test(joined)) {
    return "akamai";
  }

  if (/_px|perimeterx|press & hold|human verification/.test(joined)) {
    return "perimeterx";
  }

  if ((params.status === 401 || params.status === 403 || params.status === 429) && /captcha|bot|challenge|security verification/.test(joined)) {
    return "unknown";
  }

  return null;
}

function recordBlockProvider(diagnostics: ExtractionDiagnostics, provider: BlockProvider): void {
  if (provider && !diagnostics.block_provider) diagnostics.block_provider = provider;
}

function isXmlLikeContent(body: string | null, contentType: string | null): boolean {
  if (!body) return false;
  const lowerBody = body.trim().toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (lowerType.includes("xml")) return true;
  if (lowerBody.startsWith("<?xml")) return true;
  if (/<(?:urlset|sitemapindex)\b/i.test(body)) return true;
  if (/<!doctype html|<html\b/i.test(lowerBody)) return false;
  return false;
}

async function fetchTracked(url: string, context: FetchContext, diagnostics: ExtractionDiagnostics, accept: string): Promise<FetchOutcome> {
  const timeoutMs = clampInt(process.env.PUPPETEER_FETCH_TIMEOUT_MS, 8_000, 2_000, 120_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const finalUrl = withUrlParams(url, context.url_params || {});

  try {
    const response = await fetch(finalUrl, {
      redirect: "follow",
      signal: controller.signal,
      headers: buildFetchHeaders(context, accept),
    });

    const contentType = response.headers.get("content-type");
    const body = await response.text();
    appendHttpTrace(diagnostics, { url: finalUrl, status: response.status });
    const blockedBy = detectBlockProvider({
      status: response.status,
      headers: response.headers,
      body,
      url: finalUrl,
      title: readTitleFromHtml(body),
    });
    recordBlockProvider(diagnostics, blockedBy);

    return {
      finalUrl,
      status: response.status,
      ok: response.ok,
      body,
      contentType,
      blockedBy,
    };
  } catch {
    appendHttpTrace(diagnostics, { url: finalUrl, status: null });
    return {
      finalUrl,
      status: null,
      ok: false,
      body: null,
      contentType: null,
      blockedBy: null,
    };
  } finally {
    clearTimeout(timer);
  }
}

function readTitleFromHtml(body: string | null): string | null {
  if (!body) return null;
  const matched = body.match(/<title[^>]*>([^<]+)<\/title>/i)?.[1];
  return matched ? matched.trim() : null;
}

export async function fetchTextTracked(url: string, context: FetchContext, diagnostics: ExtractionDiagnostics): Promise<FetchOutcome> {
  return fetchTracked(url, context, diagnostics, "text/plain,text/html,application/xml;q=0.9,*/*;q=0.8");
}

export async function fetchJsonTracked<T>(
  url: string,
  context: FetchContext,
  diagnostics: ExtractionDiagnostics,
): Promise<{ data: T | null; status: number | null; blockedBy: BlockProvider; finalUrl: string }> {
  const outcome = await fetchTracked(url, context, diagnostics, "application/json");
  if (!outcome.ok || !outcome.body) {
    return { data: null, status: outcome.status, blockedBy: outcome.blockedBy, finalUrl: outcome.finalUrl };
  }

  try {
    return {
      data: JSON.parse(outcome.body) as T,
      status: outcome.status,
      blockedBy: outcome.blockedBy,
      finalUrl: outcome.finalUrl,
    };
  } catch {
    return { data: null, status: outcome.status, blockedBy: outcome.blockedBy, finalUrl: outcome.finalUrl };
  }
}

function scoreStorefrontCandidate(url: string, label: string, requestedBaseUrl: string, marketId: MarketId): number {
  const parsed = parseHttpUrl(url, requestedBaseUrl);
  const requested = parseHttpUrl(requestedBaseUrl, requestedBaseUrl);
  if (!parsed || !requested) return Number.NEGATIVE_INFINITY;
  if (!sameBrandFamily(parsed.host, requested.host)) return Number.NEGATIVE_INFINITY;
  if (isStaticAssetUrl(parsed.toString(), requestedBaseUrl)) return Number.NEGATIVE_INFINITY;

  const normalizedLabel = label.toLowerCase();
  const normalizedHost = parsed.host.toLowerCase();
  const normalizedPath = parsed.pathname.toLowerCase();

  let score = 0;
  for (const token of MARKET_KEYWORDS[marketId] || []) {
    if (normalizedLabel.includes(token)) score += 5;
    if (normalizedHost.startsWith(`${token}.`) || normalizedHost.includes(`.${token}.`)) score += 4;
    if (normalizedPath.includes(`/${token}/`) || normalizedPath.endsWith(`/${token}`)) score += 2;
  }

  if (normalizedHost !== requested.host.toLowerCase()) score += 1;
  if (normalizedPath !== "/" && normalizedPath !== "") score += 1;

  return score;
}

export function resolveStorefrontFromHtml(html: string, requestedBaseUrl: string, marketId: MarketId): { url: string | null; selectorRoot: boolean } {
  const requestedHost = parseHttpUrl(requestedBaseUrl, requestedBaseUrl)?.host;
  const candidates = extractAnchorCandidates(html, requestedBaseUrl)
    .filter((candidate) => {
      if (!requestedHost) return false;
      const candidateUrl = parseHttpUrl(candidate.href, requestedBaseUrl);
      return Boolean(candidateUrl && sameBrandFamily(candidateUrl.host, requestedHost));
    })
    .filter((candidate) => !isStaticAssetUrl(candidate.href, requestedBaseUrl));

  const selectorRoot =
    /select your country|hello,\s*please select your country|choose your country/i.test(html) ||
    candidates.length >= 4;

  let best: { url: string; score: number } | null = null;
  for (const candidate of candidates) {
    const score = scoreStorefrontCandidate(candidate.href, candidate.text, requestedBaseUrl, marketId);
    if (score <= 0) continue;
    if (!best || score > best.score) {
      best = { url: candidate.href, score };
    }
  }

  return { url: best?.url || null, selectorRoot };
}

export async function resolveStorefrontTarget(params: {
  target: ParseTargetResult;
  marketId: MarketId;
  context: FetchContext;
  diagnostics: ExtractionDiagnostics;
  log?: LoggerFn;
}): Promise<StorefrontResolution> {
  const { target, diagnostics, marketId, context, log } = params;
  if (target.seedUrl) {
    diagnostics.resolved_base_url = target.baseUrl;
    return {
      target,
      selectorRootDetected: false,
      storefrontResolved: false,
    };
  }

  const landing = await fetchTextTracked(target.baseUrl, context, diagnostics);
  if (!landing.body) {
    return {
      target,
      selectorRootDetected: false,
      storefrontResolved: false,
    };
  }

  const resolved = resolveStorefrontFromHtml(landing.body, target.baseUrl, marketId);
  if (!resolved.url) {
    return {
      target,
      selectorRootDetected: resolved.selectorRoot,
      storefrontResolved: false,
    };
  }

  const next = parseTarget(resolved.url);
  diagnostics.resolved_base_url = next.baseUrl;
  log?.("info", `Resolved storefront ${target.baseUrl} -> ${next.baseUrl}`);
  return {
    target: next,
    selectorRootDetected: resolved.selectorRoot,
    storefrontResolved: true,
  };
}

export function extractSitemapUrlsFromRobots(robotsText: string): string[] {
  const urls: string[] = [];
  for (const match of robotsText.matchAll(/^sitemap:\s*(.+)$/gim)) {
    const url = match[1]?.trim();
    if (url) urls.push(url);
  }
  return urls;
}

export function extractLocUrlsFromSitemap(xml: string): string[] {
  const urls: string[] = [];
  for (const match of xml.matchAll(/<loc>([^<]+)<\/loc>/gim)) {
    const loc = match[1]?.trim();
    if (!loc) continue;
    const cleaned = loc.replace(/^<!\[CDATA\[/, "").replace(/\]\]>$/, "").trim();
    urls.push(cleaned);
  }
  return urls;
}

function dedupeUrls(urls: string[]): string[] {
  return Array.from(new Set(urls.map((value) => value.trim()).filter(Boolean)));
}

function remoteBrowserEndpoint(): string | null {
  const enabled = (process.env.REMOTE_BROWSER_ENABLED || "1").toLowerCase();
  if (["0", "false", "no", "off"].includes(enabled)) return null;
  const endpoint = (process.env.REMOTE_BROWSER_WS_ENDPOINT || "").trim();
  return endpoint || null;
}

async function openBrowser(mode: BrowserRuntimeMode): Promise<Browser> {
  if (mode === "managed") {
    const endpoint = remoteBrowserEndpoint();
    if (!endpoint) {
      throw new Error("REMOTE_BROWSER_WS_ENDPOINT is not configured");
    }
    return puppeteer.connect({ browserWSEndpoint: endpoint });
  }

  return puppeteer.launch({
    headless: true,
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });
}

async function closeBrowser(browser: Browser, mode: BrowserRuntimeMode): Promise<void> {
  if (mode === "managed") {
    browser.disconnect();
    return;
  }
  await browser.close().catch(() => undefined);
}

export async function runBrowserTaskWithFallback<T>(
  task: (browser: Browser, mode: BrowserRuntimeMode) => Promise<T>,
  options: BrowserTaskOptions,
): Promise<{ result: T; mode: BrowserRuntimeMode }> {
  let browser = await openBrowser("local");

  try {
    return { result: await task(browser, "local"), mode: "local" };
  } catch (error) {
    await closeBrowser(browser, "local");
    browser = undefined as never;

    if (error instanceof BotChallengeError && remoteBrowserEndpoint()) {
      options.log?.("warn", `Bot challenge detected locally at ${error.url}. Retrying with managed browser.`);
      setDiscoveryStrategy(options.diagnostics, "managed_browser");
      const managed = await openBrowser("managed");
      try {
        return { result: await task(managed, "managed"), mode: "managed" };
      } finally {
        await closeBrowser(managed, "managed");
      }
    }

    throw error;
  } finally {
    if (browser) {
      await closeBrowser(browser, "local");
    }
  }
}

export async function preparePage(
  page: Page,
  params: {
    baseUrl: string;
    context: FetchContext;
    navigationTimeoutMs: number;
  },
): Promise<void> {
  await page.setUserAgent(process.env.PUPPETEER_USER_AGENT || "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36");
  page.setDefaultNavigationTimeout(params.navigationTimeoutMs);

  if (params.context.headers && Object.keys(params.context.headers).length > 0) {
    await page.setExtraHTTPHeaders(params.context.headers);
  }

  const host = new URL(params.baseUrl).hostname;
  const cookieEntries = Object.entries(params.context.cookies || {});
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
}

async function waitForPageSettle(page: Page): Promise<void> {
  await page
    .waitForNetworkIdle({
      idleTime: 500,
      timeout: 2_500,
    })
    .catch(() => undefined);
}

export async function dismissCookieBanner(page: Page): Promise<void> {
  await page
    .evaluate(() => {
      const preferred = [
        "accept all",
        "allow all",
        "allow all cookies",
        "accept cookies",
        "accept",
        "i agree",
        "agree",
        "continue",
        "ok",
      ];

      const elements = Array.from(
        document.querySelectorAll<HTMLElement>("button, [role='button'], a, input[type='button'], input[type='submit']"),
      );

      for (const element of elements) {
        const label = (
          element.innerText ||
          element.getAttribute("aria-label") ||
          element.getAttribute("title") ||
          element.getAttribute("value") ||
          ""
        )
          .trim()
          .toLowerCase();
        if (!label) continue;
        if (!preferred.some((token) => label.includes(token))) continue;
        element.click();
        return;
      }
    })
    .catch(() => undefined);
}

export async function gotoPageOrThrow(page: Page, params: {
  url: string;
  baseUrl: string;
  context: FetchContext;
  diagnostics: ExtractionDiagnostics;
}): Promise<PageVisitResult> {
  const targetUrl = withUrlParams(params.url, params.context.url_params || {});
  const response = await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
  appendHttpTrace(params.diagnostics, { url: targetUrl, status: response?.status() ?? null });
  await waitForPageSettle(page);
  await dismissCookieBanner(page);
  await waitForPageSettle(page);

  const content = await page.content();
  const title = await page.title();
  const provider = detectBlockProvider({
    status: response?.status() ?? null,
    headers: response ? response.headers() : {},
    body: content,
    title,
    url: targetUrl,
  });

  recordBlockProvider(params.diagnostics, provider);
  if (provider) {
    throw new BotChallengeError(provider, targetUrl);
  }

  return {
    url: targetUrl,
    status: response?.status() ?? null,
    content,
    title,
  };
}

async function collectAnchorsFromPage(page: Page): Promise<AnchorCandidate[]> {
  return page.evaluate(() => {
    const elements = Array.from(document.querySelectorAll<HTMLAnchorElement>("a[href]"));
    return elements
      .map((anchor) => {
        const href = anchor.href || anchor.getAttribute("href") || "";
        const text = (anchor.innerText || anchor.textContent || "").trim();
        const container =
          anchor.closest<HTMLElement>(
            "[data-product-card], [class*='product'], [class*='card'], [class*='tile'], article, li, section",
          ) || anchor.parentElement;
        const contextText = (container?.innerText || "").replace(/\s+/g, " ").trim().slice(0, 400);
        return { href, text, contextText };
      })
      .filter((candidate) => Boolean(candidate.href));
  });
}

async function discoverProductUrlsWithBrowser(params: {
  browser: Browser;
  baseUrl: string;
  seedUrl?: string;
  maxProducts: number;
  context: FetchContext;
  diagnostics: ExtractionDiagnostics;
  navigationTimeoutMs: number;
}): Promise<BrowserDiscoveryResult> {
  const pageLimit = clampInt(process.env.BROWSER_DISCOVERY_PAGE_LIMIT, 3, 1, 10);
  const startUrl = params.seedUrl || params.baseUrl;
  const visited = new Set<string>();
  const productScores = new Map<string, number>();
  const categoryQueue: string[] = [];

  const visit = async (url: string) => {
    if (visited.has(url)) return;
    visited.add(url);

    const page = await params.browser.newPage();
    try {
      await preparePage(page, {
        baseUrl: params.baseUrl,
        context: params.context,
        navigationTimeoutMs: params.navigationTimeoutMs,
      });
      await gotoPageOrThrow(page, {
        url,
        baseUrl: params.baseUrl,
        context: params.context,
        diagnostics: params.diagnostics,
      });

      const anchors = await collectAnchorsFromPage(page);
      for (const anchor of anchors) {
        const absolute = canonicalizeUrl(anchor.href, params.baseUrl);
        const score = scoreBrowserCandidate(anchor, params.baseUrl);
        if (score >= 5) {
          productScores.set(absolute, Math.max(score, productScores.get(absolute) || 0));
          continue;
        }

        if (categoryQueue.length < pageLimit && isLikelyCategoryUrl(absolute, params.baseUrl)) {
          categoryQueue.push(absolute);
        }
      }
    } finally {
      await page.close().catch(() => undefined);
    }
  };

  await visit(startUrl);
  for (const url of categoryQueue.slice(0, pageLimit)) {
    if (productScores.size >= params.maxProducts) break;
    await visit(url);
  }

  return {
    productUrls: Array.from(productScores.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([url]) => url)
      .slice(0, params.maxProducts),
    categoryUrls: dedupeUrls(categoryQueue).slice(0, pageLimit),
  };
}

export async function discoverProductUrls(params: {
  baseUrl: string;
  maxProducts: number;
  seedUrl?: string;
  context: FetchContext;
  diagnostics: ExtractionDiagnostics;
  selectorRootDetected?: boolean;
  log?: LoggerFn;
}): Promise<DiscoveryResult> {
  let deadSitemapDetected = false;
  let challengeDetected = false;
  const seedDiscoveryUrl = params.seedUrl || params.baseUrl;

  if (seedDiscoveryUrl) {
    const seed = await fetchTextTracked(seedDiscoveryUrl, params.context, params.diagnostics);
    if (seed.blockedBy) challengeDetected = true;

    if (seed.body) {
      const directProductCandidate =
        looksLikeProductPageHtml(seed.body) ||
        (seed.ok && scoreProductCandidateUrl(seedDiscoveryUrl, params.baseUrl) >= 4 && PRODUCT_SIGNAL_RE.test(seed.body));
      if (directProductCandidate) {
        setDiscoveryStrategy(params.diagnostics, "seed_page");
        return {
          sitemapUrl: undefined,
          productUrls: [canonicalizeUrl(seedDiscoveryUrl, params.baseUrl)],
          deadSitemapDetected,
          challengeDetected,
        };
      }

      const seedUrls = extractProductUrlsFromHtml(seed.body, params.baseUrl).slice(0, params.maxProducts);
      if (seedUrls.length > 0) {
        setDiscoveryStrategy(params.diagnostics, "seed_page");
        return {
          sitemapUrl: undefined,
          productUrls: seedUrls,
          deadSitemapDetected,
          challengeDetected,
        };
      }
    }
  }

  const robotsUrl = `${params.baseUrl}/robots.txt`;
  const robots = await fetchTextTracked(robotsUrl, params.context, params.diagnostics);
  if (robots.blockedBy) challengeDetected = true;
  const robotsSitemaps = extractSitemapUrlsFromRobots(robots.body || "");
  const candidateSitemaps = dedupeUrls([...robotsSitemaps, `${params.baseUrl}/sitemap.xml`, `${params.baseUrl}/sitemap_index.xml`]);

  const visited = new Set<string>();
  const queue = [...candidateSitemaps];
  const pageUrls: string[] = [];
  let chosenSitemap: string | undefined;
  const maxSitemaps = clampInt(process.env.MAX_SITEMAPS, 20, 1, 100);

  while (queue.length > 0 && visited.size < maxSitemaps) {
    const sitemapUrl = queue.shift();
    if (!sitemapUrl || visited.has(sitemapUrl)) continue;
    visited.add(sitemapUrl);

    const sitemap = await fetchTextTracked(sitemapUrl, params.context, params.diagnostics);
    if (sitemap.blockedBy) challengeDetected = true;
    if (!sitemap.body || !sitemap.ok) {
      deadSitemapDetected = deadSitemapDetected || robotsSitemaps.includes(sitemapUrl);
      continue;
    }

    if (!isXmlLikeContent(sitemap.body, sitemap.contentType)) {
      deadSitemapDetected = true;
      continue;
    }

    if (!chosenSitemap) chosenSitemap = sitemapUrl;
    const locs = extractLocUrlsFromSitemap(sitemap.body);
    const isIndex = /<sitemapindex/i.test(sitemap.body);

    if (isIndex) {
      for (const loc of locs) {
        if (!visited.has(loc)) queue.push(loc);
      }
    } else {
      pageUrls.push(...locs);
      const dedupedSoFar = dedupeUrls(pageUrls).filter((url) => url.startsWith("http"));
      const likelySoFar = dedupedSoFar.filter((url) => isLikelyProductUrl(url, params.baseUrl));
      if (likelySoFar.length >= params.maxProducts) break;
    }
  }

  const deduped = dedupeUrls(pageUrls).filter((url) => url.startsWith("http"));
  const nonAsset = deduped.filter((url) => !isStaticAssetUrl(url, params.baseUrl));
  const productLike = nonAsset.filter((url) => isLikelyProductUrl(url, params.baseUrl));
  const selected = productLike.slice(0, params.maxProducts);
  if (selected.length > 0) {
    setDiscoveryStrategy(params.diagnostics, "sitemap");
    return {
      sitemapUrl: chosenSitemap,
      productUrls: selected,
      deadSitemapDetected,
      challengeDetected,
    };
  }

  const navigationTimeoutMs = clampInt(process.env.PUPPETEER_NAV_TIMEOUT_MS, 8_000, 5_000, 120_000);
  try {
    const browserDiscovery = await runBrowserTaskWithFallback(
      async (browser, mode) => {
        const result = await discoverProductUrlsWithBrowser({
          browser,
          baseUrl: params.baseUrl,
          seedUrl: params.seedUrl,
          maxProducts: params.maxProducts,
          context: params.context,
          diagnostics: params.diagnostics,
          navigationTimeoutMs,
        });
        setDiscoveryStrategy(params.diagnostics, mode === "managed" ? "managed_browser" : "browser_discovery");
        return result;
      },
      { diagnostics: params.diagnostics, log: params.log },
    );

    if (browserDiscovery.result.productUrls.length > 0) {
      return {
        sitemapUrl: chosenSitemap,
        productUrls: browserDiscovery.result.productUrls,
        deadSitemapDetected,
        challengeDetected,
      };
    }
  } catch (error) {
    if (error instanceof BotChallengeError) {
      challengeDetected = true;
      recordBlockProvider(params.diagnostics, error.provider);
    } else if (error instanceof Error && /timed out/i.test(error.message)) {
      setFailureCategory(params.diagnostics, "timeout");
    }
  }

  if (!params.diagnostics.failure_category) {
    if (challengeDetected) {
      setFailureCategory(params.diagnostics, "bot_challenge");
    } else if (params.selectorRootDetected) {
      setFailureCategory(params.diagnostics, "non_storefront_root");
    } else if (deadSitemapDetected) {
      setFailureCategory(params.diagnostics, "dead_sitemap");
    } else {
      setFailureCategory(params.diagnostics, "no_product_urls");
    }
  }

  return {
    sitemapUrl: chosenSitemap,
    productUrls: [],
    deadSitemapDetected,
    challengeDetected,
  };
}

export function extractProductSignals(candidate: AnchorCandidate): { hasPrice: boolean; hasBuySignal: boolean } {
  const text = `${candidate.text} ${candidate.contextText || ""}`;
  return {
    hasPrice: PRICE_SIGNAL_RE.test(text),
    hasBuySignal: /add to cart|buy now|quick shop|learn more|shop now|details/i.test(text),
  };
}
