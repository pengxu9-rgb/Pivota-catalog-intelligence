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
  looksLikeProductPageHtml,
  resolveStorefrontFromHtml,
  runBrowserTaskWithFallback,
} from "../src/services/extractors/shared";

type MockRoute = {
  status?: number;
  headers?: Record<string, string>;
  body?: string;
};

function readFixture(name: string): string {
  return readFileSync(join(__dirname, "fixtures", name), "utf8");
}

function createMockResponse(route: MockRoute): Response {
  return new Response(route.body ?? "", {
    status: route.status ?? 200,
    headers: route.headers,
  });
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
