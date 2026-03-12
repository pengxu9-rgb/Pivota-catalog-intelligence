import assert from "node:assert/strict";
import test from "node:test";

import { PuppeteerExtractor } from "../src/services/extractors/puppeteer";

type MockRoute = {
  status?: number;
  headers?: Record<string, string>;
  body?: string;
};

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

test("PuppeteerExtractor honors direct Shopify PDP seed URLs", async () => {
  const extractor = new PuppeteerExtractor();
  const directProduct = {
    id: 101,
    title: "Banana Bright 15% Vitamin C Dark Spot Serum",
    handle: "banana-bright-vitamin-c-serum",
    body_html: "<p>Brightening serum</p>",
    variants: [
      {
        id: 1001,
        sku: "OH-VC-001",
        title: "Default Title",
        option1: "Default Title",
        price: 68,
        available: true,
        inventory_quantity: 12,
      },
    ],
    options: [{ name: "Variant" }],
    images: [
      { src: "https://cdn.example.com/banana-1.jpg" },
      { src: "https://cdn.example.com/banana-2.jpg" },
    ],
  };

  await withMockFetch(
    {
      "https://olehenriksen.com/products/banana-bright-vitamin-c-serum.js": {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
        body: JSON.stringify(directProduct),
      },
    },
    async () => {
      const result = await extractor.extract({
        brand: "Ole Henriksen",
        domain: "https://olehenriksen.com/products/banana-bright-vitamin-c-serum",
        limit: 5,
      });

      assert.equal(result.products.length, 1);
      assert.equal(result.products[0]?.url, "https://olehenriksen.com/products/banana-bright-vitamin-c-serum");
      assert.deepEqual(result.products[0]?.variant_skus, ["OH-VC-001"]);
      assert.deepEqual(result.products[0]?.image_urls, [
        "https://cdn.example.com/banana-1.jpg",
        "https://cdn.example.com/banana-2.jpg",
      ]);
      assert.equal(result.diagnostics?.discovery_strategy, "shopify_json");
    },
  );
});
