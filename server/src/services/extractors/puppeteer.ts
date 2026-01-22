import type { ExtractInput, ExtractResponse, Extractor } from "./types";

export class PuppeteerExtractor implements Extractor {
  async extract(input: ExtractInput): Promise<ExtractResponse> {
    const now = new Date().toISOString();
    return {
      brand: input.brand,
      domain: input.domain,
      generated_at: now,
      mode: "puppeteer",
      products: [],
      variants: [],
      pricing: { currency: "USD", min: 0, max: 0, avg: 0 },
      ad_copy: { by_variant_id: {} },
      logs: [
        { at: now, type: "warn", msg: "Puppeteer extractor not implemented yet. Falling back to empty results." },
      ],
    };
  }
}
