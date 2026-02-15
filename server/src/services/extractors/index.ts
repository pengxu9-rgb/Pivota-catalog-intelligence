import { PuppeteerExtractor } from "./puppeteer";
import { extractCatalogV2 } from "./extractV2";
import { SimulationExtractor } from "./simulation";
import type { ExtractInput, ExtractResponse, ExtractV2RequestBody, ExtractV2Response, Extractor } from "./types";

function getExtractor(): Extractor {
  const mode = (process.env.EXTRACTION_MODE || "simulation").toLowerCase();
  if (mode === "puppeteer") return new PuppeteerExtractor();
  return new SimulationExtractor();
}

export async function extractCatalog(input: ExtractInput): Promise<ExtractResponse> {
  const extractor = getExtractor();
  return extractor.extract(input);
}

export async function extractCatalogOffersV2(input: ExtractV2RequestBody): Promise<ExtractV2Response> {
  return extractCatalogV2(input);
}
