import { PuppeteerExtractor } from "./puppeteer";
import { SimulationExtractor } from "./simulation";
import type { ExtractInput, ExtractResponse, Extractor } from "./types";

function getExtractor(): Extractor {
  const mode = (process.env.EXTRACTION_MODE || "simulation").toLowerCase();
  if (mode === "puppeteer") return new PuppeteerExtractor();
  return new SimulationExtractor();
}

export async function extractCatalog(input: ExtractInput): Promise<ExtractResponse> {
  const extractor = getExtractor();
  return extractor.extract(input);
}
