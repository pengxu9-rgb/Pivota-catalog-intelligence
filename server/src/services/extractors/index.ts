import { PuppeteerExtractor } from "./puppeteer";
import { SimulationExtractor } from "./simulation";
import type { ExtractInput, ExtractResponse, Extractor } from "./types";

function allowSimulationExtractor(): boolean {
  if ((process.env.NODE_ENV || "").toLowerCase() === "test") return true;
  return process.env.ALLOW_SIMULATION_EXTRACTOR === "1";
}

function getExtractor(): Extractor {
  const mode = (process.env.EXTRACTION_MODE || "puppeteer").toLowerCase();
  if (mode === "puppeteer") return new PuppeteerExtractor();
  if (mode === "simulation" && allowSimulationExtractor()) return new SimulationExtractor();
  throw new Error(
    'Unsupported extractor mode. Use EXTRACTION_MODE=puppeteer, or explicitly opt into simulation with ALLOW_SIMULATION_EXTRACTOR=1.',
  );
}

export async function extractCatalog(input: ExtractInput): Promise<ExtractResponse> {
  const extractor = getExtractor();
  return extractor.extract(input);
}
