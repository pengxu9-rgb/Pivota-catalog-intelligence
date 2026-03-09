import { Router } from "express";

import { extractCatalog } from "../services/extractors";
import type { ExtractRequestBody, ExtractResponse, MarketId } from "../services/extractors/types";

export const extractRouter = Router();

const SUPPORTED_MARKETS = new Set<MarketId>(["US", "EU-DE", "SG", "JP"]);

extractRouter.post("/extract", async (req, res) => {
  const body = (req.body || {}) as Partial<ExtractRequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";
  const rawMarket = typeof body.market === "string" ? body.market.trim().toUpperCase() : "US";
  const rawOffset = typeof body.offset === "number" ? body.offset : Number.NaN;
  const rawLimit = typeof body.limit === "number" ? body.limit : Number.NaN;

  const offset = Number.isFinite(rawOffset) ? Math.max(0, Math.floor(rawOffset)) : undefined;
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.floor(rawLimit)) : undefined;
  const market = (SUPPORTED_MARKETS.has(rawMarket as MarketId) ? rawMarket : null) as MarketId | null;

  if (!brand || !domain) {
    return res.status(400).json({
      error: "Invalid request body. Expected { brand, domain }.",
    });
  }
  if (!market) {
    return res.status(400).json({
      error: "Invalid market. Supported values: US, EU-DE, SG, JP.",
    });
  }

  try {
    const result: ExtractResponse = await extractCatalog({ brand, domain, market, offset, limit });
    return res.status(200).json(result);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract] error", err);
    return res.status(500).json({
      error: "Extraction failed.",
    });
  }
});
