import { Router } from "express";

import { extractCatalog } from "../services/extractors";
import { extractCatalogV2 } from "../services/extractors/extractV2";
import { getSupportedMarketIds } from "../services/extractors/marketProfiles";
import type {
  ExtractRequestBody,
  ExtractResponse,
  ExtractV2RequestBody,
  ExtractV2Response,
  MarketId,
} from "../services/extractors/types";

export const extractRouter = Router();

const SUPPORTED_MARKET_IDS = getSupportedMarketIds();
const SUPPORTED_MARKETS = new Set<MarketId>(SUPPORTED_MARKET_IDS as MarketId[]);
const SUPPORTED_MARKETS_LABEL = SUPPORTED_MARKET_IDS.join(", ");

extractRouter.post("/extract", async (req, res) => {
  const body = (req.body || {}) as Partial<ExtractRequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";
  const productTitle = typeof body.product_title === "string" ? body.product_title.trim() : "";
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
      error: `Invalid market. Supported values: ${SUPPORTED_MARKETS_LABEL}.`,
    });
  }

  try {
    const result: ExtractResponse = await extractCatalog({
      brand,
      domain,
      market,
      offset,
      limit,
      ...(productTitle ? { product_title: productTitle } : {}),
    });
    return res.status(200).json(result);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract] error", err);
    return res.status(500).json({
      error: "Extraction failed.",
    });
  }
});

extractRouter.post("/extract-v2", async (req, res) => {
  const body = (req.body || {}) as Partial<ExtractV2RequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";
  const rawMarket = typeof body.market === "string" ? body.market.trim().toUpperCase() : "US";
  const rawMarkets = Array.isArray(body.markets)
    ? body.markets.map((item) => String(item || "").trim().toUpperCase()).filter(Boolean)
    : [];
  const rawOffset = typeof body.offset === "number" ? body.offset : Number.NaN;
  const rawLimit = typeof body.limit === "number" ? body.limit : Number.NaN;

  const offset = Number.isFinite(rawOffset) ? Math.max(0, Math.floor(rawOffset)) : undefined;
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.floor(rawLimit)) : undefined;
  const market = (SUPPORTED_MARKETS.has(rawMarket as MarketId) ? rawMarket : null) as MarketId | null;
  const markets = rawMarkets.length
    ? rawMarkets.filter((candidate): candidate is MarketId => SUPPORTED_MARKETS.has(candidate as MarketId))
    : [];

  if (!brand || !domain) {
    return res.status(400).json({
      error: "Invalid request body. Expected { brand, domain }.",
    });
  }
  if (rawMarkets.length && markets.length !== rawMarkets.length) {
    return res.status(400).json({
      error: `Invalid markets. Supported values: ${SUPPORTED_MARKETS_LABEL}.`,
    });
  }
  if (!rawMarkets.length && !market) {
    return res.status(400).json({
      error: `Invalid market. Supported values: ${SUPPORTED_MARKETS_LABEL}.`,
    });
  }

  try {
    const result: ExtractV2Response = await extractCatalogV2({
      brand,
      domain,
      ...(markets.length ? { markets } : { market: market || "US" }),
      offset,
      limit,
    });
    return res.status(200).json(result);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract-v2] error", err);
    return res.status(500).json({
      error: "Extraction V2 failed.",
    });
  }
});
