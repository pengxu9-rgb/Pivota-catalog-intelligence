import { Router } from "express";

import { extractCatalog, extractCatalogOffersV2 } from "../services/extractors";
import type {
  ExtractRequestBody,
  ExtractResponse,
  ExtractV2RequestBody,
  ExtractV2Response,
  MarketId,
} from "../services/extractors/types";

export const extractRouter = Router();

extractRouter.post("/extract", async (req, res) => {
  const body = (req.body || {}) as Partial<ExtractRequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";
  const rawOffset = typeof body.offset === "number" ? body.offset : Number.NaN;
  const rawLimit = typeof body.limit === "number" ? body.limit : Number.NaN;

  const offset = Number.isFinite(rawOffset) ? Math.max(0, Math.floor(rawOffset)) : undefined;
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.floor(rawLimit)) : undefined;

  if (!brand || !domain) {
    return res.status(400).json({
      error: "Invalid request body. Expected { brand, domain }.",
    });
  }

  try {
    const result: ExtractResponse = await extractCatalog({ brand, domain, offset, limit });
    return res.status(200).json(result);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract] error", err);
    return res.status(500).json({
      error: "Extraction failed.",
    });
  }
});

extractRouter.post("/extract/v2", async (req, res) => {
  const enabled = (process.env.EXTRACT_V2_ENABLED || "1").toLowerCase() !== "0";
  if (!enabled) {
    return res.status(404).json({ error: "V2 extraction is disabled by feature flag." });
  }

  const body = (req.body || {}) as Partial<ExtractV2RequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";
  const rawOffset = typeof body.offset === "number" ? body.offset : Number.NaN;
  const rawLimit = typeof body.limit === "number" ? body.limit : Number.NaN;

  const offset = Number.isFinite(rawOffset) ? Math.max(0, Math.floor(rawOffset)) : undefined;
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.floor(rawLimit)) : undefined;
  const markets = Array.isArray(body.markets)
    ? body.markets
        .filter((item): item is string => typeof item === "string")
        .map((item) => item.trim())
        .filter(Boolean)
    : undefined;

  if (!brand || !domain) {
    return res.status(400).json({
      error: "Invalid request body. Expected { brand, domain, markets? }.",
    });
  }

  try {
    const result: ExtractV2Response = await extractCatalogOffersV2({
      brand,
      domain,
      offset,
      limit,
      markets: markets as MarketId[] | undefined,
    });
    return res.status(200).json(result);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract/v2] error", err);
    return res.status(500).json({
      error: "V2 extraction failed.",
    });
  }
});
