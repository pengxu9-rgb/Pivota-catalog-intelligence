import { Router } from "express";

import { appendExtractHistory, readExtractHistory } from "../services/extractHistory";
import { extractCatalog, extractCatalogOffersV2 } from "../services/extractors";
import type {
  ExtractRequestBody,
  ExtractResponse,
  ExtractV2RequestBody,
  ExtractV2Response,
  MarketId,
} from "../services/extractors/types";

export const extractRouter = Router();

function parseSessionId(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  return trimmed.slice(0, 120);
}

function buildFallbackSessionId() {
  return `srv-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function firstQueryValue(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    const first = value[0];
    return typeof first === "string" ? first : undefined;
  }
  return undefined;
}

function parsePositiveIntQuery(value: unknown): number | undefined {
  const raw = firstQueryValue(value);
  if (!raw) return undefined;
  const num = Number.parseInt(raw, 10);
  if (!Number.isFinite(num) || num < 1) return undefined;
  return num;
}

function parseIncludeEntries(value: unknown): boolean {
  const raw = firstQueryValue(value)?.toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
}

extractRouter.post("/extract", async (req, res) => {
  const body = (req.body || {}) as Partial<ExtractRequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";
  const rawOffset = typeof body.offset === "number" ? body.offset : Number.NaN;
  const rawLimit = typeof body.limit === "number" ? body.limit : Number.NaN;
  const sessionId = parseSessionId(body.session_id) || buildFallbackSessionId();

  const offset = Number.isFinite(rawOffset) ? Math.max(0, Math.floor(rawOffset)) : undefined;
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.floor(rawLimit)) : undefined;

  if (!brand || !domain) {
    return res.status(400).json({
      error: "Invalid request body. Expected { brand, domain }.",
    });
  }

  const startedAt = Date.now();
  try {
    const result: ExtractResponse = await extractCatalog({ brand, domain, offset, limit });
    await appendExtractHistory({
      at: new Date().toISOString(),
      session_id: sessionId,
      endpoint: "v1",
      brand,
      domain,
      offset,
      limit,
      status: "ok",
      records_returned: result.variants.length,
      products_returned: result.products.length,
      has_more: result.pagination?.has_more,
      next_offset: result.pagination?.next_offset,
      duration_ms: Date.now() - startedAt,
    });

    return res.status(200).json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await appendExtractHistory({
      at: new Date().toISOString(),
      session_id: sessionId,
      endpoint: "v1",
      brand,
      domain,
      offset,
      limit,
      status: "error",
      records_returned: 0,
      products_returned: 0,
      duration_ms: Date.now() - startedAt,
      error: message,
    });

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
  const sessionId = parseSessionId(body.session_id) || buildFallbackSessionId();

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

  const startedAt = Date.now();
  try {
    const result: ExtractV2Response = await extractCatalogOffersV2({
      brand,
      domain,
      offset,
      limit,
      markets: markets as MarketId[] | undefined,
    });

    await appendExtractHistory({
      at: new Date().toISOString(),
      session_id: sessionId,
      endpoint: "v2",
      brand,
      domain,
      markets,
      offset,
      limit,
      status: "ok",
      records_returned: result.offers_v2.length,
      products_returned: 0,
      has_more: result.pagination?.has_more,
      next_offset: result.pagination?.next_offset,
      duration_ms: Date.now() - startedAt,
    });

    return res.status(200).json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await appendExtractHistory({
      at: new Date().toISOString(),
      session_id: sessionId,
      endpoint: "v2",
      brand,
      domain,
      markets,
      offset,
      limit,
      status: "error",
      records_returned: 0,
      products_returned: 0,
      duration_ms: Date.now() - startedAt,
      error: message,
    });

    // eslint-disable-next-line no-console
    console.error("[/api/extract/v2] error", err);
    return res.status(500).json({
      error: "V2 extraction failed.",
    });
  }
});

extractRouter.get("/extract/history", async (req, res) => {
  try {
    const days = parsePositiveIntQuery(req.query.days);
    const limit = parsePositiveIntQuery(req.query.limit);
    const runLimit = parsePositiveIntQuery(req.query.run_limit);
    const includeEntries = parseIncludeEntries(req.query.include_entries);

    const history = await readExtractHistory({
      days,
      limit,
      runLimit,
      includeEntries,
    });

    return res.status(200).json(history);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract/history] error", err);
    return res.status(500).json({
      error: "Failed to read extract history.",
    });
  }
});
