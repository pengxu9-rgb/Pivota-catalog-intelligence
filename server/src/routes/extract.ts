import { Router } from "express";

import { extractCatalog } from "../services/extractors";
import type { ExtractRequestBody, ExtractResponse } from "../services/extractors/types";

export const extractRouter = Router();

extractRouter.post("/extract", async (req, res) => {
  const body = (req.body || {}) as Partial<ExtractRequestBody>;

  const brand = typeof body.brand === "string" ? body.brand.trim() : "";
  const domain = typeof body.domain === "string" ? body.domain.trim() : "";

  if (!brand || !domain) {
    return res.status(400).json({
      error: "Invalid request body. Expected { brand, domain }.",
    });
  }

  try {
    const result: ExtractResponse = await extractCatalog({ brand, domain });
    return res.status(200).json(result);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/extract] error", err);
    return res.status(500).json({
      error: "Extraction failed.",
    });
  }
});
