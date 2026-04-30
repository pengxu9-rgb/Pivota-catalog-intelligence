import assert from "node:assert/strict";
import test from "node:test";

import { extractCatalog } from "../src/services/extractors";

test("extractCatalog fails closed when simulation mode is requested without explicit opt-in", async () => {
  const originalMode = process.env.EXTRACTION_MODE;
  const originalNodeEnv = process.env.NODE_ENV;
  const originalAllow = process.env.ALLOW_SIMULATION_EXTRACTOR;

  process.env.EXTRACTION_MODE = "simulation";
  process.env.NODE_ENV = "production";
  delete process.env.ALLOW_SIMULATION_EXTRACTOR;

  try {
    await assert.rejects(
      () =>
        extractCatalog({
          brand: "Example",
          domain: "https://example.com",
          limit: 1,
        }),
      /ALLOW_SIMULATION_EXTRACTOR=1/,
    );
  } finally {
    if (originalMode === undefined) delete process.env.EXTRACTION_MODE;
    else process.env.EXTRACTION_MODE = originalMode;
    if (originalNodeEnv === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = originalNodeEnv;
    if (originalAllow === undefined) delete process.env.ALLOW_SIMULATION_EXTRACTOR;
    else process.env.ALLOW_SIMULATION_EXTRACTOR = originalAllow;
  }
});
