import test from "node:test";
import assert from "node:assert/strict";

import { stockFromAvailability } from "../src/services/extractors/puppeteer";

test("structured availability is tri-state and never assumes in-stock", () => {
  assert.equal(stockFromAvailability("https://schema.org/InStock"), "In Stock");
  assert.equal(stockFromAvailability("https://schema.org/OutOfStock"), "Out of Stock");
  assert.equal(stockFromAvailability(undefined), "Unknown");
  assert.equal(stockFromAvailability(""), "Unknown");
});
