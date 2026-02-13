import type { ExtractResponse } from "./types";

const DEFAULT_BASE = "http://localhost:3001";
const DEFAULT_EXTRACT_TIMEOUT_MS = 65_000;

export async function extractCatalog(input: { brand: string; domain: string }): Promise<ExtractResponse> {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || DEFAULT_BASE;
  const timeoutMs = Number(process.env.NEXT_PUBLIC_EXTRACT_TIMEOUT_MS || DEFAULT_EXTRACT_TIMEOUT_MS);
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), Number.isFinite(timeoutMs) ? timeoutMs : DEFAULT_EXTRACT_TIMEOUT_MS);

  let res: Response;
  try {
    res = await fetch(`${baseUrl}/api/extract`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(input),
      signal: controller.signal,
    });
  } catch (err) {
    if ((err as Error)?.name === "AbortError") {
      throw new Error("Extraction timed out. Please retry or reduce crawl size.");
    }
    throw new Error("Network error while calling /api/extract.");
  } finally {
    window.clearTimeout(timer);
  }

  if (!res.ok) {
    throw new Error(`Extract failed (${res.status})`);
  }
  return (await res.json()) as ExtractResponse;
}
