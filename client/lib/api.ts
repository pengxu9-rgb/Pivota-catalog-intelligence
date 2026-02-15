import type { ExtractHistoryResponse, ExtractResponse, ExtractV2Response } from "./types";

const DEFAULT_BASE = "http://localhost:3001";
const DEFAULT_EXTRACT_TIMEOUT_MS = 65_000;

export async function extractCatalog(input: {
  brand: string;
  domain: string;
  offset?: number;
  limit?: number;
  session_id?: string;
}): Promise<ExtractResponse> {
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

export async function extractCatalogV2(input: {
  brand: string;
  domain: string;
  offset?: number;
  limit?: number;
  markets?: string[];
  session_id?: string;
}): Promise<ExtractV2Response> {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || DEFAULT_BASE;
  const timeoutMs = Number(process.env.NEXT_PUBLIC_EXTRACT_TIMEOUT_MS || DEFAULT_EXTRACT_TIMEOUT_MS);
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), Number.isFinite(timeoutMs) ? timeoutMs : DEFAULT_EXTRACT_TIMEOUT_MS);

  let res: Response;
  try {
    res = await fetch(`${baseUrl}/api/extract/v2`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(input),
      signal: controller.signal,
    });
  } catch (err) {
    if ((err as Error)?.name === "AbortError") {
      throw new Error("V2 extraction timed out. Please retry or reduce crawl size.");
    }
    throw new Error("Network error while calling /api/extract/v2.");
  } finally {
    window.clearTimeout(timer);
  }

  if (!res.ok) {
    throw new Error(`V2 extract failed (${res.status})`);
  }

  return (await res.json()) as ExtractV2Response;
}

export async function fetchExtractHistory(input?: {
  days?: number;
  limit?: number;
  run_limit?: number;
  include_entries?: boolean;
}): Promise<ExtractHistoryResponse> {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || DEFAULT_BASE;
  const params = new URLSearchParams();

  if (input?.days && Number.isFinite(input.days)) params.set("days", String(Math.max(1, Math.floor(input.days))));
  if (input?.limit && Number.isFinite(input.limit)) params.set("limit", String(Math.max(1, Math.floor(input.limit))));
  if (input?.run_limit && Number.isFinite(input.run_limit)) params.set("run_limit", String(Math.max(1, Math.floor(input.run_limit))));
  if (input?.include_entries) params.set("include_entries", "1");

  const query = params.toString();
  const url = `${baseUrl}/api/extract/history${query ? `?${query}` : ""}`;

  let res: Response;
  try {
    res = await fetch(url, { method: "GET" });
  } catch {
    throw new Error("Network error while calling /api/extract/history.");
  }

  if (!res.ok) {
    throw new Error(`History fetch failed (${res.status})`);
  }

  return (await res.json()) as ExtractHistoryResponse;
}
