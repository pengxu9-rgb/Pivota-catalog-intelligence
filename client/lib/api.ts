import type { ExtractResponse } from "./types";

const DEFAULT_BASE = "http://localhost:3001";

export async function extractCatalog(input: { brand: string; domain: string }): Promise<ExtractResponse> {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || DEFAULT_BASE;
  const res = await fetch(`${baseUrl}/api/extract`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  if (!res.ok) {
    throw new Error(`Extract failed (${res.status})`);
  }
  return (await res.json()) as ExtractResponse;
}

