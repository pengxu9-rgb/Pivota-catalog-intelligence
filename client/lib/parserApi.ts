import type { ParserReparseBatchRequest, ParserReparseBatchResponse, ParserReparseResponse } from "./parserTypes";

const DEFAULT_BASE = "http://localhost:3001/api/parser";

function baseUrl() {
  const explicit = process.env.NEXT_PUBLIC_PARSER_API_BASE_URL;
  if (explicit) return explicit;

  const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (apiBase) return `${String(apiBase).trim().replace(/\/+$/g, "")}/api/parser`;

  return DEFAULT_BASE;
}

async function assertOk(res: Response, url: string) {
  if (res.ok) return;
  const rid = res.headers.get("x-harvester-request-id");
  const text = await res.text().catch(() => "");
  const snippet = text && text.length > 800 ? `${text.slice(0, 800)}…` : text;
  const where = rid ? `${url} [${rid}]` : url;
  throw new Error(`Parser API error (${res.status}) at ${where}: ${snippet || res.statusText}`);
}

export async function reparse(raw_ingredient_text: string): Promise<ParserReparseResponse> {
  const url = `${baseUrl()}/re-parse`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ raw_ingredient_text }),
  });
  await assertOk(res, url);
  return (await res.json()) as ParserReparseResponse;
}

export async function reparseBatch(req: ParserReparseBatchRequest): Promise<ParserReparseBatchResponse> {
  const url = `${baseUrl()}/re-parse-batch`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
  await assertOk(res, url);
  return (await res.json()) as ParserReparseBatchResponse;
}

