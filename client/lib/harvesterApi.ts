import type { CreateTaskResponse, ImportResponse, ListRowsResponse, TaskProgress } from "./harvesterTypes";

const DEFAULT_BASE = "http://localhost:3001/api/ingredient-harvester";

function baseUrl() {
  const explicit = process.env.NEXT_PUBLIC_INGREDIENT_HARVESTER_BASE_URL;
  if (explicit) return explicit;

  const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (apiBase) return `${String(apiBase).trim().replace(/\/+$/g, "")}/api/ingredient-harvester`;

  return DEFAULT_BASE;
}

async function assertOk(res: Response, url: string) {
  if (res.ok) return;
  const rid = res.headers.get("x-harvester-request-id");
  const text = await res.text().catch(() => "");
  const snippet = text && text.length > 800 ? `${text.slice(0, 800)}…` : text;
  const where = rid ? `${url} [${rid}]` : url;
  throw new Error(`Harvester API error (${res.status}) at ${where}: ${snippet || res.statusText}`);
}

export async function uploadCandidatesCsv(file: File): Promise<ImportResponse> {
  const url = `${baseUrl()}/v1/imports`;
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(url, { method: "POST", body: form });
  await assertOk(res, url);
  return (await res.json()) as ImportResponse;
}

export async function listImportRows(args: {
  importId: string;
  status?: string;
  q?: string;
  limit?: number;
  offset?: number;
}): Promise<ListRowsResponse> {
  const { importId, status, q, limit = 200, offset = 0 } = args;
  const u = new URL(`${baseUrl()}/v1/imports/${encodeURIComponent(importId)}/rows`);
  if (status) u.searchParams.set("status", status);
  if (q) u.searchParams.set("q", q);
  u.searchParams.set("limit", String(limit));
  u.searchParams.set("offset", String(offset));
  const url = u.toString();
  const res = await fetch(url, { cache: "no-store" });
  await assertOk(res, url);
  return (await res.json()) as ListRowsResponse;
}

export async function startHarvestTask(args: {
  importId: string;
  rowIds?: string[];
  force?: boolean;
}): Promise<CreateTaskResponse> {
  const url = `${baseUrl()}/v1/tasks`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ import_id: args.importId, row_ids: args.rowIds || null, force: !!args.force }),
  });
  await assertOk(res, url);
  return (await res.json()) as CreateTaskResponse;
}

export async function getTaskProgress(taskId: string): Promise<TaskProgress> {
  const url = `${baseUrl()}/v1/tasks/${encodeURIComponent(taskId)}`;
  const res = await fetch(url, { cache: "no-store" });
  await assertOk(res, url);
  return (await res.json()) as TaskProgress;
}

export async function updateRow(rowId: string, patch: Record<string, unknown>) {
  const url = `${baseUrl()}/v1/rows/${encodeURIComponent(rowId)}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  await assertOk(res, url);
  return res.json();
}

export function exportImportUrl(importId: string, format: "csv" | "xlsx") {
  const u = new URL(`${baseUrl()}/v1/exports/${encodeURIComponent(importId)}`);
  u.searchParams.set("format", format);
  return u.toString();
}
