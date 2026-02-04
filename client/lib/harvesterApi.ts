import type { CreateTaskResponse, ImportResponse, ListRowsResponse, TaskProgress } from "./harvesterTypes";

const DEFAULT_BASE = "http://localhost:3001/api/ingredient-harvester";

function baseUrl() {
  const explicit = process.env.NEXT_PUBLIC_INGREDIENT_HARVESTER_BASE_URL;
  if (explicit) return explicit;

  const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (apiBase) return `${String(apiBase).trim().replace(/\/+$/g, "")}/api/ingredient-harvester`;

  return DEFAULT_BASE;
}

async function assertOk(res: Response) {
  if (res.ok) return;
  const text = await res.text().catch(() => "");
  throw new Error(`Harvester API error (${res.status}): ${text || res.statusText}`);
}

export async function uploadCandidatesCsv(file: File): Promise<ImportResponse> {
  const url = `${baseUrl()}/v1/imports`;
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(url, { method: "POST", body: form });
  await assertOk(res);
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
  const res = await fetch(u.toString(), { cache: "no-store" });
  await assertOk(res);
  return (await res.json()) as ListRowsResponse;
}

export async function startHarvestTask(args: {
  importId: string;
  rowIds?: string[];
  force?: boolean;
}): Promise<CreateTaskResponse> {
  const res = await fetch(`${baseUrl()}/v1/tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ import_id: args.importId, row_ids: args.rowIds || null, force: !!args.force }),
  });
  await assertOk(res);
  return (await res.json()) as CreateTaskResponse;
}

export async function getTaskProgress(taskId: string): Promise<TaskProgress> {
  const res = await fetch(`${baseUrl()}/v1/tasks/${encodeURIComponent(taskId)}`, { cache: "no-store" });
  await assertOk(res);
  return (await res.json()) as TaskProgress;
}

export async function updateRow(rowId: string, patch: Record<string, unknown>) {
  const res = await fetch(`${baseUrl()}/v1/rows/${encodeURIComponent(rowId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  await assertOk(res);
  return res.json();
}

export function exportImportUrl(importId: string, format: "csv" | "xlsx") {
  const u = new URL(`${baseUrl()}/v1/exports/${encodeURIComponent(importId)}`);
  u.searchParams.set("format", format);
  return u.toString();
}
