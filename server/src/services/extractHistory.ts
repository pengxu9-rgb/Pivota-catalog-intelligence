import { promises as fs } from "fs";
import path from "path";

export type ExtractHistoryEndpoint = "v1" | "v2";
export type ExtractHistoryStatus = "ok" | "error";

export type ExtractHistoryEntry = {
  at: string;
  session_id: string;
  endpoint: ExtractHistoryEndpoint;
  brand: string;
  domain: string;
  markets?: string[];
  offset?: number;
  limit?: number;
  status: ExtractHistoryStatus;
  records_returned: number;
  products_returned?: number;
  has_more?: boolean;
  next_offset?: number | null;
  duration_ms: number;
  error?: string;
};

export type ExtractHistoryRun = {
  session_id: string;
  started_at: string;
  finished_at: string;
  endpoint: ExtractHistoryEndpoint;
  brand: string;
  domain: string;
  markets: string[];
  status: ExtractHistoryStatus;
  request_count: number;
  total_records: number;
  total_products: number;
};

export type ReadExtractHistoryOptions = {
  days?: number;
  limit?: number;
  runLimit?: number;
  includeEntries?: boolean;
};

export type ReadExtractHistoryResult = {
  generated_at: string;
  days: number;
  total_entries: number;
  total_runs: number;
  runs: ExtractHistoryRun[];
  entries?: ExtractHistoryEntry[];
};

const DEFAULT_DAYS = 7;
const DEFAULT_ENTRY_LIMIT = 500;
const DEFAULT_RUN_LIMIT = 100;
const MAX_DAYS = 365;
const MAX_ENTRY_LIMIT = 5000;
const MAX_RUN_LIMIT = 1000;

function resolveHistoryPath() {
  const configured = process.env.EXTRACT_HISTORY_FILE?.trim();
  if (configured) return path.resolve(configured);
  return path.resolve(process.cwd(), "data", "extract_history.jsonl");
}

const HISTORY_PATH = resolveHistoryPath();

function clampInt(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function normalizeEntry(raw: unknown): ExtractHistoryEntry | null {
  if (!raw || typeof raw !== "object") return null;
  const obj = raw as Partial<ExtractHistoryEntry>;

  const at = typeof obj.at === "string" ? obj.at : "";
  const sessionId = typeof obj.session_id === "string" ? obj.session_id : "";
  const endpoint = obj.endpoint === "v1" || obj.endpoint === "v2" ? obj.endpoint : null;
  const brand = typeof obj.brand === "string" ? obj.brand : "";
  const domain = typeof obj.domain === "string" ? obj.domain : "";
  const status = obj.status === "ok" || obj.status === "error" ? obj.status : null;
  const recordsReturned = typeof obj.records_returned === "number" && Number.isFinite(obj.records_returned)
    ? obj.records_returned
    : null;
  const durationMs = typeof obj.duration_ms === "number" && Number.isFinite(obj.duration_ms)
    ? obj.duration_ms
    : null;

  if (!at || !sessionId || !endpoint || !brand || !domain || !status || recordsReturned == null || durationMs == null) {
    return null;
  }

  const markets = Array.isArray(obj.markets)
    ? obj.markets.filter((market): market is string => typeof market === "string" && market.length > 0)
    : undefined;

  return {
    at,
    session_id: sessionId,
    endpoint,
    brand,
    domain,
    markets,
    offset: typeof obj.offset === "number" && Number.isFinite(obj.offset) ? obj.offset : undefined,
    limit: typeof obj.limit === "number" && Number.isFinite(obj.limit) ? obj.limit : undefined,
    status,
    records_returned: recordsReturned,
    products_returned:
      typeof obj.products_returned === "number" && Number.isFinite(obj.products_returned)
        ? obj.products_returned
        : undefined,
    has_more: typeof obj.has_more === "boolean" ? obj.has_more : undefined,
    next_offset:
      typeof obj.next_offset === "number" || obj.next_offset === null ? obj.next_offset : undefined,
    duration_ms: durationMs,
    error: typeof obj.error === "string" ? obj.error : undefined,
  };
}

async function readAllEntries(): Promise<ExtractHistoryEntry[]> {
  let raw = "";
  try {
    raw = await fs.readFile(HISTORY_PATH, "utf8");
  } catch (err) {
    const code = (err as NodeJS.ErrnoException)?.code;
    if (code === "ENOENT") return [];
    throw err;
  }

  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const entries: ExtractHistoryEntry[] = [];
  for (const line of lines) {
    try {
      const parsed = JSON.parse(line) as unknown;
      const normalized = normalizeEntry(parsed);
      if (normalized) entries.push(normalized);
    } catch {
      // Skip malformed lines.
    }
  }

  return entries;
}

export async function appendExtractHistory(entry: ExtractHistoryEntry): Promise<void> {
  try {
    await fs.mkdir(path.dirname(HISTORY_PATH), { recursive: true });
    await fs.appendFile(HISTORY_PATH, `${JSON.stringify(entry)}\n`, "utf8");
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[extract-history] append failed", err);
  }
}

export async function readExtractHistory(options: ReadExtractHistoryOptions = {}): Promise<ReadExtractHistoryResult> {
  const days = clampInt(options.days ?? DEFAULT_DAYS, 1, MAX_DAYS);
  const limit = clampInt(options.limit ?? DEFAULT_ENTRY_LIMIT, 1, MAX_ENTRY_LIMIT);
  const runLimit = clampInt(options.runLimit ?? DEFAULT_RUN_LIMIT, 1, MAX_RUN_LIMIT);
  const includeEntries = options.includeEntries === true;

  const cutoffMs = Date.now() - days * 24 * 60 * 60 * 1000;
  const entries = await readAllEntries();

  const filtered = entries
    .filter((entry) => {
      const ts = Date.parse(entry.at);
      return Number.isFinite(ts) && ts >= cutoffMs;
    })
    .sort((a, b) => Date.parse(b.at) - Date.parse(a.at));

  const limitedEntries = filtered.slice(0, limit);

  const runMap = new Map<string, ExtractHistoryRun>();
  for (const entry of limitedEntries) {
    const existing = runMap.get(entry.session_id);
    if (!existing) {
      runMap.set(entry.session_id, {
        session_id: entry.session_id,
        started_at: entry.at,
        finished_at: entry.at,
        endpoint: entry.endpoint,
        brand: entry.brand,
        domain: entry.domain,
        markets: entry.markets ? [...entry.markets] : [],
        status: entry.status,
        request_count: 1,
        total_records: entry.status === "ok" ? entry.records_returned : 0,
        total_products: entry.status === "ok" ? entry.products_returned ?? 0 : 0,
      });
      continue;
    }

    if (Date.parse(entry.at) < Date.parse(existing.started_at)) existing.started_at = entry.at;
    if (Date.parse(entry.at) > Date.parse(existing.finished_at)) existing.finished_at = entry.at;
    if (existing.endpoint !== "v2" && entry.endpoint === "v2") existing.endpoint = "v2";

    if (entry.markets?.length) {
      const marketSet = new Set(existing.markets);
      for (const market of entry.markets) marketSet.add(market);
      existing.markets = Array.from(marketSet);
    }

    existing.request_count += 1;
    if (entry.status === "error") existing.status = "error";
    if (entry.status === "ok") {
      existing.total_records += entry.records_returned;
      existing.total_products += entry.products_returned ?? 0;
    }
  }

  const runs = Array.from(runMap.values())
    .sort((a, b) => Date.parse(b.finished_at) - Date.parse(a.finished_at))
    .slice(0, runLimit);

  return {
    generated_at: new Date().toISOString(),
    days,
    total_entries: limitedEntries.length,
    total_runs: runs.length,
    runs,
    entries: includeEntries ? limitedEntries : undefined,
  };
}
