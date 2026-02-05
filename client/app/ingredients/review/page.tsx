"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Filter,
  RefreshCw,
  Search,
  X,
  XCircle,
} from "lucide-react";

import type { CandidateRow } from "@/lib/harvesterTypes";
import { listImportRows } from "@/lib/harvesterApi";
import type { ParsedIngredient, ParseStatus, ParserReparseResponse } from "@/lib/parserTypes";
import { reparse, reparseBatch } from "@/lib/parserApi";
import { updateProduct } from "@/lib/productsApi";

type StatusFilter = ParseStatus | "ALL";

type DrawerState =
  | { open: false }
  | {
      open: true;
      row: CandidateRow;
      editedText: string;
      parse: ParserReparseResponse | null;
      ingredients: ParsedIngredient[];
      draggingIndex: number | null;
    };

const STATUS_BADGE: Record<ParseStatus, string> = {
  OK: "bg-emerald-100 text-emerald-800",
  NEEDS_REVIEW: "bg-amber-100 text-amber-800",
  NEEDS_SOURCE: "bg-red-100 text-red-800",
};

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function truncate(s: string, max = 180) {
  const t = (s || "").trim();
  if (!t) return "";
  return t.length > max ? `${t.slice(0, max)}…` : t;
}

function moveItem<T>(arr: T[], from: number, to: number): T[] {
  const next = arr.slice();
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}

function inciListFromIngredients(ingredients: ParsedIngredient[]) {
  return ingredients
    .map((it) => (it.standard_name || "").trim())
    .filter(Boolean)
    .join("; ");
}

export default function IngredientReviewPage() {
  const [importId, setImportId] = useState<string>("");
  const [rows, setRows] = useState<CandidateRow[]>([]);
  const [total, setTotal] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(200);
  const [page, setPage] = useState<number>(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  const [filterStatus, setFilterStatus] = useState<StatusFilter>("NEEDS_REVIEW");
  const [queryInput, setQueryInput] = useState("");
  const [queryApplied, setQueryApplied] = useState("");

  const [parsedByRowId, setParsedByRowId] = useState<Record<string, ParserReparseResponse>>({});
  const [summary, setSummary] = useState<{ total: number; ok: number; needsReview: number; needsSource: number; parsed: number }>({
    total: 0,
    ok: 0,
    needsReview: 0,
    needsSource: 0,
    parsed: 0,
  });

  const [drawer, setDrawer] = useState<DrawerState>({ open: false });
  const closeDrawer = () => setDrawer({ open: false });

  const offset = Math.max(0, (page - 1) * pageSize);
  const pageCount = Math.max(1, Math.ceil((total || 0) / pageSize));
  const showingFrom = total > 0 ? offset + 1 : 0;
  const showingTo = offset + rows.length;

  const loadRows = async () => {
    if (!importId) return;
    setLoading(true);
    setError("");
    try {
      const res = await listImportRows({ importId, q: queryApplied || undefined, limit: pageSize, offset });
      setRows(res.items || []);
      setTotal(res.total || 0);
    } catch (e: any) {
      setError(e?.message || "Failed to load rows.");
    } finally {
      setLoading(false);
    }
  };

  // Reset page on key inputs.
  useEffect(() => setPage(1), [importId, queryApplied, pageSize]);
  useEffect(() => {
    if (page > pageCount) setPage(pageCount);
  }, [page, pageCount]);

  useEffect(() => {
    void loadRows();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [importId, queryApplied, page, pageSize]);

  // Parse missing rows on the current page (for grid rendering).
  useEffect(() => {
    if (!rows.length) return;
    let cancelled = false;

    const run = async () => {
      const missing = rows.filter((r) => !parsedByRowId[r.row_id]);
      if (missing.length === 0) return;
      try {
        const resp = await reparseBatch({
          items: missing.map((r) => ({ row_id: r.row_id, raw_ingredient_text: r.raw_ingredient_text })),
        });
        if (cancelled) return;
        setParsedByRowId((prev) => {
          const next = { ...prev };
          for (const it of resp.items || []) next[it.row_id] = it.result;
          return next;
        });
      } catch (e: any) {
        if (cancelled) return;
        setError(e?.message || "Failed to parse rows.");
      }
    };

    void run();
    return () => {
      cancelled = true;
    };
  }, [rows, parsedByRowId]);

  // Background summary: scan the whole import in larger pages and keep counts updated.
  useEffect(() => {
    if (!importId) return;
    let cancelled = false;

    const run = async () => {
      let nextOffset = 0;
      let totalRows = 0;
      let ok = 0;
      let needsReview = 0;
      let needsSource = 0;
      let parsed = 0;

      setSummary({ total: 0, ok: 0, needsReview: 0, needsSource: 0, parsed: 0 });

      while (!cancelled) {
        const pageRes = await listImportRows({ importId, limit: 500, offset: nextOffset });
        if (!totalRows) totalRows = pageRes.total || 0;
        const items = pageRes.items || [];
        if (items.length === 0) break;

        const batch = await reparseBatch({
          items: items.map((r) => ({ row_id: r.row_id, raw_ingredient_text: r.raw_ingredient_text })),
        });

        if (cancelled) return;
        setParsedByRowId((prev) => {
          const next = { ...prev };
          for (const it of batch.items || []) next[it.row_id] = it.result;
          return next;
        });

        for (const it of batch.items || []) {
          parsed += 1;
          if (it.result.parse_status === "OK") ok += 1;
          else if (it.result.parse_status === "NEEDS_SOURCE") needsSource += 1;
          else needsReview += 1;
        }

        setSummary({ total: totalRows, ok, needsReview, needsSource, parsed });

        nextOffset += items.length;
        if (nextOffset >= totalRows) break;
      }
    };

    void run().catch((e: any) => {
      if (!cancelled) setError(e?.message || "Failed to build summary.");
    });

    return () => {
      cancelled = true;
    };
  }, [importId]);

  const filteredRows = useMemo(() => {
    if (filterStatus === "ALL") return rows;
    return rows.filter((r) => {
      const p = parsedByRowId[r.row_id];
      if (!p) return filterStatus === "NEEDS_REVIEW";
      return p.parse_status === filterStatus;
    });
  }, [rows, parsedByRowId, filterStatus]);

  const openDrawer = (row: CandidateRow) => {
    const parsed = parsedByRowId[row.row_id] || null;
    setDrawer({
      open: true,
      row,
      editedText: row.raw_ingredient_text || "",
      parse: parsed,
      ingredients: (parsed?.inci_list_json || []).slice().sort((a, b) => (a.order || 0) - (b.order || 0)),
      draggingIndex: null,
    });
  };

  const onReparse = async () => {
    if (!drawer.open) return;
    setError("");
    try {
      const next = await reparse(drawer.editedText);
      setParsedByRowId((prev) => ({ ...prev, [drawer.row.row_id]: next }));
      setDrawer((d) =>
        d.open
          ? {
              ...d,
              parse: next,
              ingredients: (next.inci_list_json || []).slice().sort((a, b) => (a.order || 0) - (b.order || 0)),
            }
          : d,
      );
    } catch (e: any) {
      setError(e?.message || "Re-parse failed.");
    }
  };

  const onDeleteIngredient = (idx: number) => {
    if (!drawer.open) return;
    setDrawer((d) => {
      if (!d.open) return d;
      const next = d.ingredients.slice();
      next.splice(idx, 1);
      const normalized = next.map((it, i) => ({ ...it, order: i + 1 }));
      const patchedParse = d.parse
        ? {
            ...d.parse,
            parse_status: "OK" as const,
            inci_list: inciListFromIngredients(normalized),
            inci_list_json: normalized,
          }
        : null;
      setParsedByRowId((prev) => (d.parse ? { ...prev, [d.row.row_id]: patchedParse as any } : prev));
      return { ...d, ingredients: normalized, parse: patchedParse };
    });
  };

  const onDragStart = (idx: number) => {
    setDrawer((d) => (d.open ? { ...d, draggingIndex: idx } : d));
  };

  const onDrop = (idx: number) => {
    setDrawer((d) => {
      if (!d.open) return d;
      const from = d.draggingIndex;
      if (from == null || from === idx) return { ...d, draggingIndex: null };
      const next = moveItem(d.ingredients, from, idx).map((it, i) => ({ ...it, order: i + 1 }));
      const patchedParse = d.parse
        ? {
            ...d.parse,
            parse_status: "OK" as const,
            inci_list: inciListFromIngredients(next),
            inci_list_json: next,
          }
        : null;
      setParsedByRowId((prev) => (d.parse ? { ...prev, [d.row.row_id]: patchedParse as any } : prev));
      return { ...d, ingredients: next, parse: patchedParse, draggingIndex: null };
    });
  };

  const saveAndMark = async (status: "OK" | "NEEDS_SOURCE") => {
    if (!drawer.open) return;
    setError("");
    try {
      await updateProduct(drawer.row.row_id, { status, raw_ingredient_text: drawer.editedText });
      closeDrawer();
      await loadRows();
    } catch (e: any) {
      setError(e?.message || "Save failed.");
    }
  };

  const progressText =
    summary.total > 0 ? `Parsed ${summary.parsed}/${summary.total}` : importId ? "Parsing…" : "";

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-sm font-semibold text-gray-700 hover:text-gray-900">
              ← Catalog Extraction
            </Link>
            <div className="h-6 w-px bg-gray-200" />
            <h1 className="text-xl font-bold tracking-tight text-gray-900">Ingredient Review Dashboard</h1>
          </div>
          <div className="text-xs text-gray-600 font-mono">{progressText}</div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-6">
        {error ? (
          <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-900">{error}</div>
        ) : null}

        <section className="bg-white border rounded-xl p-4">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="text-sm font-semibold text-gray-900">Import ID</div>
            <input
              value={importId}
              onChange={(e) => setImportId(e.target.value.trim())}
              placeholder="e.g. 5bc50da3-53f3-46fd-b695-fbc1512efd19"
              className="flex-1 min-w-[280px] rounded-lg border border-gray-200 px-3 py-2 text-sm font-mono"
            />
            <Link
              href="/ingredient-harvester"
              className="text-xs px-3 py-2 rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-700"
            >
              Harvester →
            </Link>
          </div>
        </section>

        <section className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="bg-white border rounded-xl p-4">
            <div className="text-xs text-gray-500">Total Scanned</div>
            <div className="mt-1 text-2xl font-bold text-gray-900">{summary.total || total || 0}</div>
          </div>
          <div className="bg-white border rounded-xl p-4">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <CheckCircle2 className="w-4 h-4 text-emerald-600" /> Verified (OK)
            </div>
            <div className="mt-1 text-2xl font-bold text-emerald-700">{summary.ok}</div>
          </div>
          <div className="bg-white border rounded-xl p-4">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <AlertTriangle className="w-4 h-4 text-amber-600" /> Needs Review
            </div>
            <div className="mt-1 text-2xl font-bold text-amber-700">{summary.needsReview}</div>
          </div>
          <div className="bg-white border rounded-xl p-4">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <XCircle className="w-4 h-4 text-red-600" /> Missing Source
            </div>
            <div className="mt-1 text-2xl font-bold text-red-700">{summary.needsSource}</div>
          </div>
        </section>

        <section className="bg-white border rounded-xl p-4 space-y-4">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div className="flex items-center gap-2 flex-wrap">
              <div className="relative">
                <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
                <input
                  value={queryInput}
                  onChange={(e) => setQueryInput(e.target.value)}
                  placeholder="Search brand / product…"
                  className="w-[260px] rounded-lg border border-gray-200 pl-9 pr-3 py-2 text-sm"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") setQueryApplied(queryInput.trim());
                  }}
                />
              </div>
              <button
                className="inline-flex items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm hover:bg-gray-50"
                onClick={() => setQueryApplied(queryInput.trim())}
              >
                <Search className="w-4 h-4" /> Apply
              </button>

              <div className="inline-flex items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm">
                <Filter className="w-4 h-4 text-gray-600" />
                <select
                  value={filterStatus}
                  onChange={(e) => setFilterStatus(e.target.value as StatusFilter)}
                  className="bg-transparent outline-none"
                >
                  <option value="NEEDS_REVIEW">NEEDS_REVIEW</option>
                  <option value="OK">OK</option>
                  <option value="NEEDS_SOURCE">NEEDS_SOURCE</option>
                  <option value="ALL">ALL</option>
                </select>
              </div>
            </div>

            <div className="flex items-center gap-2 text-xs text-gray-600">
              <span className="font-mono">
                showing {showingFrom}-{showingTo} / {total}
              </span>
              <button
                className="inline-flex items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm hover:bg-gray-50"
                onClick={() => loadRows()}
                disabled={loading || !importId}
              >
                <RefreshCw className={["w-4 h-4", loading ? "animate-spin" : ""].join(" ")} /> Refresh
              </button>
            </div>
          </div>

          <div className="overflow-auto border rounded-lg">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50 text-xs text-gray-600">
                <tr>
                  <th className="text-left px-3 py-2 w-[260px]">Product</th>
                  <th className="text-left px-3 py-2 w-[520px]">Raw Text</th>
                  <th className="text-left px-3 py-2">Parsed INCI</th>
                  <th className="text-left px-3 py-2 w-[140px]">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {filteredRows.map((r) => {
                  const p = parsedByRowId[r.row_id];
                  const status = p?.parse_status;
                  return (
                    <tr
                      key={r.row_id}
                      className="hover:bg-gray-50 cursor-pointer"
                      onClick={() => openDrawer(r)}
                      title="Click to review"
                    >
                      <td className="px-3 py-3 align-top">
                        <div className="font-semibold text-gray-900">{r.brand}</div>
                        <div className="text-gray-700">{r.product_name}</div>
                        <div className="mt-1 text-xs text-gray-500 font-mono">{r.market}</div>
                      </td>
                      <td className="px-3 py-3 align-top">
                        <div className="text-gray-800">{truncate(r.raw_ingredient_text || "", 220) || <span className="text-gray-400">—</span>}</div>
                        {p?.cleaned_text ? (
                          <div className="mt-1 text-xs text-gray-500">
                            <span className="font-mono text-[10px] uppercase tracking-wide mr-2">cleaned</span>
                            {truncate(p.cleaned_text, 220)}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-3 py-3 align-top">
                        <div className="flex flex-wrap gap-1">
                          {(p?.inci_list_json || []).slice(0, 10).map((it) => (
                            <span
                              key={`${r.row_id}:${it.order}:${it.standard_name}`}
                              className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-800"
                            >
                              {it.standard_name}
                            </span>
                          ))}
                          {(p?.inci_list_json || []).length > 10 ? (
                            <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-800">
                              +{(p?.inci_list_json || []).length - 10}
                            </span>
                          ) : null}
                          {!p ? <span className="text-xs text-gray-400">Parsing…</span> : null}
                        </div>
                      </td>
                      <td className="px-3 py-3 align-top">
                        {status ? (
                          <span className={["inline-flex rounded-full px-2.5 py-1 text-xs font-semibold", STATUS_BADGE[status]].join(" ")}>
                            {status}
                          </span>
                        ) : (
                          <span className="text-xs text-gray-400">…</span>
                        )}
                        {typeof p?.parse_confidence === "number" ? (
                          <div className="mt-1 text-xs text-gray-500 font-mono">{p.parse_confidence.toFixed(2)}</div>
                        ) : null}
                      </td>
                    </tr>
                  );
                })}
                {filteredRows.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="px-3 py-10 text-center text-sm text-gray-500">
                      No rows in this view.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="flex items-center justify-between text-xs text-gray-600">
            <div className="flex items-center gap-2">
              <span>Page size</span>
              <select
                className="rounded border border-gray-200 px-2 py-1"
                value={pageSize}
                onChange={(e) => setPageSize(Number(e.target.value))}
              >
                <option value={50}>50</option>
                <option value={100}>100</option>
                <option value={200}>200</option>
                <option value={500}>500</option>
              </select>
            </div>
            <div className="flex items-center gap-2">
              <button
                className="inline-flex items-center gap-1 rounded border border-gray-200 px-2 py-1 hover:bg-gray-50 disabled:opacity-50"
                onClick={() => setPage((p) => clamp(p - 1, 1, pageCount))}
                disabled={page <= 1}
              >
                <ChevronLeft className="w-4 h-4" /> Prev
              </button>
              <span className="font-mono">
                {page}/{pageCount}
              </span>
              <button
                className="inline-flex items-center gap-1 rounded border border-gray-200 px-2 py-1 hover:bg-gray-50 disabled:opacity-50"
                onClick={() => setPage((p) => clamp(p + 1, 1, pageCount))}
                disabled={page >= pageCount}
              >
                Next <ChevronRight className="w-4 h-4" />
              </button>
            </div>
          </div>
        </section>
      </main>

      {/* Drawer */}
      {drawer.open ? (
        <div className="fixed inset-0 z-[200]">
          <div className="absolute inset-0 bg-black/30" onClick={closeDrawer} />
          <div className="absolute right-0 top-0 h-full w-full max-w-2xl bg-white shadow-xl flex flex-col">
            <div className="border-b px-5 py-4 flex items-start justify-between gap-4">
              <div>
                <div className="text-sm font-semibold text-gray-900">
                  {drawer.row.brand} — {drawer.row.product_name}
                </div>
                <div className="mt-1 text-xs text-gray-500 font-mono">{drawer.row.row_id}</div>
              </div>
              <button className="p-2 rounded-lg hover:bg-gray-50" onClick={closeDrawer} aria-label="Close">
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="flex-1 overflow-auto px-5 py-4 space-y-6">
              <section className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-semibold text-gray-900">Raw Ingredient Text</div>
                  <button
                    className="inline-flex items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm hover:bg-gray-50"
                    onClick={onReparse}
                  >
                    <RefreshCw className="w-4 h-4" /> Re-parse
                  </button>
                </div>
                <textarea
                  className="w-full min-h-[140px] rounded-lg border border-gray-200 px-3 py-2 text-sm font-mono"
                  value={drawer.editedText}
                  onChange={(e) => setDrawer((d) => (d.open ? { ...d, editedText: e.target.value } : d))}
                />
                {drawer.parse?.cleaned_text ? (
                  <div className="text-xs text-gray-500">
                    <span className="font-mono text-[10px] uppercase tracking-wide mr-2">cleaned</span>
                    {drawer.parse.cleaned_text}
                  </div>
                ) : null}
              </section>

              <section className="space-y-2">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-semibold text-gray-900">Parsed INCI</div>
                  <div className="text-xs text-gray-600 font-mono">
                    {drawer.parse ? `${drawer.parse.parse_status} (${drawer.parse.parse_confidence.toFixed(2)})` : "—"}
                  </div>
                </div>
                <div className="rounded-lg border border-gray-200 p-3 min-h-[120px]">
                  <div className="flex flex-wrap gap-2">
                    {drawer.ingredients.map((it, idx) => (
                      <span
                        key={`${drawer.row.row_id}:tag:${idx}:${it.standard_name}`}
                        draggable
                        onDragStart={() => onDragStart(idx)}
                        onDragOver={(e) => e.preventDefault()}
                        onDrop={() => onDrop(idx)}
                        className="inline-flex items-center gap-2 rounded-full bg-slate-100 px-3 py-1 text-xs text-slate-800 cursor-move"
                        title="Drag to reorder"
                      >
                        <span className="font-mono text-[10px] opacity-60">{idx + 1}</span>
                        <span>{it.standard_name}</span>
                        <button
                          className="rounded-full hover:bg-slate-200 p-0.5"
                          onClick={(e) => {
                            e.stopPropagation();
                            onDeleteIngredient(idx);
                          }}
                          aria-label="Remove"
                        >
                          <X className="w-3.5 h-3.5" />
                        </button>
                      </span>
                    ))}
                    {drawer.ingredients.length === 0 ? (
                      <div className="text-sm text-gray-500">No parsed ingredients.</div>
                    ) : null}
                  </div>
                </div>
                {drawer.parse?.normalization_notes?.length ? (
                  <div className="text-xs text-gray-500">
                    <div className="font-semibold text-gray-700 mb-1">Notes</div>
                    <ul className="list-disc pl-5 space-y-0.5">
                      {drawer.parse.normalization_notes.slice(0, 8).map((n, i) => (
                        <li key={`n:${i}`}>{n}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </section>
            </div>

            <div className="border-t px-5 py-4 flex items-center justify-end gap-2">
              <button
                className="inline-flex items-center justify-center rounded-lg border border-gray-200 px-4 py-2 text-sm hover:bg-gray-50"
                onClick={() => saveAndMark("NEEDS_SOURCE")}
              >
                Mark as NEEDS_SOURCE
              </button>
              <button
                className="inline-flex items-center justify-center rounded-lg bg-slate-900 text-white px-4 py-2 text-sm hover:bg-slate-800"
                onClick={() => saveAndMark("OK")}
              >
                Mark as OK
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
