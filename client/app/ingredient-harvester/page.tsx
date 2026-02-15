"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { ExternalLink, RefreshCw, Upload } from "lucide-react";

import type { CandidateRow, RowStatus, TaskProgress } from "@/lib/harvesterTypes";
import {
  downloadImportFile,
  getTaskProgress,
  listImportRows,
  startHarvestTask,
  updateRow,
  uploadCandidatesCsv,
} from "@/lib/harvesterApi";

type ModalState =
  | { open: false }
  | {
      open: true;
      row: CandidateRow;
      editedText: string;
      editedUrl: string;
    };

const STATUS_BADGE: Record<RowStatus, string> = {
  EMPTY: "bg-gray-100 text-gray-700",
  OK: "bg-emerald-100 text-emerald-800",
  PENDING: "bg-amber-100 text-amber-800",
  NEEDS_SOURCE: "bg-red-100 text-red-800",
  SKIPPED: "bg-slate-100 text-slate-700",
  ERROR: "bg-rose-100 text-rose-800",
};

function fmtCounts(p: TaskProgress | null) {
  if (!p) return "";
  const parts = Object.entries(p.counts || {})
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([k, v]) => `${k}:${v}`);
  return parts.join("  ");
}

export default function IngredientHarvesterPage() {
  const [importId, setImportId] = useState<string>("");
  const [rows, setRows] = useState<CandidateRow[]>([]);
  const [total, setTotal] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [downloadFormat, setDownloadFormat] = useState<"csv" | "xlsx">("csv");
  const [error, setError] = useState<string>("");
  const [filterStatus, setFilterStatus] = useState<RowStatus | "ALL">("ALL");
  const [queryInput, setQueryInput] = useState("");
  const [queryApplied, setQueryApplied] = useState("");
  const [pageSize, setPageSize] = useState<number>(200);
  const [page, setPage] = useState<number>(1);

  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const selectedIds = useMemo(() => Object.keys(selected).filter((k) => selected[k]), [selected]);
  const selectedOnPageCount = useMemo(() => rows.filter((r) => !!selected[r.row_id]).length, [rows, selected]);
  const allOnPageSelected = rows.length > 0 && selectedOnPageCount === rows.length;
  const selectAllRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (!selectAllRef.current) return;
    selectAllRef.current.indeterminate = selectedOnPageCount > 0 && selectedOnPageCount < rows.length;
  }, [selectedOnPageCount, rows.length]);

  const [taskId, setTaskId] = useState<string>("");
  const [taskProgress, setTaskProgress] = useState<TaskProgress | null>(null);

  const [modal, setModal] = useState<ModalState>({ open: false });

  const offset = Math.max(0, (page - 1) * pageSize);
  const pageCount = Math.max(1, Math.ceil((total || 0) / pageSize));
  const showingFrom = total > 0 ? offset + 1 : 0;
  const showingTo = offset + rows.length;

  const loadRows = async () => {
    if (!importId) return;
    setLoading(true);
    setError("");
    try {
      const res = await listImportRows({
        importId,
        status: filterStatus === "ALL" ? undefined : filterStatus,
        q: queryApplied || undefined,
        limit: pageSize,
        offset,
      });
      setRows(res.items || []);
      setTotal(res.total || 0);
    } catch (e: any) {
      setError(e?.message || "Failed to load rows.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    setPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [importId, filterStatus, queryApplied, pageSize]);

  useEffect(() => {
    if (page > pageCount) setPage(pageCount);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pageCount]);

  useEffect(() => {
    loadRows();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [importId, filterStatus, queryApplied, page, pageSize]);

  useEffect(() => {
    if (!taskId) return;
    let timer: number | null = null;
    let stopped = false;

    const poll = async () => {
      try {
        const p = await getTaskProgress(taskId);
        if (stopped) return;
        setTaskProgress(p);
        if (p.status === "RUNNING") {
          timer = window.setTimeout(poll, 1200);
        } else {
          await loadRows();
        }
      } catch (e: any) {
        if (stopped) return;
        setError(e?.message || "Failed to poll task.");
        timer = window.setTimeout(poll, 2500);
      }
    };

    void poll();
    return () => {
      stopped = true;
      if (timer) window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId]);

  const startTask = async (mode: "all" | "selected") => {
    if (!importId) return;
    setError("");
    try {
      const resp = await startHarvestTask({
        importId,
        rowIds: mode === "selected" ? selectedIds : undefined,
        force: false,
      });
      setTaskId(resp.task_id);
      setTaskProgress(null);
    } catch (e: any) {
      setError(e?.message || "Failed to start task.");
    }
  };

  const openReview = (row: CandidateRow) => {
    setModal({
      open: true,
      row,
      editedText: row.raw_ingredient_text || "",
      editedUrl: row.source_ref || "",
    });
  };

  const saveReview = async (nextStatus: RowStatus) => {
    if (!modal.open) return;
    setError("");
    try {
      await updateRow(modal.row.row_id, {
        status: nextStatus,
        raw_ingredient_text: modal.editedText,
        source_ref: modal.editedUrl,
        confidence: nextStatus === "OK" ? 1.0 : modal.row.confidence,
      });
      setModal({ open: false });
      await loadRows();
    } catch (e: any) {
      setError(e?.message || "Failed to save.");
    }
  };

  const handleDownload = async () => {
    if (!importId) return;
    setError("");
    setDownloading(true);
    try {
      await downloadImportFile({ importId, format: downloadFormat });
    } catch (e: any) {
      setError(e?.message || "Download failed.");
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-sm font-semibold text-gray-700 hover:text-gray-900">
              ← Catalog Extraction
            </Link>
            <div className="h-6 w-px bg-gray-200" />
            <h1 className="text-xl font-bold tracking-tight text-gray-900">Ingredient Source Harvester</h1>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-600">
            {taskId ? (
              <span className="rounded-full bg-slate-100 px-3 py-1 font-mono">
                task {taskId.slice(0, 8)}… {taskProgress?.status || "RUNNING"} {fmtCounts(taskProgress)}
              </span>
            ) : null}
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-6">
        {error ? (
          <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-900">{error}</div>
        ) : null}

        <section className="bg-white border rounded-xl p-4">
          <div className="flex items-start justify-between gap-4 flex-wrap">
            <div>
              <div className="text-sm font-semibold text-gray-900">1) Upload candidate CSV</div>
              <div className="mt-1 text-xs text-gray-600">
                Required columns: <span className="font-mono">brand</span> and <span className="font-mono">product_name</span> (or{" "}
                <span className="font-mono">Brand</span> + <span className="font-mono">Product Title</span>). Optional:{" "}
                <span className="font-mono">market</span>, <span className="font-mono">product_url</span>/<span className="font-mono">deep_link</span>.
              </div>
            </div>
            <label className="inline-flex items-center gap-2 rounded-lg bg-slate-900 text-white px-3 py-2 text-sm cursor-pointer hover:bg-slate-800">
              <Upload className="w-4 h-4" />
              Upload CSV
              <input
                type="file"
                accept=".csv,text/csv"
                className="hidden"
                onChange={async (e) => {
                  const file = e.target.files?.[0];
                  if (!file) return;
                  setError("");
                  setLoading(true);
                  try {
                    const resp = await uploadCandidatesCsv(file);
                    setImportId(resp.import_id);
                    setSelected({});
                    setTaskId("");
                    setTaskProgress(null);
                  } catch (err: any) {
                    setError(err?.message || "Upload failed.");
                  } finally {
                    setLoading(false);
                    e.currentTarget.value = "";
                  }
                }}
              />
            </label>
          </div>

          {importId ? (
            <div className="mt-3 flex items-center gap-2 text-xs text-gray-700 flex-wrap">
              <span>
                Active import: <span className="font-mono">{importId}</span>
              </span>
              <span className="text-gray-300">|</span>
              <span>Download:</span>
              <select
                value={downloadFormat}
                onChange={(e) => setDownloadFormat(e.target.value as "csv" | "xlsx")}
                className="text-xs border rounded px-2 py-1 bg-white"
              >
                <option value="csv">CSV</option>
                <option value="xlsx">XLSX</option>
              </select>
              <button
                type="button"
                onClick={() => void handleDownload()}
                disabled={!importId || downloading}
                className="rounded border px-2 py-1 text-xs hover:bg-gray-50 disabled:opacity-50"
              >
                {downloading ? "Downloading..." : "Download"}
              </button>
            </div>
          ) : null}
        </section>

        <section className="bg-white border rounded-xl p-4 space-y-3">
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="text-sm font-semibold text-gray-900">2) Run harvesting</div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => void startTask("selected")}
                disabled={!importId || selectedIds.length === 0}
                className="rounded-lg border px-3 py-2 text-sm hover:bg-gray-50 disabled:opacity-50"
              >
                Start (Selected)
              </button>
              <button
                type="button"
                onClick={() => void startTask("all")}
                disabled={!importId}
                className="rounded-lg bg-slate-900 text-white px-3 py-2 text-sm hover:bg-slate-800 disabled:opacity-50"
              >
                Start (All)
              </button>
              <button
                type="button"
                onClick={() => void loadRows()}
                disabled={!importId || loading}
                className="rounded-lg border px-3 py-2 text-sm hover:bg-gray-50 disabled:opacity-50 inline-flex items-center gap-2"
              >
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <div className="text-xs text-gray-600">Filter:</div>
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value as any)}
              className="text-sm border rounded-lg px-3 py-2"
              disabled={!importId}
            >
              <option value="ALL">All</option>
              <option value="OK">OK</option>
              <option value="PENDING">PENDING</option>
              <option value="NEEDS_SOURCE">NEEDS_SOURCE</option>
              <option value="EMPTY">EMPTY</option>
              <option value="SKIPPED">SKIPPED</option>
              <option value="ERROR">ERROR</option>
            </select>
            <input
              value={queryInput}
              onChange={(e) => setQueryInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") setQueryApplied(queryInput.trim());
              }}
              placeholder="Search brand/product…"
              className="text-sm border rounded-lg px-3 py-2 w-64"
              disabled={!importId}
            />
            <button
              type="button"
              onClick={() => setQueryApplied(queryInput.trim())}
              className="rounded-lg border px-3 py-2 text-sm hover:bg-gray-50 disabled:opacity-50"
              disabled={!importId}
            >
              Search
            </button>
            <div className="ml-auto text-xs text-gray-600">
              Selected: <span className="font-mono">{selectedIds.length}</span>
              {selectedIds.length ? (
                <button
                  type="button"
                  className="ml-2 text-xs text-blue-700 hover:text-blue-900"
                  onClick={() => setSelected({})}
                >
                  Clear
                </button>
              ) : null}
            </div>
          </div>

          <div className="overflow-x-auto border rounded-lg">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50 text-xs text-gray-600">
                <tr>
                  <th className="px-3 py-2 text-left">
                    <input
                      ref={selectAllRef}
                      type="checkbox"
                      checked={allOnPageSelected}
                      onChange={(e) => {
                        const on = e.target.checked;
                        setSelected((prev) => {
                          const next = { ...prev };
                          rows.forEach((r) => (next[r.row_id] = on));
                          return next;
                        });
                      }}
                      disabled={!rows.length}
                    />
                  </th>
                  <th className="px-3 py-2 text-left">Product</th>
                  <th className="px-3 py-2 text-left">Market</th>
                  <th className="px-3 py-2 text-left">Source</th>
                  <th className="px-3 py-2 text-left">Status</th>
                  <th className="px-3 py-2 text-left">Confidence</th>
                  <th className="px-3 py-2 text-left">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y bg-white">
                {rows.map((r) => (
                  <tr key={r.row_id} className="hover:bg-gray-50">
                    <td className="px-3 py-2">
                      <input
                        type="checkbox"
                        checked={!!selected[r.row_id]}
                        onChange={(e) => setSelected((prev) => ({ ...prev, [r.row_id]: e.target.checked }))}
                      />
                    </td>
                    <td className="px-3 py-2">
                      <div className="font-medium text-gray-900">
                        {r.brand} — {r.product_name}
                      </div>
                      <div className="text-xs text-gray-500 font-mono">#{r.row_index}</div>
                      {r.error ? <div className="text-xs text-rose-700 mt-1">{r.error}</div> : null}
                    </td>
                    <td className="px-3 py-2 font-mono text-xs">{r.market}</td>
                    <td className="px-3 py-2">
                      {r.source_ref ? (
                        <a
                          href={r.source_ref}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-blue-700 hover:text-blue-900"
                        >
                          {r.source_type || "Source"} <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      ) : (
                        <span className="text-xs text-gray-400">—</span>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${STATUS_BADGE[r.status]}`}>
                        {r.status}
                      </span>
                    </td>
                    <td className="px-3 py-2 font-mono text-xs">{r.confidence == null ? "—" : r.confidence.toFixed(2)}</td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <button
                          type="button"
                          className="text-xs rounded border px-2 py-1 hover:bg-gray-50"
                          onClick={() => {
                            void startHarvestTask({ importId, rowIds: [r.row_id], force: true }).then((t) => {
                              setTaskId(t.task_id);
                              setTaskProgress(null);
                            });
                          }}
                          disabled={!importId}
                        >
                          Re-run
                        </button>
                        {["PENDING", "NEEDS_SOURCE", "ERROR"].includes(r.status) ? (
                          <button
                            type="button"
                            className="text-xs rounded bg-amber-600 text-white px-2 py-1 hover:bg-amber-700"
                            onClick={() => openReview(r)}
                          >
                            Review
                          </button>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td className="px-3 py-6 text-sm text-gray-500" colSpan={7}>
                      {importId ? "No rows (try changing filters)." : "Upload a CSV to begin."}
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          {importId ? (
            <div className="flex items-center justify-between flex-wrap gap-2 text-xs text-gray-600">
              <div>
                Showing <span className="font-mono">{showingFrom}</span>–<span className="font-mono">{showingTo}</span> of{" "}
                <span className="font-mono">{total}</span>
              </div>
              <div className="flex items-center gap-2">
                <div>
                  Page <span className="font-mono">{page}</span>/<span className="font-mono">{pageCount}</span>
                </div>
                <button
                  type="button"
                  className="rounded border px-2 py-1 hover:bg-gray-50 disabled:opacity-50"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1}
                >
                  Prev
                </button>
                <button
                  type="button"
                  className="rounded border px-2 py-1 hover:bg-gray-50 disabled:opacity-50"
                  onClick={() => setPage((p) => Math.min(pageCount, p + 1))}
                  disabled={page >= pageCount}
                >
                  Next
                </button>
                <select
                  value={pageSize}
                  onChange={(e) => setPageSize(Number(e.target.value))}
                  className="text-xs border rounded px-2 py-1"
                >
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                  <option value={200}>200</option>
                  <option value={500}>500</option>
                </select>
                <span className="text-xs text-gray-500">/ page</span>
              </div>
            </div>
          ) : null}
        </section>

        {modal.open ? (
          <div className="fixed inset-0 z-[9999] bg-black/40 flex items-end md:items-center justify-center p-4">
            <div className="w-full max-w-5xl bg-white rounded-2xl shadow-xl border overflow-hidden">
              <div className="px-4 py-3 border-b flex items-center justify-between">
                <div>
                  <div className="text-sm font-semibold text-gray-900">Review ({modal.row.status})</div>
                  <div className="text-xs text-gray-500">
                    {modal.row.brand} — {modal.row.product_name} ({modal.row.market})
                  </div>
                </div>
                <button
                  className="text-sm px-3 py-2 rounded-lg border hover:bg-gray-50"
                  onClick={() => setModal({ open: false })}
                >
                  Close
                </button>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-12 gap-0">
                <div className="md:col-span-6 p-4 border-b md:border-b-0 md:border-r">
                  <div className="text-xs font-medium text-gray-700">Extracted raw_ingredient_text</div>
                  <textarea
                    value={modal.editedText}
                    onChange={(e) => setModal((s) => (s.open ? { ...s, editedText: e.target.value } : s))}
                    className="mt-2 w-full min-h-[220px] border rounded-lg px-3 py-2 text-sm font-mono"
                    placeholder="Paste or edit ingredient list…"
                  />
                  <div className="mt-3">
                    <div className="text-xs font-medium text-gray-700">source_ref</div>
                    <input
                      value={modal.editedUrl}
                      onChange={(e) => setModal((s) => (s.open ? { ...s, editedUrl: e.target.value } : s))}
                      className="mt-2 w-full border rounded-lg px-3 py-2 text-sm font-mono"
                      placeholder="https://..."
                    />
                  </div>
                </div>
                <div className="md:col-span-6 p-4">
                  <div className="flex items-center justify-between">
                    <div className="text-xs font-medium text-gray-700">Source preview</div>
                    {modal.editedUrl ? (
                      <a
                        href={modal.editedUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="text-xs text-blue-700 hover:text-blue-900 inline-flex items-center gap-1"
                      >
                        Open <ExternalLink className="w-3.5 h-3.5" />
                      </a>
                    ) : null}
                  </div>
                  <div className="mt-2 rounded-lg border overflow-hidden bg-gray-50">
                    {modal.editedUrl ? (
                      <iframe src={modal.editedUrl} className="w-full h-[340px]" />
                    ) : (
                      <div className="h-[340px] flex items-center justify-center text-sm text-gray-500">
                        No source URL
                      </div>
                    )}
                  </div>
                  <div className="mt-3 text-xs text-gray-500">
                    Tip: If the site blocks iframe, use “Open” and copy/paste ingredients.
                  </div>
                </div>
              </div>
              <div className="px-4 py-3 border-t flex gap-2 justify-end">
                <button
                  className="px-3 py-2 rounded-lg border hover:bg-gray-50 text-sm"
                  onClick={() => void saveReview("NEEDS_SOURCE")}
                >
                  Reject
                </button>
                <button
                  className="px-3 py-2 rounded-lg bg-slate-900 text-white hover:bg-slate-800 text-sm"
                  onClick={() => void saveReview("OK")}
                >
                  Confirm
                </button>
              </div>
            </div>
          </div>
        ) : null}
      </main>
    </div>
  );
}
