"use client";

import { useCallback, useMemo, useRef, useState } from "react";
import { Copy, ExternalLink, Sparkles, X } from "lucide-react";

import { DashboardStats } from "./DashboardStats";
import { ExtractionForm } from "./ExtractionForm";
import { ResultsTable } from "./ResultsTable";
import { AIModal } from "./AIModal";

import type { ExtractResponse, ExtractedVariantRow, LogLine } from "@/lib/types";
import { extractCatalog } from "@/lib/api";
import { buildCsv, downloadTextFile } from "@/lib/csv";
import { copyTextToClipboard } from "@/lib/clipboard";

type ToastState = { message: string; visible: boolean };

const EMPTY_VARIANTS: ExtractedVariantRow[] = [];
const EMPTY_LOGS: LogLine[] = [];

export function CatalogIntelligenceApp() {
  const [brand, setBrand] = useState("Tom Ford Beauty");
  const [domain, setDomain] = useState("www.tomfordbeauty.com");
  const [activeStep, setActiveStep] = useState<number>(-1);
  const [isRunning, setIsRunning] = useState(false);

  const [data, setData] = useState<ExtractResponse | null>(null);
  const variants = data?.variants ?? EMPTY_VARIANTS;
  const logs = data?.logs ?? EMPTY_LOGS;

  const [toast, setToast] = useState<ToastState>({ message: "Notification", visible: false });
  const toastTimerRef = useRef<number | null>(null);

  const [modalOpen, setModalOpen] = useState(false);
  const [modalVariant, setModalVariant] = useState<ExtractedVariantRow | null>(null);

  const recordCountText = useMemo(() => `${variants.length} records found`, [variants.length]);

  const showToast = useCallback((message: string) => {
    setToast({ message, visible: true });
    if (toastTimerRef.current) window.clearTimeout(toastTimerRef.current);
    toastTimerRef.current = window.setTimeout(() => setToast((t) => ({ ...t, visible: false })), 3000);
  }, []);

  const openAIModal = useCallback((variant: ExtractedVariantRow) => {
    setModalVariant(variant);
    setModalOpen(true);
  }, []);

  const closeAIModal = useCallback(() => {
    setModalOpen(false);
  }, []);

  const handleCopyLink = useCallback(
    async (url: string) => {
      const ok = await copyTextToClipboard(url);
      showToast(ok ? "Link copied to clipboard!" : "Failed to copy link.");
    },
    [showToast],
  );

  const handleCopyModalText = useCallback(async () => {
    if (!modalVariant) return;
    const ok = await copyTextToClipboard(modalVariant.ad_copy);
    showToast(ok ? "Copied!" : "Copy failed.");
  }, [modalVariant, showToast]);

  const handleCopyCsv = useCallback(async () => {
    const csv = buildCsv(variants);
    const ok = await copyTextToClipboard(csv);
    showToast(ok ? "CSV copied!" : "Copy failed.");
  }, [variants, showToast]);

  const handleDownloadCsv = useCallback(() => {
    const csv = buildCsv(variants);
    downloadTextFile(csv, "tom_ford_beauty_catalog_export.csv", "text/csv;charset=utf-8;");
    showToast("Download started.");
  }, [variants, showToast]);

  const appendLocalLogs = useCallback((lines: LogLine[]) => {
    setData((prev) => {
      const base: ExtractResponse =
        prev ??
        ({
          brand,
          domain,
          generated_at: new Date().toISOString(),
          mode: "simulation",
          products: [],
          variants: [],
          pricing: { currency: "USD", min: 0, max: 0, avg: 0 },
          ad_copy: { by_variant_id: {} },
          logs: [],
        } satisfies ExtractResponse);

      return { ...base, logs: [...base.logs, ...lines] };
    });
  }, [brand, domain]);

  const handleRunExtraction = useCallback(async () => {
    if (isRunning) return;

    setIsRunning(true);
    setActiveStep(0);
    setData(null);

    // Lightweight UI pacing to match the prototype feel.
    const t0 = Date.now();
    appendLocalLogs([
      { at: new Date().toISOString(), type: "info", msg: `Initializing Pivota Extraction for: ${brand}` },
      { at: new Date().toISOString(), type: "info", msg: `Checking connectivity to ${domain}...` },
      { at: new Date().toISOString(), type: "info", msg: "POST /api/extract" },
    ]);

    const stepTimer1 = window.setTimeout(() => setActiveStep(1), 700);
    const stepTimer2 = window.setTimeout(() => setActiveStep(2), 1400);

    try {
      const result = await extractCatalog({ brand, domain });
      setData(result);
      setActiveStep(-1);
      showToast(`Extraction complete (${result.variants.length} rows).`);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(err);
      appendLocalLogs([{ at: new Date().toISOString(), type: "error", msg: "Extraction failed." }]);
      showToast("Extraction failed.");
      setActiveStep(-1);
    } finally {
      window.clearTimeout(stepTimer1);
      window.clearTimeout(stepTimer2);
      setIsRunning(false);

      // Keep a minimum “run” time so the UI doesn’t flicker on fast localhost responses.
      const elapsed = Date.now() - t0;
      if (elapsed < 350) await new Promise((r) => setTimeout(r, 350 - elapsed));
    }
  }, [appendLocalLogs, brand, domain, isRunning, showToast]);

  return (
    <>
      {/* Toast */}
      <div
        id="toast"
        className={[
          "fixed left-1/2 bottom-[30px] z-[1000] -translate-x-1/2 rounded px-4 py-3 text-sm transition-all",
          toast.visible ? "opacity-100 bottom-[50px] visible" : "opacity-0 invisible",
          "bg-[#333] text-white min-w-[250px] text-center",
        ].join(" ")}
      >
        {toast.message}
      </div>

      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center text-white font-bold text-xl shadow-sm">
              P
            </div>
            <h1 className="text-xl font-bold tracking-tight text-gray-900">
              Pivota <span className="text-gray-400 font-normal">| Catalog Intelligence</span>
            </h1>
          </div>
          <div className="flex items-center gap-4 text-sm">
            <span className="px-3 py-1 bg-gradient-to-r from-indigo-500 to-purple-500 text-white rounded-full font-medium text-xs shadow-sm flex items-center gap-1">
              <Sparkles className="w-3.5 h-3.5" />
              Auto-Gen Enabled
            </span>
            <span className="px-3 py-1 bg-blue-50 text-blue-700 rounded-full font-medium">Prototype v2.1 (Full)</span>
          </div>
        </div>
      </header>

      <main className="flex-grow max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 w-full">
        {/* Intro */}
        <div className="mb-8">
          <h2 className="text-2xl font-bold text-gray-900">Affiliate Catalog Extraction Prototype</h2>
          <p className="mt-2 text-gray-600 max-w-3xl">
            This tool simulates extracting the full product universe for Tom Ford Beauty. It handles variant discovery,
            safe deep-link generation, and AI marketing copy creation.
          </p>
        </div>

        <DashboardStats />

        {/* Main interface */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
          <div className="lg:col-span-4 space-y-6">
            <ExtractionForm
              brand={brand}
              domain={domain}
              isRunning={isRunning}
              activeStep={activeStep}
              onBrandChange={setBrand}
              onDomainChange={setDomain}
              onSubmit={handleRunExtraction}
              onAskGemini={() => {
                showToast("Gemini helper not configured in this app.");
              }}
            />
          </div>

          <div className="lg:col-span-8 flex flex-col gap-6">
            <ResultsTable
              logs={logs}
              variants={variants}
              recordCountText={recordCountText}
              onCopyCsv={handleCopyCsv}
              onDownloadCsv={handleDownloadCsv}
              onOpenAdCopy={openAIModal}
              onCopyLink={handleCopyLink}
              isActionsEnabled={variants.length > 0}
            />
          </div>
        </div>
      </main>

      <AIModal
        open={modalOpen}
        variant={modalVariant}
        onClose={closeAIModal}
        onCopyText={handleCopyModalText}
        onRegenerate={() => showToast("Regeneration not configured (Gemini key not set).")}
        CloseIcon={X}
        CopyIcon={Copy}
        ExternalLinkIcon={ExternalLink}
      />
    </>
  );
}
