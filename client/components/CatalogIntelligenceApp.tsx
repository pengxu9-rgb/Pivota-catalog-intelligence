"use client";

import { useCallback, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { Sparkles } from "lucide-react";

import { DashboardStats } from "./DashboardStats";
import { ExtractionForm } from "./ExtractionForm";
import { ResultsTable } from "./ResultsTable";

import type { ExtractResponse, ExtractV2Response, ExtractedVariantRow, LogLine, OfferV2 } from "@/lib/types";
import { extractCatalog, extractCatalogV2 } from "@/lib/api";
import { buildCsv, buildProductCsv, downloadTextFile } from "@/lib/csv";
import { copyTextToClipboard } from "@/lib/clipboard";

type ToastState = { message: string; visible: boolean };

const EMPTY_VARIANTS: ExtractedVariantRow[] = [];
const EMPTY_LOGS: LogLine[] = [];
const DEFAULT_BATCH_LIMIT = 10;
const DEFAULT_MAX_BATCH_ROUNDS = 60;
const DEFAULT_V2_MARKETS = ["US"];

function computeMergedPricing(variants: ExtractedVariantRow[]) {
  const nums = variants
    .map((v) => Number.parseFloat(v.price))
    .filter((n) => Number.isFinite(n));
  const currency = variants.find((v) => Boolean(v.currency))?.currency || "";
  if (nums.length === 0) return { currency, min: 0, max: 0, avg: 0 };

  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const avg = nums.reduce((a, b) => a + b, 0) / nums.length;
  return { currency, min, max, avg: Number(avg.toFixed(2)) };
}

function toLegacyStock(availability?: string) {
  const lower = (availability || "").toLowerCase();
  if (lower.includes("out")) return "Out of Stock" as const;
  if (lower.includes("low")) return "Low Stock" as const;
  return "In Stock" as const;
}

function toLegacyVariant(offer: OfferV2, brand: string, index: number): ExtractedVariantRow {
  const bestPriceText =
    offer.price_amount !== null
      ? String(offer.price_amount)
      : (offer.price_display_raw || "").replace(/[^\d.,-]/g, "").trim();
  const expectedCurrency = offer.market_context_debug.expected_currency || "";
  const currency = offer.price_currency || expectedCurrency || "";

  return {
    id: `${offer.source_product_id}-${offer.market_id}-${index}`,
    sku: offer.source_product_id,
    url: offer.url_canonical,
    option_name: "Market",
    option_value: offer.market_id,
    price: bestPriceText || "0",
    currency,
    stock: toLegacyStock(offer.availability),
    description: `price_type=${offer.price_type}; switch=${offer.market_switch_status}; confidence=${offer.currency_confidence}`,
    image_url: "",
    ad_copy: "",
    brand,
    product_title: offer.source_product_id,
    product_url: offer.url_canonical,
    deep_link: offer.url_canonical,
    simulated: false,
  };
}

function convertV2ResponseToLegacy(v2: ExtractV2Response, brand: string): ExtractResponse {
  const variants = v2.offers_v2.map((offer, idx) => toLegacyVariant(offer, brand, idx));
  const productsMap = new Map<string, { title: string; url: string; variants: ExtractResponse["products"][number]["variants"] }>();

  for (const row of variants) {
    const key = `${row.product_url}|${row.product_title}`;
    const existing = productsMap.get(key) || {
      title: row.product_title,
      url: row.product_url,
      variants: [],
    };
    existing.variants.push({
      id: row.id,
      sku: row.sku,
      url: row.url,
      option_name: row.option_name,
      option_value: row.option_value,
      price: row.price,
      currency: row.currency,
      stock: row.stock,
      description: row.description,
      image_url: row.image_url,
      ad_copy: row.ad_copy,
    });
    productsMap.set(key, existing);
  }

  return {
    brand: v2.brand,
    domain: v2.domain,
    generated_at: v2.generated_at,
    mode: v2.mode,
    products: Array.from(productsMap.values()),
    variants,
    pricing: computeMergedPricing(variants),
    ad_copy: { by_variant_id: {} },
    logs: v2.logs,
  };
}

function mergeExtractResponses(base: ExtractResponse | null, next: ExtractResponse): ExtractResponse {
  if (!base) return next;

  const products = [...base.products];
  const seenProducts = new Set(products.map((p) => `${p.url}|${p.title}`));
  for (const product of next.products) {
    const key = `${product.url}|${product.title}`;
    if (seenProducts.has(key)) continue;
    seenProducts.add(key);
    products.push(product);
  }

  const variants = [...base.variants];
  const seenVariants = new Set(variants.map((v) => `${v.id}|${v.sku}`));
  for (const variant of next.variants) {
    const key = `${variant.id}|${variant.sku}`;
    if (seenVariants.has(key)) continue;
    seenVariants.add(key);
    variants.push(variant);
  }

  return {
    ...next,
    products,
    variants,
    pricing: computeMergedPricing(variants),
    ad_copy: { by_variant_id: { ...base.ad_copy.by_variant_id, ...next.ad_copy.by_variant_id } },
    logs: [...base.logs, ...next.logs],
  };
}

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

  const recordCountText = useMemo(() => `${variants.length} records found`, [variants.length]);

  const showToast = useCallback((message: string) => {
    setToast({ message, visible: true });
    if (toastTimerRef.current) window.clearTimeout(toastTimerRef.current);
    toastTimerRef.current = window.setTimeout(() => setToast((t) => ({ ...t, visible: false })), 3000);
  }, []);

  const handleCopyLink = useCallback(
    async (url: string) => {
      const ok = await copyTextToClipboard(url);
      showToast(ok ? "Link copied to clipboard!" : "Failed to copy link.");
    },
    [showToast],
  );

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

  const handleCopyProductCsv = useCallback(async () => {
    const csv = buildProductCsv(variants);
    const ok = await copyTextToClipboard(csv);
    showToast(ok ? "Product CSV copied!" : "Copy failed.");
  }, [variants, showToast]);

  const handleDownloadProductCsv = useCallback(() => {
    const csv = buildProductCsv(variants);
    downloadTextFile(csv, "tom_ford_beauty_product_export.csv", "text/csv;charset=utf-8;");
    showToast("Product download started.");
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
          pricing: { currency: "", min: 0, max: 0, avg: 0 },
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
    const v2Enabled = (process.env.NEXT_PUBLIC_EXTRACT_V2_ENABLED || "0") === "1";
    const configuredMarkets = (process.env.NEXT_PUBLIC_EXTRACT_V2_MARKETS || "")
      .split(",")
      .map((v) => v.trim())
      .filter(Boolean);
    const v2Markets = configuredMarkets.length > 0 ? configuredMarkets : DEFAULT_V2_MARKETS;
    const batchLimitRaw = Number(process.env.NEXT_PUBLIC_EXTRACT_BATCH_LIMIT || DEFAULT_BATCH_LIMIT);
    const maxRoundsRaw = Number(process.env.NEXT_PUBLIC_EXTRACT_MAX_BATCH_ROUNDS || DEFAULT_MAX_BATCH_ROUNDS);
    const batchLimit = Number.isFinite(batchLimitRaw) ? Math.max(1, Math.floor(batchLimitRaw)) : DEFAULT_BATCH_LIMIT;
    const maxRounds = Number.isFinite(maxRoundsRaw) ? Math.max(1, Math.floor(maxRoundsRaw)) : DEFAULT_MAX_BATCH_ROUNDS;

    appendLocalLogs([
      { at: new Date().toISOString(), type: "info", msg: `Initializing Pivota Extraction for: ${brand}` },
      { at: new Date().toISOString(), type: "info", msg: `Checking connectivity to ${domain}...` },
      {
        at: new Date().toISOString(),
        type: "info",
        msg: v2Enabled
          ? `V2 mode enabled: endpoint=/api/extract/v2, markets=${v2Markets.join(",")}`
          : `Batch mode enabled: limit=${batchLimit}`,
      },
    ]);

    const stepTimer1 = window.setTimeout(() => setActiveStep(1), 700);
    const stepTimer2 = window.setTimeout(() => setActiveStep(2), 1400);

    let merged: ExtractResponse | null = null;
    try {
      if (v2Enabled) {
        appendLocalLogs([
          {
            at: new Date().toISOString(),
            type: "info",
            msg: `POST /api/extract/v2 (markets=${v2Markets.join(",")}, limit=${batchLimit})`,
          },
        ]);

        const v2Result = await extractCatalogV2({ brand, domain, offset: 0, limit: batchLimit, markets: v2Markets });
        merged = convertV2ResponseToLegacy(v2Result, brand);
        setData(merged);
        setActiveStep(-1);
        showToast(`V2 extraction complete (${merged.variants.length} rows).`);
      } else {
      let offset = 0;
      for (let round = 0; round < maxRounds; round++) {
        appendLocalLogs([
          {
            at: new Date().toISOString(),
            type: "info",
            msg: `POST /api/extract (batch=${round + 1}, offset=${offset}, limit=${batchLimit})`,
          },
        ]);

        const result = await extractCatalog({ brand, domain, offset, limit: batchLimit });
        merged = mergeExtractResponses(merged, result);
        setData(merged);

        const page = result.pagination;
        if (!page?.has_more || page.next_offset == null || page.next_offset <= offset) break;
        offset = page.next_offset;
      }

        if (!merged) throw new Error("Extraction returned no data.");
        setActiveStep(-1);
        showToast(`Extraction complete (${merged.variants.length} rows).`);
      }
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(err);
      const message = err instanceof Error ? err.message : "Extraction failed.";
      appendLocalLogs([{ at: new Date().toISOString(), type: "error", msg: message }]);
      if (merged && merged.variants.length > 0) {
        setData(merged);
        showToast(`Partial extraction (${merged.variants.length} rows): ${message}`);
      } else {
        showToast(message);
      }
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
          <div className="flex items-center gap-2">
            <Link
              href="/ingredient-harvester"
              className="text-xs px-3 py-2 rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-700"
            >
              Ingredient Harvester →
            </Link>
            <Link
              href="/ingredients/review"
              className="text-xs px-3 py-2 rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-700"
            >
              Ingredient Review →
            </Link>
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
            safe deep-link generation, and export-ready descriptions.
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
              onCopyProductCsv={handleCopyProductCsv}
              onDownloadProductCsv={handleDownloadProductCsv}
              onCopyLink={handleCopyLink}
              isActionsEnabled={variants.length > 0}
            />
          </div>
        </div>
      </main>
    </>
  );
}
