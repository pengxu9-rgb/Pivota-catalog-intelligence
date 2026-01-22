"use client";

import { Copy, ExternalLink } from "lucide-react";

import type { ExtractedVariantRow, LogLine } from "@/lib/types";
import { getAdCopySubjectPreview } from "@/lib/utils";

type Props = {
  logs: LogLine[];
  variants: ExtractedVariantRow[];
  recordCountText: string;
  isActionsEnabled: boolean;
  onCopyCsv: () => void;
  onDownloadCsv: () => void;
  onOpenAdCopy: (variant: ExtractedVariantRow) => void;
  onCopyLink: (url: string) => void;
};

export function ResultsTable({
  logs,
  variants,
  recordCountText,
  isActionsEnabled,
  onCopyCsv,
  onDownloadCsv,
  onOpenAdCopy,
  onCopyLink,
}: Props) {
  return (
    <>
      {/* Simulation Terminal */}
      <div className="bg-gray-900 rounded-xl shadow-lg overflow-hidden flex flex-col h-80">
        <div className="bg-gray-800 px-4 py-2 flex items-center justify-between border-b border-gray-700">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-red-500" />
            <div className="w-3 h-3 rounded-full bg-yellow-500" />
            <div className="w-3 h-3 rounded-full bg-green-500" />
          </div>
          <div className="text-xs text-gray-400 font-mono">system_output.log</div>
        </div>
        <div className="p-4 font-mono text-xs md:text-sm text-gray-300 overflow-y-auto terminal-scroll flex-grow whitespace-pre-wrap">
          {logs.length === 0 ? (
            <span className="text-gray-500">{"// Ready to extract. Waiting for input..."}</span>
          ) : (
            <div className="space-y-1">
              {logs.map((line, idx) => (
                <div key={`${line.at}-${idx}`} className="fade-in">
                  <span className="text-gray-600">[{formatTime(line.at)}]</span>{" "}
                  <span className={logColor(line.type)}>{line.msg}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Results Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col h-[500px]">
        <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center bg-gray-50">
          <h3 className="font-bold text-gray-900">Extracted Data Preview (CSV Format)</h3>
          <div className="flex gap-2">
            <span className="text-sm text-gray-500 self-center">{recordCountText}</span>
            <button
              onClick={onCopyCsv}
              disabled={!isActionsEnabled}
              className="text-xs bg-gray-100 border border-gray-300 px-3 py-1 rounded hover:bg-gray-200 text-gray-700 font-medium disabled:opacity-50"
            >
              Copy CSV
            </button>
            <button
              onClick={onDownloadCsv}
              disabled={!isActionsEnabled}
              className="text-xs bg-blue-600 border border-blue-600 px-3 py-1 rounded hover:bg-blue-700 text-white font-medium disabled:opacity-50"
            >
              Download CSV
            </button>
          </div>
        </div>
        <div className="overflow-auto flex-grow">
          <table className="min-w-full divide-y divide-gray-200 relative">
            <thead className="bg-gray-50 sticky top-0 z-10 shadow-sm">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  Img
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  SKU/ID
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  Product Title
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  Description (AI Merged)
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50 w-48">
                  ✨ AI Copy
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  Price
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {variants.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-6 py-10 text-center text-gray-500 text-sm italic">
                    Run extraction to view data
                  </td>
                </tr>
              ) : (
                variants.map((variant) => (
                  <ResultRow
                    key={`${variant.id}-${variant.sku}`}
                    variant={variant}
                    onOpenAdCopy={onOpenAdCopy}
                    onCopyLink={onCopyLink}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function ResultRow({
  variant,
  onOpenAdCopy,
  onCopyLink,
}: {
  variant: ExtractedVariantRow;
  onOpenAdCopy: (variant: ExtractedVariantRow) => void;
  onCopyLink: (url: string) => void;
}) {
  const subjectPreview = getAdCopySubjectPreview(variant.ad_copy);

  return (
    <tr className="hover:bg-gray-50 transition-colors fade-in group">
      <td className="px-6 py-4 whitespace-nowrap">
        <img
          src="https://via.placeholder.com/40x40/e5e7eb/9ca3af?text=IMG"
          alt="Thumb"
          className="product-thumb"
          title={`Actual URL in CSV: ${variant.image_url}`}
        />
      </td>
      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{variant.sku}</td>
      <td
        className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 max-w-xs truncate"
        title={variant.product_title}
      >
        {variant.product_title}
        {variant.simulated ? (
          <span className="ml-2 inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800">
            Sim
          </span>
        ) : null}
      </td>
      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 max-w-xs truncate" title={variant.description}>
        {variant.description}
      </td>
      <td
        className="px-6 py-4 text-xs text-gray-500 cursor-pointer hover:bg-indigo-50 transition-colors"
        onClick={() => onOpenAdCopy(variant)}
      >
        <div className="flex items-center gap-1">
          <span>✨</span>
          <span className="font-medium text-indigo-600 truncate max-w-[150px]" title={subjectPreview}>
            {subjectPreview}
          </span>
        </div>
      </td>
      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${variant.price}</td>
      <td className="px-6 py-4 whitespace-nowrap text-sm flex items-center gap-3">
        <a
          href={variant.deep_link}
          target="_blank"
          rel="noreferrer"
          title={variant.deep_link}
          className="text-blue-600 hover:text-blue-800 text-xs font-medium flex items-center gap-1"
        >
          Link <ExternalLink className="w-3 h-3" />
        </a>
        <button onClick={() => onCopyLink(variant.deep_link)} className="text-gray-400 hover:text-gray-600" title="Copy URL">
          <Copy className="w-4 h-4" />
        </button>
      </td>
    </tr>
  );
}

function logColor(type: LogLine["type"]) {
  if (type === "success") return "text-green-400";
  if (type === "warn") return "text-yellow-400";
  if (type === "error") return "text-red-400";
  if (type === "data") return "text-blue-300";
  return "text-gray-300";
}

function formatTime(at: string) {
  const d = new Date(at);
  if (Number.isNaN(d.getTime())) return "--:--:--";
  return d.toLocaleTimeString("en-US", { hour12: false });
}
