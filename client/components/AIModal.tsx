"use client";

import type { LucideIcon } from "lucide-react";
import { RefreshCw } from "lucide-react";
import ReactMarkdown from "react-markdown";

import type { ExtractedVariantRow } from "@/lib/types";
import { getAdCopySubjectPreview } from "@/lib/utils";

type Props = {
  open: boolean;
  variant: ExtractedVariantRow | null;
  onClose: () => void;
  onCopyText: () => void;
  onRegenerate: () => void;
  CloseIcon: LucideIcon;
  CopyIcon: LucideIcon;
  ExternalLinkIcon: LucideIcon;
};

export function AIModal({
  open,
  variant,
  onClose,
  onCopyText,
  onRegenerate,
  CloseIcon,
  CopyIcon,
  ExternalLinkIcon,
}: Props) {
  if (!open || !variant) return null;

  const subject = getAdCopySubjectPreview(variant.ad_copy);

  return (
    <div className="fixed inset-0 z-[100]">
      <div className="modal-overlay absolute inset-0" onClick={onClose} />
      <div className="absolute top-1/2 left-1/2 w-full max-w-lg max-h-[90vh] -translate-x-1/2 -translate-y-1/2 bg-white rounded-xl shadow-2xl overflow-hidden flex flex-col">
        <div className="bg-gradient-to-r from-indigo-600 to-purple-600 px-6 py-4 flex justify-between items-center">
          <h3 className="text-white font-bold flex items-center gap-2">
            <span>✨</span> Marketing Ad Copy
          </h3>
          <button onClick={onClose} className="text-white hover:bg-white/20 rounded-full p-1 transition-colors">
            <CloseIcon className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 overflow-y-auto">
          <div className="prose prose-sm max-w-none text-gray-700">
            <div className="mb-4">
              <div className="text-xs text-gray-500">Preview</div>
              <div className="text-sm font-medium text-gray-900">{subject}</div>
              <div className="mt-1 text-xs text-gray-500">
                {variant.product_title} · {variant.option_value} · ${variant.price}
              </div>
              <a
                href={variant.deep_link}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1 text-xs font-medium text-indigo-600 hover:text-indigo-800 mt-2"
              >
                Open deep link <ExternalLinkIcon className="w-3.5 h-3.5" />
              </a>
            </div>
            <ReactMarkdown>{variant.ad_copy}</ReactMarkdown>
          </div>
        </div>

        <div className="bg-gray-50 px-6 py-3 border-t border-gray-100 flex justify-between items-center">
          <button
            onClick={onRegenerate}
            className="text-xs text-indigo-600 font-medium hover:text-indigo-800 flex items-center gap-1"
          >
            <RefreshCw className="w-4 h-4" />
            Regenerate with Gemini
          </button>
          <div className="flex gap-3">
            <button onClick={onCopyText} className="text-sm font-medium text-gray-700 hover:text-gray-900 inline-flex items-center gap-1">
              <CopyIcon className="w-4 h-4" />
              Copy Text
            </button>
            <button
              onClick={onClose}
              className="px-4 py-2 bg-gray-200 hover:bg-gray-300 text-gray-800 text-sm font-medium rounded-md transition-colors"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
