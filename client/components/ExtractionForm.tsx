"use client";

import { Sparkles } from "lucide-react";

type Props = {
  brand: string;
  domain: string;
  isRunning: boolean;
  activeStep: number;
  onBrandChange: (v: string) => void;
  onDomainChange: (v: string) => void;
  onSubmit: () => void;
  onAskGemini: () => void;
};

export function ExtractionForm({
  brand,
  domain,
  isRunning,
  activeStep,
  onBrandChange,
  onDomainChange,
  onSubmit,
  onAskGemini,
}: Props) {
  return (
    <>
      {/* Tool Inputs */}
      <div className="bg-white p-6 rounded-xl shadow-lg border border-gray-200">
        <h3 className="text-lg font-bold text-gray-900 mb-4 flex items-center">
          <span className="mr-2 text-xl">⚙️</span> Run Extraction
        </h3>

        <form
          className="space-y-4"
          onSubmit={(e) => {
            e.preventDefault();
            onSubmit();
          }}
        >
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Brand Name</label>
            <input
              type="text"
              value={brand}
              onChange={(e) => onBrandChange(e.target.value)}
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 border p-2 text-sm"
              placeholder="e.g. Tom Ford"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Store Domain</label>
            <div className="flex">
              <span className="inline-flex items-center px-3 rounded-l-md border border-r-0 border-gray-300 bg-gray-50 text-gray-500 text-sm">
                https://
              </span>
              <input
                type="text"
                value={domain}
                onChange={(e) => onDomainChange(e.target.value)}
                className="flex-1 min-w-0 block w-full px-3 py-2 rounded-none rounded-r-md border border-gray-300 focus:ring-blue-500 focus:border-blue-500 text-sm"
                placeholder="store.com"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">UTM Source</label>
              <input
                type="text"
                value="pivota"
                readOnly
                className="w-full rounded-md border-gray-300 border p-2 text-sm text-gray-500 bg-gray-50"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Campaign</label>
              <input
                type="text"
                value="catalog_v1"
                readOnly
                className="w-full rounded-md border-gray-300 border p-2 text-sm text-gray-500 bg-gray-50"
              />
            </div>
          </div>

          <button
            type="submit"
            disabled={isRunning}
            className={[
              "w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white",
              "bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors",
              isRunning ? "opacity-75 cursor-not-allowed" : "",
            ].join(" ")}
          >
            {isRunning ? "Extracting..." : "Run Extraction"}
          </button>
        </form>
      </div>

      {/* Logic Flow */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
        <h4 className="font-semibold text-gray-900 mb-3">Logic Flow</h4>
        <div className="space-y-4 relative">
          <div className="absolute left-3 top-2 bottom-2 w-0.5 bg-gray-200" />

          <LogicStep
            index={0}
            activeStep={activeStep}
            title="1. Product Universe"
            description={
              <>
                Check <span className="font-mono bg-gray-100 px-1 rounded">/products.json</span> (Shopify). Else parse{" "}
                <span className="font-mono bg-gray-100 px-1 rounded">robots.txt</span> -&gt; Sitemap.
              </>
            }
          />
          <LogicStep
            index={1}
            activeStep={activeStep}
            title="2. Variant Discovery"
            description={
              <>
                Extract schema/JSON-LD. <strong className="text-indigo-600">Auto-generate Ad Copy &amp; Desc.</strong>
              </>
            }
          />
          <LogicStep
            index={2}
            activeStep={activeStep}
            title="3. Deep Link Generation"
            description={
              <>
                Construct URL + UTMs. Pattern: <span className="font-mono text-xs">/product/ID?variant=123</span>
              </>
            }
          />
        </div>
      </div>

      {/* AI Helper */}
      <div className="bg-gradient-to-br from-indigo-50 to-purple-50 p-6 rounded-xl shadow-sm border border-indigo-100">
        <div className="flex items-center gap-2 mb-3">
          <Sparkles className="w-4 h-4 text-indigo-600" />
          <h4 className="font-semibold text-gray-900">AI Extraction Assistant</h4>
        </div>
        <p className="text-xs text-gray-600 mb-3">Ask Gemini to write regex or selectors for your target site.</p>
        <div className="space-y-2">
          <textarea
            rows={3}
            className="w-full text-xs p-2 rounded border border-gray-300 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
            placeholder="e.g. How do I match a price like '$1,299.00' inside a span?"
          />
          <button
            type="button"
            onClick={onAskGemini}
            className="w-full py-1.5 bg-white border border-indigo-200 text-indigo-700 text-xs font-medium rounded hover:bg-indigo-50 transition-colors shadow-sm"
          >
            Ask Gemini
          </button>
        </div>
      </div>
    </>
  );
}

function LogicStep({
  index,
  activeStep,
  title,
  description,
}: {
  index: number;
  activeStep: number;
  title: string;
  description: React.ReactNode;
}) {
  const isActive = index === activeStep;
  return (
    <div className={["relative pl-8 transition-all duration-300", isActive ? "opacity-100" : "opacity-50"].join(" ")}>
      <div
        className={[
          "absolute left-1.5 top-1.5 w-3 h-3 rounded-full border-2 border-white transform -translate-x-1/2",
          isActive ? "bg-blue-600" : "bg-gray-400",
        ].join(" ")}
      />
      <h5 className="text-sm font-medium text-gray-900">{title}</h5>
      <p className="text-xs text-gray-500 mt-1">{description}</p>
    </div>
  );
}

