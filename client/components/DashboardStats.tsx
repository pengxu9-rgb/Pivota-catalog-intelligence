import { DonutStat } from "@/components/DonutStat";

export function DashboardStats() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      {/* Goal 1 */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 flex flex-col">
        <div className="flex justify-between items-start mb-4">
          <div>
            <h3 className="font-semibold text-gray-900">Category A: Makeup</h3>
            <p className="text-sm text-gray-500">Target: 50 DTC Brands</p>
          </div>
          <span className="text-emerald-600 text-xs font-bold bg-emerald-50 px-2 py-1 rounded">ON TRACK</span>
        </div>
        <div className="h-[250px] flex items-center justify-center">
          <DonutStat value={42} total={50} color="#10b981" />
        </div>
      </div>

      {/* Goal 2 */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 flex flex-col">
        <div className="flex justify-between items-start mb-4">
          <div>
            <h3 className="font-semibold text-gray-900">Category B: Tools</h3>
            <p className="text-sm text-gray-500">Target: 50 DTC Brands</p>
          </div>
          <span className="text-blue-600 text-xs font-bold bg-blue-50 px-2 py-1 rounded">PROCESSING</span>
        </div>
        <div className="h-[250px] flex items-center justify-center">
          <DonutStat value={15} total={50} color="#3b82f6" />
        </div>
      </div>

      {/* Methodology Stats */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 flex flex-col">
        <h3 className="font-semibold text-gray-900 mb-2">Extraction Strategy</h3>
        <p className="text-sm text-gray-500 mb-4">Preferred methods across dataset</p>
        <div className="space-y-4">
          <div>
            <div className="flex justify-between text-xs font-medium text-gray-700 mb-1">
              <span>Sitemap Parsing</span>
              <span>65%</span>
            </div>
            <div className="w-full bg-gray-100 rounded-full h-2">
              <div className="bg-indigo-500 h-2 rounded-full" style={{ width: "65%" }} />
            </div>
          </div>

          <div>
            <div className="flex justify-between text-xs font-medium text-gray-700 mb-1">
              <span>Shopify/Public Feed</span>
              <span>25%</span>
            </div>
            <div className="w-full bg-gray-100 rounded-full h-2">
              <div className="bg-emerald-500 h-2 rounded-full" style={{ width: "25%" }} />
            </div>
          </div>

          <div>
            <div className="flex justify-between text-xs font-medium text-gray-700 mb-1">
              <span>Category Crawl (Fallback)</span>
              <span>10%</span>
            </div>
            <div className="w-full bg-gray-100 rounded-full h-2">
              <div className="bg-amber-500 h-2 rounded-full" style={{ width: "10%" }} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

