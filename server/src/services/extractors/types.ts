export type MarketId = "US" | "EU-DE" | "SG" | "JP";

export type DiscoveryStrategy =
  | "shopify_json"
  | "seed_page"
  | "sitemap"
  | "browser_discovery"
  | "managed_browser";

export type FailureCategory =
  | "non_storefront_root"
  | "dead_sitemap"
  | "bot_challenge"
  | "no_product_urls"
  | "product_schema_missing"
  | "timeout"
  | "unknown";

export type BlockProvider = "cloudflare" | "akamai" | "perimeterx" | "unknown" | null;

export type HttpTraceEntry = {
  url: string;
  status: number | null;
};

export type ExtractionDiagnostics = {
  requested_domain: string;
  resolved_base_url: string;
  discovery_strategy: DiscoveryStrategy | null;
  failure_category: FailureCategory | null;
  block_provider: BlockProvider;
  http_trace: HttpTraceEntry[];
};

export type ExtractLogEntry = {
  at: string;
  type: "info" | "success" | "warn" | "error" | "data";
  msg: string;
};

export type ExtractRequestBody = {
  brand: string;
  domain: string;
  market?: MarketId;
  offset?: number;
  limit?: number;
};

export type ExtractInput = ExtractRequestBody;

export type CurrencyCode = "USD";
export type StockStatus = "In Stock" | "Low Stock" | "Out of Stock";

export type ExtractedVariant = {
  id: string;
  sku: string;
  url: string;
  option_name: string;
  option_value: string;
  price: string;
  currency: CurrencyCode;
  stock: StockStatus;
  description: string;
  image_url: string;
  image_urls: string[];
  ad_copy: string;
};

export type ExtractedProduct = {
  title: string;
  url: string;
  image_url: string;
  image_urls: string[];
  variant_skus: string[];
  variants: ExtractedVariant[];
};

export type ExtractedVariantRow = ExtractedVariant & {
  brand: string;
  product_title: string;
  product_url: string;
  deep_link: string;
  simulated: boolean;
};

export type ExtractPagination = {
  offset: number;
  limit: number;
  next_offset: number | null;
  has_more: boolean;
  discovered_urls: number;
};

export type ExtractResponse = {
  brand: string;
  domain: string;
  generated_at: string;
  mode: "simulation" | "puppeteer";
  platform?: string;
  sitemap?: string;
  products: ExtractedProduct[];
  variants: ExtractedVariantRow[];
  pricing: {
    currency: CurrencyCode;
    min: number;
    max: number;
    avg: number;
  };
  ad_copy: {
    by_variant_id: Record<string, string>;
  };
  pagination?: ExtractPagination;
  logs: ExtractLogEntry[];
  diagnostics?: ExtractionDiagnostics;
};

export type Extractor = {
  extract(input: ExtractInput): Promise<ExtractResponse>;
};

export type CurrencyConfidence = "high" | "medium" | "low";
export type PriceType = "list" | "sale" | "from" | "member" | "range" | "unknown";
export type MarketSwitchStatus = "ok" | "mismatch" | "failed" | "unknown";

export type MarketProfile = {
  market_id: MarketId;
  country: string;
  currency_target: string;
  locale: string;
  headers: Record<string, string>;
  cookies: Record<string, string>;
  url_params: Record<string, string>;
  geo_hint?: string;
  shipping_destination?: string;
};

export type MarketContextDebug = {
  headers: Record<string, string>;
  cookies: Record<string, string>;
  url_params: Record<string, string>;
  geo_hint?: string;
  expected_currency: string;
  observed_currency: string | null;
};

export type OfferV2 = {
  source_site: string;
  source_product_id: string;
  url_canonical: string;
  product_title?: string | null;
  product_description?: string | null;
  variant_sku?: string | null;
  market_id: MarketId | string;
  price_amount: number | null;
  price_currency: string | null;
  price_display_raw: string | null;
  price_type: PriceType;
  range_min?: number;
  range_max?: number;
  tax_included: true | false | "unknown";
  availability?: string;
  captured_at: string;
  currency_confidence: CurrencyConfidence;
  market_switch_status: MarketSwitchStatus;
  market_context_debug: MarketContextDebug;
};

export type SiteMarketCounters = {
  source_site: string;
  market_id: string;
  total_offers: number;
  native_currency_hit_rate: number;
  price_parse_success_rate: number;
  currency_confidence_low_rate: number;
  market_switch_fail_rate: number;
};

export type ExtractV2RequestBody = {
  brand: string;
  domain: string;
  market?: MarketId;
  markets?: MarketId[];
  offset?: number;
  limit?: number;
};

export type ExtractV2Response = {
  brand: string;
  domain: string;
  generated_at: string;
  mode: "simulation" | "puppeteer";
  offers_v2: OfferV2[];
  counters_by_site_market: SiteMarketCounters[];
  pagination: ExtractPagination;
  logs: ExtractLogEntry[];
  diagnostics?: ExtractionDiagnostics;
};
