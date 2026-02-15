export type ExtractRequestBody = {
  brand: string;
  domain: string;
  offset?: number;
  limit?: number;
  session_id?: string;
};

export type ExtractInput = ExtractRequestBody;

export type CurrencyCode = string;
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
  ad_copy: string;
};

export type ExtractedProduct = {
  title: string;
  url: string;
  variants: ExtractedVariant[];
};

export type ExtractedVariantRow = ExtractedVariant & {
  brand: string;
  product_title: string;
  product_url: string;
  deep_link: string;
  simulated: boolean;
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
  pagination?: {
    offset: number;
    limit: number;
    next_offset: number | null;
    has_more: boolean;
    discovered_urls: number;
  };
  logs: Array<{
    at: string;
    type: "info" | "success" | "warn" | "error" | "data";
    msg: string;
  }>;
};

export type Extractor = {
  extract(input: ExtractInput): Promise<ExtractResponse>;
};

export type MarketId = string;
export type CurrencyConfidence = "high" | "medium" | "low";
export type PriceType = "list" | "sale" | "from" | "range" | "member" | "unknown";
export type TaxIncluded = true | false | "unknown";
export type MarketSwitchStatus = "ok" | "failed" | "mismatch" | "unknown";

export type MarketProfile = {
  market_id: MarketId;
  country: string;
  currency_target: string;
  locale: string;
  headers: Record<string, string>;
  cookies: Record<string, string>;
  url_params: Record<string, string>;
  shipping_destination?: string;
  geo_hint?: string;
};

export type ExtractV2RequestBody = {
  brand: string;
  domain: string;
  offset?: number;
  limit?: number;
  markets?: MarketId[];
  session_id?: string;
};

export type OfferV2 = {
  source_site: string;
  source_product_id: string;
  url_canonical: string;
  product_title?: string | null;
  product_description?: string | null;
  variant_sku?: string | null;
  market_id: MarketId;
  price_amount: number | null;
  price_currency: string | null;
  price_display_raw: string | null;
  price_type: PriceType;
  range_min?: number;
  range_max?: number;
  tax_included: TaxIncluded;
  availability?: string;
  captured_at: string;
  currency_confidence: CurrencyConfidence;
  market_switch_status: MarketSwitchStatus;
  market_context_debug: {
    headers: Record<string, string>;
    cookies: Record<string, string>;
    url_params: Record<string, string>;
    geo_hint?: string;
    expected_currency?: string;
    observed_currency?: string | null;
  };
};

export type SiteMarketCounters = {
  source_site: string;
  market_id: MarketId;
  total_offers: number;
  native_currency_hit_rate: number;
  price_parse_success_rate: number;
  currency_confidence_low_rate: number;
  market_switch_fail_rate: number;
};

export type ExtractV2Response = {
  brand: string;
  domain: string;
  generated_at: string;
  mode: "simulation" | "puppeteer";
  offers_v2: OfferV2[];
  counters_by_site_market: SiteMarketCounters[];
  pagination?: {
    offset: number;
    limit: number;
    next_offset: number | null;
    has_more: boolean;
    discovered_urls: number;
  };
  logs: Array<{
    at: string;
    type: "info" | "success" | "warn" | "error" | "data";
    msg: string;
  }>;
};
