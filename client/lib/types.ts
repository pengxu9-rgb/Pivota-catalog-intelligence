export type LogLine = {
  at: string;
  type: "info" | "success" | "warn" | "error" | "data";
  msg: string;
};

export type ExtractedVariantRow = {
  id: string;
  sku: string;
  url: string;
  option_name: string;
  option_value: string;
  price: string;
  currency: string;
  stock: "In Stock" | "Low Stock" | "Out of Stock";
  description: string;
  image_url: string;
  ad_copy: string;

  brand: string;
  product_title: string;
  product_url: string;
  deep_link: string;
  simulated: boolean;
};

export type ExtractedProduct = {
  title: string;
  url: string;
  variants: Array<{
    id: string;
    sku: string;
    url: string;
    option_name: string;
    option_value: string;
    price: string;
    currency: string;
    stock: "In Stock" | "Low Stock" | "Out of Stock";
    description: string;
    image_url: string;
    ad_copy: string;
  }>;
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
    currency: string;
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
  logs: LogLine[];
};

export type OfferV2 = {
  source_site: string;
  source_product_id: string;
  url_canonical: string;
  product_title?: string | null;
  variant_sku?: string | null;
  market_id: string;
  price_amount: number | null;
  price_currency: string | null;
  price_display_raw: string | null;
  price_type: "list" | "sale" | "from" | "range" | "member" | "unknown";
  range_min?: number;
  range_max?: number;
  tax_included: true | false | "unknown";
  availability?: string;
  captured_at: string;
  currency_confidence: "high" | "medium" | "low";
  market_switch_status: "ok" | "failed" | "mismatch" | "unknown";
  market_context_debug: {
    headers: Record<string, string>;
    cookies: Record<string, string>;
    url_params: Record<string, string>;
    geo_hint?: string;
    expected_currency?: string;
    observed_currency?: string | null;
  };
};

export type SiteMarketCounter = {
  source_site: string;
  market_id: string;
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
  counters_by_site_market: SiteMarketCounter[];
  logs: LogLine[];
};
