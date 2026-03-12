export type CurrencyCode = "USD" | "EUR" | "SGD" | "JPY";

export type LogLine = {
  at: string;
  type: "info" | "success" | "warn" | "error" | "data";
  msg: string;
};

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

export type ExtractionDiagnostics = {
  requested_domain: string;
  resolved_base_url: string;
  discovery_strategy: DiscoveryStrategy | null;
  failure_category: FailureCategory | null;
  block_provider: "cloudflare" | "akamai" | "perimeterx" | "unknown" | null;
  http_trace: Array<{
    url: string;
    status: number | null;
  }>;
};

export type ExtractedVariantRow = {
  id: string;
  sku: string;
  url: string;
  option_name: string;
  option_value: string;
  price: string;
  currency: CurrencyCode;
  stock: "In Stock" | "Low Stock" | "Out of Stock";
  description: string;
  image_url: string;
  image_urls: string[];
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
  image_url: string;
  image_urls: string[];
  variant_skus: string[];
  variants: Array<{
    id: string;
    sku: string;
    url: string;
    option_name: string;
    option_value: string;
    price: string;
    currency: CurrencyCode;
    stock: "In Stock" | "Low Stock" | "Out of Stock";
    description: string;
    image_url: string;
    image_urls: string[];
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
  logs: LogLine[];
  diagnostics?: ExtractionDiagnostics;
};
