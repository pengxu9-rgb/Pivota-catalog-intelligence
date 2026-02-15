export type ExtractRequestBody = {
  brand: string;
  domain: string;
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
