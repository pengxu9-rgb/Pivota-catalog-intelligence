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
  currency: "USD";
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
    currency: "USD";
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
    currency: "USD";
    min: number;
    max: number;
    avg: number;
  };
  ad_copy: {
    by_variant_id: Record<string, string>;
  };
  logs: LogLine[];
};

