export type ReviewedVariantOverride = {
  brand: string;
  product_url: string;
  product_title: string;
  sku?: string;
  barcode?: string;
  option_name: string;
  option_value: string;
  reviewer: string;
  reviewed_at: string;
  evidence_urls: string[];
  note?: string;
};

export const reviewedVariantOverrides: ReviewedVariantOverride[] = [
  {
    brand: "TIRTIR Global",
    product_url: "https://tirtir.global/products/reflect-glow-prep-primer",
    product_title: "Reflect Glow Prep Primer",
    sku: "01TTF0862",
    barcode: "8800349020538",
    option_name: "Size",
    option_value: "30ml",
    reviewer: "codex_reviewed_override",
    reviewed_at: "2026-05-02",
    evidence_urls: [
      "https://shop.tiktok.com/us/pdp/1732350857836139098",
      "https://shopee.co.th/tirtirofficial.th",
    ],
    note:
      "Official TIRTIR DTC PDP omits a displayable size label. Reviewed override uses exact product-title + official-shop evidence to surface the single-SKU size without inventing a selector axis.",
  },
];
