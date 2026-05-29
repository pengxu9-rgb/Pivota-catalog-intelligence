export type MarketId = "US" | "EU-DE" | "SG" | "JP" | "CN" | "KR";

export type DiscoveryStrategy =
  | "shopify_json"
  | "seed_page"
  | "site_search"
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
  product_title?: string;
  market?: MarketId;
  offset?: number;
  limit?: number;
  storefront_password?: string;
};

export type ExtractInput = ExtractRequestBody;

export type CurrencyCode = "USD" | "EUR" | "SGD" | "JPY" | "CNY" | "KRW";
export type StockStatus = "In Stock" | "Low Stock" | "Out of Stock";
export type ExtractedFieldSourceOrigin =
  | "shopify_json"
  | "jsonld"
  | "retail_pdp"
  | "manual_override"
  | "browser_fallback"
  | "image_vision"
  | "simulation"
  | "unknown";
export type ExtractedFieldQualityStatus = "high" | "medium" | "low" | "quarantined";

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
  source_origin?: ExtractedFieldSourceOrigin;
  source_quality_status?: ExtractedFieldQualityStatus;
  hidden_from_selector?: boolean;
};

export type ExtractedProductDetailSection = {
  heading: string;
  body: string;
  source_kind: string;
  media_urls?: string[];
};

export type ExtractedProductFaqItem = {
  question: string;
  answer: string;
  source_kind: string;
  source_url?: string;
  source_title?: string;
};

export type ExtractedReviewMediaItem = {
  type: string;
  url: string;
  thumbnail_url?: string;
  source?: string;
  source_kind?: string;
  source_scope?: string;
  content_review_state?: string;
  public_visible?: boolean;
};

export type ExtractedReviewPreviewItem = {
  review_id: string;
  rating?: number;
  author_label?: string;
  title?: string;
  text_snippet?: string;
  media?: ExtractedReviewMediaItem[];
  source?: string;
  source_kind?: string;
  source_scope?: string;
  content_review_state?: string;
  public_visible?: boolean;
  verified_buyer?: boolean;
};

export type ExtractedReviewDistributionRow = {
  stars: number;
  count?: number;
  percent?: number;
};

export type ExtractedReviewSummaryQuestion = {
  question: string;
  answer?: string;
  source?: string;
  source_label?: string;
  support_count?: number;
  replies?: number;
  content_review_state?: string;
  public_visible?: boolean;
};

export type ExtractedReviewBrandCard = {
  name?: string;
  subtitle?: string;
};

export type ExtractedProductReviewSummary = {
  rating?: number;
  review_count?: number;
  scale?: number;
  source?: string;
  source_kind?: string;
  source_origin?: string;
  source_url?: string;
  preview_items?: ExtractedReviewPreviewItem[];
  questions?: ExtractedReviewSummaryQuestion[];
  star_distribution?: ExtractedReviewDistributionRow[];
  rating_distribution?: ExtractedReviewDistributionRow[];
  brand_card?: ExtractedReviewBrandCard;
  aggregation_scope?: string;
  exact_item_review_count?: number;
  product_line_review_count?: number;
  scope_label?: string;
};

export type ExtractedProductKind =
  | "single_formula"
  | "bundle"
  | "accessory"
  | "fragrance"
  | "general_merchandise";

export type ExtractedBundleComponent = {
  name: string;
  quantity?: string;
  source_kind: string;
  raw_text?: string;
};

export type ExtractedProductFieldCaptureStatus = {
  description_raw: "present" | "missing";
  details_sections: "present" | "missing";
  ingredients_raw: "present" | "missing";
  active_ingredients_raw: "present" | "missing";
  how_to_use_raw: "present" | "missing";
  faq_items: "present" | "missing";
};

export type ExtractedFieldQualityRecord = {
  source_origin: ExtractedFieldSourceOrigin;
  source_quality_status: ExtractedFieldQualityStatus;
  source_kinds: string[];
  reason_codes: string[];
};

export type ExtractedProductFieldQualitySummary = Partial<
  Record<keyof ExtractedProductFieldCaptureStatus, ExtractedFieldQualityRecord>
>;

export type ExtractedProductQuarantinedPdpFields = Partial<
  Record<
    keyof ExtractedProductFieldCaptureStatus,
    string | ExtractedProductDetailSection[] | ExtractedProductFaqItem[]
  >
>;

export type ExtractedProduct = {
  title: string;
  url: string;
  image_url: string;
  image_urls: string[];
  content_image_urls?: string[];
  size?: string;
  volume?: string;
  product_volume?: string;
  net_content?: string;
  net_size?: string;
  size_detail_label?: string;
  variant_skus: string[];
  variants: ExtractedVariant[];
  description_raw?: string;
  details_sections?: ExtractedProductDetailSection[];
  ingredients_raw?: string;
  active_ingredients_raw?: string;
  how_to_use_raw?: string;
  faq_items?: ExtractedProductFaqItem[];
  review_summary?: ExtractedProductReviewSummary;
  product_kind?: ExtractedProductKind;
  bundle_components?: ExtractedBundleComponent[];
  field_capture_status?: ExtractedProductFieldCaptureStatus;
  field_sources?: Record<string, string[]>;
  field_quality_summary?: ExtractedProductFieldQualitySummary;
  quarantined_pdp_fields?: ExtractedProductQuarantinedPdpFields;
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
export type CommerceFactConfidence = CurrencyConfidence | "unknown";
export type PriceType = "list" | "sale" | "from" | "member" | "range" | "unknown";
export type MarketSwitchStatus = "ok" | "mismatch" | "failed" | "unknown";
export type CommerceFactSourceAuthority =
  | "merchant_api"
  | "checkout_api"
  | "storefront_structured"
  | "catalog_extract_v2"
  | "manual_override";
export type SellableRegionStatus = "eligible" | "not_eligible" | "unknown";
export type NormalizedAvailabilityStatus =
  | "in_stock"
  | "out_of_stock"
  | "low_stock"
  | "preorder"
  | "unknown";
export type ShippingStatus = "available" | "unavailable" | "unknown";
export type ReturnsStatus = "available" | "unavailable" | "unknown";

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
  compare_at_amount?: number | null;
  compare_at_currency?: string | null;
  compare_at_display_raw?: string | null;
  range_min?: number;
  range_max?: number;
  tax_included: true | false | "unknown";
  availability?: string;
  captured_at: string;
  currency_confidence: CurrencyConfidence;
  market_switch_status: MarketSwitchStatus;
  market_context_debug: MarketContextDebug;
  commerce_facts_v1?: CommerceFactsV1;
};

export type CommerceFactsV1 = {
  contract_version: "commerce_facts.v1";
  market_id: MarketId | string;
  country?: string;
  currency_target: string;
  source_authority: CommerceFactSourceAuthority;
  captured_at: string;
  evidence_url: string;
  sellable_region: {
    status: SellableRegionStatus;
    countries: string[];
    evidence_source: CommerceFactSourceAuthority;
    confidence: CommerceFactConfidence;
    checked_at: string;
    reason_codes: string[];
    evidence_url: string;
  };
  regional_price: {
    amount: number | null;
    currency: string | null;
    display_raw: string | null;
    price_type: PriceType;
    compare_at_amount?: number | null;
    compare_at_currency?: string | null;
    compare_at_display_raw?: string | null;
    range_min?: number;
    range_max?: number;
    tax_included: true | false | "unknown";
    confidence: CurrencyConfidence;
    market_switch_status: MarketSwitchStatus;
    observed_currency: string | null;
    source_url: string;
    captured_at: string;
  };
  availability: {
    status: NormalizedAvailabilityStatus;
    source: CommerceFactSourceAuthority;
    confidence: CommerceFactConfidence;
    captured_at: string;
  };
  shipping: {
    status: ShippingStatus;
    destination_country?: string;
    method_label?: string;
    eta_days_range?: [number, number];
    cost?: { amount: number; currency: string };
    free_shipping_threshold?: { amount: number; currency: string };
    source: CommerceFactSourceAuthority;
    confidence: CommerceFactConfidence;
    reason_codes: string[];
    checked_at: string;
  };
  promotions: Array<{
    promo_type: string;
    summary: string;
    terms?: string;
    starts_at?: string;
    ends_at?: string;
    source: CommerceFactSourceAuthority;
    confidence: CommerceFactConfidence;
    evidence_url?: string;
  }>;
  returns: {
    status: ReturnsStatus;
    source: CommerceFactSourceAuthority;
    confidence: CommerceFactConfidence;
    reason_codes: string[];
    checked_at: string;
  };
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
