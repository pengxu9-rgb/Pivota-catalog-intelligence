import type { ExtractedProduct, ExtractedVariantRow } from "./types";

function escapeCsvField(value: string) {
  const escaped = value.replace(/\"/g, '\"\"');
  return `"${escaped}"`;
}

function joinImageUrls(values: string[]) {
  return escapeCsvField(values.join(" | "));
}

function normalizeImageUrls(...sources: Array<unknown>) {
  const out: string[] = [];

  const push = (value: unknown) => {
    const url = typeof value === "string" ? value.trim() : "";
    if (!url) return;
    if (out.includes(url)) return;
    out.push(url);
  };

  for (const source of sources) {
    if (typeof source === "string") {
      push(source);
      continue;
    }

    if (!Array.isArray(source)) continue;
    for (const item of source) {
      if (typeof item === "string") {
        push(item);
      } else if (item && typeof item === "object") {
        const image = item as { image_url?: string; url?: string; src?: string };
        push(image.image_url);
        push(image.url);
        push(image.src);
      }
    }
  }

  return out;
}

export function buildCsv(variants: ExtractedVariantRow[]) {
  let csv =
    "Brand,Product Title,Product URL,Variant ID,SKU,Option Name,Option Value,Price,Currency,Availability,Variant Image URL,Variant Image URLs,AI Merged Description,Deep Link\n";

  for (const v of variants) {
    const imageUrls = normalizeImageUrls(Array.isArray(v.image_urls) ? v.image_urls : [], v.image_url);
    const row = [
      escapeCsvField(v.brand),
      escapeCsvField(v.product_title),
      escapeCsvField(v.url),
      escapeCsvField(v.id),
      escapeCsvField(v.sku),
      escapeCsvField(v.option_name),
      escapeCsvField(v.option_value),
      escapeCsvField(v.price),
      escapeCsvField(v.currency),
      escapeCsvField(v.stock),
      escapeCsvField(v.image_url),
      joinImageUrls(imageUrls),
      escapeCsvField(v.description),
      escapeCsvField(v.deep_link),
    ].join(",");
    csv += `${row}\n`;
  }

  return csv;
}

type ProductAggregate = {
  brand: string;
  productTitle: string;
  productUrl: string;
  variantCount: number;
  minPrice: number;
  maxPrice: number;
  currency: "USD";
  stockSummary: string;
  imageUrl: string;
  imageUrls: string[];
  description: string;
  deepLink: string;
};

function summarizeStock(stocks: Array<ExtractedVariantRow["stock"]>) {
  const unique = Array.from(new Set(stocks));
  if (unique.length === 0) return "Unknown";
  if (unique.length === 1) return unique[0];
  return unique.join(" | ");
}

function aggregateProducts(products: ExtractedProduct[], variants: ExtractedVariantRow[]): ProductAggregate[] {
  const grouped = new Map<string, ExtractedVariantRow[]>();
  for (const variant of variants) {
    const key = variant.product_url || variant.url;
    const bucket = grouped.get(key) || [];
    bucket.push(variant);
    grouped.set(key, bucket);
  }

  const byUrl = new Map(products.map((product) => [product.url, product]));
  const keys = new Set<string>([...byUrl.keys(), ...grouped.keys()]);
  const aggregates: ProductAggregate[] = [];

  for (const key of keys) {
    const product = byUrl.get(key);
    const bucket = grouped.get(key) || [];
    const variantPrices = [
      ...(product?.variants || []).map((variant) => Number.parseFloat(variant.price)),
      ...bucket.map((variant) => Number.parseFloat(variant.price)),
    ].filter((value) => Number.isFinite(value));
    const imageUrls = normalizeImageUrls(
      product?.image_urls,
      product?.variants,
      bucket.map((variant) => variant.image_url),
      bucket.flatMap((variant) => (Array.isArray(variant.image_urls) ? variant.image_urls : [])),
      product?.image_url,
    );
    const representativeVariant = (product?.variants || []).find((variant) => variant.description) || bucket.find((variant) => variant.description);
    const productTitle = product?.title || bucket[0]?.product_title || key;
    const currency = product?.variants[0]?.currency || bucket[0]?.currency || "USD";

    aggregates.push({
      brand: bucket[0]?.brand || "",
      productTitle,
      productUrl: key,
      variantCount: product?.variants.length || bucket.length,
      minPrice: variantPrices.length > 0 ? Math.min(...variantPrices) : 0,
      maxPrice: variantPrices.length > 0 ? Math.max(...variantPrices) : 0,
      currency,
      stockSummary: summarizeStock([
        ...(product?.variants || []).map((variant) => variant.stock),
        ...bucket.map((variant) => variant.stock),
      ]),
      imageUrl: product?.image_url || imageUrls[0] || "",
      imageUrls,
      description: representativeVariant?.description || "",
      deepLink: bucket.find((variant) => Boolean(variant.deep_link))?.deep_link || product?.url || key,
    });
  }

  return aggregates.sort((a, b) => a.productTitle.localeCompare(b.productTitle));
}

export function buildProductCsv(products: ExtractedProduct[], variants: ExtractedVariantRow[]) {
  const aggregates = aggregateProducts(products, variants);
  let csv =
    "Brand,Product Title,Product URL,Variant Count,Min Price,Max Price,Currency,Stock Summary,Representative Image URL,Product Image URLs,AI Merged Description,Representative Deep Link\n";

  for (const product of aggregates) {
    const row = [
      escapeCsvField(product.brand),
      escapeCsvField(product.productTitle),
      escapeCsvField(product.productUrl),
      String(product.variantCount),
      product.minPrice.toFixed(2),
      product.maxPrice.toFixed(2),
      escapeCsvField(product.currency),
      escapeCsvField(product.stockSummary),
      escapeCsvField(product.imageUrl),
      joinImageUrls(product.imageUrls),
      escapeCsvField(product.description),
      escapeCsvField(product.deepLink),
    ].join(",");
    csv += `${row}\n`;
  }

  return csv;
}

export function downloadTextFile(contents: string, filename: string, mime: string) {
  const blob = new Blob([contents], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.setAttribute("href", url);
  a.setAttribute("download", filename);
  a.style.visibility = "hidden";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
