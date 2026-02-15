import type { ExtractedVariantRow } from "./types";

function escapeCsvField(value: string) {
  const escaped = value.replace(/\"/g, '\"\"');
  return `"${escaped}"`;
}

export function buildCsv(variants: ExtractedVariantRow[]) {
  let csv =
    "Brand,Product Title,Product URL,Variant ID,SKU,Option Name,Option Value,Market ID,Price,Currency,Availability,Variant Image URL,AI Merged Description,Deep Link\n";

  for (const v of variants) {
    const marketId = v.option_name === "Market" ? v.option_value : "";
    const row = [
      v.brand,
      escapeCsvField(v.product_title),
      v.url,
      v.id,
      v.sku,
      v.option_name,
      v.option_value,
      marketId,
      v.price,
      v.currency,
      v.stock,
      v.image_url,
      escapeCsvField(v.description),
      v.deep_link,
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
  currency: string;
  marketId: string;
  stockSummary: string;
  imageUrl: string;
  description: string;
  deepLink: string;
};

function summarizeStock(stocks: Array<ExtractedVariantRow["stock"]>) {
  const unique = Array.from(new Set(stocks));
  if (unique.length === 0) return "Unknown";
  if (unique.length === 1) return unique[0];
  return unique.join(" | ");
}

function aggregateProducts(variants: ExtractedVariantRow[]): ProductAggregate[] {
  const grouped = new Map<string, ExtractedVariantRow[]>();
  for (const variant of variants) {
    const key = variant.product_url || variant.url;
    const bucket = grouped.get(key) || [];
    bucket.push(variant);
    grouped.set(key, bucket);
  }

  const products: ProductAggregate[] = [];
  for (const [productUrl, bucket] of grouped.entries()) {
    const first = bucket[0];
    const prices = bucket
      .map((v) => Number.parseFloat(v.price))
      .filter((n) => Number.isFinite(n));
    const minPrice = prices.length > 0 ? Math.min(...prices) : 0;
    const maxPrice = prices.length > 0 ? Math.max(...prices) : 0;

    products.push({
      brand: first.brand,
      productTitle: first.product_title,
      productUrl,
      variantCount: bucket.length,
      minPrice,
      maxPrice,
      currency: first.currency,
      marketId: first.option_name === "Market" ? first.option_value : "",
      stockSummary: summarizeStock(bucket.map((v) => v.stock)),
      imageUrl: bucket.find((v) => Boolean(v.image_url))?.image_url || "",
      description: bucket.find((v) => Boolean(v.description))?.description || "",
      deepLink: bucket.find((v) => Boolean(v.deep_link))?.deep_link || "",
    });
  }

  return products.sort((a, b) => a.productTitle.localeCompare(b.productTitle));
}

export function buildProductCsv(variants: ExtractedVariantRow[]) {
  const products = aggregateProducts(variants);
  let csv =
    "Brand,Product Title,Product URL,Variant Count,Min Price,Max Price,Currency,Market ID,Stock Summary,Representative Image URL,AI Merged Description,Representative Deep Link\n";

  for (const product of products) {
    const row = [
      product.brand,
      escapeCsvField(product.productTitle),
      product.productUrl,
      String(product.variantCount),
      product.minPrice.toFixed(2),
      product.maxPrice.toFixed(2),
      product.currency,
      product.marketId,
      escapeCsvField(product.stockSummary),
      product.imageUrl,
      escapeCsvField(product.description),
      product.deepLink,
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
