import type { ExtractedVariantRow } from "./types";

function escapeCsvField(value: string) {
  const escaped = value.replace(/\"/g, '\"\"');
  return `"${escaped}"`;
}

export function buildCsv(variants: ExtractedVariantRow[]) {
  let csv =
    "Brand,Product Title,Product URL,Variant ID,SKU,Option Name,Option Value,Price,Currency,Availability,Variant Image URL,AI Merged Description,Deep Link\n";

  for (const v of variants) {
    const row = [
      v.brand,
      escapeCsvField(v.product_title),
      v.url,
      v.id,
      v.sku,
      v.option_name,
      v.option_value,
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
