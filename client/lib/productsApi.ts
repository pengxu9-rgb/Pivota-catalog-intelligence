const DEFAULT_BASE = "http://localhost:3001/api/products";

function baseUrl() {
  const explicit = process.env.NEXT_PUBLIC_PRODUCTS_API_BASE_URL;
  if (explicit) return explicit;

  const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (apiBase) return `${String(apiBase).trim().replace(/\/+$/g, "")}/api/products`;

  return DEFAULT_BASE;
}

async function assertOk(res: Response, url: string) {
  if (res.ok) return;
  const rid = res.headers.get("x-harvester-request-id");
  const text = await res.text().catch(() => "");
  const snippet = text && text.length > 800 ? `${text.slice(0, 800)}…` : text;
  const where = rid ? `${url} [${rid}]` : url;
  throw new Error(`Products API error (${res.status}) at ${where}: ${snippet || res.statusText}`);
}

export async function updateProduct(productId: string, patch: Record<string, unknown>) {
  const url = `${baseUrl()}/${encodeURIComponent(productId)}/update`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  await assertOk(res, url);
  return res.json();
}

