import { Router } from "express";

function normalizeBaseUrl(raw: string) {
  let url = raw.trim().replace(/\/+$/g, "");
  if (!url) return url;
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(url)) {
    const isLocal = url.startsWith("localhost") || url.startsWith("127.0.0.1") || url.startsWith("0.0.0.0");
    url = `${isLocal ? "http" : "https"}://${url}`;
  }
  return url;
}

function upstreamBaseUrl() {
  return normalizeBaseUrl(process.env.INGREDIENT_HARVESTER_BASE_URL || "http://localhost:8008");
}

function copyRequestHeaders(req: any) {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(req.headers || {})) {
    const k = String(key).toLowerCase();
    if (k === "host") continue;
    if (k === "connection") continue;
    if (k === "content-length") continue;
    if (typeof value === "string") out[key] = value;
    else if (Array.isArray(value)) out[key] = value.join(",");
  }
  return out;
}

function copyResponseHeaders(res: any, upstreamHeaders: Headers) {
  upstreamHeaders.forEach((value, key) => {
    const k = key.toLowerCase();
    if (k === "transfer-encoding") return;
    if (k === "connection") return;
    if (k === "content-length") return;
    res.setHeader(key, value);
  });
}

export const productsRouter = Router();

productsRouter.post("/:id/update", async (req, res) => {
  const base = upstreamBaseUrl();
  const url = `${base}/v1/rows/${encodeURIComponent(String(req.params.id || ""))}`;

  try {
    const upstream = await fetch(url, {
      method: "PATCH",
      headers: { ...copyRequestHeaders(req), "Content-Type": "application/json" },
      body: JSON.stringify(req.body || {}),
    });

    res.status(upstream.status);
    copyResponseHeaders(res, upstream.headers);
    const text = await upstream.text();
    res.send(text);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("[/api/products] proxy error", err);
    res.status(502).json({ error: "Upstream harvester unavailable." });
  }
});

