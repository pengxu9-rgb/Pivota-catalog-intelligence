import { Router } from "express";
import { Readable } from "stream";

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
    if (typeof value === "string") {
      out[key] = value;
      continue;
    }
    if (Array.isArray(value)) {
      out[key] = value.join(",");
    }
  }
  return out;
}

function copyResponseHeaders(res: any, upstreamHeaders: Headers) {
  upstreamHeaders.forEach((value, key) => {
    const k = key.toLowerCase();
    if (k === "transfer-encoding") return;
    if (k === "connection") return;
    // Let Node handle content-length for streamed responses.
    if (k === "content-length") return;
    res.setHeader(key, value);
  });
}

export function createIngredientHarvesterProxyRouter() {
  const router = Router();

  router.all("*", async (req, res) => {
    const base = upstreamBaseUrl();
    const url = `${base}${req.url.startsWith("/") ? "" : "/"}${req.url}`;

    const controller = new AbortController();
    const timeoutMs = Number(process.env.INGREDIENT_HARVESTER_PROXY_TIMEOUT_MS || "60000");
    const timeout = setTimeout(() => controller.abort("timeout"), Math.max(1, timeoutMs));

    try {
      const method = String(req.method || "GET").toUpperCase();
      const hasBody = !["GET", "HEAD"].includes(method);

      const body =
        hasBody && req.body && Buffer.isBuffer(req.body) && req.body.length
          ? req.body
          : hasBody
            ? req
            : undefined;

      const init: any = {
        method,
        headers: copyRequestHeaders(req),
        body: body as any,
        redirect: "follow",
        signal: controller.signal,
      };
      if (hasBody && body && !Buffer.isBuffer(body)) {
        // Required by Node's fetch when streaming request bodies.
        init.duplex = "half";
      }

      const upstream = await fetch(url, init);

      res.status(upstream.status);
      copyResponseHeaders(res, upstream.headers);

      if (!upstream.body) {
        res.end();
        return;
      }

      const nodeStream = Readable.fromWeb(upstream.body as any);
      nodeStream.on("error", (err) => {
        // eslint-disable-next-line no-console
        console.error("[ingredient-harvester-proxy] stream error", err);
        if (!res.headersSent) res.status(502);
        res.end();
      });
      nodeStream.pipe(res);
    } catch (err: any) {
      // eslint-disable-next-line no-console
      console.error("[ingredient-harvester-proxy] error", err);
      res.status(502).json({ error: "Upstream harvester unavailable." });
    } finally {
      clearTimeout(timeout);
    }
  });

  return router;
}
