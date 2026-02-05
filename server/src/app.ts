import cors from "cors";
import express from "express";

import { extractRouter } from "./routes/extract";
import { createIngredientHarvesterProxyRouter } from "./routes/ingredientHarvesterProxy";
import { parserRouter } from "./routes/parser";
import { productsRouter } from "./routes/products";

type CorsRule =
  | { kind: "any" }
  | { kind: "origin"; origin: string }
  | { kind: "host"; host: string }
  | { kind: "hostname"; hostname: string }
  | { kind: "suffix"; suffix: string };

function normalizeToken(value: string) {
  return value.trim().replace(/\/+$/g, "");
}

function parseCorsRules(raw: string | undefined): CorsRule[] {
  const tokens = (raw || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map(normalizeToken);

  const rules: CorsRule[] = [];

  for (const token of tokens) {
    if (token === "*") {
      rules.push({ kind: "any" });
      continue;
    }

    if (token.startsWith("*.")) {
      const suffix = token.slice(2).toLowerCase();
      if (suffix) rules.push({ kind: "suffix", suffix });
      continue;
    }

    if (token.includes("://")) {
      try {
        const u = new URL(token);
        rules.push({ kind: "origin", origin: u.origin });
      } catch {
        // Fall back to exact string compare (still normalized above).
        rules.push({ kind: "origin", origin: token });
      }
      continue;
    }

    const lower = token.toLowerCase();
    if (lower.includes(":")) {
      rules.push({ kind: "host", host: lower });
      continue;
    }

    rules.push({ kind: "hostname", hostname: lower });
  }

  return rules;
}

function isOriginAllowed(origin: string | undefined, rules: CorsRule[]): boolean {
  if (rules.length === 0) return true;
  if (!origin) return true;

  let url: URL;
  try {
    url = new URL(origin);
  } catch {
    return false;
  }

  const normalizedOrigin = url.origin;
  const host = url.host.toLowerCase();
  const hostname = url.hostname.toLowerCase();

  for (const rule of rules) {
    if (rule.kind === "any") return true;
    if (rule.kind === "origin" && normalizedOrigin === rule.origin) return true;
    if (rule.kind === "host" && host === rule.host) return true;
    if (rule.kind === "hostname" && hostname === rule.hostname) return true;
    if (rule.kind === "suffix" && (hostname === rule.suffix || hostname.endsWith(`.${rule.suffix}`))) return true;
  }

  return false;
}

export function createApp() {
  const app = express();

  const corsRules = parseCorsRules(process.env.CORS_ORIGIN);
  const corsMiddleware = cors({
    origin: (origin, cb) => cb(null, isOriginAllowed(origin, corsRules)),
    optionsSuccessStatus: 204,
  });

  app.use(corsMiddleware);
  app.options("*", corsMiddleware);
  app.use("/api/ingredient-harvester", createIngredientHarvesterProxyRouter());
  app.use(express.json({ limit: "2mb" }));
  app.use("/api/parser", parserRouter);
  app.use("/api/products", productsRouter);

  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.use("/api", extractRouter);

  return app;
}
