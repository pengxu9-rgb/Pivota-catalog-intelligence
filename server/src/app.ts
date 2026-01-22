import cors from "cors";
import express from "express";

import { extractRouter } from "./routes/extract";

export function createApp() {
  const app = express();

  const allowedOrigins = (process.env.CORS_ORIGIN || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  app.use(
    cors({
      origin:
        allowedOrigins.length === 0
          ? true
          : (origin, cb) => {
              if (!origin) return cb(null, true);
              if (allowedOrigins.includes(origin)) return cb(null, true);
              return cb(new Error("Not allowed by CORS"));
            },
    }),
  );
  app.use(express.json({ limit: "2mb" }));

  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.use("/api", extractRouter);

  return app;
}
