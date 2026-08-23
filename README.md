# Pivota Catalog Intelligence

Monorepo:
- `client/` — Next.js 14 + Tailwind UI
- `server/` — Express + TypeScript API (`POST /api/extract`)
- `ingredient-harvester/` — FastAPI + RQ ingredient harvesting service

## Local dev

Backend:
```bash
cd server
npm install
npm run dev
```

Ingredient harvester (Python):
```bash
cd ingredient-harvester
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export HARVESTER_DB_URL="sqlite:///./harvester.sqlite3"
export HARVESTER_API_CORS_ORIGINS="http://localhost:3000"
# Optional: set SERPER_API_KEY or GOOGLE_CSE_API_KEY/GOOGLE_CSE_ID for search.

uvicorn app.main:app --host 0.0.0.0 --port 8008 --reload
```

Frontend:
```bash
cd client
npm install
npm run dev
```

## Deploy

Backend (GCP Cloud Run):
- Service name: `catalog-intelligence` (region: `us-west1`).
- Build from `server/`; the checked-in Dockerfile includes the Chromium runtime
  libraries required by Puppeteer.
- Runtime env: `EXTRACTION_MODE=puppeteer`, `REMOTE_BROWSER_ENABLED=0`,
  `CORS_ORIGIN=<approved frontend origins>`, and the bounded Puppeteer settings
  below. Keep Cloud Run unauthenticated only while the browser client consumes
  this API directly; do not place secrets in the service environment unless an
  extractor feature requires them.
- Before changing the frontend API base URL, run the production smoke audit
  against `/healthz` and an approved single-PDP extraction. Retain Railway as
  the rollback endpoint until the GCP endpoint passes those checks.

Backend (legacy Railway; rollback only during migration):
- Root directory: `server`
- Build command: `npm run build`
- Start command: `npm start`
- Env: `CORS_ORIGIN=<your-vercel-url>`, `EXTRACTION_MODE=puppeteer`
- `EXTRACTION_MODE=simulation` is for explicit local/test-only use. In deployed environments the extractor now fails closed if simulation mode is requested without explicit opt-in.
- Optional: `MAX_PRODUCTS=50`, `PUPPETEER_CONCURRENCY=2`, `PUPPETEER_NAV_TIMEOUT_MS=30000`, `SHOPIFY_VARIANT_DISCOVERY=auto`

Frontend (Vercel):
- Root directory: `client`
- Env:
  - `NEXT_PUBLIC_API_BASE_URL=<your-railway-backend-url>`
  - (optional) `NEXT_PUBLIC_INGREDIENT_HARVESTER_BASE_URL=<your-backend-url>/api/ingredient-harvester` (proxy; recommended)

Ingredient harvester (Railway/Docker):
- Root directory: `ingredient-harvester`
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Env: `HARVESTER_DB_URL` (or `DATABASE_URL`), `HARVESTER_API_CORS_ORIGINS`, search keys
- For production (esp. with a worker), use Postgres instead of SQLite (SQLite is not shared across services).
- Optional async queue: add Redis + set `REDIS_URL`, then run a separate worker with `python -m app.worker`

Ingredient harvester proxy (Express server):
- The `server/` app exposes a proxy at `POST/GET/PATCH /api/ingredient-harvester/*`.
- Configure `INGREDIENT_HARVESTER_BASE_URL=<your-harvester-url>` on the `server` Railway service.
