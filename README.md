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

Backend (Railway):
- Root directory: `server`
- Build command: `npm run build`
- Start command: `npm start`
- Env: `CORS_ORIGIN=<your-vercel-url>`, `EXTRACTION_MODE=simulation` (or `puppeteer` for real extraction)
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
