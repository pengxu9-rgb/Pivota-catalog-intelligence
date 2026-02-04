# Ingredient Source Harvester (PCI)

FastAPI + RQ service for batch harvesting `raw_ingredient_text` for beauty/personal-care SKUs.

## Local dev

```bash
cd Pivota-catalog-intelligence/ingredient-harvester
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# (optional) Redis for queue
# export REDIS_URL="redis://localhost:6379/0"

export HARVESTER_DB_URL="sqlite:///./harvester.sqlite3"
export HARVESTER_API_CORS_ORIGINS="http://localhost:3000"

uvicorn app.main:app --host 0.0.0.0 --port 8008 --reload
```

Run a worker (RQ):

```bash
cd Pivota-catalog-intelligence/ingredient-harvester
source .venv/bin/activate
export HARVESTER_DB_URL="sqlite:///./harvester.sqlite3"
export REDIS_URL="redis://localhost:6379/0"
python -m app.worker
```

## Env vars

- `HARVESTER_DB_URL`: SQLAlchemy DB URL (default: `sqlite:///./harvester.sqlite3`); falls back to `DATABASE_URL` if present (Railway Postgres).
- `HARVESTER_API_CORS_ORIGINS`: comma-separated origins for CORS (default: `*`)
- `REDIS_URL`: enable RQ queue + worker
- `HARVESTER_QUEUE_MODE`: `rq` (default if `REDIS_URL` set) | `inline` (run jobs in API process)
- `SERPER_API_KEY`: Serper.dev key (optional)
- `SERPAPI_API_KEY` (or `SERP_API_KEY`): SerpAPI key (optional)
- `GOOGLE_CSE_API_KEY`, `GOOGLE_CSE_ID`: Google Custom Search (optional)
