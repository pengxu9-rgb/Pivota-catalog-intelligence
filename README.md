# Pivota Catalog Intelligence

Monorepo:
- `client/` — Next.js 14 + Tailwind UI
- `server/` — Express + TypeScript API (`POST /api/extract`)

## Local dev

Backend:
```bash
cd server
npm install
npm run dev
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
- Env: `CORS_ORIGIN=<your-vercel-url>`, `EXTRACTION_MODE=simulation`

Frontend (Vercel):
- Root directory: `client`
- Env: `NEXT_PUBLIC_API_BASE_URL=<your-railway-backend-url>`
