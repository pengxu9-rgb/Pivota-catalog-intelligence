FROM node:22-bookworm-slim

ENV NODE_ENV=production \
    PORT=8080 \
    PUPPETEER_CACHE_DIR=/opt/puppeteer

WORKDIR /app

# Puppeteer downloads Chrome for Testing during npm ci. These shared libraries are
# needed by that browser at runtime in the Cloud Run container.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
# NODE_ENV is production for the final runtime, but TypeScript and type
# declarations are build-time dev dependencies.
RUN npm ci --include=dev

COPY . ./
RUN npm run build \
  && npm prune --omit=dev \
  && chown -R node:node /app /opt/puppeteer

USER node

CMD ["node", "dist/index.js"]
