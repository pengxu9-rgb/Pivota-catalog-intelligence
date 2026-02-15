import type { MarketId, MarketProfile } from "./types";

export const DEFAULT_MARKET_ID = "US";

const PROFILE_MAP: Record<string, MarketProfile> = {
  US: {
    market_id: "US",
    country: "US",
    currency_target: "USD",
    locale: "en-US",
    headers: {
      "Accept-Language": "en-US,en;q=0.9",
    },
    cookies: {
      localization: "US",
      cart_currency: "USD",
      country: "US",
      locale: "en-US",
    },
    url_params: {
      country: "US",
      locale: "en-US",
      currency: "USD",
    },
    geo_hint: "US",
  },
  "EU-DE": {
    market_id: "EU-DE",
    country: "DE",
    currency_target: "EUR",
    locale: "de-DE",
    headers: {
      "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
    },
    cookies: {
      localization: "DE",
      cart_currency: "EUR",
      country: "DE",
      locale: "de-DE",
    },
    url_params: {
      country: "DE",
      locale: "de-DE",
      currency: "EUR",
    },
    geo_hint: "DE",
  },
  SG: {
    market_id: "SG",
    country: "SG",
    currency_target: "SGD",
    locale: "en-SG",
    headers: {
      "Accept-Language": "en-SG,en;q=0.9",
    },
    cookies: {
      localization: "SG",
      cart_currency: "SGD",
      country: "SG",
      locale: "en-SG",
    },
    url_params: {
      country: "SG",
      locale: "en-SG",
      currency: "SGD",
    },
    shipping_destination: "SG",
    geo_hint: "SG",
  },
  JP: {
    market_id: "JP",
    country: "JP",
    currency_target: "JPY",
    locale: "ja-JP",
    headers: {
      "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.6",
    },
    cookies: {
      localization: "JP",
      cart_currency: "JPY",
      country: "JP",
      locale: "ja-JP",
      shipping_destination: "JP",
    },
    url_params: {
      country: "JP",
      locale: "ja-JP",
      currency: "JPY",
      shipping_destination: "JP",
    },
    shipping_destination: "JP",
    geo_hint: "JP",
  },
};

function normalizeMarketId(value: string): string {
  return value.trim().toUpperCase();
}

export function getMarketProfile(marketId: MarketId): MarketProfile {
  const key = normalizeMarketId(marketId);
  const profile = PROFILE_MAP[key] || PROFILE_MAP[DEFAULT_MARKET_ID];
  return {
    ...profile,
    headers: { ...profile.headers },
    cookies: { ...profile.cookies },
    url_params: { ...profile.url_params },
  };
}

export function getMarketProfiles(markets: MarketId[] | undefined): MarketProfile[] {
  if (!markets || markets.length === 0) return [getMarketProfile(DEFAULT_MARKET_ID)];

  const seen = new Set<string>();
  const out: MarketProfile[] = [];

  for (const market of markets) {
    const key = normalizeMarketId(market);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(getMarketProfile(key));
  }

  return out.length > 0 ? out : [getMarketProfile(DEFAULT_MARKET_ID)];
}

export function getSupportedMarketIds(): string[] {
  return Object.keys(PROFILE_MAP);
}
