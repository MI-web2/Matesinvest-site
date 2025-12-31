// netlify/functions/snapshot-asx200-fundamentals.js
//
// Snapshot for ASX200 fundamentals only.
// - For each ticker, fetches EODHD fundamentals
// - Extracts market cap, PE, dividend yield (and leaves room for more)
// - Stores a single map in Upstash:
//     asx200:fundamentals:latest
//     asx200:fundamentals:YYYY-MM-DD
//
// Env:
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//   TRY_SUFFIXES (optional, default "AU,AX,ASX")

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];

function fetchWithTimeout(url, opts = {}, timeout = 12000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  return fetch(url, { ...opts, signal: controller.signal })
    .finally(() => clearTimeout(id));
}

// Simple AEST YYYY-MM-DD for keys
function getAestDateString(date = new Date()) {
  const AEST_OFFSET_MINUTES = 10 * 60; // Brisbane UTC+10 year-round
  const aestTime = new Date(date.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
  return aestTime.toISOString().slice(0, 10);
}

function normalizeCode(code) {
  return String(code || "")
    .replace(/\.[A-Z0-9]{1,6}$/i, "")
    .toUpperCase();
}

// Helper to safely convert to number, returning null for NaN
function toFiniteNumber(v) {
  if (v === null || typeof v === "undefined" || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function readAsx200ListSync() {
  const candidates = [
    path.join(__dirname, "asx200.txt"),
    path.join(__dirname, "asx200"),
    path.join(process.cwd(), "data", "asx200.txt"),
    path.join(process.cwd(), "data", "asx200"),
  ];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw
          .split(",")
          .join("\n")
          .split("\n")
          .map((s) => s.trim())
          .filter(Boolean);
        console.log(`[snapshot-asx200-fundamentals] using file: ${p}`);
        return parts.map((x) => x.toUpperCase());
      }
    } catch (err) {
      console.warn(
        `[snapshot-asx200-fundamentals] read failure for ${p}:`,
        err && err.message
      );
    }
  }

  throw new Error(
    "Failed to read asx200 list: asx200.txt not found in expected locations."
  );
}

async function redisSet(urlBase, token, key, value, ttlSeconds) {
  if (!urlBase || !token) return false;
  try {
    const payload = typeof value === "string" ? value : JSON.stringify(value);
    const ttl = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : "";
    const res = await fetchWithTimeout(
      `${urlBase}/set/${encodeURIComponent(key)}/${encodeURIComponent(
        payload
      )}${ttl}`,
      { method: "POST", headers: { Authorization: `Bearer ${token}` } },
      10000
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn(
        "redisSet failed",
        key,
        res.status,
        txt && txt.slice(0, 300)
      );
      return false;
    }
    return true;
  } catch (err) {
    console.warn("redisSet error", key, err && err.message);
    return false;
  }
}

async function fetchFundamentals(EODHD_TOKEN, code, trySuffixes) {
  let lastErr = null;

  for (const suffix of trySuffixes) {
    const symbol = `${code}.${suffix}`;
    const url = `https://eodhd.com/api/fundamentals/${encodeURIComponent(
      symbol
    )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;

    try {
      const res = await fetchWithTimeout(url, {}, 15000);
      const text = await res.text().catch(() => "");
      if (!res.ok) {
        lastErr = `${res.status} ${text && text.slice(0, 120)}`;
        // if 404, try next suffix; otherwise bail
        if (res.status !== 404) break;
        continue;
      }
      try {
        const json = text ? JSON.parse(text) : null;
        return json || null;
      } catch (e) {
        lastErr = e && e.message;
        break;
      }
    } catch (err) {
      lastErr = err && err.message;
      break;
    }
  }

  console.warn("fundamentals failed for", code, lastErr);
  return null;
}

exports.handler = async function () {
  const start = Date.now();
  const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  if (!EODHD_TOKEN) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }),
    };
  }
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing Upstash env" }),
    };
  }

  const TRY_SUFFIXES = (
    process.env.TRY_SUFFIXES
      ? process.env.TRY_SUFFIXES.split(",")
      : DEFAULT_TRY_SUFFIXES
  )
    .map((s) => s.trim())
    .filter(Boolean);

  let tickers;
  try {
    tickers = readAsx200ListSync();
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message }),
    };
  }

  // Fundamentals change slowly; basic sequential loop is fine
  const fundamentalsMap = {};
  const failures = [];

for (const rawCode of tickers) {
  const base = normalizeCode(rawCode);
  const data = await fetchFundamentals(EODHD_TOKEN, base, TRY_SUFFIXES);
  if (!data) {
    failures.push(base);
    continue;
  }

  const h = (data && data.Highlights) || {};
  const v = (data && data.Valuation) || {};

  fundamentalsMap[base] = {
    // === Financial highlights / key metrics ===
    marketCap: h.MarketCapitalization != null ? Number(h.MarketCapitalization) : null,
    ebitda: h.EBITDA != null ? Number(h.EBITDA) : null,
    peRatio: h.PERatio != null ? Number(h.PERatio) : null,
    pegRatio: h.PEGRatio != null ? Number(h.PEGRatio) : null,

    eps: h.EarningsShare != null ? Number(h.EarningsShare) : null,
    bookValue: h.BookValue != null ? Number(h.BookValue) : null,
    dividendPerShare: h.DividendShare != null ? Number(h.DividendShare) : null,
    dividendYield: h.DividendYield != null ? Number(h.DividendYield) : null,

    profitMargin: h.ProfitMargin != null ? Number(h.ProfitMargin) : null,
    operatingMargin:
      h.OperatingMarginTTM != null ? Number(h.OperatingMarginTTM) : null,
    returnOnAssets:
      h.ReturnOnAssetsTTM != null ? Number(h.ReturnOnAssetsTTM) : null,
    returnOnEquity:
      h.ReturnOnEquityTTM != null ? Number(h.ReturnOnEquityTTM) : null,

    revenue: h.RevenueTTM != null ? Number(h.RevenueTTM) : null,
    revenuePerShare:
      h.RevenuePerShareTTM != null ? Number(h.RevenuePerShareTTM) : null,
    grossProfit: h.GrossProfitTTM != null ? Number(h.GrossProfitTTM) : null,
    dilutedEps: h.DilutedEpsTTM != null ? Number(h.DilutedEpsTTM) : null,
    quarterlyRevenueGrowthYoy:
      h.QuarterlyRevenueGrowthYOY != null
        ? Number(h.QuarterlyRevenueGrowthYOY)
        : null,
    quarterlyEarningsGrowthYoy:
      h.QuarterlyEarningsGrowthYOY != null
        ? Number(h.QuarterlyEarningsGrowthYOY)
        : null,

    // === Valuation metrics ===
    trailingPE: v.TrailingPE != null ? toFiniteNumber(v.TrailingPE) : null,
    forwardPE: v.ForwardPE != null ? toFiniteNumber(v.ForwardPE) : null,
    priceToSales: v.PriceSalesTTM != null ? toFiniteNumber(v.PriceSalesTTM) : null,
    priceToBook: v.PriceBookMRQ != null ? toFiniteNumber(v.PriceBookMRQ) : null,
    enterpriseValue:
      v.EnterpriseValue != null ? toFiniteNumber(v.EnterpriseValue) : null,
    evToRevenue:
      v.EnterpriseValueRevenue != null ? toFiniteNumber(v.EnterpriseValueRevenue) : null,
    evToEbitda:
      v.EnterpriseValueEbitda != null ? toFiniteNumber(v.EnterpriseValueEbitda) : null,
  };
}


  const asOfDate = getAestDateString();
  const dailyKey = `asx200:fundamentals:${asOfDate}`;
  const latestKey = `asx200:fundamentals:latest`;

  const okDaily = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, dailyKey, fundamentalsMap);
  const okLatest = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestKey, fundamentalsMap);

  return {
    statusCode: 200,
    body: JSON.stringify({
      ok: okDaily && okLatest,
      asOfDate,
      requested: tickers.length,
      rows: Object.keys(fundamentalsMap).length,
      failures: failures.slice(0, 30),
      elapsedMs: Date.now() - start,
    }),
  };
};
