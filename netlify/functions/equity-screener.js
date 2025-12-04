// netlify/functions/equity-screener.js
//
// Lightweight screener endpoint.
// - Reads precomputed fundamentals from Upstash:
//      asx:universe:fundamentals:latest
// - Reads latest prices from Upstash:
//      asx:universe:eod:latest
// - Optionally can filter by code query (?code=BHP) or inAsx200 flag.
// - Optional screener-level ETF exclusion via ?excludeETF=1 or env EXCLUDE_ETF_DEFAULT=1
//
// This function does *not* call EODHD directly, so it's cheap and fast per request.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

async function fetchWithTimeout(url, opts = {}, timeout = 8000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
}

async function redisGet(urlBase, token, key) {
  if (!urlBase || !token) return null;
  try {
    const res = await fetchWithTimeout(
      `${urlBase}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${token}` } },
      10000
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j && typeof j.result !== "undefined" ? j.result : null;
  } catch (err) {
    console.warn("redisGet error", key, err && err.message);
    return null;
  }
}

function json(statusCode, bodyObj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify(bodyObj),
  };
}

// Helper: detect names that end with "ETF" (case-insensitive, tolerant of punctuation)
function isEtfName(name) {
  if (!name || typeof name !== "string") return false;
  const cleaned = name
    .replace(/[\u2013\u2014–—\-()]/g, " ")
    .replace(/[\s\.,;:]+/g, " ")
    .trim();
  return /\bETF$/i.test(cleaned);
}

// ---------------------------
// Helpers to load snapshots
// ---------------------------

async function getUniverseFundamentals() {
  const raw = await redisGet(
    UPSTASH_URL,
    UPSTASH_TOKEN,
    "asx:universe:fundamentals:latest"
  );
  if (!raw) return null;
  const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (!parsed || !Array.isArray(parsed.items)) return null;
  return parsed;
}

// Price snapshot is stored as an array of rows:
//   [{ code, date, close, prevClose, pctChange, volume }, ...]
async function getUniversePriceMap() {
  const raw = await redisGet(
    UPSTASH_URL,
    UPSTASH_TOKEN,
    "asx:universe:eod:latest"
  );
  if (!raw) return null;

  let arr = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (!Array.isArray(arr)) return null;

  const map = {};
  for (const row of arr) {
    if (!row || !row.code) continue;
    const code = String(row.code).toUpperCase();
    map[code] = {
      date: row.date || null,
      close:
        typeof row.close === "number" && Number.isFinite(row.close)
          ? row.close
          : null,
      prevClose:
        typeof row.prevClose === "number" && Number.isFinite(row.prevClose)
          ? row.prevClose
          : null,
      pctChange:
        typeof row.pctChange === "number" && Number.isFinite(row.pctChange)
          ? row.pctChange
          : null,
      volume:
        typeof row.volume === "number" && Number.isFinite(row.volume)
          ? row.volume
          : null,
    };
  }
  return map;
}

exports.handler = async function (event) {
  // CORS preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
      },
      body: "",
    };
  }

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return json(500, {
      error: "Missing Upstash env for screener",
    });
  }

  const qs = event.queryStringParameters || {};
  const codeFilter = qs.code ? String(qs.code).trim().toUpperCase() : null;
  const onlyAsx200 =
    qs.asx200 === "1" || qs.asx200 === "true" || qs.index === "asx200";

  // screener-level ETF exclusion: per-request override or env default
  const EXCLUDE_ETF_DEFAULT = String(process.env.EXCLUDE_ETF_DEFAULT || "0") === "1";
  const excludeETF =
    qs.excludeETF === "1" ||
    qs.excludeETF === "true" ||
    EXCLUDE_ETF_DEFAULT;

  try {
    // Load fundamentals snapshot
    const fundamentals = await getUniverseFundamentals();
    if (!fundamentals) {
      return json(503, {
        error: "No screener fundamentals snapshot available yet",
      });
    }

    let items = Array.isArray(fundamentals.items)
      ? fundamentals.items.slice()
      : [];

    // Apply screener-level ETF exclusion early (so price merge skips them)
    if (excludeETF) {
      items = items.filter((it) => !isEtfName(it.name));
    }

    // Load prices (best-effort; if missing we just fall back to fundamentals)
    let priceMap = null;
    try {
      priceMap = await getUniversePriceMap();
    } catch (e) {
      console.warn("equity-screener: failed to load price map", e && e.message);
      priceMap = null;
    }

    // Merge price data into each item (without mutating the original)
    if (priceMap) {
      items = items.map((it) => {
        const code = String(it.code || "").toUpperCase();
        const p = priceMap[code];

        if (!p) {
          // No dedicated price row; just return fundamentals row as-is
          return it;
        }

        const mergedPrice =
          typeof p.close === "number" && Number.isFinite(p.close)
            ? p.close
            : it.price ?? null;

        const mergedPct =
          typeof p.pctChange === "number" && Number.isFinite(p.pctChange)
            ? p.pctChange
            : typeof it.pctChange === "number" && Number.isFinite(it.pctChange)
            ? it.pctChange
            : null;

        return {
          ...it,
          price: mergedPrice,
          pctChange: mergedPct,
          lastDate: p.date || it.lastDate || null,
          yesterdayPrice:
            typeof p.prevClose === "number" && Number.isFinite(p.prevClose)
              ? p.prevClose
              : it.yesterdayPrice ?? null,
          volume:
            typeof p.volume === "number" && Number.isFinite(p.volume)
              ? p.volume
              : it.volume ?? null,
        };
      });
    }

    // Optional query filters
    if (codeFilter) {
      items = items.filter((it) => String(it.code).toUpperCase() === codeFilter);
    }
    if (onlyAsx200) {
      items = items.filter((it) => it.inAsx200 === 1);
    }

    // Normalize universeSize field for compatibility
    const universeSize =
      fundamentals.universeSize || fundamentals.universeTotal || items.length;

    return json(200, {
      generatedAt: fundamentals.generatedAt || null,
      universeSize,
      count: items.length,
      items,
      // echo the exclusion mode so callers can see it in responses
      excludeETF: !!excludeETF,
    });
  } catch (err) {
    console.error("equity-screener error", err);
    return json(500, {
      error: "Failed to read screener dataset",
      detail: String(err),
    });
  }
};