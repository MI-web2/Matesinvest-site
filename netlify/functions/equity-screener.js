// netlify/functions/equity-screener.js
//
// Lightweight screener endpoint.
// - Reads precomputed fundamentals from Upstash:
//      asx:universe:fundamentals:latest
// - Optionally can filter by code query (?code=BHP) or inAsx200 flag later.
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
  const codeFilter = qs.code
    ? String(qs.code).trim().toUpperCase()
    : null;
  const onlyAsx200 =
    qs.asx200 === "1" || qs.asx200 === "true" || qs.index === "asx200";

  try {
    const raw = await redisGet(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      "asx:universe:fundamentals:latest"
    );
    if (!raw) {
      return json(503, {
        error: "No screener snapshot available yet",
      });
    }

    const parsed =
      typeof raw === "string" ? JSON.parse(raw) : raw;

    let items = Array.isArray(parsed.items) ? parsed.items : [];

    // Optional query filters
    if (codeFilter) {
      items = items.filter((it) => it.code === codeFilter);
    }
    if (onlyAsx200) {
      items = items.filter((it) => it.inAsx200 === 1);
    }

    return json(200, {
      generatedAt: parsed.generatedAt || null,
      universeSize: parsed.universeSize || items.length,
      count: items.length,
      items,
    });
  } catch (err) {
    console.error("equity-screener error", err);
    return json(500, {
      error: "Failed to read screener dataset",
      detail: String(err),
    });
  }
};
