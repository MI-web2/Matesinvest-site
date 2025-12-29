// netlify/functions/market-pulse-read.js
//
// Fast reader: serves precomputed market pulse window caches.
// ?period=1d|5d|1m (defaults to 1d)

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const WIN_KEYS = {
  "1d": "asx:market:pulse:window:1d",
  "5d": "asx:market:pulse:window:5d",
  "1m": "asx:market:pulse:window:1m",
};

async function redisGet(key) {
  try {
    const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j?.result ?? null;
  } catch {
    return null;
  }
}

exports.handler = async function (event) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  const periodRaw = event?.queryStringParameters?.period;
  const period = (periodRaw ? String(periodRaw) : "1d").toLowerCase();
  const key = WIN_KEYS[period] || WIN_KEYS["1d"];

  const raw = await redisGet(key);

  if (!raw) {
    return {
      statusCode: 503,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store",
      },
      body: JSON.stringify({ error: `Market pulse cache not ready (${period})` }),
    };
  }

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store",
    },
    body: typeof raw === "string" ? raw : JSON.stringify(raw),
  };
};
