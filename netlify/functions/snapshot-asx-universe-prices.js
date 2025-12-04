// netlify/functions/snapshot-asx-universe-prices.js
//
// Nightly snapshot of last close prices for the full ASX universe.
// Uses EODHD bulk last-day endpoint and derives previous close + pct change.
//
// Stores into Upstash as:
//   asx:universe:eod:YYYY-MM-DD
//   asx:universe:eod:latest

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function fetchWithTimeout(url, opts = {}, timeout = 20000) {
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

// Normalize symbols like "BHP.AX" -> "BHP"
function normalizeCode(code) {
  return String(code || "")
    .replace(/\.[A-Z0-9]{1,6}$/i, "")
    .toUpperCase();
}

// Read asx-universe.txt so we only keep codes we care about
function readUniverseSync() {
  const candidates = [
    path.join(__dirname, "asx-universe.txt"),
    path.join(process.cwd(), "netlify", "functions", "asx-universe.txt"),
  ];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw
          .split(/[\s,]+/)
          .map((s) => s.trim())
          .filter(Boolean);
        console.log(
          `[snapshot-asx-universe-prices] using universe file: ${p} (entries=${parts.length})`
        );
        return new Set(parts.map((c) => c.toUpperCase()));
      }
    } catch (err) {
      console.warn(
        `[snapshot-asx-universe-prices] universe read failed for ${p}: ${
          err && err.message
        }`
      );
    }
  }

  throw new Error(
    "asx-universe.txt not found next to this function (or in netlify/functions)."
  );
}

async function redisSet(urlBase, token, key, value, ttlSeconds) {
  if (!urlBase || !token) return false;
  try {
    const payload =
      typeof value === "string" ? value : JSON.stringify(value);
    const ttl = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : "";
    const res = await fetchWithTimeout(
      `${urlBase}/set/${encodeURIComponent(key)}/${encodeURIComponent(
        payload
      )}${ttl}`,
      { method: "POST", headers: { Authorization: `Bearer ${token}` } },
      15000
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn(
        "redisSet failed",
        key,
        res.status,
        txt && txt.slice(0, 200)
      );
      return false;
    }
    return true;
  } catch (err) {
    console.warn("redisSet error", key, err && err.message);
    return false;
  }
}

exports.handler = async function () {
  const start = Date.now();

  if (!EODHD_TOKEN || !UPSTASH_URL || !UPSTASH_TOKEN) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Missing EODHD_API_TOKEN or Upstash env vars",
      }),
    };
  }

  let universeSet;
  try {
    universeSet = readUniverseSync();
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }

  // Bulk last-day prices for AU
  const url = `https://eodhd.com/api/eod-bulk-last-day/AU?api_token=${encodeURIComponent(
    EODHD_TOKEN
  )}&fmt=json`;

  let rawArray;
  try {
    const res = await fetchWithTimeout(url, {}, 30000);
    const text = await res.text().catch(() => "");
    if (!res.ok) {
      return {
        statusCode: 502,
        body: JSON.stringify({
          error: "Bulk EOD request failed",
          status: res.status,
          body: text && text.slice(0, 200),
        }),
      };
    }
    rawArray = text ? JSON.parse(text) : [];
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Bulk EOD fetch error",
        detail: err && err.message,
      }),
    };
  }

  const rows = [];

  for (const r of rawArray) {
    if (!r) continue;

    // EODHD sometimes uses Code / code / Symbol / symbol etc
    const rawCode =
      r.Code || r.code || r.Symbol || r.symbol || r.ticker || r.Ticker;
    const base = normalizeCode(rawCode);
    if (!base || !universeSet.has(base)) continue;

    // Close price
    const close = Number(r.close ?? r.Close ?? NaN);
    const closeVal =
      typeof close === "number" && Number.isFinite(close)
        ? Number(close)
        : null;

    // Raw change percent if provided
    let changePct = Number(
      r.change_p ??
        r.changeP ??
        r.ChangeP ??
        r.changePercent ??
        r.ChangePercent ??
        NaN
    );
    if (!Number.isFinite(changePct)) changePct = null;

    // Previous close straight from API if present
    let prevRaw =
      r.previousClose ??
      r.PreviousClose ??
      r.previous_close ??
      r.prev_close ??
      r.Previous_Close ??
      null;
    let prevClose = Number(prevRaw ?? NaN);
    if (!Number.isFinite(prevClose)) prevClose = null;

    // If we don't have prevClose but we *do* have changePct, derive it
    if (
      prevClose === null &&
      closeVal !== null &&
      changePct !== null &&
      changePct !== -100
    ) {
      const denom = 1 + changePct / 100;
      if (denom !== 0) {
        const calcPrev = closeVal / denom;
        if (Number.isFinite(calcPrev)) prevClose = calcPrev;
      }
    }

    // If we have close + prevClose but no pctChange, derive pctChange
    if (
      changePct === null &&
      closeVal !== null &&
      prevClose !== null &&
      prevClose !== 0
    ) {
      const pc = ((closeVal - prevClose) / prevClose) * 100;
      if (Number.isFinite(pc)) changePct = pc;
    }

    const volume = Number(r.volume ?? r.Volume ?? NaN);
    const volVal =
      typeof volume === "number" && Number.isFinite(volume)
        ? Number(volume)
        : null;

    rows.push({
      code: base,
      date: r.date || r.Date || null,
      close: closeVal,
      prevClose:
        typeof prevClose === "number" && Number.isFinite(prevClose)
          ? Number(prevClose)
          : null,
      pctChange:
        typeof changePct === "number" && Number.isFinite(changePct)
          ? Number(changePct)
          : null,
      volume: volVal,
    });
  }

  // Use the date from the first row, or today as fallback
  const snapshotDate =
    (rows[0] && rows[0].date) || new Date().toISOString().slice(0, 10);

  const dailyKey = `asx:universe:eod:${snapshotDate}`;
  const latestKey = `asx:universe:eod:latest`;

  const okDaily = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, dailyKey, rows);
  const okLatest = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestKey, rows);

  const ok = okDaily && okLatest;

  return {
    statusCode: ok ? 200 : 500,
    body: JSON.stringify({
      ok,
      snapshotDate,
      dailyKey,
      latestKey,
      rows: rows.length,
      elapsedMs: Date.now() - start,
    }),
  };
};
