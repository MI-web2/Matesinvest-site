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

// Helper: try a list of candidate fields on the row and return first finite number
function tryNumberFields(obj, candidates = []) {
  for (const k of candidates) {
    if (!obj) continue;
    const v = obj[k];
    if (v === null || typeof v === "undefined") continue;
    // If string with percent, strip and parse
    if (typeof v === "string") {
      const str = v.trim();
      if (str.endsWith("%")) {
        const num = Number(str.slice(0, -1));
        if (Number.isFinite(num)) return num;
      }
      const parsed = Number(str.replace(/[, ]+/g, ""));
      if (Number.isFinite(parsed)) return parsed;
      continue;
    }
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
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

    const close = Number(r.close ?? r.Close ?? NaN);

    // Try to extract percent change robustly (handles "1.23%" or numeric)
    let changePct = tryNumberFields(r, [
      "change_p",
      "changeP",
      "ChangeP",
      "changePercent",
      "ChangePercent",
      "change_pct",
      "chgPercent",
      "percentage_change",
      "pct_change",
      "percent_change",
    ]);

    // Also try common absolute change fields (we use them later if percent is missing)
    const changeAbs = tryNumberFields(r, [
      "change",
      "Change",
      "chg",
      "delta",
      "diff",
      "priceChange",
    ]);

    // Try explicit prev/previous close fields first (many payloads include one of these)
    let prevClose = tryNumberFields(r, [
      "previousClose",
      "prevClose",
      "PrevClose",
      "previous_close",
      "prev_close",
      "previous_close_price",
      "previous_close_value",
      "previousclose",
      "previous",
      "yesterdayClose",
      "previouscloseprice"
    ]);

    // If prevClose not provided, derive it:
    if ((prevClose === null || prevClose === undefined) && Number.isFinite(close)) {
      // If we have an absolute change value, prefer that: prev = close - change
      if (changeAbs !== null && Number.isFinite(changeAbs)) {
        prevClose = close - Number(changeAbs);
      } else if (changePct !== null && Number.isFinite(changePct)) {
        // If percent is given as, e.g., 1.23 => denom = 1 + pct/100
        const denom = 1 + Number(changePct) / 100;
        if (denom !== 0) {
          prevClose = close / denom;
        }
      }
    }

    // Normalize changePct to a numeric value if still missing but we can compute from close & prevClose
    if ((changePct === null || changePct === undefined) && Number.isFinite(close) && Number.isFinite(prevClose) && prevClose !== 0) {
      changePct = ((close - prevClose) / prevClose) * 100;
    }

    // Validate numeric types
    const pct = Number.isFinite(changePct) ? Number(changePct) : null;
    const prev = Number.isFinite(prevClose) ? Number(prevClose) : null;
    const volume = Number(r.volume ?? r.Volume ?? NaN);

    rows.push({
      code: base,
      date: r.date || r.Date || null,
      close: Number.isFinite(close) ? Number(close) : null,
      prevClose: prev,
      pctChange: pct,
      volume:
        typeof volume === "number" && Number.isFinite(volume)
          ? Number(volume)
          : null,
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