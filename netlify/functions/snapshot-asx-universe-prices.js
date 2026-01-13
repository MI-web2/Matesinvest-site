// netlify/functions/snapshot-asx-universe-prices.js
//
// Nightly snapshot of last close prices for the full ASX universe.
// Uses EODHD bulk last-day endpoint and derives previous close + pct change.
//
// Stores into Upstash as:
//   asx:universe:eod:YYYY-MM-DD
//   asx:universe:eod:latest
//   asx:universe:eod:latestDate

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

async function redisGet(urlBase, token, key) {
  if (!urlBase || !token) return null;
  try {
    const res = await fetchWithTimeout(
      `${urlBase}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${token}` } },
      12000
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j?.result ?? null;
  } catch {
    return null;
  }
}

function parse(raw) {
  if (!raw) return null;
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
  return raw;
}

function ymd(d) {
  const yy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function dateAddDays(ymdStr, deltaDays) {
  const [y, m, d] = ymdStr.split("-").map((x) => parseInt(x, 10));
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + deltaDays);
  return ymd(dt);
}

async function getPrevTradingDayCloseMap(snapshotDate, maxLookbackDays = 7) {
  // Look back from snapshotDate-1 to find the most recent trading day with data
  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = dateAddDays(snapshotDate, -i);
    const key = `asx:universe:eod:${d}`;
    const raw = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, key);
    const obj = parse(raw);
    
    // Support both direct array and object with .rows
    let rows = null;
    if (Array.isArray(obj)) {
      rows = obj;
    } else if (obj && Array.isArray(obj.rows)) {
      rows = obj.rows;
    }
    
    if (rows && rows.length > 0) {
      const map = new Map();
      for (const r of rows) {
        const code = r?.code ? String(r.code).toUpperCase() : null;
        if (!code) continue;
        // Use close price from that day as the previous close
        const close = Number(r.close ?? r.last ?? r.price ?? NaN);
        if (Number.isFinite(close)) {
          map.set(code, close);
        }
      }
      console.log(
        `[snapshot-asx-universe-prices] Found previous trading day data: ${d} (${map.size} stocks)`
      );
      return { map, prevDate: d };
    }
    // No sleep needed - just check the next day
  }
  
  console.warn(
    `[snapshot-asx-universe-prices] No previous trading day data found within ${maxLookbackDays} days of ${snapshotDate}`
  );
  return { map: new Map(), prevDate: null };
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

// Bulk last-day prices for AU, including previous close / change%
const url = `https://eodhd.com/api/eod-bulk-last-day/AU?api_token=${encodeURIComponent(
  EODHD_TOKEN
)}&fmt=json&previous_close=1`;


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

  // Extract snapshot date from API data first, validate it exists
  let snapshotDate = null;
  if (rawArray && rawArray.length > 0 && (rawArray[0].date || rawArray[0].Date)) {
    snapshotDate = rawArray[0].date || rawArray[0].Date;
  }
  
  // If no date in API response, something is wrong - fail early
  if (!snapshotDate) {
    console.error(
      `[snapshot-asx-universe-prices] No date found in API response. Response length: ${rawArray.length}`
    );
    return {
      statusCode: 502,
      body: JSON.stringify({
        error: "No date found in EODHD API response",
        detail: "The API response does not contain a valid date field",
      }),
    };
  }

  // Fetch previous trading day close prices from historical data
  console.log(
    `[snapshot-asx-universe-prices] Looking for previous trading day data for snapshot date: ${snapshotDate}`
  );
  const { map: prevTradingDayMap, prevDate } = await getPrevTradingDayCloseMap(snapshotDate, 7);

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

    // Get previous trading day close from our historical lookup (preferred)
    let prevClose = prevTradingDayMap.get(base) ?? null;

    // Fallback: if we don't have historical data, use API's previousClose
    if (prevClose === null) {
      let prevRaw =
        r.previousClose ??
        r.PreviousClose ??
        r.previous_close ??
        r.prev_close ??
        r.Previous_Close ??
        null;
      let apiPrevClose = Number(prevRaw ?? NaN);
      if (Number.isFinite(apiPrevClose)) prevClose = apiPrevClose;
    }

    // Calculate pctChange from current close and prevClose
    let changePct = null;
    if (closeVal !== null && prevClose !== null && prevClose !== 0) {
      const pc = ((closeVal - prevClose) / prevClose) * 100;
      if (Number.isFinite(pc)) changePct = pc;
    }

    // Fallback: use API's change percent if we couldn't calculate it
    if (changePct === null) {
      let apiChangePct = Number(
        r.change_p ??
          r.changeP ??
          r.ChangeP ??
          r.changePercent ??
          r.ChangePercent ??
          NaN
      );
      if (Number.isFinite(apiChangePct)) changePct = apiChangePct;
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

  const dailyKey = `asx:universe:eod:${snapshotDate}`;
  const latestKey = `asx:universe:eod:latest`;
  const latestDateKey = `asx:universe:eod:latestDate`;

  const okDaily = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, dailyKey, rows);
  const okLatest = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestKey, rows);
  const okLatestDate = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestDateKey, snapshotDate);

  const ok = okDaily && okLatest && okLatestDate;

  console.log(
    `[snapshot-asx-universe-prices] Completed: snapshotDate=${snapshotDate}, prevDate=${prevDate || 'none'}, rows=${rows.length}, prevTradingDayLookupSize=${prevTradingDayMap.size}`
  );

  return {
    statusCode: ok ? 200 : 500,
    body: JSON.stringify({
      ok,
      snapshotDate,
      prevDateUsed: prevDate,
      dailyKey,
      latestKey,
      latestDateKey,
      rows: rows.length,
      prevTradingDayLookupSize: prevTradingDayMap.size,
      elapsedMs: Date.now() - start,
    }),
  };
};