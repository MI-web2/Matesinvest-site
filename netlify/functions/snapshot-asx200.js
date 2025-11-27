// netlify/functions/snapshot-asx200.js
// Snapshot for ASX200 static universe (asx200.txt located next to this function file).
// For each ticker this retrieves:
//   - recent EOD bars for the last 2 business days to compute today's price, yesterday's price and pct change
//   - company name (from EODHD exchange-symbol-list/AU) — cached in Upstash to avoid repeated calls
// Stores results to Upstash as:
//   asx200:daily:YYYY-MM-DD
//   asx200:latest
//
// NOTE: marketCap/fundamentals fetching has been removed per request to keep this snapshot focused on prices only.
//
// Requirements (set in Netlify env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env:
//   QUICK=1 or query ?quick=1      -> only process QUICK_LIMIT tickers (dev)
//   QUICK_LIMIT (default 20)
//   CONCURRENCY (default 6)
//   RETRIES (default 2)
//   BACKOFF_BASE_MS (default 300)
//   TRY_SUFFIXES (default "AU,AX,ASX")
//   EOD_LOOKBACK_DAYS (default 2)
//   EXCHANGE_LIST_CACHE_TTL (seconds, default 86400)

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const DEFAULT_QUICK_LIMIT = 20;
const DEFAULT_CONCURRENCY = 6;
const DEFAULT_RETRIES = 2;
const DEFAULT_BACKOFF_BASE_MS = 300;
const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_EOD_LOOKBACK_DAYS = 2; // last 2 business days
const DEFAULT_EXCHANGE_LIST_CACHE_TTL = 24 * 60 * 60; // 24h

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function fetchWithTimeout(url, opts = {}, timeout = 12000) {
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

// Convert a JS Date to an AEST (Brisbane, UTC+10) YYYY-MM-DD string
function getAestDateString(date) {
  const AEST_OFFSET_MINUTES = 10 * 60; // Brisbane is UTC+10 all year
  const aestTime = new Date(date.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
  return aestTime.toISOString().slice(0, 10);
}

// Last N *completed* business days BEFORE today, as UTC YYYY-MM-DD strings
function getLastCompletedBusinessDays(n, now = new Date()) {
  const days = [];
  let d = new Date(now);
  // start from "yesterday"
  d.setDate(d.getDate() - 1);

  while (days.length < n) {
    const dow = d.getDay(); // 0 = Sun, 6 = Sat
    if (dow !== 0 && dow !== 6) {
      days.push(new Date(d));
    }
    d.setDate(d.getDate() - 1);
  }

  return days.reverse().map((dt) => dt.toISOString().slice(0, 10));
}

// Today’s date in AEST (for Redis key only)
function getTodayAestDateString(baseDate = new Date()) {
  return getAestDateString(baseDate);
}

function normalizeCode(code) {
  return String(code || "").replace(/\.[A-Z0-9]{1,6}$/i, "").toUpperCase();
}

async function redisGet(urlBase, token, key, timeout = 8000) {
  if (!urlBase || !token) return null;
  try {
    const res = await fetchWithTimeout(`${urlBase}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${token}` },
    }, timeout);
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j && typeof j.result !== "undefined" ? j.result : null;
  } catch (err) {
    console.warn("redisGet error", key, err && err.message);
    return null;
  }
}

async function redisSet(urlBase, token, key, value, ttlSeconds) {
  if (!urlBase || !token) return false;
  try {
    const payload = typeof value === "string" ? value : JSON.stringify(value);
    const ttl = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : "";
    const res = await fetchWithTimeout(
      `${urlBase}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}${ttl}`,
      { method: "POST", headers: { Authorization: `Bearer ${token}` } },
      10000
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisSet failed", key, res.status, txt && txt.slice(0, 300));
      return false;
    }
    return true;
  } catch (err) {
    console.warn("redisSet error", key, err && err.message);
    return false;
  }
}

// Read asx200.txt located next to this file (preferred) and fall back to repo/data paths
function readAsx200ListSync() {
  const candidates = [
    // file next to this function (preferred)
    path.join(__dirname, "asx200.txt"),
    path.join(__dirname, "asx200"),
    // repo root data folder (fallback)
    path.join(process.cwd(), "data", "asx200.txt"),
    path.join(process.cwd(), "data", "asx200"),
    // older layout fallback
    path.join(__dirname, "..", "data", "asx200.txt"),
    path.join(__dirname, "..", "..", "data", "asx200.txt"),
  ];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw.split(",").map((s) => s.trim()).filter(Boolean);
        console.log(`[snapshot-asx200] using data file: ${p} (entries=${parts.length})`);
        return parts.map((p) => p.toUpperCase());
      }
    } catch (err) {
      console.warn(`[snapshot-asx200] read failure for ${p}: ${err && err.message}`);
    }
  }

  throw new Error("Failed to read asx200 list: asx200.txt not found in expected locations. Place asx200.txt next to the function or in repo/data/");
}

exports.handler = async function (event) {
  const start = Date.now();
  const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  if (!EODHD_TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }) };
  }
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: "Missing Upstash env" }) };
  }

  const qs = (event && event.queryStringParameters) || {};
  const QUICK = (qs.quick === "1") || (String(process.env.QUICK || "0") === "1");
  const QUICK_LIMIT = Number(process.env.QUICK_LIMIT || DEFAULT_QUICK_LIMIT);
  const CONCURRENCY = Number(process.env.CONCURRENCY || DEFAULT_CONCURRENCY);
  const RETRIES = Number(process.env.RETRIES || DEFAULT_RETRIES);
  const BACKOFF_BASE_MS = Number(process.env.BACKOFF_BASE_MS || DEFAULT_BACKOFF_BASE_MS);
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES ? process.env.TRY_SUFFIXES.split(",") : DEFAULT_TRY_SUFFIXES).map((s) => s.trim()).filter(Boolean);
  const EOD_LOOKBACK_DAYS = Number(process.env.EOD_LOOKBACK_DAYS || DEFAULT_EOD_LOOKBACK_DAYS);
  const EXCHANGE_LIST_CACHE_TTL = Number(process.env.EXCHANGE_LIST_CACHE_TTL || DEFAULT_EXCHANGE_LIST_CACHE_TTL);

  let tickers;
  try {
    tickers = readAsx200ListSync();
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }

  if (QUICK) tickers = tickers.slice(0, Math.min(QUICK_LIMIT, tickers.length));

  // Build code -> company name map by fetching exchange-symbol-list/AU (cached)
  const exchangeCacheKey = "asx:exchange-list:latest";
  let exchangeList = null;
  try {
    const cached = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, exchangeCacheKey);
    if (cached) {
      exchangeList = typeof cached === "string" ? JSON.parse(cached) : cached;
    } else {
      // fetch from EODHD
      const url = `https://eodhd.com/api/exchange-symbol-list/AU?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;
      const res = await fetchWithTimeout(url, {}, 15000);
      const text = await res.text().catch(() => "");
      if (res.ok && text) {
        try {
          const json = JSON.parse(text);
          if (Array.isArray(json)) {
            exchangeList = json;
            // cache it
            await redisSet(UPSTASH_URL, UPSTASH_TOKEN, exchangeCacheKey, exchangeList, EXCHANGE_LIST_CACHE_TTL);
          }
        } catch (e) {
          console.warn("exchange-list parse failed", e && e.message);
          exchangeList = null;
        }
      } else {
        console.warn("exchange-list fetch failed", res && res.status, text && text.slice(0,200));
      }
    }
  } catch (err) {
    console.warn("exchange list fetch error", err && err.message);
    exchangeList = null;
  }

  const codeNameMap = {};
  if (Array.isArray(exchangeList)) {
    for (const it of exchangeList) {
      try {
        if (!it) continue;
        if (typeof it === "string") {
          // sometimes the list is an array of strings like "CBA.AX"
          const base = normalizeCode(it);
          if (!codeNameMap[base]) codeNameMap[base] = "";
        } else if (typeof it === "object") {
          const code = String(it.code || it.symbol || it.Code || it.Symbol || "").trim();
          const name = String(it.name || it.companyName || it.Name || it.CompanyName || "").trim();
          if (code) {
            const base = normalizeCode(code);
            if (name) codeNameMap[base] = name;
            else if (!codeNameMap[base]) codeNameMap[base] = "";
          }
        }
      } catch (e) {
        // ignore mapping errors per-item
      }
    }
  }

// last N *completed* business days window (default 2)
const days = getLastCompletedBusinessDays(EOD_LOOKBACK_DAYS);
  if (days.length < 2) {
    return { statusCode: 500, body: JSON.stringify({ error: "Not enough business days in lookback window", days }) };
  }
  const from = days[0];
  const to = days[days.length - 1];

  // EOD fetch with retries/backoff
  async function fetchEod(fullCode) {
    const url = `https://eodhd.com/api/eod/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&period=d&from=${from}&to=${to}&fmt=json`;
    let attempt = 0;
    let lastText = null;
    while (attempt <= RETRIES) {
      try {
        const res = await fetchWithTimeout(url, {}, 12000);
        const text = await res.text().catch(() => "");
        if (!res.ok) {
          lastText = text || lastText;
          if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
            const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
            await sleep(backoff + Math.random() * 200);
            attempt++;
            continue;
          }
          return { ok: false, status: res.status, text };
        }
        try {
          const json = text ? JSON.parse(text) : null;
          if (!Array.isArray(json)) return { ok: false, status: res.status, text };
          const arr = json.slice().sort((a, b) => new Date(a.date) - new Date(b.date));
          return { ok: true, data: arr };
        } catch (e) {
          return { ok: false, status: res.status, text };
        }
      } catch (err) {
        lastText = String(err && err.message) || lastText;
        const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
        await sleep(backoff + Math.random() * 200);
        attempt++;
      }
    }
    return { ok: false, status: 0, text: lastText };
  }

  // try suffixes if symbol lacks dot
  async function getSymbolData(symbol) {
    if (symbol.includes(".")) {
      const eod = await fetchEod(symbol);
      return { symbol, eod, attempts: [symbol] };
    }
    const attempts = [];
    for (const sfx of TRY_SUFFIXES) {
      const full = `${symbol}.${sfx}`;
      attempts.push(full);
      const eod = await fetchEod(full);
      if (!eod.ok || !Array.isArray(eod.data) || eod.data.length === 0) continue;
      return { symbol, eod, attempts };
    }
    return { symbol, eod: { ok: false }, attempts };
  }

  // parallel workers with limited concurrency
  const results = [];
  let idx = 0;
  const workers = new Array(Math.min(CONCURRENCY, tickers.length)).fill(null).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= tickers.length) return;
      const t = tickers[i];
      try {
        const data = await getSymbolData(t);
        results.push({ requested: t, ...data });
      } catch (err) {
        results.push({ requested: t, eod: { ok: false, error: err && err.message }, attempts: [] });
      }
    }
  });
  await Promise.all(workers);

  // build rows
  const rows = [];
  const failures = [];
  for (const res of results) {
    const symbol = res.symbol;
    const attempts = res.attempts || [];
    if (!res.eod || !res.eod.ok || !Array.isArray(res.eod.data) || res.eod.data.length === 0) {
      failures.push({ code: symbol, attempts, reason: "no-eod" });
      continue;
    }
    const arr = res.eod.data;
    const last = arr[arr.length - 1];
    let prev = null;
    for (let k = arr.length - 2; k >= 0; k--) {
      if (arr[k] && typeof arr[k].close !== "undefined" && arr[k].close !== null) {
        prev = arr[k];
        break;
      }
    }
    const lastPrice = last && typeof last.close === "number" ? last.close : Number(last && last.close);
    const yesterdayPrice = prev ? (typeof prev.close === "number" ? prev.close : Number(prev.close)) : null;
    const pctChange = (yesterdayPrice !== null && yesterdayPrice !== 0) ? ((lastPrice - yesterdayPrice) / yesterdayPrice) * 100 : null;

    const base = normalizeCode(symbol);
    const companyName = codeNameMap[base] || "";

    rows.push({
      code: base,
      fullCode: symbol,
      name: companyName,
      lastDate: last && last.date ? last.date : null,
      lastPrice: typeof lastPrice === "number" && !Number.isNaN(lastPrice) ? Number(lastPrice) : null,
      yesterdayDate: prev && prev.date ? prev.date : null,
      yesterdayPrice: typeof yesterdayPrice === "number" && !Number.isNaN(yesterdayPrice) ? Number(yesterdayPrice) : null,
      pctChange: typeof pctChange === "number" && Number.isFinite(pctChange) ? Number(pctChange.toFixed(6)) : null,
      attempts
    });
  }

  // persist to Upstash using AEST date (Brisbane time)
  const todayDateAest = getTodayAestDateString();
  const todayKey = `asx200:daily:${todayDateAest}`;
  const latestKey = `asx200:latest`;

  const okDaily = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, todayKey, rows);
  const okLatest = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestKey, rows);

  const payload = {
    ok: okDaily && okLatest,
    savedDailyKey: todayKey,
    savedLatestKey: latestKey,
    requested: tickers.length,
    rowsCollected: rows.length,
    failures: failures.slice(0, 30),
    elapsedMs: Date.now() - start
  };

  return { statusCode: 200, body: JSON.stringify(payload) };
};
