// netlify/functions/snapshot-asx200.js
//
// Snapshot for ASX200 static universe (data/asx200.txt).
// For each ticker this retrieves:
//   - marketCap (from EODHD fundamentals endpoints)
//   - recent EOD bars for the last 2 business days to compute today's price, yesterday's price and pct change
// Stores results to Upstash as:
//   asx200:daily:YYYY-MM-DD
//   asx200:latest
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
//   EOD_LOOKBACK_DAYS (default 2)  <-- now defaults to 2 business days (most recent + previous)
//

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const DEFAULT_QUICK_LIMIT = 20;
const DEFAULT_CONCURRENCY = 6;
const DEFAULT_RETRIES = 2;
const DEFAULT_BACKOFF_BASE_MS = 300;
const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_EOD_LOOKBACK_DAYS = 2; // <- use last 2 business days

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function fmt(n) {
  return typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(4)) : null;
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

function getLastBusinessDays(n, endDate = new Date()) {
  const days = [];
  let d = new Date(endDate);
  while (days.length < n) {
    const dow = d.getDay();
    if (dow !== 0 && dow !== 6) days.push(new Date(d));
    d.setDate(d.getDate() - 1);
  }
  return days.reverse().map((dt) => dt.toISOString().slice(0, 10));
}

function normalizeCode(code) {
  return String(code || "").replace(/\.[A-Z0-9]{1,6}$/i, "").toUpperCase();
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

async function readAsx200List() {
  const filePath = path.join(__dirname, "..", "..", "data", "asx200.txt");
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const parts = raw.split(",").map((s) => s.trim()).filter(Boolean);
    return parts.map((p) => p.toUpperCase());
  } catch (err) {
    try {
      const alt = path.join(process.cwd(), "data", "asx200.txt");
      const raw = fs.readFileSync(alt, "utf8");
      return raw.split(",").map((s) => s.trim()).filter(Boolean).map((p) => p.toUpperCase());
    } catch (e) {
      throw new Error("Failed to read data/asx200.txt: " + (err && err.message));
    }
  }
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

  let tickers;
  try {
    tickers = await readAsx200List();
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }

  if (QUICK) {
    tickers = tickers.slice(0, Math.min(QUICK_LIMIT, tickers.length));
  }

  // prepare date window: last EOD_LOOKBACK_DAYS business days (default 2)
  const days = getLastBusinessDays(EOD_LOOKBACK_DAYS);
  if (days.length < 2) {
    return { statusCode: 500, body: JSON.stringify({ error: "Not enough business days in lookback window", days }) };
  }
  const from = days[0];
  const to = days[days.length - 1];

  // helpers for EOD and fundamentals
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

  async function fetchFund(fullCode) {
    // try a few endpoints; return raw JSON when available
    const endpoints = [
      `https://eodhd.com/api/fundamental/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
      `https://eodhd.com/api/fundamentals/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
      `https://eodhd.com/api/company/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`
    ];
    for (const url of endpoints) {
      let attempt = 0;
      let lastText = null;
      while (attempt <= RETRIES) {
        try {
          const res = await fetchWithTimeout(url, {}, 10000);
          const text = await res.text().catch(() => "");
          if (!res.ok) {
            lastText = text || lastText;
            if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
              const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
              await sleep(backoff + Math.random() * 200);
              attempt++;
              continue;
            }
            break; // try next endpoint
          }
          try {
            const json = text ? JSON.parse(text) : null;
            return { ok: true, data: json };
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
    }
    return { ok: false, status: 0, text: "no-fundamentals" };
  }

  function extractMarketCapFromFund(raw) {
    if (!raw) return null;
    const tryCandidates = (obj) => {
      const keys = ["market_capitalization", "market_cap", "MarketCapitalization", "Market_Capital", "marketCap"];
      for (const k of keys) {
        if (typeof obj[k] === "number") return obj[k];
        if (typeof obj[k] === "string") {
          const parsed = Number(obj[k].replace(/[^0-9.\-eEBKmMbB]/g, "").replace(/,/g, ""));
          if (!Number.isNaN(parsed)) return parsed;
          const m = obj[k].match(/^([\d.,]+)\s*([KMBkmb])$/);
          if (m) {
            const n = Number(m[1].replace(/,/g, ""));
            const suf = m[2].toUpperCase();
            if (suf === "B") return n * 1e9;
            if (suf === "M") return n * 1e6;
            if (suf === "K") return n * 1e3;
          }
        }
      }
      return null;
    };

    const top = tryCandidates(raw);
    if (top !== null) return top;
    if (raw.result && typeof raw.result === "object") {
      const r = tryCandidates(raw.result);
      if (r !== null) return r;
    }
    if (raw.data && typeof raw.data === "object") {
      const d = tryCandidates(raw.data);
      if (d !== null) return d;
    }
    try {
      const s = JSON.stringify(raw);
      const m = s.match(/market[_ -]?cap(?:italization)?["']?\s*[:=]\s*"?\$?([0-9,.\-KMmBb]+)/);
      if (m && m[1]) {
        const cand = m[1].replace(/,/g, "");
        const m2 = cand.match(/^([\d.]+)([KMkmbB])?$/);
        if (m2) {
          const num = Number(m2[1]);
          const suf = (m2[2] || "").toUpperCase();
          if (suf === "B") return num * 1e9;
          if (suf === "M") return num * 1e6;
          return num;
        }
      }
    } catch (e) {
      // ignore
    }
    return null;
  }

  // try suffixes if symbol lacks dot
  async function getSymbolData(symbol) {
    if (symbol.includes(".")) {
      const eod = await fetchEod(symbol);
      const fund = await fetchFund(symbol);
      return { symbol, eod, fund, attempts: [symbol] };
    }
    const attempts = [];
    for (const sfx of TRY_SUFFIXES) {
      const full = `${symbol}.${sfx}`;
      attempts.push(full);
      const eod = await fetchEod(full);
      if (!eod.ok || !Array.isArray(eod.data) || eod.data.length === 0) continue;
      const fund = await fetchFund(full);
      return { symbol, eod, fund, attempts };
    }
    return { symbol, eod: { ok: false }, fund: { ok: false }, attempts };
  }

  // parallel map with limited concurrency
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
        results.push({ requested: t, eod: { ok: false, error: err && err.message }, fund: { ok: false }, attempts: [] });
      }
    }
  });
  await Promise.all(workers);

  // build rows using the two business days only
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
    // Expect arr to contain up to EOD_LOOKBACK_DAYS entries for the last business days; use the last entry and previous valid one.
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

    let marketCap = null;
    if (res.fund && res.fund.ok && res.fund.data) {
      marketCap = extractMarketCapFromFund(res.fund.data);
    }

    rows.push({
      code: normalizeCode(symbol),
      fullCode: symbol,
      lastDate: last && last.date ? last.date : null,
      lastPrice: typeof lastPrice === "number" && !Number.isNaN(lastPrice) ? Number(lastPrice) : null,
      yesterdayDate: prev && prev.date ? prev.date : null,
      yesterdayPrice: typeof yesterdayPrice === "number" && !Number.isNaN(yesterdayPrice) ? Number(yesterdayPrice) : null,
      pctChange: typeof pctChange === "number" && Number.isFinite(pctChange) ? Number(pctChange.toFixed(6)) : null,
      marketCap: typeof marketCap === "number" && Number.isFinite(marketCap) ? Math.round(marketCap) : null,
      attempts
    });
  }

  // persist to Upstash
  const todayKey = `asx200:daily:${new Date().toISOString().slice(0, 10)}`;
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