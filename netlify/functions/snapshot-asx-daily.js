// netlify/functions/snapshot-asx-daily.js
//
// Daily snapshot of ASX symbols: code, name, market cap, last price, yesterday price, pct change.
// Adds caching for exchange-symbol-list to avoid 429s and respects Retry-After from EODHD.
// Writes to Upstash as:
//   asx:daily:YYYY-MM-DD   (array of rows)
//   asx:latest             (alias for today's snapshot)
//   asx:exchange-list:latest (cached exchange-symbol-list)
// Requirements (env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
// Optional env (tweak for your plan & rate limits):
//   MAX_SYMBOLS (default 2500)
//   CHUNK_SIZE (default 300)
//   CONCURRENCY (default 4)           <- lowered default to be conservative
//   RETRIES (default 3)
//   BACKOFF_BASE_MS (default 400)     <- slightly larger default backoff
//   TRY_SUFFIXES (default "AU,AX,ASX")
//   SNAPSHOT_TTL_SECONDS (optional)
//   QUICK_MODE (default 0)
//   QUICK_LIMIT (default 50)
//   EXCHANGE_LIST_CACHE_TTL (seconds, default 86400 i.e. 24h)

const fetch = (...args) => global.fetch(...args);

const DEFAULT_MAX_SYMBOLS = 2500;
const DEFAULT_CHUNK_SIZE = 300;
const DEFAULT_CONCURRENCY = 4;
const DEFAULT_RETRIES = 3;
const DEFAULT_BACKOFF_BASE_MS = 400;
const DEFAULT_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_QUICK_LIMIT = 50;
const DEFAULT_EXCHANGE_LIST_CACHE_TTL = 24 * 60 * 60; // 24h

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function fmt(n) {
  return typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(2)) : null;
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

function normalizeCode(code) {
  return String(code || "").replace(/\.[A-Z0-9]{1,6}$/i, "").toUpperCase();
}

async function redisGet(upstashUrl, upstashToken, key) {
  if (!upstashUrl || !upstashToken) return null;
  try {
    const res = await fetchWithTimeout(`${upstashUrl}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${upstashToken}` },
    }, 8000);
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j && typeof j.result !== "undefined" ? j.result : null;
  } catch (err) {
    console.warn("redisGet error", key, err && err.message);
    return null;
  }
}

async function redisSet(upstashUrl, upstashToken, key, value, ttlSeconds) {
  if (!upstashUrl || !upstashToken) return false;
  try {
    const payload = typeof value === "string" ? value : JSON.stringify(value);
    const ttlQuery = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : "";
    const url = `${upstashUrl}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}${ttlQuery}`;
    const res = await fetchWithTimeout(url, {
      method: "POST",
      headers: { Authorization: `Bearer ${upstashToken}` },
    }, 10000);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisSet failed", key, res.status, txt);
      return false;
    }
    return true;
  } catch (err) {
    console.warn("redisSet error", key, err && err.message);
    return false;
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

exports.handler = async function (event) {
  const startTs = Date.now();
  const debug = { steps: [] };

  const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  if (!EODHD_TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }) };
  }
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: "Missing Upstash env" }) };
  }

  const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || DEFAULT_MAX_SYMBOLS);
  const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || DEFAULT_CHUNK_SIZE);
  const CONCURRENCY = Number(process.env.CONCURRENCY || DEFAULT_CONCURRENCY);
  const RETRIES = Number(process.env.RETRIES || DEFAULT_RETRIES);
  const BACKOFF_BASE_MS = Number(process.env.BACKOFF_BASE_MS || DEFAULT_BACKOFF_BASE_MS);
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES || DEFAULT_SUFFIXES.join(",")).split(",").map(s => s.trim()).filter(Boolean);
  const SNAPSHOT_TTL_SECONDS = process.env.SNAPSHOT_TTL_SECONDS ? Number(process.env.SNAPSHOT_TTL_SECONDS) : undefined;
  const QUICK_MODE = (String(process.env.QUICK_MODE || "0") === "1") || ((event && event.queryStringParameters && event.queryStringParameters.quick) === "1");
  const QUICK_LIMIT = Number(process.env.QUICK_LIMIT || DEFAULT_QUICK_LIMIT);
  const EXCHANGE_LIST_CACHE_TTL = Number(process.env.EXCHANGE_LIST_CACHE_TTL || DEFAULT_EXCHANGE_LIST_CACHE_TTL);

  try {
    // 0) Try to get cached exchange-symbol-list from Upstash
    const exchangeListCacheKey = "asx:exchange-list:latest";
    const cached = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, exchangeListCacheKey);
    let listJson = null;
    if (cached) {
      try {
        listJson = typeof cached === "string" ? JSON.parse(cached) : cached;
        debug.steps.push({ source: "exchange-list-from-cache", totalFound: Array.isArray(listJson) ? listJson.length : 0 });
      } catch (err) {
        debug.steps.push({ source: "exchange-list-cache-parse-failed", error: err && err.message });
      }
    }

    // If no cached list, fetch from EODHD with retry/backoff and respect Retry-After
    if (!listJson) {
      const listUrlBase = `https://eodhd.com/api/exchange-symbol-list/AU?fmt=json&api_token=${encodeURIComponent(EODHD_TOKEN)}`;
      let attempt = 0;
      let lastErr = null;
      let retryAfterMs = 0;

      while (attempt <= RETRIES) {
        try {
          debug.steps.push({ action: "fetch-exchange-symbol-list", attempt, url: listUrlBase });
          const res = await fetchWithTimeout(listUrlBase, {}, 15000);
          const text = await res.text().catch(() => "");
          // If rate limited, look for Retry-After and backoff
          if (res.status === 429) {
            lastErr = { status: 429, text: text };
            // check Retry-After header (seconds)
            const ra = res.headers && (res.headers.get ? res.headers.get("Retry-After") : null);
            if (ra) {
              const raSec = Number(ra);
              if (!Number.isNaN(raSec)) retryAfterMs = Math.max(retryAfterMs, raSec * 1000);
            }
            const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt) + (retryAfterMs || 0);
            debug.steps.push({ source: "exchange-list-429", attempt, retryAfterMs, backoff });
            await sleep(backoff + Math.random() * 200);
            attempt++;
            continue;
          }
          if (!res.ok) {
            lastErr = { status: res.status, text };
            // retry on server errors
            if (res.status >= 500) {
              const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
              await sleep(backoff + Math.random() * 200);
              attempt++;
              continue;
            }
            // client error -> bail and return error
            debug.steps.push({ source: "exchange-list-failed", status: res.status, text: text ? (text + "").slice(0, 800) : null });
            return { statusCode: 502, body: JSON.stringify({ error: "Failed to load exchange symbol list", debug }) };
          }
          // success -> parse JSON
          try {
            listJson = text ? JSON.parse(text) : [];
            // cache it in Upstash for EXCHANGE_LIST_CACHE_TTL seconds to avoid repeated calls
            await redisSet(UPSTASH_URL, UPSTASH_TOKEN, exchangeListCacheKey, listJson, EXCHANGE_LIST_CACHE_TTL);
            debug.steps.push({ source: "exchange-list-fetched-and-cached", totalFound: Array.isArray(listJson) ? listJson.length : 0, cacheTtlSec: EXCHANGE_LIST_CACHE_TTL });
            break;
          } catch (err) {
            lastErr = { status: res.status, text: text, parseError: err && err.message };
            debug.steps.push({ source: "exchange-list-parse-failed", error: err && err.message });
            return { statusCode: 502, body: JSON.stringify({ error: "Invalid exchange-list response", debug }) };
          }
        } catch (err) {
          lastErr = { error: err && err.message };
          const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
          debug.steps.push({ source: "exchange-list-fetch-exception", attempt, error: err && err.message, backoff });
          await sleep(backoff + Math.random() * 200);
          attempt++;
        }
      }

      if (!listJson) {
        debug.steps.push({ source: "exchange-list-giveup", lastErr });
        return { statusCode: 502, body: JSON.stringify({ error: "Failed to load exchange symbol list after retries", debug }) };
      }
    }

    // Normalize list entries
    const normalized = (Array.isArray(listJson) ? listJson : [])
      .map(it => {
        if (!it) return null;
        if (typeof it === "string") return { code: it, name: "" };
        const code = it.code || it.symbol || it.Code || it[0] || "";
        const name = it.name || it.companyName || it.Name || it[1] || "";
        return { code, name };
      })
      .filter(Boolean)
      .filter(x => x.code && !x.code.includes("^") && !x.code.includes("/"));

    debug.steps.push({ source: "exchange-list-normalized", totalFound: normalized.length });

    // limit to MAX_SYMBOLS
    const maxUse = Math.min(MAX_SYMBOLS, normalized.length);
    let symbols = normalized.slice(0, maxUse).map(s => ({ code: (s.code || "").toString().toUpperCase(), name: s.name || "" }));

    if (QUICK_MODE) {
      symbols = symbols.slice(0, Math.min(QUICK_LIMIT, symbols.length));
      debug.steps.push({ source: "quick-mode", used: symbols.length });
    } else {
      debug.steps.push({ source: "universe-built", used: symbols.length });
    }

    // helper: per-symbol EOD fetch with retries/backoff (identical to previous impl)
    async function fetchEodWithRetries(fullCode, from, to) {
      const url = `https://eodhd.com/api/eod/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&period=d&from=${from}&to=${to}&fmt=json`;
      let attempt = 0;
      let lastText = null;
      while (attempt <= RETRIES) {
        try {
          const r = await fetchWithTimeout(url, {}, 12000);
          const text = await r.text().catch(() => "");
          if (!r.ok) {
            lastText = text || lastText;
            if (r.status === 429 || (r.status >= 500 && r.status < 600)) {
              const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
              await sleep(backoff + Math.random() * 200);
              attempt++;
              continue;
            }
            return { ok: false, status: r.status, text };
          }
          try {
            const json = text ? JSON.parse(text) : null;
            if (!Array.isArray(json)) return { ok: false, status: r.status, text };
            const arr = json.slice().sort((a, b) => new Date(a.date) - new Date(b.date));
            return { ok: true, data: arr };
          } catch (err) {
            return { ok: false, status: r.status, text };
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

    async function fetchFundamentalsWithRetries(fullCode) {
      const endpoints = [
        `https://eodhd.com/api/fundamental/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
        `https://eodhd.com/api/fundamentals/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
        `https://eodhd.com/api/company/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
      ];
      for (const url of endpoints) {
        let attempt = 0;
        let lastText = null;
        while (attempt <= RETRIES) {
          try {
            const r = await fetchWithTimeout(url, {}, 12000);
            const text = await r.text().catch(() => "");
            if (!r.ok) {
              lastText = text || lastText;
              if (r.status === 429 || (r.status >= 500 && r.status < 600)) {
                const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
                await sleep(backoff + Math.random() * 200);
                attempt++;
                continue;
              }
              break; // try next endpoint
            }
            try {
              const json = text ? JSON.parse(text) : null;
              if (json && typeof json === "object") {
                const mc =
                  (typeof json.market_capitalization === "number" && json.market_capitalization) ||
                  (typeof json.market_cap === "number" && json.market_cap) ||
                  (typeof json.MarketCapitalization === "number" && json.MarketCapitalization) ||
                  (json.data && typeof json.data.market_capitalization === "number" && json.data.market_capitalization) ||
                  (json.result && typeof json.result.market_capitalization === "number" && json.result.market_capitalization) ||
                  null;
                if (mc === null) {
                  const maybe =
                    (json.market_capitalization || json.market_cap || json.MarketCapitalization || (json.data && json.data.market_capitalization) || (json.result && json.result.market_capitalization) || null);
                  const parsed = maybe !== null && maybe !== undefined ? Number(maybe) : null;
                  if (!Number.isNaN(parsed)) return { ok: true, data: { marketCap: parsed }, raw: json };
                } else {
                  return { ok: true, data: { marketCap: mc }, raw: json };
                }
              }
              return { ok: true, data: null, raw: json };
            } catch (err) {
              lastText = text || lastText;
              break;
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

    async function fetchSymbolRow(symbol, name, from, to) {
      if (symbol.includes(".")) {
        const eodRes = await fetchEodWithRetries(symbol, from, to);
        const fundRes = await fetchFundamentalsWithRetries(symbol);
        return { symbol, name, eodRes, fundRes, attempts: [symbol] };
      }
      const attempts = [];
      for (const sfx of TRY_SUFFIXES) {
        const full = `${symbol}.${sfx}`;
        attempts.push(full);
        const eodRes = await fetchEodWithRetries(full, from, to);
        if (!eodRes.ok || !Array.isArray(eodRes.data) || eodRes.data.length === 0) {
          continue;
        }
        const fundRes = await fetchFundamentalsWithRetries(full);
        return { symbol, name, eodRes, fundRes, attempts };
      }
      return { symbol, name, eodRes: { ok: false }, fundRes: { ok: false }, attempts };
    }

    function chunkArray(arr, size) {
      const out = [];
      for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
      return out;
    }

    const lastTwoDays = getLastBusinessDays(2);
    const from = lastTwoDays[0];
    const to = lastTwoDays[lastTwoDays.length - 1];

    const chunks = chunkArray(symbols, CHUNK_SIZE);
    const allRows = [];
    const failureSamples = [];
    let processedCount = 0;
    let fetchedFundCount = 0;

    for (let c = 0; c < chunks.length; c++) {
      const group = chunks[c];
      debug.steps.push({ source: "processing-chunk", index: c, chunkSize: group.length });

      // map with limited concurrency per chunk
      let idx = 0;
      const results = new Array(group.length);
      const workers = new Array(Math.min(CONCURRENCY, group.length)).fill(null).map(async () => {
        while (true) {
          const i = idx++;
          if (i >= group.length) return;
          const s = group[i];
          try {
            const res = await fetchSymbolRow(s.code, s.name, from, to);
            results[i] = res;
          } catch (err) {
            results[i] = { symbol: s.code, name: s.name, eodRes: { ok: false, error: err && err.message }, fundRes: { ok: false } };
          }
        }
      });
      await Promise.all(workers);

      for (const r of results) {
        processedCount++;
        if (!r) continue;
        const { symbol, name, eodRes, fundRes, attempts } = r;
        if (!eodRes || !eodRes.ok || !Array.isArray(eodRes.data) || eodRes.data.length === 0) {
          if (failureSamples.length < 25) {
            failureSamples.push({
              code: symbol,
              name,
              attempts: attempts || [],
              eodStatus: eodRes && eodRes.status ? eodRes.status : null,
              eodTextSnippet: eodRes && eodRes.text ? String(eodRes.text).slice(0, 800) : null
            });
          }
          continue;
        }
        const arr = eodRes.data;
        const last = arr[arr.length - 1];
        const prev = arr.length >= 2 ? arr[arr.length - 2] : null;
        const lastPrice = last && typeof last.close === "number" ? last.close : Number(last && last.close);
        const yesterdayPrice = prev ? (typeof prev.close === "number" ? prev.close : Number(prev.close)) : null;
        const pctChange = (yesterdayPrice !== null && yesterdayPrice !== 0) ? ((lastPrice - yesterdayPrice) / yesterdayPrice) * 100 : null;

        let marketCap = null;
        if (fundRes && fundRes.ok && fundRes.data && typeof fundRes.data.marketCap === "number") {
          marketCap = fundRes.data.marketCap;
          fetchedFundCount++;
        } else {
          if (fundRes && fundRes.ok && fundRes.raw) {
            const raw = fundRes.raw;
            const maybe =
              (raw.market_capitalization || raw.market_cap || raw.MarketCapitalization || (raw.data && raw.data.market_capitalization) || (raw.result && raw.result.market_capitalization));
            const parsed = maybe !== undefined && maybe !== null ? Number(maybe) : null;
            if (!Number.isNaN(parsed)) {
              marketCap = parsed;
              fetchedFundCount++;
            }
          }
        }

        allRows.push({
          code: normalizeCode(symbol),
          fullCode: symbol,
          name: name || "",
          lastDate: last && last.date ? last.date : null,
          lastPrice: typeof lastPrice === "number" && !Number.isNaN(lastPrice) ? Number(lastPrice) : null,
          yesterdayDate: prev && prev.date ? prev.date : null,
          yesterdayPrice: typeof yesterdayPrice === "number" && !Number.isNaN(yesterdayPrice) ? Number(yesterdayPrice) : null,
          pctChange: typeof pctChange === "number" && Number.isFinite(pctChange) ? Number(pctChange.toFixed(4)) : null,
          marketCap: typeof marketCap === "number" && Number.isFinite(marketCap) ? Math.round(marketCap) : null,
          attempts: attempts || []
        });
      }

      // small pause between chunks
      await sleep(300 + Math.random() * 200);
      debug.steps.push({ source: "chunk-complete", chunkIndex: c, accumulated: allRows.length });
    }

    debug.steps.push({
      source: "fetch-complete",
      processedSymbols: processedCount,
      rowsCollected: allRows.length,
      fundamentalsFetched: fetchedFundCount,
      failuresSample: failureSamples.length,
      elapsedMs: Date.now() - startTs
    });

    allRows.sort((a, b) => (a.code || "").localeCompare(b.code || ""));

    const todayKeyDate = new Date().toISOString().slice(0, 10);
    const dailyKey = `asx:daily:${todayKeyDate}`;
    const latestKey = `asx:latest`;

    const okDaily = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, dailyKey, allRows, SNAPSHOT_TTL_SECONDS);
    const okLatest = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestKey, allRows, SNAPSHOT_TTL_SECONDS);

    const WRITE_PER_SYMBOL = String(process.env.WRITE_PER_SYMBOL || "0") === "1";
    if (WRITE_PER_SYMBOL) {
      for (const row of allRows) {
        const k = `asx:symbol:LATEST:${encodeURIComponent(row.code)}`;
        await redisSet(UPSTASH_URL, UPSTASH_TOKEN, k, row);
      }
      debug.steps.push({ source: "per-symbol-write", count: allRows.length });
    }

    const payload = {
      ok: okDaily && okLatest,
      dailyKey,
      latestKey,
      rows: allRows.length,
      failuresSample: failureSamples.slice(0, 25),
      debug
    };

    return { statusCode: 200, body: JSON.stringify(payload) };
  } catch (err) {
    console.error("snapshot-asx-daily error", err && (err.stack || err.message || err));
    debug.steps.push({ source: "fatal", error: err && (err.stack || err.message) });
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message, debug }) };
  }
};
