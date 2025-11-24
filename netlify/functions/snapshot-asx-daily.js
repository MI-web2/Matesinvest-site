// netlify/functions/snapshot-asx-daily.js
//
// Daily snapshot of ASX symbols: code, name, market cap, last price, yesterday price, pct change.
// Writes to Upstash as:
//   asx:daily:YYYY-MM-DD   (array of rows)
//   asx:latest             (alias for today's snapshot)
//   optionally per-symbol keys asx:symbol:LATEST:<CODE>
//
// Requirements (env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env (tweak for your plan & rate limits):
//   MAX_SYMBOLS (default 2500)      - max symbols to consider from exchange-symbol-list
//   CHUNK_SIZE (default 300)        - process symbols in chunks to keep memory and time bounded
//   CONCURRENCY (default 6)         - number of parallel fetch workers for per-symbol work
//   RETRIES (default 3)             - per-request retries for transient errors
//   BACKOFF_BASE_MS (default 300)   - base ms for exponential backoff
//   TRY_SUFFIXES (default "AU,AX,ASX")
//   SNAPSHOT_TTL_SECONDS (optional) - TTL to set on daily snapshot key in seconds
//   QUICK_MODE (default 0)          - when "1", only first QUICK_LIMIT symbols processed
//   QUICK_LIMIT (default 50)
//
// Behavior summary
// - Calls EODHD /exchange-symbol-list/AU to get the full ASX universe (no screener offset paging).
// - Normalizes and slices to MAX_SYMBOLS.
// - For each symbol tries common ASX suffixes (AU, AX, ASX) when fetching EOD (last 2 business days)
//   and fundamentals (market capitalization). Uses retries+backoff for transient 429/5xx.
// - Produces rows:
//     { code, name, lastDate, lastPrice, yesterdayDate, yesterdayPrice, pctChange, marketCap }
// - Stores full array to asx:daily:YYYY-MM-DD and writes asx:latest alias.
// - Returns a JSON payload with counts and debug failure samples for troubleshooting.
//
// Notes:
// - This function is intentionally defensive: if fundamentals fail for certain symbols, we still
//   include price data and set marketCap to null. You can filter downstream by marketCap >= 300e6.
// - Tune CHUNK_SIZE, CONCURRENCY and RETRIES to avoid hitting EODHD rate limits.

const fetch = (...args) => global.fetch(...args);

const DEFAULT_MAX_SYMBOLS = 2500;
const DEFAULT_CHUNK_SIZE = 300;
const DEFAULT_CONCURRENCY = 6;
const DEFAULT_RETRIES = 3;
const DEFAULT_BACKOFF_BASE_MS = 300;
const DEFAULT_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_QUICK_LIMIT = 50;

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

  try {
    // 1) Get full ASX symbol list
    const listUrl = `https://eodhd.com/api/exchange-symbol-list/AU?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;
    debug.steps.push({ action: "fetch-exchange-symbol-list", url: listUrl });
    const listRes = await fetchWithTimeout(listUrl, {}, 15000);
    const listText = await listRes.text().catch(() => "");
    if (!listRes.ok) {
      const errTxt = listText || `HTTP ${listRes.status}`;
      debug.steps.push({ source: "exchange-list-failed", status: listRes.status, text: errTxt });
      return { statusCode: 502, body: JSON.stringify({ error: "Failed to load exchange symbol list", debug }) };
    }

    let listJson;
    try {
      listJson = listText ? JSON.parse(listText) : [];
    } catch (err) {
      debug.steps.push({ source: "exchange-list-parse-failed", error: err && err.message });
      return { statusCode: 502, body: JSON.stringify({ error: "Invalid exchange-list response", debug }) };
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

    debug.steps.push({ source: "exchange-list-loaded", totalFound: normalized.length });

    // limit to MAX_SYMBOLS
    const maxUse = Math.min(MAX_SYMBOLS, normalized.length);
    let symbols = normalized.slice(0, maxUse).map(s => ({ code: (s.code || "").toString().toUpperCase(), name: s.name || "" }));

    if (QUICK_MODE) {
      symbols = symbols.slice(0, Math.min(QUICK_LIMIT, symbols.length));
      debug.steps.push({ source: "quick-mode", used: symbols.length });
    } else {
      debug.steps.push({ source: "universe-built", used: symbols.length });
    }

    // utility: per-symbol EOD fetch with retries/backoff
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
            // retry on rate-limit or server error
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

    // fundamentals fetch - try single-symbol fundamentals endpoint(s)
    async function fetchFundamentalsWithRetries(fullCode) {
      // Try a couple of likely endpoints: /api/fundamental/{fullCode} and /api/fundamentals/{fullCode}
      const endpoints = [
        `https://eodhd.com/api/fundamental/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
        `https://eodhd.com/api/fundamentals/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
        // company/profile endpoints sometimes include market cap in different APIs:
        `https://eodhd.com/api/company/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
      ];

      let attempt = 0;
      let lastText = null;
      for (const url of endpoints) {
        attempt = 0;
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
              // Try to extract market capitalization from common fields
              if (json && typeof json === "object") {
                // sources may vary: json.market_capitalization, json.market_cap, json.MarketCapitalization or nested fields
                const mc =
                  (typeof json.market_capitalization === "number" && json.market_capitalization) ||
                  (typeof json.market_cap === "number" && json.market_cap) ||
                  (typeof json.MarketCapitalization === "number" && json.MarketCapitalization) ||
                  // try nested structures
                  (json.data && typeof json.data.market_capitalization === "number" && json.data.market_capitalization) ||
                  (json.result && typeof json.result.market_capitalization === "number" && json.result.market_capitalization) ||
                  null;
                // if not number, try parsing fields that may be strings
                if (mc === null) {
                  const maybe =
                    (json.market_capitalization || json.market_cap || json.MarketCapitalization || (json.data && json.data.market_capitalization) || (json.result && json.result.market_capitalization) || null);
                  const parsed = maybe !== null && maybe !== undefined ? Number(maybe) : null;
                  if (!Number.isNaN(parsed)) return { ok: true, data: { marketCap: parsed }, raw: json };
                } else {
                  return { ok: true, data: { marketCap: mc }, raw: json };
                }
              }
              // If we didn't find a market cap, return ok with raw response so caller can inspect
              return { ok: true, data: null, raw: json };
            } catch (err) {
              lastText = text || lastText;
              break; // try next endpoint
            }
          } catch (err) {
            lastText = String(err && err.message) || lastText;
            const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt);
            await sleep(backoff + Math.random() * 200);
            attempt++;
          }
        }
      }
      return { ok: false, status: 0, text: lastText };
    }

    // For a symbol without dot, try suffixes for both EOD and fundamentals.
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
        // If we got no EOD data, try next suffix
        if (!eodRes.ok || !Array.isArray(eodRes.data) || eodRes.data.length === 0) {
          // continue to next suffix
          continue;
        }
        // fundamentals: attempt same suffix (best-effort)
        const fundRes = await fetchFundamentalsWithRetries(full);
        return { symbol, name, eodRes, fundRes, attempts };
      }
      // none of the suffixes produced EOD data
      return { symbol, name, eodRes: { ok: false }, fundRes: { ok: false }, attempts };
    }

    // process symbols in chunks to avoid long single-run spikes
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

      // handle results
      for (const r of results) {
        processedCount++;
        if (!r) continue;
        const { symbol, name, eodRes, fundRes, attempts } = r;
        if (!eodRes || !eodRes.ok || !Array.isArray(eodRes.data) || eodRes.data.length === 0) {
          // record failure sample (limited)
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
          // marketCap may be in raw object (defensive extraction)
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

      // small pause between chunks to be nice to the API
      await sleep(250 + Math.random() * 150);
      debug.steps.push({ source: "chunk-complete", chunkIndex: c, accumulated: allRows.length });
    }

    debug.steps.push({
      source: "fetch-complete",
      processedSymbols: processedCount,
      rowsCollected: allRows.length,
      fundamentalsFetched: fetchedFundCount,
      failuresSampled: failureSamples.length,
      elapsedMs: Date.now() - startTs
    });

    // Sort rows by code for deterministic output (or by market cap if you prefer)
    allRows.sort((a, b) => (a.code || "").localeCompare(b.code || ""));

    // Save to Upstash
    const todayKeyDate = new Date().toISOString().slice(0, 10);
    const dailyKey = `asx:daily:${todayKeyDate}`;
    const latestKey = `asx:latest`;

    const okDaily = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, dailyKey, allRows, SNAPSHOT_TTL_SECONDS);
    const okLatest = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, latestKey, allRows, SNAPSHOT_TTL_SECONDS);

    // Optionally write per-symbol latest keys for fast lookups (best-effort, avoid too many calls)
    const WRITE_PER_SYMBOL = String(process.env.WRITE_PER_SYMBOL || "0") === "1";
    if (WRITE_PER_SYMBOL) {
      // write in reasonable batches to avoid many small requests
      for (const row of allRows) {
        const k = `asx:symbol:LATEST:${encodeURIComponent(row.code)}`;
        // no ttl for per-symbol keys by default
        /* eslint-disable no-await-in-loop */
        await redisSet(UPSTASH_URL, UPSTASH_TOKEN, k, row);
        /* eslint-enable no-await-in-loop */
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
