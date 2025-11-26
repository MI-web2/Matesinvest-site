// netlify/functions/metals-backfill-once.js
//
// One-off backfill for missing metals history.
// - Uses Metals-API /timeseries ONLY (no /latest).
// - Backfills a 6-month window in ~30-day chunks per symbol.
// - Writes to Upstash keys: history:metal:daily:<SYMBOL>
//
// Usage (manual, once-off):
//   /.netlify/functions/metals-backfill-once        -> default symbols NI,LITH-CAR
//   /.netlify/functions/metals-backfill-once?symbols=NI,LITH-CAR,XAU
//
// Env required:
//   METALS_API_KEY
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// This does NOT touch metals:latest or metals:YYYY-MM-DD.
// Your snapshot-metals.js continues to run daily and append new points.
//

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // --- Simple date helpers (AEST aware) ---
  function getTodayAestDateString(baseDate = new Date()) {
    const AEST_OFFSET_MINUTES = 10 * 60; // Brisbane UTC+10
    const aestTime = new Date(
      baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000
    );
    return aestTime.toISOString().slice(0, 10);
  }

  function monthsAgoDateStringAest(months, baseDate = new Date()) {
    const AEST_OFFSET_MINUTES = 10 * 60;
    const aestTime = new Date(
      baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000
    );
    aestTime.setMonth(aestTime.getMonth() - months);
    return aestTime.toISOString().slice(0, 10);
  }

  function addDays(dateStr, days) {
    const d = new Date(dateStr + "T00:00:00Z");
    d.setDate(d.getDate() + days);
    return d.toISOString().slice(0, 10);
  }

  // --- Upstash helpers ---
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

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

  async function redisSet(key, value) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return false;
    try {
      const encoded =
        typeof value === "string" ? value : JSON.stringify(value);
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/set/${encodeURIComponent(
          key
        )}/${encodeURIComponent(encoded)}`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        console.warn("redisSet failed", key, res.status, txt.slice(0, 300));
      }
      return res.ok;
    } catch (e) {
      console.warn("redisSet error", key, e && e.message);
      return false;
    }
  }

  async function redisGet(key) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
    try {
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
        {
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );
      if (!res.ok) return null;
      const j = await res.json().catch(() => null);
      if (!j || typeof j.result === "undefined") return null;
      return j.result;
    } catch (e) {
      console.warn("redisGet error", key, e && e.message);
      return null;
    }
  }

  async function redisGetJson(key) {
    const raw = await redisGet(key);
    if (!raw) return null;
    if (typeof raw === "string") {
      try {
        return JSON.parse(raw);
      } catch (e) {
        console.warn("redisGetJson parse error", key, e && e.message);
        return null;
      }
    }
    if (typeof raw === "object") return raw;
    return null;
  }

  const fmt = (n) =>
    typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(2)) : null;

  // --- sanity ranges + helper reused from snapshot-metals.js ---
  const VALID_RANGES = {
    XAU: { min: 800, max: 7000 }, // gold per oz
    XAG: { min: 5, max: 150 }, // silver per oz
    IRON: { min: 30, max: 500 }, // iron ore per tonne
    "LITH-CAR": { min: 3000, max: 150000 }, // lithium carbonate per tonne
    NI: { min: 8000, max: 150000 }, // nickel per tonne
    URANIUM: { min: 10, max: 500 }, // uranium per lb
  };

  function isInRange(symbol, price) {
    const range = VALID_RANGES[symbol];
    if (!range || typeof price !== "number" || !Number.isFinite(price)) {
      return false;
    }
    return price >= range.min && price <= range.max;
  }

  function deriveFromRawRate(symbol, rateRaw) {
    if (typeof rateRaw !== "number" || !Number.isFinite(rateRaw) || rateRaw <= 0) {
      return { priceUSD: null, mode: "invalid_rate" };
    }

    const range = VALID_RANGES[symbol];
    const candDirect = rateRaw;
    const candInverse = 1 / rateRaw;

    if (!range) {
      return { priceUSD: candInverse, mode: "inverse_no_range" };
    }

    const directOK = isInRange(symbol, candDirect);
    const inverseOK = isInRange(symbol, candInverse);

    if (directOK && !inverseOK) return { priceUSD: candDirect, mode: "direct" };
    if (!directOK && inverseOK) return { priceUSD: candInverse, mode: "inverse" };

    if (directOK && inverseOK) {
      const mid = (range.min + range.max) / 2;
      const dDist = Math.abs(candDirect - mid);
      const iDist = Math.abs(candInverse - mid);
      return dDist <= iDist
        ? { priceUSD: candDirect, mode: "direct_both_ok" }
        : { priceUSD: candInverse, mode: "inverse_both_ok" };
    }

    return {
      priceUSD: null,
      mode: "out_of_range",
    };
  }

  // Metals-API unit mapping for timeseries
  const METAL_UNIT_PARAMS = {
    XAU: "Troy Ounce",
    XAG: "Troy Ounce",
    IRON: "Ton",
    "LITH-CAR": "Ton",
    NI: "Ton",
    URANIUM: "Pound",
  };

  const HISTORY_MONTHS = Number(process.env.HISTORY_MONTHS || 6);

  async function fetchUsdToAud() {
    let usdToAud = null;
    try {
      let fRes = await fetchWithTimeout(
        "https://open.er-api.com/v6/latest/USD",
        {},
        7000
      );
      let ftxt = await fRes.text().catch(() => "");
      let fj = null;
      try {
        fj = ftxt ? JSON.parse(ftxt) : null;
      } catch {
        fj = null;
      }
      if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === "number") {
        usdToAud = Number(fj.rates.AUD);
      } else {
        fRes = await fetchWithTimeout(
          "https://api.exchangerate.host/latest?base=USD&symbols=AUD",
          {},
          7000
        );
        ftxt = await fRes.text().catch(() => "");
        try {
          fj = ftxt ? JSON.parse(ftxt) : null;
        } catch {
          fj = null;
        }
        if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === "number") {
          usdToAud = Number(fj.rates.AUD);
        }
      }
    } catch (e) {
      console.warn("fx fetch error", e && e.message);
    }
    return usdToAud;
  }

  // Backfill a single symbol using /timeseries in ~30-day windows
  async function backfillSymbol(symbol, usdToAud, apiKey) {
    const unitParam = METAL_UNIT_PARAMS[symbol] || "Troy Ounce";
    const today = getTodayAestDateString();
    const fromStr = monthsAgoDateStringAest(HISTORY_MONTHS);
    const WINDOW_DAYS = 30;

    const pointsMap = new Map(); // date -> AUD/ USD value
    const debugChunkErrors = [];

    let cursor = fromStr;

    while (cursor <= today) {
      const endCandidate = addDays(cursor, WINDOW_DAYS - 1);
      const chunkEnd = endCandidate > today ? today : endCandidate;

      const url =
        `https://metals-api.com/api/timeseries` +
        `?access_key=${encodeURIComponent(apiKey)}` +
        `&base=USD` +
        `&symbols=${encodeURIComponent(symbol)}` +
        `&unit=${encodeURIComponent(unitParam)}` +
        `&start_date=${encodeURIComponent(cursor)}` +
        `&end_date=${encodeURIComponent(chunkEnd)}`;

      let json = null;
      let txt = "";
      let status = null;

      try {
        const res = await fetchWithTimeout(url, {}, 12000);
        status = res.status;
        txt = await res.text().catch(() => "");
        try {
          json = txt ? JSON.parse(txt) : null;
        } catch {
          json = null;
        }

        if (!res.ok || !json || json.success === false || !json.rates) {
          debugChunkErrors.push({
            start: cursor,
            end: chunkEnd,
            status,
            bodyPreview: txt.slice(0, 300),
          });

          // If timeframe is invalid or similar, stop trying more chunks
          if (
            json &&
            json.error &&
            typeof json.error.type === "string" &&
            json.error.type.toLowerCase().includes("timeframe")
          ) {
            break;
          }

          // Otherwise skip this chunk and continue
          cursor = addDays(chunkEnd, 1);
          continue;
        }
      } catch (e) {
        debugChunkErrors.push({
          start: cursor,
          end: chunkEnd,
          status,
          error: e && e.message,
        });
        cursor = addDays(chunkEnd, 1);
        continue;
      }

      const ratesByDate = json.rates || {};
      const usdKey = `USD${symbol}`;

      for (const [date, dailyRates] of Object.entries(ratesByDate)) {
        if (!dailyRates || typeof dailyRates !== "object") continue;

        const directUsd =
          typeof dailyRates[usdKey] === "number" && dailyRates[usdKey] > 0
            ? dailyRates[usdKey]
            : null;

        const rawRate =
          typeof dailyRates[symbol] === "number" && dailyRates[symbol] > 0
            ? dailyRates[symbol]
            : null;

        let priceUSD = null;

        if (directUsd !== null && isInRange(symbol, directUsd)) {
          priceUSD = directUsd;
        } else {
          const derived = deriveFromRawRate(symbol, rawRate);
          priceUSD = derived.priceUSD;
        }

        if (priceUSD == null) continue;

        const priceAUD =
          usdToAud != null ? fmt(priceUSD * usdToAud) : null;
        const value =
          priceAUD != null ? priceAUD : fmt(priceUSD);

        if (value == null) continue;

        // First write wins for a given day
        if (!pointsMap.has(date)) {
          pointsMap.set(date, value);
        }
      }

      cursor = addDays(chunkEnd, 1);
    }

    const dates = Array.from(pointsMap.keys()).sort();
    const points = dates.map((d) => [d, pointsMap.get(d)]);

    if (points.length === 0) {
      return {
        symbol,
        skipped: false,
        points: 0,
        error: "no_points_returned",
        chunkErrors: debugChunkErrors,
      };
    }

    const historyKey = `history:metal:daily:${symbol}`;
    const existing = (await redisGetJson(historyKey)) || null;

    // Merge with existing points if present, preferring backfill for overlaps
    const mergedMap = new Map();

    if (existing && Array.isArray(existing.points)) {
      for (const [d, v] of existing.points) {
        if (!mergedMap.has(d)) mergedMap.set(d, v);
      }
    }
    for (const [d, v] of points) {
      mergedMap.set(d, v);
    }

    const mergedDates = Array.from(mergedMap.keys()).sort();
    const mergedPoints = mergedDates.map((d) => [d, mergedMap.get(d)]);

    const out = {
      symbol,
      startDate: mergedDates[0],
      endDate: mergedDates[mergedDates.length - 1],
      lastUpdated: nowIso,
      points: mergedPoints,
    };

    await redisSet(historyKey, out);

    return {
      symbol,
      skipped: false,
      points: mergedPoints.length,
      error: null,
      chunkErrors: debugChunkErrors,
    };
  }

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) {
      return { statusCode: 500, body: "Missing METALS_API_KEY" };
    }
    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return { statusCode: 500, body: "Missing Upstash env vars" };
    }

    // Parse optional ?symbols= query, default to NI + LITH-CAR
    const qs = (event && event.queryStringParameters) || {};
    const symbolsParam = qs.symbols || qs.symbol || "";
    let symbols;
    if (symbolsParam) {
      symbols = symbolsParam
        .split(",")
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean);
    } else {
      symbols = ["NI", "LITH-CAR"];
    }

    const usdToAud = await fetchUsdToAud();

    const results = [];
    for (const sym of symbols) {
      try {
        const r = await backfillSymbol(sym, usdToAud, METALS_API_KEY);
        results.push(r);
      } catch (e) {
        results.push({
          symbol: sym,
          skipped: false,
          points: 0,
          error: e && e.message ? String(e.message) : "unknown_error",
        });
      }
    }

    const payload = {
      ranAt: nowIso,
      usdToAud,
      symbols,
      results,
    };

    return {
      statusCode: 200,
      body: JSON.stringify(payload),
    };
  } catch (err) {
    console.error(
      "metals-backfill-once error",
      err && (err.stack || err.message || err)
    );
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: (err && err.message) || String(err),
      }),
    };
  }
};
