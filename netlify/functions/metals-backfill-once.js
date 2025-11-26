// netlify/functions/metals-backfill-once.js
//
// One-off backfill for metals history using Metals-API timeseries.
// Writes to Upstash keys:
//   - history:metal:daily:<SYMBOL>
//
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Usage examples:
//   /.netlify/functions/metals-backfill-once
//     -> backfills NI and LITH-CAR by default
//
//   /.netlify/functions/metals-backfill-once?symbols=NI,LITH-CAR
//   /.netlify/functions/metals-backfill-once?symbols=IRON,NI,LITH-CAR

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // AEST helpers
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

  // Upstash helpers
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

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

  // Sanity ranges (same as snapshot function)
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
    if (
      typeof rateRaw !== "number" ||
      !Number.isFinite(rateRaw) ||
      rateRaw <= 0
    ) {
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
      debug: { candDirect, candInverse, range },
    };
  }

  const HISTORY_MONTHS = Number(process.env.HISTORY_MONTHS || 6);

  // Metals-API unit mapping for timeseries backfill
  const METAL_UNIT_PARAMS = {
    XAU: "Troy Ounce",
    XAG: "Troy Ounce",
    IRON: "Ton",
    "LITH-CAR": "Ton",
    NI: "Ton",
    URANIUM: "Pound",
  };

  // Fetch USD->AUD once
  async function getUsdToAud() {
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
        return Number(fj.rates.AUD);
      }

      // fallback
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
        return Number(fj.rates.AUD);
      }
    } catch (e) {
      console.warn("fx fetch error", e && e.message);
    }
    return null;
  }

  // Backfill ~HISTORY_MONTHS using timeseries in 30-day chunks
  async function backfillMetalHistory(symbol, usdToAud, apiKey) {
    const fromStr = monthsAgoDateStringAest(HISTORY_MONTHS);
    const toStr = getTodayAestDateString();
    const unitParam = METAL_UNIT_PARAMS[symbol] || "Troy Ounce";

    const pointsMap = new Map(); // date -> value (AUD preferred)

    let cursor = fromStr;
    const WINDOW_DAYS = 30;

    while (cursor <= toStr) {
      const endStrCandidate = addDays(cursor, WINDOW_DAYS - 1);
      const endStr = endStrCandidate > toStr ? toStr : endStrCandidate;

      const url =
        `https://metals-api.com/api/timeseries` +
        `?access_key=${encodeURIComponent(apiKey)}` +
        `&base=USD` +
        `&symbols=${encodeURIComponent(symbol)}` +
        `&unit=${encodeURIComponent(unitParam)}` +
        `&start_date=${encodeURIComponent(cursor)}` +
        `&end_date=${encodeURIComponent(endStr)}`;

      let json = null;
      let txt = "";
      try {
        const res = await fetchWithTimeout(url, {}, 12000);
        txt = await res.text().catch(() => "");
        try {
          json = txt ? JSON.parse(txt) : null;
        } catch {
          json = null;
        }
        if (!res.ok || !json || json.success === false) {
          console.warn(
            "metals-api timeseries backfill error",
            symbol,
            cursor,
            endStr,
            txt.slice(0, 300)
          );
          break; // stop on error; partial history is OK
        }
      } catch (e) {
        console.warn(
          "metals-api timeseries fetch error",
          symbol,
          cursor,
          endStr,
          e && e.message
        );
        break;
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
        const value = priceAUD != null ? priceAUD : fmt(priceUSD);
        if (value == null) continue;

        // don't overwrite if already set (first write wins)
        if (!pointsMap.has(date)) {
          pointsMap.set(date, value);
        }
      }

      cursor = addDays(endStr, 1);
    }

    const dates = Array.from(pointsMap.keys()).sort();
    const points = dates.map((d) => [d, pointsMap.get(d)]);

    return {
      symbol,
      startDate: dates[0] || fromStr,
      endDate: dates[dates.length - 1] || toStr,
      lastUpdated: nowIso,
      points,
    };
  }

  async function ensureMetalHistorySeed(symbol, usdToAud, apiKey) {
    const key = `history:metal:daily:${symbol}`;
    const existing = await redisGetJson(key);

    const todayAest = getTodayAestDateString();
    const fromStr = monthsAgoDateStringAest(HISTORY_MONTHS);

    if (
      existing &&
      existing.startDate &&
      existing.endDate &&
      existing.startDate <= fromStr &&
      existing.endDate >= todayAest &&
      Array.isArray(existing.points) &&
      existing.points.length > 0
    ) {
      // already have full-ish window, don't redo
      return { symbol, skipped: true, reason: "already_seeded" };
    }

    const history = await backfillMetalHistory(symbol, usdToAud, apiKey);

    if (history && Array.isArray(history.points) && history.points.length > 0) {
      await redisSet(key, history);
      return {
        symbol,
        skipped: false,
        points: history.points.length,
        startDate: history.startDate,
        endDate: history.endDate,
      };
    }

    return {
      symbol,
      skipped: false,
      points: 0,
      error: "no_points_returned",
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

    const qs = (event && event.queryStringParameters) || {};
    const symbolsParam = (qs.symbols || "").trim();

    // Default to NI and LITH-CAR (the two that were short)
    const defaultSymbols = ["NI", "LITH-CAR"];
    let symbols;
    if (!symbolsParam) {
      symbols = defaultSymbols;
    } else {
      symbols = symbolsParam
        .split(",")
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean);
    }

    // Only allow the known set
    const allowed = ["XAU", "XAG", "IRON", "LITH-CAR", "NI", "URANIUM"];
    symbols = symbols.filter((s) => allowed.includes(s));

    if (!symbols.length) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "No valid symbols to backfill",
          allowed,
        }),
      };
    }

    const usdToAud = await getUsdToAud();

    const results = [];
    for (const sym of symbols) {
      try {
        const res = await ensureMetalHistorySeed(sym, usdToAud, METALS_API_KEY);
        results.push(res);
      } catch (e) {
        console.warn("ensureMetalHistorySeed fatal", sym, e && e.message);
        results.push({
          symbol: sym,
          skipped: false,
          error: e && e.message,
        });
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        ranAt: nowIso,
        usdToAud,
        symbols,
        results,
      }),
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
