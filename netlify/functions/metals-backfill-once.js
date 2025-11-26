// netlify/functions/metals-backfill-once.js
//
// One-off backfill for NI and LITH-CAR into Upstash:
//   - history:metal:daily:<SYMBOL>
//
// Usage examples:
//   /.netlify/functions/metals-backfill-once
//   /.netlify/functions/metals-backfill-once?symbols=NI,LITH-CAR
//
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // AEST helpers
  function getTodayAestDateString(baseDate = new Date()) {
    const AEST_OFFSET_MIN = 10 * 60; // Brisbane UTC+10 year-round
    const aest = new Date(baseDate.getTime() + AEST_OFFSET_MIN * 60 * 1000);
    return aest.toISOString().slice(0, 10);
  }

  function monthsAgoDateStringAest(months, baseDate = new Date()) {
    const AEST_OFFSET_MIN = 10 * 60;
    const aest = new Date(baseDate.getTime() + AEST_OFFSET_MIN * 60 * 1000);
    aest.setMonth(aest.getMonth() - months);
    return aest.toISOString().slice(0, 10);
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

  const fmt = (n) =>
    typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(2)) : null;

  // Sanity ranges (same as snapshot-metals)
  const VALID_RANGES = {
    XAU: { min: 800, max: 7000 },
    XAG: { min: 5, max: 150 },
    IRON: { min: 30, max: 500 },
    "LITH-CAR": { min: 3000, max: 150000 },
    NI: { min: 8000, max: 150000 },
    URANIUM: { min: 10, max: 500 },
  };

  function isInRange(symbol, price) {
    const range = VALID_RANGES[symbol];
    if (!range || typeof price !== "number" || !Number.isFinite(price)) {
      return false;
    }
    return price >= range.min && price <= range.max;
  }

  // Same fallback logic as snapshot-metals
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

  const METAL_UNIT_PARAMS = {
    XAU: "Troy Ounce",
    XAG: "Troy Ounce",
    IRON: "Ton",
    "LITH-CAR": "Ton",
    NI: "Ton",
    URANIUM: "Pound",
  };

  const HISTORY_MONTHS = Number(process.env.HISTORY_MONTHS || 6);

  async function getUsdToAud() {
    let usdToAud = null;
    try {
      let res = await fetchWithTimeout(
        "https://open.er-api.com/v6/latest/USD",
        {},
        7000
      );
      let txt = await res.text().catch(() => "");
      let j = null;
      try {
        j = txt ? JSON.parse(txt) : null;
      } catch {
        j = null;
      }
      if (res.ok && j && j.rates && typeof j.rates.AUD === "number") {
        usdToAud = Number(j.rates.AUD);
      } else {
        res = await fetchWithTimeout(
          "https://api.exchangerate.host/latest?base=USD&symbols=AUD",
          {},
          7000
        );
        txt = await res.text().catch(() => "");
        try {
          j = txt ? JSON.parse(txt) : null;
        } catch {
          j = null;
        }
        if (res.ok && j && j.rates && typeof j.rates.AUD === "number") {
          usdToAud = Number(j.rates.AUD);
        }
      }
    } catch (e) {
      console.warn("fx fetch error", e && e.message);
    }
    return usdToAud;
  }

  async function backfillSymbol(symbol, usdToAud, apiKey) {
    const unitParam = METAL_UNIT_PARAMS[symbol] || "Troy Ounce";
    const startDate = monthsAgoDateStringAest(HISTORY_MONTHS);
    const endDate = getTodayAestDateString();

    const url =
      `https://metals-api.com/api/timeseries` +
      `?access_key=${encodeURIComponent(apiKey)}` +
      `&base=USD` +
      `&symbols=${encodeURIComponent(symbol)}` +
      `&unit=${encodeURIComponent(unitParam)}` +
      `&start_date=${encodeURIComponent(startDate)}` +
      `&end_date=${encodeURIComponent(endDate)}`;

    let txt = "";
    let json = null;

    try {
      const res = await fetchWithTimeout(url, {}, 15000);
      txt = await res.text().catch(() => "");
      try {
        json = txt ? JSON.parse(txt) : null;
      } catch {
        json = null;
      }

      if (!res.ok || !json) {
        return {
          symbol,
          skipped: false,
          points: 0,
          error: `http_${res.status || "?"}`,
          apiBodyPreview: txt.slice(0, 220),
        };
      }

      if (json.success === false) {
        return {
          symbol,
          skipped: false,
          points: 0,
          error: json.error && json.error.type
            ? json.error.type
            : "api_success_false",
          apiErrorInfo: json.error || null,
        };
      }

      const ratesByDate = json.rates || {};
      const dates = Object.keys(ratesByDate).sort();
      const usdKey = `USD${symbol}`;

      const points = [];

      for (const d of dates) {
        const dailyRates = ratesByDate[d];
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
          usdToAud != null ? fmt(priceUSD * usdToAud) : fmt(priceUSD);

        if (priceAUD == null) continue;

        points.push([d, priceAUD]);
      }

      if (!points.length) {
        return {
          symbol,
          skipped: false,
          points: 0,
          error: "no_points_returned",
          sampleDates: dates.slice(0, 5),
          sampleFirstDay: dates[0]
            ? ratesByDate[dates[0]]
            : null,
        };
      }

      // Persist into same key shape used by instrument-details.js
      const historyKey = `history:metal:daily:${symbol}`;
      const historyPayload = {
        symbol,
        startDate,
        endDate,
        lastUpdated: nowIso,
        points,
      };
      await redisSet(historyKey, historyPayload);

      return {
        symbol,
        skipped: false,
        points: points.length,
        error: null,
        startDate,
        endDate,
      };
    } catch (e) {
      console.warn("backfillSymbol error", symbol, e && e.message);
      return {
        symbol,
        skipped: false,
        points: 0,
        error: e && e.message ? e.message : "exception",
      };
    }
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
    const symbolsParam = (qs.symbols || qs.symbol || "").trim();
    const symbols = symbolsParam
      ? symbolsParam
          .split(",")
          .map((s) => s.trim().toUpperCase())
          .filter(Boolean)
      : ["NI", "LITH-CAR"]; // default

    const usdToAud = await getUsdToAud();

    const results = [];
    for (const s of symbols) {
      const r = await backfillSymbol(s, usdToAud, METALS_API_KEY);
      results.push(r);
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
      "metals-backfill-once fatal",
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
