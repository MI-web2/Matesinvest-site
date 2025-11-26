// netlify/functions/metals-history-backfill-once.js
//
// One-off backfill for metals history using Metals-API /timeseries.
// Intended mainly to fix NI & LITH-CAR which weren’t being backfilled.
//
// It writes to Upstash keys:
//   history:metal:daily:<SYMBOL>
//
// Usage (once off):
//   /.netlify/functions/metals-history-backfill-once
//   /.netlify/functions/metals-history-backfill-once?symbols=NI,LITH-CAR
//
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Notes:
// - For NI & LITH-CAR we DO NOT send &unit=… on timeseries,
//   and we treat the raw Metals-API rate as an index (no sanity ranges).

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // --- Date helpers (AEST) ---

  function getTodayAestDateString(baseDate = new Date()) {
    const AEST_OFFSET_MINUTES = 10 * 60; // UTC+10
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

  // --- Fetch + Upstash helpers ---

  async function fetchWithTimeout(url, opts = {}, timeout = 10000) {
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

  // --- FX to AUD (for consistency – we can still chart in "index" if needed) ---

  async function fetchUsdToAud() {
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

  // --- Metals-API backfill for a single symbol ---
  // IMPORTANT: no &unit=... here, to match your working NI request.

  async function backfillSymbolTimeseries(symbol, fromStr, toStr, apiKey, usdToAud) {
    const WINDOW_DAYS = 30;
    let cursor = fromStr;
    const pointsMap = new Map(); // date -> value

    const chunkErrors = [];

    while (cursor <= toStr) {
      const endCandidate = addDays(cursor, WINDOW_DAYS - 1);
      const endStr = endCandidate > toStr ? toStr : endCandidate;

      const url =
        `https://metals-api.com/api/timeseries` +
        `?access_key=${encodeURIComponent(apiKey)}` +
        `&base=USD` +
        `&symbols=${encodeURIComponent(symbol)}` +
        `&start_date=${encodeURIComponent(cursor)}` +
        `&end_date=${encodeURIComponent(endStr)}`;

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

        if (!res.ok || !json || json.success === false) {
          console.warn(
            "metals-api timeseries backfill error",
            symbol,
            cursor,
            endStr,
            txt.slice(0, 300)
          );
          chunkErrors.push({
            start: cursor,
            end: endStr,
            status,
            bodyPreview: txt.slice(0, 300),
          });
          // break instead of looping forever
          break;
        }
      } catch (e) {
        console.warn(
          "metals-api timeseries fetch error",
          symbol,
          cursor,
          endStr,
          e && e.message
        );
        chunkErrors.push({
          start: cursor,
          end: endStr,
          status: null,
          bodyPreview: String(e && e.message),
        });
        break;
      }

      const ratesByDate = json.rates || {};
      for (const [date, dailyRates] of Object.entries(ratesByDate)) {
        if (!dailyRates || typeof dailyRates !== "object") continue;

        const rawRate = dailyRates[symbol];
        if (typeof rawRate !== "number" || !Number.isFinite(rawRate) || rawRate <= 0) {
          continue;
        }

        // For NI & LITH-CAR we just treat rawRate as an index.
        // (We don't try to enforce USD/ton sanity ranges here.)
        let value = rawRate;

        // If you really want to see these in "AUD index" you can multiply:
        if (usdToAud != null) {
          value = rawRate * usdToAud;
        }

        const v = fmt(value);
        if (v == null) continue;

        // Only set if not already present (earlier chunk shouldn't be overwritten)
        if (!pointsMap.has(date)) {
          pointsMap.set(date, v);
        }
      }

      cursor = addDays(endStr, 1);
    }

    const dates = Array.from(pointsMap.keys()).sort();
    const points = dates.map((d) => [d, pointsMap.get(d)]);

    return { points, chunkErrors };
  }

  // --- Main ---

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) {
      return { statusCode: 500, body: "Missing METALS_API_KEY" };
    }
    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return { statusCode: 500, body: "Missing Upstash env vars" };
    }

    const qs = (event && event.queryStringParameters) || {};
    const SYMBOLS_PARAM = (qs.symbols || "").trim();
    const symbols =
      SYMBOLS_PARAM.length > 0
        ? SYMBOLS_PARAM.split(",").map((s) => s.trim().toUpperCase())
        : ["NI", "LITH-CAR"]; // default to the two problematic ones

    const HISTORY_MONTHS = Number(process.env.HISTORY_MONTHS || 6);
    const todayAest = getTodayAestDateString();
    const fromStr = monthsAgoDateStringAest(HISTORY_MONTHS);

    const usdToAud = await fetchUsdToAud();

    const results = [];

    for (const symbol of symbols) {
      // Backfill entire 6m window for this symbol
      const { points, chunkErrors } = await backfillSymbolTimeseries(
        symbol,
        fromStr,
        todayAest,
        METALS_API_KEY,
        usdToAud
      );

      if (!points || points.length === 0) {
        results.push({
          symbol,
          skipped: false,
          points: 0,
          error: "no_points_returned",
          chunkErrors,
        });
        continue;
      }

      // Overwrite any existing history for this symbol in Upstash
      const historyKey = `history:metal:daily:${symbol}`;
      const historyObj = {
        symbol,
        startDate: points[0][0],
        endDate: points[points.length - 1][0],
        lastUpdated: nowIso,
        points,
      };

      await redisSet(historyKey, historyObj);

      results.push({
        symbol,
        skipped: false,
        points: points.length,
        error: null,
        chunkErrors,
      });
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
      "metals-history-backfill-once error",
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
