// netlify/functions/snapshot-crypto.js
// Snapshot for key cryptos (BTC, ETH, SOL, ADA).
//
// For each symbol this retrieves the last 2 EOD bars from EODHD to compute:
//   - today's close (latest EOD)
//   - yesterday's close
//   - percent change
//
// Stores results to Upstash as:
//   crypto:YYYY-MM-DD
//   crypto:latest
//
// Requirements (set in Netlify env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional:
//   QUICK=1 or query ?quick=1 -> process fewer symbols if you want to later.

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;
  if (!EODHD_API_TOKEN) {
    console.error("snapshot-crypto: missing EODHD_API_TOKEN");
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }),
    };
  }

  // ---------- AEST helper ----------
  function getAestDateString(daysOffset = 0, baseDate = new Date()) {
    const AEST_OFFSET_MINUTES = 10 * 60; // UTC+10, no DST
    const aest = new Date(baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
    aest.setDate(aest.getDate() + daysOffset);
    return aest.toISOString().slice(0, 10); // YYYY-MM-DD
  }

  // ---------- Helpers ----------
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
    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      console.warn("snapshot-crypto: missing Upstash env");
      return false;
    }

    try {
      const valString =
        typeof value === "string" ? value : JSON.stringify(value);

      const url =
        `${UPSTASH_URL}/set/` +
        `${encodeURIComponent(key)}/` +
        `${encodeURIComponent(valString)}`;

      const res = await fetchWithTimeout(
        url,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );

      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        console.warn("snapshot-crypto redisSet failed", key, res.status, txt);
        return false;
      }
      return true;
    } catch (e) {
      console.warn("snapshot-crypto redisSet error", key, e && e.message);
      return false;
    }
  }

  const CRYPTO_SYMBOLS = {
    BTC: "BTC-USD.CC",
    ETH: "ETH-USD.CC",
    SOL: "SOL-USD.CC",
    ADA: "ADA-USD.CC",
  };

  async function fetchCryptoTwoDayWindow(prettyCode, eodSymbol) {
    const url = `https://eodhd.com/api/eod/${eodSymbol}?api_token=${EODHD_API_TOKEN}&order=d&fmt=json&limit=2`;
    const res = await fetchWithTimeout(url, {}, 10000);

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(
        `EODHD error for ${prettyCode} (${eodSymbol}): ${res.status} ${res.statusText} ${txt}`
      );
    }

    const data = await res.json();
    if (!Array.isArray(data) || data.length < 1) {
      throw new Error(`Not enough history for ${prettyCode} (${eodSymbol})`);
    }

    const [latest, prev] = data;
    const latestClose =
      typeof latest.close === "number"
        ? latest.close
        : latest.close
        ? Number(latest.close)
        : null;
    const prevClose =
      prev && (typeof prev.close === "number" || prev.close)
        ? Number(prev.close)
        : null;

    let pctChange = null;
    if (
      latestClose !== null &&
      prevClose !== null &&
      prevClose !== 0 &&
      Number.isFinite(latestClose) &&
      Number.isFinite(prevClose)
    ) {
      const raw = ((latestClose - prevClose) / prevClose) * 100;
      if (Number.isFinite(raw) && Math.abs(raw) < 1000) {
        pctChange = Number(raw.toFixed(2));
      }
    }

    return {
      code: prettyCode,
      symbol: eodSymbol,
      todayDate: latest.date || null,
      todayCloseUSD: latestClose,
      yesterdayDate: prev ? prev.date || null : null,
      yesterdayCloseUSD: prevClose,
      pctChange,
      raw: { latest, prev },
    };
  }

  const debug = { steps: [] };

  try {
    const results = [];
    for (const [pretty, eodSymbol] of Object.entries(CRYPTO_SYMBOLS)) {
      try {
        const r = await fetchCryptoTwoDayWindow(pretty, eodSymbol);
        results.push(r);
        debug.steps.push({
          source: "crypto-fetched",
          code: pretty,
          todayDate: r.todayDate,
          pctChange: r.pctChange,
        });
      } catch (e) {
        console.warn("snapshot-crypto fetch error", pretty, e && e.message);
        debug.steps.push({
          source: "crypto-fetch-error",
          code: pretty,
          error: e && e.message,
        });
      }
    }

    const symbols = {};
    let latestDate = null;

    for (const r of results) {
      symbols[r.code] = {
        todayDate: r.todayDate,
        todayCloseUSD: r.todayCloseUSD,
        yesterdayDate: r.yesterdayDate,
        yesterdayCloseUSD: r.yesterdayCloseUSD,
        pctChange: r.pctChange,
      };
      if (r.todayDate && (!latestDate || r.todayDate > latestDate)) {
        latestDate = r.todayDate;
      }
    }

    const snapshot = {
      snappedAt: nowIso,
      base: "USD",
      symbols,
    };

    // AEST date key based on the latest EOD date we saw
    let keyDate;
    if (latestDate) {
      const base = new Date(`${latestDate}T00:00:00Z`);
      keyDate = getAestDateString(0, base);
    } else {
      keyDate = getAestDateString(0);
    }

    await redisSet("crypto:latest", snapshot);
    await redisSet(`crypto:${keyDate}`, snapshot);

    debug.keyDate = keyDate;

    console.log("[snapshot-crypto] snapshot saved", {
      keyDate,
      symbols: Object.keys(symbols),
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        snappedAt: nowIso,
        keyDate,
        symbols: Object.keys(symbols),
        _debug: debug,
      }),
    };
  } catch (err) {
    console.error("snapshot-crypto fatal error", err && err.message);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: (err && err.message) || String(err),
        _debug: debug,
      }),
    };
  }
};
