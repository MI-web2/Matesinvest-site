// netlify/functions/market-pulse.js
//
// Market Pulse summary endpoint (website-first).
// Reads cached universe data from Upstash (same keys used by equity-screener).
//
// Keys used:
//  - asx:universe:eod:latest                (array or {items:[]})
//  - asx:universe:eod:YYYY-MM-DD            (daily snapshots; used for prev close lookback)
//  - asx:universe:fundamentals:latest       (optional; for generatedAt/universe metadata)
//
// Returns a compact JSON summary suitable for a dashboard header.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

async function fetchWithTimeout(url, opts = {}, timeout = 8000) {
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

async function redisGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  try {
    const res = await fetchWithTimeout(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      10000
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j && typeof j.result !== "undefined" ? j.result : null;
  } catch (err) {
    console.warn("market-pulse redisGet error", key, err && err.message);
    return null;
  }
}

function json(statusCode, bodyObj, cacheSeconds = 120) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": `public, max-age=${cacheSeconds}`,
    },
    body: JSON.stringify(bodyObj),
  };
}

function safeNum(x) {
  return typeof x === "number" && Number.isFinite(x) ? x : null;
}

function parseMaybeJson(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function median(nums) {
  const arr = nums
    .filter((n) => typeof n === "number" && Number.isFinite(n))
    .sort((a, b) => a - b);
  if (arr.length === 0) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}

function toISODate(d) {
  // returns YYYY-MM-DD in UTC
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

async function getPrevDayCloseMapFromCache(latestDateStr) {
  // Looks back up to 7 days to find the previous cached daily snapshot.
  // Returns { prevCloseMap, prevDateUsed } or { {}, null }.

  if (!latestDateStr) return { prevCloseMap: {}, prevDateUsed: null };

  // Parse latestDateStr as UTC date
  const base = new Date(`${latestDateStr}T00:00:00Z`);
  if (Number.isNaN(base.getTime())) return { prevCloseMap: {}, prevDateUsed: null };

  for (let i = 1; i <= 7; i++) {
    const d = new Date(base.getTime());
    d.setUTCDate(d.getUTCDate() - i);
    const keyDate = toISODate(d);
    const key = `asx:universe:eod:${keyDate}`;

    const raw = await redisGet(key);
    const parsed = parseMaybeJson(raw);

    const rows = Array.isArray(parsed)
      ? parsed
      : Array.isArray(parsed?.items)
      ? parsed.items
      : null;

    if (!rows || rows.length < 50) continue;

    const map = {};
    for (const r of rows) {
      if (!r || !r.code) continue;
      const code = String(r.code).toUpperCase();
      const close = safeNum(r.close ?? r.price ?? r.last);
      if (close != null) map[code] = close;
    }

    // If we got a decent map, accept this as prev day
    if (Object.keys(map).length > 50) {
      return { prevCloseMap: map, prevDateUsed: keyDate };
    }
  }

  return { prevCloseMap: {}, prevDateUsed: null };
}

exports.handler = async function (event) {
  // CORS preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
      },
      body: "",
    };
  }

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return json(500, { error: "Missing Upstash env for market pulse" }, 0);
  }

  try {
    const rawPrices = await redisGet("asx:universe:eod:latest");
    const pricesParsed = parseMaybeJson(rawPrices);

    const rows = Array.isArray(pricesParsed)
      ? pricesParsed
      : Array.isArray(pricesParsed?.items)
      ? pricesParsed.items
      : null;

    if (!rows || rows.length < 50) {
      return json(503, { error: "No price snapshot available yet" }, 30);
    }

    // Optional: fundamentals metadata
    const rawFund = await redisGet("asx:universe:fundamentals:latest");
    const fundParsed = parseMaybeJson(rawFund);

    // Determine asOfDate from row date (preferred), else from pricesParsed.generatedAt
    const anyWithDate = rows.find((r) => r && r.date);
    const asOfDate = anyWithDate?.date ? String(anyWithDate.date).slice(0, 10) : null;

    // Build prevCloseMap for pctChange fallback (like equity-screener)
    const { prevCloseMap, prevDateUsed } = await getPrevDayCloseMapFromCache(asOfDate);

    let adv = 0;
    let dec = 0;
    let flat = 0;
    const pctArr = [];
    let turnoverAud = 0;
    let turnoverCount = 0;

    const movers = [];

    for (const r of rows) {
      if (!r || !r.code) continue;

      const code = String(r.code).toUpperCase();
      const close = safeNum(r.close ?? r.price ?? r.last);
      const vol = safeNum(r.volume);

      // Turnover proxy: close * volume
      if (close != null && vol != null) {
        const t = close * vol;
        if (Number.isFinite(t) && t >= 0) {
          turnoverAud += t;
          turnoverCount++;
        }
      }

      // pctChange: prefer what exists, else derive from prevCloseMap
      let pct =
        safeNum(r.pctChange) ??
        safeNum(r.changePct) ??
        safeNum(r.change_percent) ??
        null;

      if (pct == null && close != null) {
        const prev = safeNum(r.prevClose) ?? safeNum(prevCloseMap[code]);
        if (prev != null && prev > 0) {
          pct = ((close - prev) / prev) * 100;
        }
      }

      if (pct != null) {
        pctArr.push(pct);
        if (pct > 0) adv++;
        else if (pct < 0) dec++;
        else flat++;
      }

      movers.push({
        code,
        pct,
        close,
        volume: vol,
      });
    }

    const breadthDen = adv + dec;
    const breadthPct = breadthDen > 0 ? (adv / breadthDen) * 100 : null;
    const medianPctChange = median(pctArr);

    const withPct = movers.filter((m) => typeof m.pct === "number" && Number.isFinite(m.pct));
    withPct.sort((a, b) => b.pct - a.pct);

    const topGainers = withPct.slice(0, 5);
    const topLosers = withPct.slice(-5).sort((a, b) => a.pct - b.pct);

    const universeSize =
      (fundParsed && (fundParsed.universeSize || fundParsed.universeTotal)) || rows.length;

    return json(200, {
      asOfDate,
      prevDateUsed, // <-- useful to debug holidays/weekends
      generatedAt:
        (fundParsed && fundParsed.generatedAt) ||
        (pricesParsed && pricesParsed.generatedAt) ||
        null,

      universeCount: rows.length,
      universeSize,

      advancers: adv,
      decliners: dec,
      flat,

      breadthPct,
      medianPctChange,

      totalTurnoverAud: turnoverCount > 0 ? turnoverAud : null,
      turnoverCoverage: turnoverCount,

      topGainers,
      topLosers,
    });
  } catch (err) {
    console.error("market-pulse error", err);
    return json(500, { error: "Failed to build market pulse", detail: String(err) }, 0);
  }
};
