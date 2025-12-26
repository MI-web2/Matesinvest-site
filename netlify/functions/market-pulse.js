// netlify/functions/market-pulse.js
//
// Market Pulse summary endpoint (website-first).
// Reads cached universe data from Upstash (same keys used by equity-screener).
//
// Keys used:
//  - asx:universe:eod:latest                (array or {items:[]})
//  - asx:universe:eod:YYYY-MM-DD            (optional; not required for pulse)
//  - asx:universe:fundamentals:latest       (optional, for count/universe metadata)
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
      // Small cache to keep it snappy (tweak as you like)
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
  const arr = nums.filter((n) => typeof n === "number" && Number.isFinite(n)).sort((a, b) => a - b);
  if (arr.length === 0) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
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

    // Optional: fundamentals for universe metadata (count / generatedAt)
    const rawFund = await redisGet("asx:universe:fundamentals:latest");
    const fundParsed = parseMaybeJson(rawFund);

    // Determine asOfDate from any row that has a date
    const anyWithDate = rows.find((r) => r && r.date);
    const asOfDate = anyWithDate?.date ? String(anyWithDate.date).slice(0, 10) : null;

    let adv = 0;
    let dec = 0;
    let flat = 0;
    const pctArr = [];
    let turnoverAud = 0;
    let turnoverCount = 0;

    // Prepare gainers/losers lists
    const movers = [];

    for (const r of rows) {
      if (!r || !r.code) continue;

      const pct =
        safeNum(r.pctChange) ??
        safeNum(r.changePct) ??
        safeNum(r.change_percent) ??
        null;

      if (pct != null) {
        pctArr.push(pct);
        if (pct > 0) adv++;
        else if (pct < 0) dec++;
        else flat++;
      }

      const price = safeNum(r.close ?? r.price ?? r.last);
      const vol = safeNum(r.volume);

      // Turnover proxy: price * volume (only if both present)
      if (price != null && vol != null) {
        const t = price * vol;
        if (Number.isFinite(t) && t >= 0) {
          turnoverAud += t;
          turnoverCount++;
        }
      }

      movers.push({
        code: String(r.code).toUpperCase(),
        pct: pct,
        close: price,
        volume: vol,
      });
    }

    // Breadth (ignore flat)
    const breadthDen = adv + dec;
    const breadthPct = breadthDen > 0 ? (adv / breadthDen) * 100 : null;

    // Median % change
    const medianPctChange = median(pctArr);

    // Top movers
    const withPct = movers.filter((m) => typeof m.pct === "number" && Number.isFinite(m.pct));
    withPct.sort((a, b) => b.pct - a.pct);

    const topGainers = withPct.slice(0, 5);
    const topLosers = withPct.slice(-5).sort((a, b) => a.pct - b.pct);

    // Universe size (prefer fundamentals metadata if present)
    const universeSize =
      (fundParsed && (fundParsed.universeSize || fundParsed.universeTotal)) || rows.length;

    return json(200, {
      asOfDate,
      generatedAt:
        (fundParsed && fundParsed.generatedAt) ||
        (pricesParsed && pricesParsed.generatedAt) ||
        null,

      universeCount: rows.length,
      universeSize,

      advancers: adv,
      decliners: dec,
      flat,

      breadthPct, // % of advancers among adv+dec
      medianPctChange,

      // Turnover is only a proxy and may be partial depending on volume coverage
      totalTurnoverAud: turnoverCount > 0 ? turnoverAud : null,
      turnoverCoverage: turnoverCount, // how many rows had price+volume

      topGainers,
      topLosers,
    });
  } catch (err) {
    console.error("market-pulse error", err);
    return json(500, { error: "Failed to build market pulse", detail: String(err) }, 0);
  }
};
