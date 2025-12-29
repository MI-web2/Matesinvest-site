// netlify/functions/market-pulse-read.js
//
// Read-only endpoint for ASX Market Pulse
// Supports ?period=1d|5d|1m
//
// Reads (Upstash):
//  - asx:market:pulse:daily
//  - asx:market:pulse:dates
//  - asx:market:pulse:day:YYYY-MM-DD

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const PULSE_LATEST_KEY = "asx:market:pulse:daily";
const PULSE_DATES_SET = "asx:market:pulse:dates";
const PULSE_DAY_PREFIX = "asx:market:pulse:day:";

async function redisGet(key) {
  try {
    const res = await fetch(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j?.result ?? null;
  } catch {
    return null;
  }
}

async function redisSMembers(key) {
  try {
    const res = await fetch(
      `${UPSTASH_URL}/smembers/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
    );
    if (!res.ok) return [];
    const j = await res.json().catch(() => null);
    const arr = j?.result;
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function parse(raw) {
  if (!raw) return null;
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
  return raw;
}

function toFiniteNumber(x) {
  const n = typeof x === "number" ? x : typeof x === "string" ? Number(x) : NaN;
  return Number.isFinite(n) ? n : null;
}

function compoundPct(pcts) {
  // pcts are in percent units (e.g., -0.34)
  let acc = 1;
  let used = 0;
  for (const p of pcts) {
    const v = toFiniteNumber(p);
    if (v == null) continue;
    acc *= 1 + v / 100;
    used++;
  }
  if (used === 0) return null;
  return (acc - 1) * 100;
}

exports.handler = async function (event) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  const periodRaw = event?.queryStringParameters?.period;
  const period = (periodRaw ? String(periodRaw) : "1d").toLowerCase();
  const windowN = period === "5d" ? 5 : period === "1m" ? 21 : 1;

  // ✅ Default behaviour (no change): return latest snapshot for 1d
  if (windowN === 1) {
    const raw = await redisGet(PULSE_LATEST_KEY);
    if (!raw) {
      return {
        statusCode: 503,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-store",
        },
        body: JSON.stringify({ error: "Market pulse not ready yet" }),
      };
    }

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store",
      },
      body: typeof raw === "string" ? raw : JSON.stringify(raw),
    };
  }

  // ✅ 5d / 1m: aggregate from daily history
  const datesRaw = await redisSMembers(PULSE_DATES_SET);
  const dates = datesRaw
    .map((d) => String(d))
    .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort() // ascending
    .reverse(); // descending

  // If no history yet (e.g. before first run with new keys), fallback to latest
  if (dates.length === 0) {
    const raw = await redisGet(PULSE_LATEST_KEY);
    if (!raw) {
      return {
        statusCode: 503,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-store",
        },
        body: JSON.stringify({ error: "Market pulse not ready yet" }),
      };
    }
    const obj = parse(raw) || {};
    obj.period = period;
    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store",
      },
      body: JSON.stringify(obj),
    };
  }

  const takeDates = dates.slice(0, windowN);

  const snapshots = [];
  for (const d of takeDates) {
    const raw = await redisGet(`${PULSE_DAY_PREFIX}${d}`);
    const obj = parse(raw);
    if (obj && obj.asOfDate) snapshots.push(obj);
  }

  if (snapshots.length === 0) {
    return {
      statusCode: 503,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store",
      },
      body: JSON.stringify({ error: "Market pulse history not ready yet" }),
    };
  }

  // Sort snapshots by date descending (most recent first)
  snapshots.sort((a, b) => String(b.asOfDate).localeCompare(String(a.asOfDate)));

  const latest = snapshots[0];
  const oldest = snapshots[snapshots.length - 1];

  const asx200WindowPct = compoundPct(snapshots.map((s) => s?.asx200?.pct));

  // average breadth across window
  const breadthVals = snapshots
    .map((s) => toFiniteNumber(s?.breadthPct))
    .filter((v) => v != null);
  const breadthAvg =
    breadthVals.length > 0
      ? breadthVals.reduce((a, b) => a + b, 0) / breadthVals.length
      : null;

  // sum turnover across window
  const turnoverVals = snapshots
    .map((s) => toFiniteNumber(s?.totalTurnoverAud))
    .filter((v) => v != null);
  const turnoverSum =
    turnoverVals.length > 0 ? turnoverVals.reduce((a, b) => a + b, 0) : null;

  const out = {
    generatedAt: new Date().toISOString(),
    period,
    windowDays: snapshots.length,
    windowStartDate: oldest?.asOfDate ?? null,
    windowEndDate: latest?.asOfDate ?? null,

    // keep some headline fields from latest
    asOfDate: latest?.asOfDate ?? null,
    prevDateUsed: latest?.prevDateUsed ?? null,
    universeCount: latest?.universeCount ?? null,

    asx200: {
      pct: asx200WindowPct,
      constituentsUsed: latest?.asx200?.constituentsUsed ?? null,
    },

    // keep latest day A/D/F (simple + intuitive)
    advancers: latest?.advancers ?? null,
    decliners: latest?.decliners ?? null,
    flat: latest?.flat ?? null,

    // window mood driver
    breadthPct: breadthAvg,

    totalTurnoverAud: turnoverSum,
    turnoverCoverage: latest?.turnoverCoverage ?? null,

    // keep latest movers lists (also simplest)
    topGainers: Array.isArray(latest?.topGainers) ? latest.topGainers : [],
    topLosers: Array.isArray(latest?.topLosers) ? latest.topLosers : [],
  };

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store",
    },
    body: JSON.stringify(out),
  };
};
