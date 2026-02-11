// netlify/functions/market-pulse-window-cache.js
//
// Scheduled @ 6:15am AEST:
// Precomputes 1D / 5D / 1M market pulse “window” snapshots so the UI can fetch instantly.
//
// Reads:
//  - asx:market:pulse:dates (set of YYYY-MM-DD)
//  - asx:market:pulse:day:YYYY-MM-DD
//
// Writes:
//  - asx:market:pulse:window:1d
//  - asx:market:pulse:window:5d
//  - asx:market:pulse:window:1m
//
// Also (optional): writes asx:market:pulse:daily to match 1d window, if you want.
// (Left OFF by default.)

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const PULSE_DATES_SET = "asx:market:pulse:dates";
const PULSE_DAY_PREFIX = "asx:market:pulse:day:";

const WIN_1D = "asx:market:pulse:window:1d";
const WIN_5D = "asx:market:pulse:window:5d";
const WIN_1M = "asx:market:pulse:window:1m";

// Optional: keep old key in sync (leave false unless you want it)
const ALSO_WRITE_DAILY_POINTER = true;
const PULSE_LATEST_KEY = "asx:market:pulse:daily";

async function redisGet(key) {
  const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  if (!res.ok) return null;
  const j = await res.json().catch(() => null);
  return j?.result ?? null;
}

async function redisSetJson(key, obj) {
  const payload = JSON.stringify(obj);
  const res = await fetch(
    `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}`,
    { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SET failed (${res.status}): ${txt}`);
  }
}

async function redisSMembers(key) {
  const res = await fetch(`${UPSTASH_URL}/smembers/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  if (!res.ok) return [];
  const j = await res.json().catch(() => null);
  return Array.isArray(j?.result) ? j.result : [];
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

function toNum(x) {
  const n = typeof x === "number" ? x : typeof x === "string" ? Number(x) : NaN;
  return Number.isFinite(n) ? n : null;
}

function compoundPct(pcts) {
  let acc = 1;
  let used = 0;
  for (const p of pcts) {
    const v = toNum(p);
    if (v == null) continue;
    acc *= 1 + v / 100;
    used++;
  }
  return used ? (acc - 1) * 100 : null;
}

function avg(nums) {
  const clean = nums.map(toNum).filter((v) => v != null);
  if (!clean.length) return null;
  return clean.reduce((a, b) => a + b, 0) / clean.length;
}

function sum(nums) {
  const clean = nums.map(toNum).filter((v) => v != null);
  if (!clean.length) return null;
  return clean.reduce((a, b) => a + b, 0);
}

function buildWindow(period, snapshots) {
  // snapshots should be DESC by date (most recent first)
  const latest = snapshots[0];
  const oldest = snapshots[snapshots.length - 1];

  const out = {
    generatedAt: new Date().toISOString(),
    period,
    windowDays: snapshots.length,
    windowStartDate: oldest?.asOfDate ?? null,
    windowEndDate: latest?.asOfDate ?? null,

    asOfDate: latest?.asOfDate ?? null,
    prevDateUsed: latest?.prevDateUsed ?? null,
    universeCount: latest?.universeCount ?? null,

    asx200: {
      pct: compoundPct(snapshots.map((s) => s?.asx200?.pct)),
      constituentsUsed: latest?.asx200?.constituentsUsed ?? null,
    },

    // keep latest A/D/F + movers (simple, fast, readable)
    advancers: latest?.advancers ?? null,
    decliners: latest?.decliners ?? null,
    flat: latest?.flat ?? null,

    // window mood driver
    breadthPct: avg(snapshots.map((s) => s?.breadthPct)),

    totalTurnoverAud: sum(snapshots.map((s) => s?.totalTurnoverAud)),
    turnoverCoverage: latest?.turnoverCoverage ?? null,

    topGainers: Array.isArray(latest?.topGainers) ? latest.topGainers : [],
    topLosers: Array.isArray(latest?.topLosers) ? latest.topLosers : [],
  };

  return out;
}

async function loadMostRecentSnapshots(n) {
  const datesRaw = await redisSMembers(PULSE_DATES_SET);
  const dates = datesRaw
    .map((d) => String(d))
    .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort()
    .reverse(); // DESC

  const take = dates.slice(0, n);
  const snaps = [];

  for (const d of take) {
    const raw = await redisGet(`${PULSE_DAY_PREFIX}${d}`);
    const obj = parse(raw);
    if (obj?.asOfDate) snaps.push(obj);
  }

  // ensure DESC
  snaps.sort((a, b) => String(b.asOfDate).localeCompare(String(a.asOfDate)));
  return snaps;
}

exports.handler = async function () {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    // 1D uses latest available day
    const snaps1 = await loadMostRecentSnapshots(1);
    if (!snaps1.length) {
      return { statusCode: 503, body: "No pulse history available to build windows" };
    }

    // 5D + 1M
    const snaps5 = await loadMostRecentSnapshots(5);
    const snaps21 = await loadMostRecentSnapshots(21);

    const win1 = buildWindow("1d", snaps1);
    const win5 = buildWindow("5d", snaps5.length ? snaps5 : snaps1);
    const win1m = buildWindow("1m", snaps21.length ? snaps21 : snaps5.length ? snaps5 : snaps1);

    await redisSetJson(WIN_1D, win1);
    await redisSetJson(WIN_5D, win5);
    await redisSetJson(WIN_1M, win1m);

    if (ALSO_WRITE_DAILY_POINTER) {
      await redisSetJson(PULSE_LATEST_KEY, win1);
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "text/plain" },
      body: `OK window cache: ${win1.asOfDate} (1d), ${win5.windowDays}d, ${win1m.windowDays}d`,
    };
  } catch (err) {
    console.error("market-pulse-window-cache error", err);
    return { statusCode: 500, body: `Failed: ${err?.message || String(err)}` };
  }
};
