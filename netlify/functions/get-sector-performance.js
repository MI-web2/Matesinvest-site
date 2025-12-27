// netlify/functions/get-sector-performance.js
// Reads cached sector snapshots and returns sector performance for 1d/5d/1m.
// Requires market-pulse writer to have written:
//   asx:sectors:day:YYYY-MM-DD
//   asx:sectors:latest

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

function assertEnv() {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) throw new Error("Upstash not configured");
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

async function redisGet(key) {
  const res = await fetchWithTimeout(
    `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
    { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    12000
  );
  if (!res.ok) return null;
  const j = await res.json().catch(() => null);
  return j?.result ?? null;
}

function parse(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function parseYmd(s) {
  const [Y, M, D] = String(s).slice(0, 10).split("-").map(Number);
  return new Date(Date.UTC(Y, M - 1, D));
}
function ymd(d) {
  return d.toISOString().slice(0, 10);
}

async function exists(key) {
  const res = await fetchWithTimeout(
    `${UPSTASH_URL}/exists/${encodeURIComponent(key)}`,
    { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    12000
  );
  if (!res.ok) return 0;
  const j = await res.json().catch(() => null);
  return j?.result ?? 0;
}

async function findNthPrevSectorDate(latestDate, nTradingDaysBack, maxLookbackDays) {
  const base = parseYmd(latestDate);
  let found = 0;

  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() - i);
    const cand = ymd(d);

    const ok = await exists(`asx:sectors:day:${cand}`);
    if (ok === 1) {
      found++;
      if (found === nTradingDaysBack) return cand;
    }
  }
  return null;
}

exports.handler = async function (event) {
  try {
    assertEnv();

    const url = new URL(event.rawUrl);
    const period = (url.searchParams.get("period") || "1d").toLowerCase();

    const latestDate = await redisGet("asx:sectors:latest");
    if (!latestDate) {
      return { statusCode: 404, body: JSON.stringify({ error: "No sector data yet" }) };
    }

    const today = parse(await redisGet(`asx:sectors:day:${latestDate}`));
    if (!today?.sectors) {
      return { statusCode: 404, body: JSON.stringify({ error: "Latest sector snapshot missing" }) };
    }

    if (period === "1d") {
      const out = today.sectors
        .map((s) => ({ sector: s.sector, value: s.ret1d }))
        .filter((x) => typeof x.value === "number" && Number.isFinite(x.value));

      out.sort((a, b) => b.value - a.value);

return {
  statusCode: 200,
  headers: {
    "Content-Type": "application/json",
    "Cache-Control": "public, max-age=300, s-maxage=300, stale-while-revalidate=600"
  },
  body: JSON.stringify({
    period: "1d",
    asOf: latestDate,
    baseDate: today.prevDate || null,
    sectors: out
  }),
};
    }

    const n = period === "5d" ? 5 : period === "1m" ? 20 : null;
    if (!n) {
      return { statusCode: 400, body: JSON.stringify({ error: "period must be 1d|5d|1m" }) };
    }

    const baseDate = await findNthPrevSectorDate(latestDate, n, period === "1m" ? 140 : 80);
    if (!baseDate) {
      return { statusCode: 409, body: JSON.stringify({ error: `Not enough history for ${period}` }) };
    }

    const base = parse(await redisGet(`asx:sectors:day:${baseDate}`));
    if (!base?.sectors) {
      return { statusCode: 500, body: JSON.stringify({ error: "Base sector snapshot missing" }) };
    }

    const baseLevel = new Map();
    for (const s of base.sectors) baseLevel.set(s.sector, Number(s.level));

    const out = [];
    for (const s of today.sectors) {
      const lt = Number(s.level);
      const lb = baseLevel.get(s.sector);
      if (!Number.isFinite(lt) || !Number.isFinite(lb) || lb <= 0) continue;
      out.push({ sector: s.sector, value: lt / lb - 1 });
    }

    out.sort((a, b) => b.value - a.value);

return {
  statusCode: 200,
  headers: {
    "Content-Type": "application/json",
    "Cache-Control": "public, max-age=300, s-maxage=300, stale-while-revalidate=600"
  },
  body: JSON.stringify({
    period,
    asOf: latestDate,
    baseDate,
    sectors: out
  }),
};
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message || String(err) }) };
  }
};
