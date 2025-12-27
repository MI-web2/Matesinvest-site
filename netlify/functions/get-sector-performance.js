// netlify/functions/get-sector-performance.js
// Returns sector performance for period=1d|5d|1m using stored daily sector levels.
// Reads:
//   asx:sectors:latest
//   asx:sectors:day:YYYY-MM-DD
//
// Strategy:
// - For 1d: use today's ret1d
// - For 5d / 1m: return = level_today / level_then - 1, where "then" is N trading days back
//   (we find trading days by walking back and checking key existence)

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

function assertEnv() {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) throw new Error("Upstash env missing");
}

async function redis(cmdArr) {
  const res = await fetch(`${UPSTASH_URL}/${cmdArr.map(encodeURIComponent).join("/")}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  const json = await res.json();
  if (json.error) throw new Error(json.error);
  return json.result;
}

function parseYmd(s) {
  const [Y, M, D] = s.split("-").map(Number);
  return new Date(Date.UTC(Y, M - 1, D));
}
function ymd(d) {
  return d.toISOString().slice(0, 10);
}

async function loadJson(key) {
  const v = await redis(["GET", key]);
  return v ? JSON.parse(v) : null;
}

async function findNthPrevSectorDate(latestDate, nTradingDaysBack, maxLookbackDays = 60) {
  // Find the date that is N trading days back by walking back and checking existence
  const base = parseYmd(latestDate);
  let found = 0;
  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() - i);
    const cand = ymd(d);
    const exists = await redis(["EXISTS", `asx:sectors:day:${cand}`]);
    if (exists === 1) {
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

    const latestDate = await redis(["GET", "asx:sectors:latest"]);
    if (!latestDate) return { statusCode: 404, body: "No sector data yet" };

    const today = await loadJson(`asx:sectors:day:${latestDate}`);
    if (!today?.sectors) return { statusCode: 404, body: "Sector latest snapshot missing" };

    if (period === "1d") {
      return {
        statusCode: 200,
        body: JSON.stringify({
          period: "1d",
          asOf: latestDate,
          baseDate: today.prevDate || null,
          sectors: today.sectors.map(s => ({
            sector: s.sector,
            value: s.ret1d,
          })),
        }),
      };
    }

    const n = period === "5d" ? 5 : period === "1m" ? 20 : null;
    if (!n) return { statusCode: 400, body: "period must be 1d|5d|1m" };

    const baseDate = await findNthPrevSectorDate(latestDate, n, period === "1m" ? 120 : 60);
    if (!baseDate) {
      return {
        statusCode: 409,
        body: JSON.stringify({ ok: false, reason: `Not enough history for ${period}` }),
      };
    }

    const base = await loadJson(`asx:sectors:day:${baseDate}`);
    if (!base?.sectors) return { statusCode: 500, body: "Base snapshot missing" };

    const baseLevel = new Map(base.sectors.map(s => [s.sector, Number(s.level)]));
    const out = today.sectors.map(s => {
      const lt = Number(s.level);
      const lb = baseLevel.get(s.sector);
      const ret = lb && lb > 0 ? lt / lb - 1 : null;
      return { sector: s.sector, value: ret };
    }).filter(x => x.value != null);

    // Sort best -> worst for nicer bars
    out.sort((a, b) => (b.value ?? 0) - (a.value ?? 0));

    return {
      statusCode: 200,
      body: JSON.stringify({ period, asOf: latestDate, baseDate, sectors: out }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: `Error: ${err.message}` };
  }
};
