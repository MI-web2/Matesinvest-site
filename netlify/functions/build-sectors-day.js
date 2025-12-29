// netlify/functions/build-sectors-day.js
// Builds daily sector returns + a simple sector "level" series from cached Upstash data.
// Uses:
//   asx:universe:eod:YYYY-MM-DD         (array of {code,date,close,volume,...})
//   asx:universe:fundamentals:latest    (array of {code, sector, marketCap,...})
//
// Writes:
//   asx:sectors:day:YYYY-MM-DD          (json)
//   asx:sectors:latest                  (string YYYY-MM-DD)
//   asx:sectors:dates                   (SET of YYYY-MM-DD strings, used for fast lookbacks)

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

function ymd(d) {
  return d.toISOString().slice(0, 10);
}

function parseYmd(s) {
  // Treat as UTC midnight
  const [Y, M, D] = s.split("-").map(Number);
  return new Date(Date.UTC(Y, M - 1, D));
}

async function findPrevTradingDate(asOfDate, maxLookbackDays = 10) {
  // Walk backwards day-by-day until we find an existing eod snapshot key
  const base = parseYmd(asOfDate);
  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() - i);
    const cand = ymd(d);
    const exists = await redis(["EXISTS", `asx:universe:eod:${cand}`]);
    if (exists === 1) return cand;
  }
  return null;
}

async function loadJson(key) {
  const v = await redis(["GET", key]);
  if (!v) return null;
  return JSON.parse(v);
}

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

exports.handler = async function (event) {
  try {
    assertEnv();

    // You can pass ?date=YYYY-MM-DD, otherwise use sectors:latest or universe latest
    const url = new URL(event.rawUrl);
    const asOfDate = url.searchParams.get("date");

    // If no date passed, try read "asx:market:pulse:latest" (if you have it) then fall back to sectors:latest + 1
    // But simplest: require date from your scheduler
    if (!asOfDate) {
      return { statusCode: 400, body: "Missing ?date=YYYY-MM-DD" };
    }

    const prevDate = await findPrevTradingDate(asOfDate, 14);
    if (!prevDate) {
      return { statusCode: 409, body: `No prior trading day snapshot found before ${asOfDate}` };
    }

    const [todayArr, prevArr, fundamentalsArr] = await Promise.all([
      loadJson(`asx:universe:eod:${asOfDate}`),
      loadJson(`asx:universe:eod:${prevDate}`),
      loadJson(`asx:universe:fundamentals:latest`),
    ]);

    if (!Array.isArray(todayArr) || !Array.isArray(prevArr) || !Array.isArray(fundamentalsArr)) {
      return { statusCode: 500, body: "Missing/invalid cached arrays (eod or fundamentals)" };
    }

    // Build maps for joins
    const prevCloseByCode = new Map();
    for (const r of prevArr) {
      if (!r || !r.code) continue;
      const c = safeNum(r.close);
      if (c != null && c > 0) prevCloseByCode.set(r.code, c);
    }

    const fundamentalsByCode = new Map();
    for (const f of fundamentalsArr) {
      if (!f || !f.code) continue;
      fundamentalsByCode.set(f.code, {
        sector: f.sector || "Other",
        marketCap: safeNum(f.marketCap),
      });
    }

    // Aggregate market-cap weighted return by sector
    // ret = (close_today / close_prev) - 1
    const agg = new Map(); // sector -> {wRetSum, wSum, stocks, mcapCovered}
    let used = 0;
    for (const r of todayArr) {
      if (!r || !r.code) continue;
      const close = safeNum(r.close);
      const prevClose = prevCloseByCode.get(r.code);
      if (close == null || close <= 0 || prevClose == null || prevClose <= 0) continue;

      const f = fundamentalsByCode.get(r.code);
      if (!f) continue;

      const sector = f.sector || "Other";
      const mcap = f.marketCap;

      // Require market cap for weighting; if you want, you can fall back to equal-weight here
      if (mcap == null || mcap <= 0) continue;

      const ret = close / prevClose - 1;

      const cur = agg.get(sector) || { wRetSum: 0, wSum: 0, stocks: 0, mcapCovered: 0 };
      cur.wRetSum += mcap * ret;
      cur.wSum += mcap;
      cur.stocks += 1;
      cur.mcapCovered += mcap;
      agg.set(sector, cur);
      used++;
    }

    if (used < 50) {
      // sanity check - adjust as needed
      console.warn(`Low coverage: used=${used}`);
    }

    // Load yesterday sector snapshot to roll a simple "level" series
    const prevSectorSnap = await loadJson(`asx:sectors:day:${prevDate}`);
    const prevLevelBySector = new Map();
    if (prevSectorSnap && Array.isArray(prevSectorSnap.sectors)) {
      for (const s of prevSectorSnap.sectors) {
        if (s?.sector && Number.isFinite(Number(s.level))) {
          prevLevelBySector.set(s.sector, Number(s.level));
        }
      }
    }

    const sectorsOut = [];
    for (const [sector, v] of agg.entries()) {
      const ret1d = v.wSum > 0 ? v.wRetSum / v.wSum : 0;
      const prevLevel = prevLevelBySector.get(sector) ?? 100; // start at 100 if no history
      const level = prevLevel * (1 + ret1d);

      sectorsOut.push({
        sector,
        ret1d,
        level,
        coverage: { stocks: v.stocks, mcap: v.mcapCovered },
      });
    }

    // Sort by performance desc (nice for chart)
    sectorsOut.sort((a, b) => (b.ret1d ?? 0) - (a.ret1d ?? 0));

    const out = {
      date: asOfDate,
      prevDate,
      method: "mcap_weighted",
      sectors: sectorsOut,
    };

    await Promise.all([
      redis(["SET", `asx:sectors:day:${asOfDate}`, JSON.stringify(out)]),
      redis(["SET", `asx:sectors:latest`, asOfDate]),
      redis(["SADD", `asx:sectors:dates`, asOfDate]),
    ]);

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, date: asOfDate, prevDate, sectors: sectorsOut.length }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: `Error: ${err.message}` };
  }
};
