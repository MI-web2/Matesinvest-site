// netlify/functions/build-sectors-day.js
// Builds daily sector returns + a simple sector "level" series from cached Upstash data.
// Uses:
//   asx:universe:eod:YYYY-MM-DD         (array of {code,date,close,volume,...})
//   asx:universe:eod:latest             (string YYYY-MM-DD OR json {date/asOfDate/...})
//   asx:universe:fundamentals:latest    (array of {code, sector, marketCap,...})
//
// Writes:
//   asx:sectors:day:YYYY-MM-DD          (json)
//   asx:sectors:latest                  (string YYYY-MM-DD)
//   asx:sectors:dates                   (SET of YYYY-MM-DD)

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const SECTOR_DATES_SET = "asx:sectors:dates";

function assertEnv() {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) throw new Error("Upstash env missing");
}

async function redis(cmdArr) {
  const res = await fetch(`${UPSTASH_URL}/${cmdArr.map(encodeURIComponent).join("/")}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`Upstash HTTP ${res.status}`);
  if (json.error) throw new Error(json.error);
  return json.result;
}

function ymd(d) {
  return d.toISOString().slice(0, 10);
}

function parseYmd(s) {
  const [Y, M, D] = String(s).slice(0, 10).split("-").map(Number);
  return new Date(Date.UTC(Y, M - 1, D));
}

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

async function loadJson(key) {
  const v = await redis(["GET", key]);
  if (!v) return null;
  try {
    return JSON.parse(v);
  } catch {
    return null;
  }
}

function extractYmdFromUnknown(v) {
  if (!v) return null;

  // If it is already YYYY-MM-DD
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // If it's JSON stored as string
  try {
    const j = JSON.parse(s);
    const cand =
      j?.date ||
      j?.asOf ||
      j?.asOfDate ||
      j?.latest ||
      j?.result ||
      null;
    if (cand && /^\d{4}-\d{2}-\d{2}$/.test(String(cand).slice(0, 10))) {
      return String(cand).slice(0, 10);
    }
  } catch {
    // ignore
  }

  // If it's something like "2025-12-30T..." just slice
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);

  return null;
}

async function getLatestEodDate() {
  const v = await redis(["GET", "asx:universe:eod:latest"]);
  return extractYmdFromUnknown(v);
}

async function existsKey(key) {
  const ex = await redis(["EXISTS", key]);
  return ex === 1;
}

async function findPrevTradingDate(asOfDate, maxLookbackDays = 14) {
  const base = parseYmd(asOfDate);
  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() - i);
    const cand = ymd(d);
    if (await existsKey(`asx:universe:eod:${cand}`)) return cand;
  }
  return null;
}

exports.handler = async function (event) {
  try {
    assertEnv();

    // ✅ Scheduler-safe: handle both HTTP requests and scheduled invocations
    let asOfDate = null;
    
    // Try to get date from query parameters if this is an HTTP request
    if (event?.queryStringParameters?.date) {
      asOfDate = event.queryStringParameters.date;
      console.log('build-sectors-day: Using date from query params:', asOfDate);
    } else if (event?.rawUrl) {
      try {
        const url = new URL(event.rawUrl);
        asOfDate = url.searchParams.get("date");
        if (asOfDate) {
          console.log('build-sectors-day: Using date from rawUrl:', asOfDate);
        }
      } catch (e) {
        // URL parsing failed - could be scheduled invocation or malformed URL
        console.log('build-sectors-day: URL parsing failed:', e.message);
      }
    } else {
      // No rawUrl - likely a scheduled invocation
      console.log('build-sectors-day: No rawUrl (likely scheduled invocation), using latest EOD date');
    }

    // If no ?date= provided or scheduled invocation, use universe EOD latest
    if (!asOfDate) {
      asOfDate = await getLatestEodDate();
      console.log('build-sectors-day: Using latest EOD date:', asOfDate);
    }
    asOfDate = extractYmdFromUnknown(asOfDate);
    console.log('build-sectors-day: Final asOfDate after extraction:', asOfDate);

    if (!asOfDate) {
      const errorMsg = "No date provided and asx:universe:eod:latest not set";
      console.error('build-sectors-day ERROR:', errorMsg);
      return { statusCode: 409, body: errorMsg };
    }

    // Ensure the EOD snapshot exists for that date
    const eodKey = `asx:universe:eod:${asOfDate}`;
    const eodExists = await existsKey(eodKey);
    console.log(`build-sectors-day: Checking if ${eodKey} exists:`, eodExists);
    
    if (!eodExists) {
      const errorMsg = `Missing EOD snapshot for ${asOfDate} (${eodKey})`;
      console.error('build-sectors-day ERROR:', errorMsg);
      return { statusCode: 409, body: errorMsg };
    }

    const prevDate = await findPrevTradingDate(asOfDate, 20);
    console.log('build-sectors-day: Previous trading date:', prevDate);
    
    if (!prevDate) {
      const errorMsg = `No prior trading day snapshot found before ${asOfDate}`;
      console.error('build-sectors-day ERROR:', errorMsg);
      return { statusCode: 409, body: errorMsg };
    }

    const [todayArr, prevArr, fundamentalsData] = await Promise.all([
      loadJson(`asx:universe:eod:${asOfDate}`),
      loadJson(`asx:universe:eod:${prevDate}`),
      loadJson(`asx:universe:fundamentals:latest`),
    ]);

    const todayCount = Array.isArray(todayArr) ? todayArr.length : 0;
    const prevCount = Array.isArray(prevArr) ? prevArr.length : 0;
    const fundType = Array.isArray(fundamentalsData) ? 'array' : (fundamentalsData?.items ? 'object' : 'other');
    console.log(`build-sectors-day: Data loaded - today:${todayCount} prev:${prevCount} fund:${fundType}`);

    // Support both direct array and object with .items property
    let fundamentalsArr = [];
    if (Array.isArray(fundamentalsData)) {
      fundamentalsArr = fundamentalsData;
    } else if (fundamentalsData && Array.isArray(fundamentalsData.items)) {
      fundamentalsArr = fundamentalsData.items;
    } else {
      // Support partitioned manifest: { fallback: true, parts: [ "key1", ... ] }
      const partKeys = Array.isArray(fundamentalsData?.parts)
        ? fundamentalsData.parts
        : Array.isArray(fundamentalsData?.partKeys)
        ? fundamentalsData.partKeys
        : null;

      if (partKeys && partKeys.length > 0) {
        // Load fundamentals from parts
        console.log(`build-sectors-day: Loading fundamentals from ${partKeys.length} parts`);
        for (const pk of partKeys) {
          try {
            const partData = await loadJson(pk);
            if (partData && Array.isArray(partData.items)) {
              fundamentalsArr.push(...partData.items);
            } else if (Array.isArray(partData)) {
              fundamentalsArr.push(...partData);
            }
          } catch (e) {
            console.warn(`build-sectors-day: failed to fetch part ${pk}:`, e?.message || e);
          }
        }
        console.log(`build-sectors-day: Loaded ${fundamentalsArr.length} fundamentals from parts`);
      }
    }

    if (!Array.isArray(todayArr) || !Array.isArray(prevArr) || !Array.isArray(fundamentalsArr) || fundamentalsArr.length === 0) {
      const errorMsg = "Missing/invalid cached arrays (eod or fundamentals)";
      console.error('build-sectors-day ERROR:', errorMsg, {
        todayOk: Array.isArray(todayArr),
        prevOk: Array.isArray(prevArr),
        fundOk: Array.isArray(fundamentalsArr),
        fundLen: fundamentalsArr?.length ?? 0
      });
      return { statusCode: 500, body: errorMsg };
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

    if (used < 200) {
      console.warn(`build-sectors-day: Sector coverage lower than expected: used=${used}`);
    } else {
      console.log(`build-sectors-day: Processed ${used} stocks across ${agg.size} sectors`);
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
      const prevLevel = prevLevelBySector.get(sector) ?? 100;
      const level = prevLevel * (1 + ret1d);

      sectorsOut.push({
        sector,
        ret1d,
        level,
        coverage: { stocks: v.stocks, mcap: v.mcapCovered },
      });
    }

    sectorsOut.sort((a, b) => (b.ret1d ?? 0) - (a.ret1d ?? 0));

    const out = {
      generatedAt: new Date().toISOString(),
      date: asOfDate,
      prevDate,
      method: "mcap_weighted",
      usedStocks: used,
      sectors: sectorsOut,
    };

    await Promise.all([
      redis(["SET", `asx:sectors:day:${asOfDate}`, JSON.stringify(out)]),
      redis(["SET", `asx:sectors:latest`, asOfDate]),
      redis(["SADD", SECTOR_DATES_SET, asOfDate]),
    ]);

    console.log('build-sectors-day SUCCESS: Written sector data for', asOfDate, 'with', sectorsOut.length, 'sectors');

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, date: asOfDate, prevDate, usedStocks: used, sectors: sectorsOut.length }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: `Error: ${err.message || String(err)}` };
  }
};
