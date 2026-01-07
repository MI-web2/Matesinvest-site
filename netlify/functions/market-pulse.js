// netlify/functions/market-pulse.js
//
// WRITER: Daily Market Pulse snapshot builder.
// Scheduled to run once per day (e.g. 6:10am AEST) and cache the result in Upstash.
//
// Reads (Upstash):
//  - asx:universe:eod:latest
//  - asx:universe:eod:YYYY-MM-DD        (prev trading day lookup, up to 7 days back)
//  - asx:universe:fundamentals:latest   (ASX200 membership + market cap + sector) ✅ supports ASX200 constituents
//
// Writes (Upstash):
//  - asx:market:pulse:daily                      (latest pointer)
//  - asx:market:pulse:day:YYYY-MM-DD             (NEW: historical snapshot)
//  - asx:market:pulse:dates (set of YYYY-MM-DD)  (NEW: date index)

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const EOD_LATEST_KEY = "asx:universe:eod:latest";
const EOD_PREFIX = "asx:universe:eod:";

const FUND_LATEST_KEY = "asx:universe:fundamentals:latest";

const PULSE_LATEST_KEY = "asx:market:pulse:daily";              // existing
const PULSE_DAY_PREFIX = "asx:market:pulse:day:";               // NEW
const PULSE_DATES_SET = "asx:market:pulse:dates";               // NEW

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
}

async function redisGet(key) {
  try {
    const res = await fetchWithTimeout(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      12000
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j?.result ?? null;
  } catch {
    return null;
  }
}

async function redisSetJson(key, obj) {
  const payload = JSON.stringify(obj);
  const res = await fetchWithTimeout(
    `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}`,
    { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    12000
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SET failed (${res.status}): ${txt}`);
  }
  return true;
}

async function redisSetString(key, value) {
  const res = await fetchWithTimeout(
    `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(String(value))}`,
    { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    12000
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SET failed (${res.status}): ${txt}`);
  }
  return true;
}

async function redisSAdd(key, member) {
  const url = `${UPSTASH_URL}/sadd/${encodeURIComponent(key)}/${encodeURIComponent(String(member))}`;
  const res = await fetchWithTimeout(
    url,
    { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    12000
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SADD failed (${res.status}): ${txt}`);
  }
  return true;
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

function ymd(d) {
  const yy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function dateAddDays(ymdStr, deltaDays) {
  const [y, m, d] = ymdStr.split("-").map((x) => parseInt(x, 10));
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + deltaDays);
  return ymd(dt);
}

function num(n) {
  const x = typeof n === "number" ? n : typeof n === "string" ? Number(n) : NaN;
  return Number.isFinite(x) ? x : null;
}

function safePctChange(last, prev) {
  const l = num(last);
  const p = num(prev);
  if (l == null || p == null || p === 0) return null;
  return ((l - p) / p) * 100;
}

function asNumberOrZero(v) {
  const n = num(v);
  return n == null ? 0 : n;
}

async function getPrevCloseMap(asOfDate, maxLookbackDays = 7) {
  // tries asOfDate-1, asOfDate-2, ...
  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = dateAddDays(asOfDate, -i);
    const raw = await redisGet(`${EOD_PREFIX}${d}`);
    const obj = parse(raw);
    
    // Support both direct array and object with .rows property
    let rows = null;
    if (Array.isArray(obj)) {
      rows = obj;
    } else if (obj && Array.isArray(obj.rows)) {
      rows = obj.rows;
    }
    
    if (rows && rows.length > 0) {
      const map = new Map();
      for (const r of rows) {
        const code = r?.code ? String(r.code).toUpperCase() : null;
        if (!code) continue;
        // prefer explicit prevClose/close if present; otherwise try price
        const prevClose = num(r.prevClose ?? r.close ?? r.last ?? r.price);
        if (prevClose != null) map.set(code, prevClose);
      }
      return { map, prevDateUsed: d };
    }
    await sleep(50);
  }
  return { map: new Map(), prevDateUsed: null };
}

exports.handler = async function () {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    // Load latest EOD universe snapshot
    const latestRaw = await redisGet(EOD_LATEST_KEY);
    const latestObj = parse(latestRaw);

    // Support both array format and object with .rows property
    let priceRows, asOfDate;
    if (Array.isArray(latestObj)) {
      // Direct array format (from snapshot-asx-universe-prices)
      priceRows = latestObj;
      asOfDate = priceRows[0]?.date || null;
    } else if (latestObj && Array.isArray(latestObj.rows)) {
      // Object format with .rows property
      priceRows = latestObj.rows;
      asOfDate = latestObj.asOfDate || latestObj.date || null;
    } else {
      return {
        statusCode: 503,
        headers: { "Content-Type": "text/plain" },
        body: "No latest EOD universe available yet",
      };
    }

    if (!priceRows || priceRows.length === 0) {
      return {
        statusCode: 503,
        headers: { "Content-Type": "text/plain" },
        body: "No latest EOD universe available yet",
      };
    }

    // Load fundamentals for ASX200 membership + market caps + sectors
    const fundRaw = await redisGet(FUND_LATEST_KEY);
    const fundObj = parse(fundRaw) || {};
    
    // Support both .items (new format) and .rows (legacy format)
    let fundRows = [];
    if (Array.isArray(fundObj.items)) {
      fundRows = fundObj.items;
    } else if (Array.isArray(fundObj.rows)) {
      fundRows = fundObj.rows;
    } else {
      // Support partitioned manifest: { fallback: true, parts: [ "key1", ... ] }
      const partKeys = Array.isArray(fundObj.parts)
        ? fundObj.parts
        : Array.isArray(fundObj.partKeys)
        ? fundObj.partKeys
        : null;

      if (partKeys && partKeys.length > 0) {
        console.log(`market-pulse: Loading fundamentals from ${partKeys.length} parts`);
        for (const pk of partKeys) {
          try {
            const rawPart = await redisGet(pk);
            const partObj = parse(rawPart);
            if (!partObj) continue;
            
            if (Array.isArray(partObj.items)) {
              fundRows.push(...partObj.items);
            } else if (Array.isArray(partObj)) {
              fundRows.push(...partObj);
            }
          } catch (e) {
            console.warn(`market-pulse: failed to fetch part ${pk}:`, e?.message || e);
          }
        }
        console.log(`market-pulse: Loaded ${fundRows.length} fundamentals from parts`);
      }
    }

    // Map code -> fundamentals
    const fundMap = new Map();
    for (const f of fundRows) {
      const code = f?.code ? String(f.code).toUpperCase() : null;
      if (!code) continue;
      fundMap.set(code, f);
    }

    // Previous close map
    const { map: prevMap, prevDateUsed } = asOfDate
      ? await getPrevCloseMap(asOfDate, 7)
      : { map: new Map(), prevDateUsed: null };
    
    console.log(`market-pulse: asOfDate=${asOfDate}, prevDateUsed=${prevDateUsed}, prevMapSize=${prevMap.size}`);
    
    // Warn if we couldn't find previous day data (this would cause all metrics to be zero/null)
    if (prevMap.size === 0) {
      console.warn(`market-pulse WARNING: No previous day data found for asOfDate=${asOfDate}. Market pulse calculations will be incomplete.`);
    }

    // Build metrics
    let adv = 0,
      dec = 0,
      flat = 0;

    let turnoverAud = 0;
    let turnoverCount = 0;

    let asx200SumMc = 0;
    let asx200SumMcPrev = 0;
    let asx200CountUsed = 0;

    // Top gainers/losers
    const movers = [];

    for (const r of priceRows) {
      const code = r?.code ? String(r.code).toUpperCase() : null;
      if (!code) continue;

      const last = num(r.last ?? r.close ?? r.price ?? r.lastClose ?? r.last_price);
      if (last == null) continue;

      const prev = prevMap.get(code);
      const pct = prev != null ? safePctChange(last, prev) : null;

      if (pct != null) {
        if (pct > 0.00001) adv++;
        else if (pct < -0.00001) dec++;
        else flat++;
      }

      // turnover
      const vol = num(r.volume ?? r.vol ?? r.turnoverVolume ?? r.v);
      if (vol != null && vol >= 0) {
        turnoverAud += last * vol;
        turnoverCount++;
      }

      // ASX200 (weighted by market cap if available)
      const f = fundMap.get(code);
      const isAsx200 =
        f?.inAsx200 === 1 ||
        f?.inAsx200 === true ||
        f?.asx200 === 1 ||
        f?.asx200 === true ||
        f?.asx200Member === true ||
        f?.asx200_member === true ||
        f?.index === "ASX200";

      if (isAsx200) {
        const mc = num(f.marketCap ?? f.market_cap ?? f.mktCap ?? f.mkt_cap);
        if (mc != null && mc > 0) {
          asx200SumMc += mc;
          // approximate prev market cap by scaling with pct change (if we have it)
          if (pct != null) {
            const prevMc = mc / (1 + pct / 100);
            if (Number.isFinite(prevMc)) asx200SumMcPrev += prevMc;
          } else {
            asx200SumMcPrev += mc;
          }
          asx200CountUsed++;
        }
      }

      if (pct != null && Number.isFinite(pct)) {
        movers.push({
          code,
          name: String(r.name ?? f?.name ?? "").trim(),
          pct,
        });
      }
    }

    const breadthDen = adv + dec;
    const breadthPct = breadthDen > 0 ? (adv / breadthDen) * 100 : null;
    
    console.log(`market-pulse: advancers=${adv}, decliners=${dec}, flat=${flat}, breadthPct=${breadthPct}, turnoverAud=${turnoverAud}`);

    // ASX200 pct (market-cap weighted, approx)
    let asx200Pct = null;
    if (asx200CountUsed > 0 && asx200SumMcPrev > 0) {
      asx200Pct = ((asx200SumMc - asx200SumMcPrev) / asx200SumMcPrev) * 100;
    }

    // top gainers/losers
    movers.sort((a, b) => b.pct - a.pct);
    const topGainers = movers.slice(0, 5);
    const topLosers = movers.slice(-5).sort((a, b) => a.pct - b.pct);

    const payload = {
      generatedAt: new Date().toISOString(),
      asOfDate,
      prevDateUsed,

      universeCount: priceRows.length,

      asx200: {
        pct: asx200Pct,
        constituentsUsed: asx200CountUsed,
      },

      advancers: adv,
      decliners: dec,
      flat,

      breadthPct,

      totalTurnoverAud: turnoverCount > 0 ? turnoverAud : null,
      turnoverCoverage: turnoverCount > 0 ? turnoverCount : null,

      topGainers,
      topLosers,
    };

    // ✅ Existing: latest pointer
    await redisSetJson(PULSE_LATEST_KEY, payload);

    // ✅ NEW: per-day snapshot + date index (only if we have asOfDate)
    if (payload.asOfDate) {
      const dayKey = `${PULSE_DAY_PREFIX}${payload.asOfDate}`;
      await redisSetJson(dayKey, payload);
      await redisSAdd(PULSE_DATES_SET, payload.asOfDate);
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "text/plain" },
      body: `OK: cached ${PULSE_LATEST_KEY} (asOf=${payload.asOfDate || "unknown"}, prev=${
        payload.prevDateUsed || "n/a"
      }, asx200Used=${payload.asx200.constituentsUsed})`,
    };
  } catch (err) {
    console.error("market-pulse error", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: `Failed to build market pulse: ${err?.message || String(err)}`,
    };
  }
};
