// netlify/functions/backfill-market-pulse.js
//
// One-off backfill job:
// Builds historical "market pulse" snapshots from cached EOD data.
//
// Reads (Upstash):
//  - asx:universe:eod:YYYY-MM-DD  (CAN be an array OR object with rows/items)
//  - asx:universe:fundamentals:latest
//  - asx:universe:fundamentals:latest:part:<offset> (items)
//
// Writes (Upstash):
//  - asx:market:pulse:day:YYYY-MM-DD
//  - asx:market:pulse:dates  (set of YYYY-MM-DD)
//
// Usage:
//  /.netlify/functions/backfill-market-pulse?start=2025-09-30&end=2025-12-29
//  /.netlify/functions/backfill-market-pulse?days=60
//  /.netlify/functions/backfill-market-pulse?days=60&dryrun=1

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const EOD_PREFIX = "asx:universe:eod:";
const FUND_LATEST_KEY = "asx:universe:fundamentals:latest";
const FUND_PART_PREFIX = "asx:universe:fundamentals:latest:part:";

const PULSE_DAY_PREFIX = "asx:market:pulse:day:";
const PULSE_DATES_SET = "asx:market:pulse:dates";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 15000) {
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
      15000
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
    15000
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SET failed (${res.status}): ${txt}`);
  }
}

async function redisSAdd(key, member) {
  const res = await fetchWithTimeout(
    `${UPSTASH_URL}/sadd/${encodeURIComponent(key)}/${encodeURIComponent(String(member))}`,
    { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    15000
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SADD failed (${res.status}): ${txt}`);
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

function getArrayFromAny(obj) {
  // Supports:
  //  - [ ... ]
  //  - { rows: [...] }
  //  - { items: [...] }
  if (!obj) return [];
  if (Array.isArray(obj)) return obj;
  if (Array.isArray(obj.rows)) return obj.rows;
  if (Array.isArray(obj.items)) return obj.items;
  return [];
}

function ymdUTC(dateObj) {
  const yy = dateObj.getUTCFullYear();
  const mm = String(dateObj.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dateObj.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function fromYmd(ymd) {
  const [y, m, d] = ymd.split("-").map((x) => parseInt(x, 10));
  return new Date(Date.UTC(y, m - 1, d));
}

function addDays(ymd, deltaDays) {
  const dt = fromYmd(ymd);
  dt.setUTCDate(dt.getUTCDate() + deltaDays);
  return ymdUTC(dt);
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

async function getPrevCloseMap(asOfDate, maxLookbackDays = 7) {
  for (let i = 1; i <= maxLookbackDays; i++) {
    const d = addDays(asOfDate, -i);
    const raw = await redisGet(`${EOD_PREFIX}${d}`);
    const obj = parse(raw);
    const rows = getArrayFromAny(obj);
    if (rows.length > 0) {
      const map = new Map();
      for (const r of rows) {
        const code = r?.code ? String(r.code).toUpperCase() : null;
        if (!code) continue;
        // In your EOD, 'close' is the usable previous close for next day.
        const prevClose = num(r.prevClose ?? r.close ?? r.last ?? r.price);
        if (prevClose != null) map.set(code, prevClose);
      }
      return { map, prevDateUsed: d };
    }
    await sleep(40);
  }
  return { map: new Map(), prevDateUsed: null };
}

function isAsx200Member(f) {
  return (
    f?.inAsx200 === 1 ||
    f?.inAsx200 === true ||
    f?.asx200 === 1 ||
    f?.asx200 === true ||
    f?.asx200Member === true ||
    f?.asx200_member === true ||
    f?.index === "ASX200"
  );
}

async function loadFundamentalsMap() {
  // Try the merged latest first
  const raw = await redisGet(FUND_LATEST_KEY);
  const obj = parse(raw);
  const rows = getArrayFromAny(obj);

  const all = [];
  if (rows.length > 0) {
    all.push(...rows);
  } else {
    // Stitch parts: part offsets are multiples of 500 typically (0,500,1000,...)
    // We'll scan until we hit a few consecutive misses.
    let misses = 0;
    for (let offset = 0; offset <= 50000; offset += 500) {
      const pr = await redisGet(`${FUND_PART_PREFIX}${offset}`);
      const po = parse(pr);
      const items = getArrayFromAny(po);
      if (items.length > 0) {
        all.push(...items);
        misses = 0;
      } else {
        misses++;
        if (misses >= 4) break; // stop after 4 empty parts
      }
      await sleep(25);
    }
  }

  const map = new Map();
  for (const f of all) {
    const code = f?.code ? String(f.code).toUpperCase() : null;
    if (!code) continue;
    map.set(code, f);
  }
  return map;
}

exports.handler = async function (event) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    const qs = event?.queryStringParameters || {};
    const dryrun = String(qs.dryrun || "").trim() === "1";

    const startParam = qs.start ? String(qs.start) : null;
    const endParam = qs.end ? String(qs.end) : null;
    const daysParam = qs.days ? Math.max(1, Math.min(365, parseInt(qs.days, 10))) : 60;

    let startDate, endDate;
    if (startParam && endParam) {
      startDate = startParam;
      endDate = endParam;
    } else {
      endDate = ymdUTC(new Date());
      startDate = addDays(endDate, -daysParam);
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      return { statusCode: 400, body: "Invalid date format. Use YYYY-MM-DD for start/end." };
    }

    // Load fundamentals map (needed for ASX200 membership + sector if you want it later)
    const fundMap = await loadFundamentalsMap();

    // Date list inclusive
    const dates = [];
    let cursor = startDate;
    while (cursor <= endDate) {
      dates.push(cursor);
      cursor = addDays(cursor, 1);
      if (dates.length > 400) break;
    }

    let processed = 0;
    let written = 0;
    let skippedNoEOD = 0;
    let errors = 0;

    for (const asOfDate of dates) {
      processed++;

      const eodRaw = await redisGet(`${EOD_PREFIX}${asOfDate}`);
      const eodObj = parse(eodRaw);
      const priceRows = getArrayFromAny(eodObj);

      if (priceRows.length === 0) {
        skippedNoEOD++;
        continue;
      }

      const { map: prevMap, prevDateUsed } = await getPrevCloseMap(asOfDate, 7);

      let adv = 0, dec = 0, flat = 0;
      let turnoverAud = 0, turnoverCount = 0;

      let asx200SumMc = 0;
      let asx200SumMcPrev = 0;
      let asx200CountUsed = 0;

      const movers = [];

      for (const r of priceRows) {
        const code = r?.code ? String(r.code).toUpperCase() : null;
        if (!code) continue;

        const last = num(r.close ?? r.last ?? r.price ?? r.lastClose ?? r.last_price);
        if (last == null) continue;

        const prev = prevMap.get(code);
        const pct = prev != null ? safePctChange(last, prev) : null;

        if (pct != null) {
          if (pct > 0.00001) adv++;
          else if (pct < -0.00001) dec++;
          else flat++;
        }

        const vol = num(r.volume ?? r.vol ?? r.v);
        if (vol != null && vol >= 0) {
          turnoverAud += last * vol;
          turnoverCount++;
        }

        const f = fundMap.get(code);
        if (f && isAsx200Member(f)) {
          const mc = num(f.marketCap ?? f.market_cap ?? f.mktCap ?? f.mkt_cap);
          if (mc != null && mc > 0) {
            asx200SumMc += mc;
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
            sector: String(f?.sector ?? "").trim(), // optional
            pct,
          });
        }
      }

      const breadthDen = adv + dec;
      const breadthPct = breadthDen > 0 ? (adv / breadthDen) * 100 : null;

      let asx200Pct = null;
      if (asx200CountUsed > 0 && asx200SumMcPrev > 0) {
        asx200Pct = ((asx200SumMc - asx200SumMcPrev) / asx200SumMcPrev) * 100;
      }

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

      try {
        if (!dryrun) {
          await redisSetJson(`${PULSE_DAY_PREFIX}${asOfDate}`, payload);
          await redisSAdd(PULSE_DATES_SET, asOfDate);
        }
        written++;
      } catch (e) {
        errors++;
      }

      await sleep(60);
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(
        { ok: true, dryrun, startDate, endDate, processedDays: processed, written, skippedNoEOD, errors },
        null,
        2
      ),
    };
  } catch (err) {
    console.error("backfill-market-pulse error", err);
    return { statusCode: 500, body: `Backfill failed: ${err?.message || String(err)}` };
  }
};
