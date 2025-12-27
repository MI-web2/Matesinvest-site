// netlify/functions/market-pulse.js
//
// WRITER: Daily Market Pulse snapshot builder.
// Scheduled to run once per day (e.g. 6:10am AEST) and cache the result in Upstash.
//
// Reads (Upstash):
//  - asx:universe:eod:latest
//  - asx:universe:eod:YYYY-MM-DD        (prev trading day lookup, up to 7 days back)
//  - asx:universe:fundamentals:latest   (ASX200 membership + market cap + sector) ✅ supports fallback manifest parts
//  - asx:sectors:day:YYYY-MM-DD         (prev day sector levels, optional)
//
// Writes (Upstash):
//  - asx:market:pulse:daily
//  - asx:sectors:day:YYYY-MM-DD
//  - asx:sectors:latest
//  - asx:sectors:dates                 (SET of YYYY-MM-DD strings, used for fast lookbacks)
//
// Notes:
//  - ASX 200 is calculated internally using fundamentals.inAsx200 == 1
//  - Market-cap-weighted % move
//  - Safe on weekends / holidays

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const PULSE_KEY = "asx:market:pulse:daily";
const SECTORS_LATEST_KEY = "asx:sectors:latest";
const SECTOR_DATES_SET = "asx:sectors:dates";

/* ------------------ Helpers ------------------ */

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
  try {
    const res = await fetchWithTimeout(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      12000
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j?.result ?? null;
  } catch (e) {
    console.warn("market-pulse redisGet error", key, e && e.message);
    return null;
  }
}

// Upstash REST `SET` takes value as URL path segment.
async function redisSetJson(key, obj) {
  const value = JSON.stringify(obj);
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(value)}`;

  const res = await fetchWithTimeout(
    url,
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
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(String(value))}`;
  const res = await fetchWithTimeout(
    url,
    { method: "POST", headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
    12000
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SET failed (${res.status}): ${txt}`);
  }
  return true;
}

// NEW: add date to a SET index (idempotent)
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
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function num(x) {
  if (typeof x === "number" && Number.isFinite(x)) return x;
  if (typeof x === "string") {
    const n = Number(x.replace(/,/g, ""));
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function isoDate(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/* -------- Prev trading day close lookup -------- */

async function getPrevCloseMap(asOfDate) {
  if (!asOfDate) return { map: {}, prevDateUsed: null };

  const base = new Date(`${asOfDate}T00:00:00Z`);
  if (Number.isNaN(base.getTime())) return { map: {}, prevDateUsed: null };

  for (let i = 1; i <= 7; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() - i);
    const keyDate = isoDate(d);

    const raw = parse(await redisGet(`asx:universe:eod:${keyDate}`));
    const rows = Array.isArray(raw) ? raw : Array.isArray(raw?.items) ? raw.items : null;
    if (!rows || rows.length < 50) continue;

    const map = {};
    for (const r of rows) {
      if (!r?.code) continue;
      const close = num(r.close ?? r.price ?? r.last);
      if (close != null) map[String(r.code).toUpperCase()] = close;
    }

    if (Object.keys(map).length > 50) {
      return { map, prevDateUsed: keyDate };
    }
  }

  return { map: {}, prevDateUsed: null };
}

/* -------- Fundamentals loader (supports fallback manifest parts) -------- */

async function getUniverseFundamentals() {
  const raw = await redisGet("asx:universe:fundamentals:latest");
  if (!raw) return null;

  const parsed = parse(raw);
  if (!parsed) return null;

  // Normal merged case: { items:[...], generatedAt, ... }
  if (Array.isArray(parsed.items)) {
    return parsed;
  }

  // Fallback manifest case
  const partKeys = Array.isArray(parsed.parts)
    ? parsed.parts
    : Array.isArray(parsed.partKeys)
    ? parsed.partKeys
    : null;

  if (partKeys && partKeys.length) {
    const items = [];

    for (const pk of partKeys) {
      const rawPart = await redisGet(pk);
      if (!rawPart) continue;

      const p = parse(rawPart);
      if (!p) continue;

      if (Array.isArray(p.items)) items.push(...p.items);
      else if (Array.isArray(p)) items.push(...p);
    }

    if (!items.length) return null;

    return {
      generatedAt: parsed.generatedAt || new Date().toISOString(),
      universeTotal: parsed.universeTotal || parsed.universeSize || items.length,
      count: items.length,
      items,
    };
  }

  return null;
}

/* -------- Build daily sector snapshot (mcap-weighted) -------- */

function normSectorName(s) {
  const x = (s == null ? "" : String(s)).trim();
  return x || "Other";
}

async function buildAndCacheSectorSnapshot({
  asOfDate,
  prevDateUsed,
  priceRows,
  prevCloseMap,
  fundByCode,
}) {
  if (!asOfDate || !prevDateUsed) return null;

  // Load previous sector snapshot (optional) to roll levels
  const prevSnapRaw = parse(await redisGet(`asx:sectors:day:${prevDateUsed}`));
  const prevSectors = Array.isArray(prevSnapRaw?.sectors) ? prevSnapRaw.sectors : [];
  const prevLevelBySector = {};
  for (const s of prevSectors) {
    if (!s?.sector) continue;
    const lv = num(s.level);
    if (lv != null && lv > 0) prevLevelBySector[String(s.sector)] = lv;
  }

  // Aggregate weighted returns
  const agg = {}; // sector -> {wRetSum, wSum, stocks, mcap}
  let used = 0;

  for (const r of priceRows) {
    if (!r?.code) continue;

    const code = String(r.code).toUpperCase();
    const close = num(r.close ?? r.price ?? r.last);
    if (close == null || close <= 0) continue;

    const prev = num(prevCloseMap[code]);
    if (prev == null || prev <= 0) continue;

    const f = fundByCode[code];
    if (!f) continue;

    const sector = normSectorName(f.sector);
    const mcap = num(f.marketCap ?? f.marketCapAud ?? f.marketcap ?? f.mktCap);
    if (mcap == null || mcap <= 0) continue;

    const ret = close / prev - 1;

    const cur = agg[sector] || { wRetSum: 0, wSum: 0, stocks: 0, mcap: 0 };
    cur.wRetSum += mcap * ret;
    cur.wSum += mcap;
    cur.stocks += 1;
    cur.mcap += mcap;
    agg[sector] = cur;
    used++;
  }

  const sectors = Object.keys(agg).map((sector) => {
    const v = agg[sector];
    const ret1d = v.wSum > 0 ? v.wRetSum / v.wSum : null;
    const prevLevel = prevLevelBySector[sector] ?? 100;
    const level = ret1d == null ? prevLevel : prevLevel * (1 + ret1d);

    return {
      sector,
      ret1d,
      level,
      coverage: { stocks: v.stocks, mcap: v.mcap },
    };
  });

  sectors.sort((a, b) => (b.ret1d ?? -999) - (a.ret1d ?? -999));

  const payload = {
    generatedAt: new Date().toISOString(),
    date: asOfDate,
    prevDate: prevDateUsed,
    method: "mcap_weighted",
    usedStocks: used,
    sectors,
  };

  await Promise.all([
    redisSetJson(`asx:sectors:day:${asOfDate}`, payload),
    redisSetString(SECTORS_LATEST_KEY, asOfDate),
    redisSAdd(SECTOR_DATES_SET, asOfDate), // ✅ fast lookbacks for 1M
  ]);

  return payload;
}

/* ------------------ Handler ------------------ */

exports.handler = async function () {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    // Load latest price snapshot
    const latestRaw = parse(await redisGet("asx:universe:eod:latest"));
    const priceRows = Array.isArray(latestRaw)
      ? latestRaw
      : Array.isArray(latestRaw?.items)
      ? latestRaw.items
      : null;

    if (!priceRows || priceRows.length < 50) {
      return { statusCode: 503, body: "No price snapshot available" };
    }

    const anyDate = priceRows.find((r) => r?.date)?.date;
    const asOfDate = anyDate ? String(anyDate).slice(0, 10) : null;

    const { map: prevCloseMap, prevDateUsed } = await getPrevCloseMap(asOfDate);

    // Load fundamentals
    const fundamentals = await getUniverseFundamentals();
    const fundItems = fundamentals && Array.isArray(fundamentals.items) ? fundamentals.items : [];

    const fundByCode = {};
    for (const f of fundItems) {
      if (!f?.code) continue;
      fundByCode[String(f.code).toUpperCase()] = f;
    }

    // Aggregations
    let adv = 0,
      dec = 0,
      flat = 0;

    let turnoverAud = 0,
      turnoverCount = 0;

    // ASX200 cap-weighted move
    let asx200WeightedSum = 0;
    let asx200MarketCapSum = 0;
    let asx200CountUsed = 0;

    const movers = [];

    for (const r of priceRows) {
      if (!r?.code) continue;

      const code = String(r.code).toUpperCase();
      const close = num(r.close ?? r.price ?? r.last);
      const volume = num(r.volume);

      // Turnover proxy
      if (close != null && volume != null) {
        const t = close * volume;
        if (Number.isFinite(t) && t >= 0) {
          turnoverAud += t;
          turnoverCount++;
        }
      }

      // % change
      let pct = num(r.pctChange) ?? num(r.changePct) ?? num(r.change_percent);

      if (pct == null && close != null) {
        const prev = num(prevCloseMap[code]);
        if (prev != null && prev > 0) {
          pct = ((close - prev) / prev) * 100;
        }
      }

      if (pct != null) {
        if (pct > 0) adv++;
        else if (pct < 0) dec++;
        else flat++;
      }

      const f = fundByCode[code];
      const inAsx200 = f && (f.inAsx200 === 1 || f.inAsx200 === "1");

      if (inAsx200 && pct != null) {
        const mcap = num(f.marketCap ?? f.marketCapAud ?? f.marketcap ?? f.mktCap);
        if (mcap != null && mcap > 0) {
          asx200WeightedSum += pct * mcap;
          asx200MarketCapSum += mcap;
          asx200CountUsed++;
        }
      }

      movers.push({ code, pct });
    }

    const breadthDen = adv + dec;
    const breadthPct = breadthDen > 0 ? (adv / breadthDen) * 100 : null;

    const asx200Pct = asx200MarketCapSum > 0 ? asx200WeightedSum / asx200MarketCapSum : null;

    const withPct = movers.filter((m) => typeof m.pct === "number" && Number.isFinite(m.pct));
    withPct.sort((a, b) => b.pct - a.pct);

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
      turnoverCoverage: turnoverCount,

      topGainers: withPct.slice(0, 5),
      topLosers: withPct.slice(-5).sort((a, b) => a.pct - b.pct),
    };

    // Build + cache sector snapshot for this day
    await buildAndCacheSectorSnapshot({
      asOfDate,
      prevDateUsed,
      priceRows,
      prevCloseMap,
      fundByCode,
    });

    await redisSetJson(PULSE_KEY, payload);

    return {
      statusCode: 200,
      headers: { "Content-Type": "text/plain" },
      body: `OK: cached ${PULSE_KEY} (asOf=${payload.asOfDate || "unknown"}, prev=${
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
