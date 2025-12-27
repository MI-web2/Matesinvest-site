// netlify/functions/market-pulse.js
//
// WRITER: Daily Market Pulse snapshot builder.
// Scheduled to run once per day (e.g. 6:10am AEST) and cache the result in Upstash.
//
// Reads (Upstash):
//  - asx:universe:eod:latest
//  - asx:universe:eod:YYYY-MM-DD        (prev trading day lookup, up to 7 days back)
//  - asx:universe:fundamentals:latest   (ASX200 membership + market cap)
//
// Writes (Upstash):
//  - asx:market:pulse:daily             (single JSON payload overwritten daily)
//
// Notes:
//  - ASX 200 is calculated internally using fundamentals.inAsx200 === 1
//  - Market-cap-weighted % move
//  - Safe on weekends / holidays

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const PULSE_KEY = "asx:market:pulse:daily";

/* ------------------ Helpers ------------------ */

async function redisGet(key) {
  try {
    const res = await fetch(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
    );
    if (!res.ok) return null;
    const j = await res.json();
    return j?.result ?? null;
  } catch {
    return null;
  }
}

// Upstash REST `SET` takes the value as a URL path segment.
// Use JSON string as the value.
async function redisSetJson(key, obj) {
  const value = JSON.stringify(obj);
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(value)}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Upstash SET failed (${res.status}): ${txt}`);
  }

  return true;
}

function parse(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try { return JSON.parse(raw); } catch { return null; }
}

function num(x) {
  return typeof x === "number" && Number.isFinite(x) ? x : null;
}

function isoDate(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function todayUTC() {
  // used only as a last-resort fallback label
  return isoDate(new Date());
}

/* -------- Prev trading day close lookup -------- */

async function getPrevCloseMap(asOfDate) {
  if (!asOfDate) return { map: {}, prevDateUsed: null };

  const base = new Date(`${asOfDate}T00:00:00Z`);
  if (Number.isNaN(base.getTime())) {
    return { map: {}, prevDateUsed: null };
  }

  for (let i = 1; i <= 7; i++) {
    const d = new Date(base);
    d.setUTCDate(d.getUTCDate() - i);
    const keyDate = isoDate(d);

    const raw = parse(await redisGet(`asx:universe:eod:${keyDate}`));
    const rows = Array.isArray(raw) ? raw : raw?.items;
    if (!rows || rows.length < 50) continue;

    const map = {};
    for (const r of rows) {
      if (!r?.code) continue;
      const close = num(r.close ?? r.price ?? r.last);
      if (close != null) {
        map[String(r.code).toUpperCase()] = close;
      }
    }

    if (Object.keys(map).length > 50) {
      return { map, prevDateUsed: keyDate };
    }
  }

  return { map: {}, prevDateUsed: null };
}

/* ------------------ Handler ------------------ */

exports.handler = async function () {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    /* ---------- Load price snapshot ---------- */

    const latestRaw = parse(await redisGet("asx:universe:eod:latest"));
    const priceRows = Array.isArray(latestRaw) ? latestRaw : latestRaw?.items;

    if (!priceRows || priceRows.length < 50) {
      return { statusCode: 503, body: "No price snapshot available" };
    }

    const universeCount = priceRows.length;

    // Prefer row.date, then latestRaw.generatedAt if present, then UTC date fallback
    const anyDate = priceRows.find((r) => r?.date)?.date;
    const asOfDate =
      anyDate ? String(anyDate).slice(0, 10)
      : latestRaw?.generatedAt ? String(latestRaw.generatedAt).slice(0, 10)
      : todayUTC();

    const { map: prevCloseMap, prevDateUsed } = await getPrevCloseMap(asOfDate);

    /* ---------- Load fundamentals ---------- */

    const fundRaw = parse(await redisGet("asx:universe:fundamentals:latest"));
    const fundRows = Array.isArray(fundRaw) ? fundRaw : fundRaw?.items;

    const fundByCode = {};
    if (Array.isArray(fundRows)) {
      for (const f of fundRows) {
        if (!f?.code) continue;
        fundByCode[String(f.code).toUpperCase()] = f;
      }
    }

    /* ---------- Aggregations ---------- */

    let adv = 0, dec = 0, flat = 0;
    let turnoverAud = 0, turnoverCount = 0;

    // ASX 200 cap-weighted move
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

      // ASX 200 calculation (membership flag is in fundamentals)
      const f = fundByCode[code];
      if (f && f.inAsx200 === 1 && pct != null) {
        const mcap = num(f.marketCap ?? f.marketCapAud);
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

    const asx200Pct =
      asx200MarketCapSum > 0 ? asx200WeightedSum / asx200MarketCapSum : null;

    const withPct = movers.filter((m) => typeof m.pct === "number" && Number.isFinite(m.pct));
    withPct.sort((a, b) => b.pct - a.pct);

    const payload = {
      generatedAt: new Date().toISOString(),

      // For the dashboard header
      asOfDate,
      prevDateUsed,
      universeCount,

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

    // Cache the daily pulse snapshot
    await redisSetJson(PULSE_KEY, payload);

    return {
      statusCode: 200,
      headers: { "Content-Type": "text/plain" },
      body: `OK: cached ${PULSE_KEY} (asOf=${payload.asOfDate}, prev=${payload.prevDateUsed || "n/a"}, universe=${payload.universeCount})`,
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
