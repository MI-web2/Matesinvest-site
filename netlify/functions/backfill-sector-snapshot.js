// netlify/functions/backfill-sector-snapshots.js
//
// One-off backfill: builds asx:sectors:day:YYYY-MM-DD from existing
// asx:universe:eod:YYYY-MM-DD snapshots + asx:universe:fundamentals:latest.
// Supports "manifest/parts" fundamentals storage.
//
// Writes:
//  - asx:sectors:day:YYYY-MM-DD
//  - asx:sectors:latest
//  - asx:sectors:dates
//
// Run manually:
//  /.netlify/functions/backfill-sector-snapshots
//
// Optional query params:
//  ?from=2025-12-04
//  ?to=2025-12-24
//  ?limit=25       (default 50, max 200)
//  ?force=1        (recompute even if sector day exists)

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const SECTORS_LATEST_KEY = "asx:sectors:latest";
const SECTOR_DATES_SET = "asx:sectors:dates";

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

async function redisCmd(cmd, ...args) {
  const parts = [cmd, ...args].map((x) => encodeURIComponent(String(x)));
  const url = `${UPSTASH_URL}/${parts.join("/")}`;
  const res = await fetchWithTimeout(url, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  const json = await res.json().catch(() => null);
  if (!res.ok || json?.error) {
    throw new Error(json?.error || `Upstash ${cmd} failed (${res.status})`);
  }
  return json.result;
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

function normSectorName(s) {
  const x = (s == null ? "" : String(s)).trim();
  return x || "Other";
}

function isYmd(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s));
}

function cmpYmd(a, b) {
  return String(a).localeCompare(String(b));
}

async function loadJson(key) {
  const raw = await redisCmd("GET", key);
  return parse(raw);
}

async function exists(key) {
  const r = await redisCmd("EXISTS", key);
  return r === 1;
}

// SCAN keys like asx:universe:eod:2025-12-24
async function scanAllKeys(matchPattern) {
  let cursor = "0";
  const keys = [];
  do {
    const res = await redisCmd("SCAN", cursor, "MATCH", matchPattern, "COUNT", "500");
    cursor = res?.[0] ?? "0";
    const batch = res?.[1] ?? [];
    for (const k of batch) keys.push(k);
  } while (cursor !== "0");
  return keys;
}

function extractDateFromEodKey(key) {
  const m = String(key).match(/^asx:universe:eod:(\d{4}-\d{2}-\d{2})$/);
  return m ? m[1] : null;
}

function buildPrevCloseMap(prevRows) {
  const map = {};
  for (const r of prevRows) {
    if (!r?.code) continue;
    const c = num(r.close ?? r.price ?? r.last);
    if (c != null && c > 0) map[String(r.code).toUpperCase()] = c;
  }
  return map;
}

/**
 * Load fundamentals items, supporting:
 * 1) merged object: { items:[...] }
 * 2) raw array: [...]
 * 3) manifest: { parts:[key1,key2,...] } where each part is {items:[...]} or [...]
 */
async function loadFundamentalsItems() {
  const raw = await redisCmd("GET", "asx:universe:fundamentals:latest");
  if (!raw) return null;

  const parsed = parse(raw);
  if (!parsed) return null;

  if (Array.isArray(parsed.items)) return parsed.items;
  if (Array.isArray(parsed)) return parsed;

  const partKeys = Array.isArray(parsed.parts)
    ? parsed.parts
    : Array.isArray(parsed.partKeys)
    ? parsed.partKeys
    : null;

  if (partKeys && partKeys.length) {
    const items = [];
    for (const pk of partKeys) {
      const pr = await redisCmd("GET", pk);
      if (!pr) continue;
      const p = parse(pr);
      if (!p) continue;
      if (Array.isArray(p.items)) items.push(...p.items);
      else if (Array.isArray(p)) items.push(...p);
    }
    return items.length ? items : null;
  }

  // Some installs store a separate manifest key:
  // asx:universe:fundamentals:latest:manifest
  const raw2 = await redisCmd("GET", "asx:universe:fundamentals:latest:manifest").catch(() => null);
  const parsed2 = parse(raw2);
  const pk2 = parsed2 && (parsed2.parts || parsed2.partKeys);
  if (Array.isArray(pk2) && pk2.length) {
    const items = [];
    for (const pk of pk2) {
      const pr = await redisCmd("GET", pk);
      if (!pr) continue;
      const p = parse(pr);
      if (!p) continue;
      if (Array.isArray(p.items)) items.push(...p.items);
      else if (Array.isArray(p)) items.push(...p);
    }
    return items.length ? items : null;
  }

  return null;
}

function buildSectorSnapshot({
  date,
  prevDate,
  todayRows,
  prevCloseMap,
  fundByCode,
  prevLevelBySector,
}) {
  const agg = {}; // sector -> {wRetSum,wSum,stocks,mcap}
  let used = 0;

  for (const r of todayRows) {
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
    return { sector, ret1d, level, coverage: { stocks: v.stocks, mcap: v.mcap } };
  });

  sectors.sort((a, b) => (b.ret1d ?? -999) - (a.ret1d ?? -999));

  return {
    generatedAt: new Date().toISOString(),
    date,
    prevDate,
    method: "mcap_weighted",
    usedStocks: used,
    sectors,
  };
}

exports.handler = async function (event) {
  try {
    assertEnv();

    const url = new URL(event.rawUrl);
    const from = url.searchParams.get("from");
    const to = url.searchParams.get("to");
    const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") || 50)));
    const force = url.searchParams.get("force") === "1";

    // 1) Load fundamentals once (manifest-aware)
    const fundItems = await loadFundamentalsItems();
    if (!Array.isArray(fundItems) || !fundItems.length) {
      return { statusCode: 500, body: "Missing fundamentals: asx:universe:fundamentals:latest" };
    }

    const fundByCode = {};
    for (const f of fundItems) {
      if (!f?.code) continue;
      fundByCode[String(f.code).toUpperCase()] = f;
    }

    // 2) Discover all EOD date keys
    const keys = await scanAllKeys("asx:universe:eod:20??-??-??");
    const dates = keys
      .map(extractDateFromEodKey)
      .filter((d) => d && isYmd(d))
      .sort(cmpYmd);

    const filtered = dates.filter((d) => (!from || d >= from) && (!to || d <= to));
    if (!filtered.length) {
      return { statusCode: 404, body: "No EOD dates found for backfill range" };
    }

    // 3) Walk dates in order, compute sector snapshots
    let processed = 0;
    let skippedExisting = 0;
    let lastWritten = null;

    const rollingLevelBySector = {};

    for (let i = 0; i < filtered.length; i++) {
      if (processed >= limit) break;

      const date = filtered[i];
      const sectorKey = `asx:sectors:day:${date}`;

      if (!force && (await exists(sectorKey))) {
        skippedExisting++;
        const existing = await loadJson(sectorKey);
        if (existing?.sectors?.length) {
          for (const s of existing.sectors) {
            const lv = num(s.level);
            if (lv != null && lv > 0) rollingLevelBySector[s.sector] = lv;
          }
          lastWritten = date;
        }
        continue;
      }

      const prevDate = i > 0 ? filtered[i - 1] : null;
      if (!prevDate) continue;

      const [todayRowsRaw, prevRowsRaw] = await Promise.all([
        loadJson(`asx:universe:eod:${date}`),
        loadJson(`asx:universe:eod:${prevDate}`),
      ]);

      const todayRows = Array.isArray(todayRowsRaw)
        ? todayRowsRaw
        : Array.isArray(todayRowsRaw?.items)
        ? todayRowsRaw.items
        : [];
      const prevRows = Array.isArray(prevRowsRaw)
        ? prevRowsRaw
        : Array.isArray(prevRowsRaw?.items)
        ? prevRowsRaw.items
        : [];

      if (todayRows.length < 50 || prevRows.length < 50) continue;

      const prevCloseMap = buildPrevCloseMap(prevRows);

      const snap = buildSectorSnapshot({
        date,
        prevDate,
        todayRows,
        prevCloseMap,
        fundByCode,
        prevLevelBySector: rollingLevelBySector,
      });

      for (const s of snap.sectors) {
        const lv = num(s.level);
        if (lv != null && lv > 0) rollingLevelBySector[s.sector] = lv;
      }

      await redisCmd("SET", sectorKey, JSON.stringify(snap));
      await redisCmd("SADD", SECTOR_DATES_SET, date);
      lastWritten = date;
      processed++;
    }

    if (lastWritten) await redisCmd("SET", SECTORS_LATEST_KEY, lastWritten);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ok: true,
        totalEodDatesFound: dates.length,
        datesInRange: filtered.length,
        processed,
        skippedExisting,
        latestSectorDate: lastWritten,
        note:
          processed >= limit
            ? `Hit limit=${limit}. Re-run to continue, or increase ?limit=`
            : "Backfill complete for selected range.",
      }),
    };
  } catch (err) {
    console.error(err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ok: false, error: err.message || String(err) }),
    };
  }
};
