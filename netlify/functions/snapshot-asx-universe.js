// netlify/functions/snapshot-asx-universe.js
//
// Batched snapshot writer: writes each processed batch to asx:universe:fundamentals:part:<offset>
// and persists progress to asx:universe:offset. Does NOT overwrite the final "latest" key.
//
// Requirements (env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env:
//   TRY_SUFFIXES (default "AU,AX,ASX")
//   CONCURRENCY (default 8)
//   RETRIES (default 2)
//   BATCH_SIZE (default 200)
//   EXCLUDE_ETF  (default "1" to exclude ETFs at snapshot level)

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_CONCURRENCY = 8;
const DEFAULT_RETRIES = 2;

function sleep(ms) { return new Promise((res) => setTimeout(res, ms)); }

async function fetchWithTimeout(url, opts = {}, timeout = 15000) {
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

// Upstash helpers
async function redisGet(urlBase, token, key) {
  if (!urlBase || !token) return null;
  try {
    const res = await fetchWithTimeout(`${urlBase}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${token}` }
    }, 10000);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisGet failed", key, res.status, txt && txt.slice(0, 200));
      return null;
    }
    const j = await res.json().catch(() => null);
    return j && typeof j.result !== "undefined" ? j.result : null;
  } catch (err) {
    console.warn("redisGet error", key, err && err.message);
    return null;
  }
}

async function redisSet(urlBase, token, key, value, ttlSeconds) {
  if (!urlBase || !token) return false;
  try {
    const payload = typeof value === "string" ? value : JSON.stringify(value);
    const ttl = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : "";
    const res = await fetchWithTimeout(
      `${urlBase}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}${ttl}`,
      { method: "POST", headers: { Authorization: `Bearer ${token}` } },
      15000
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisSet failed", key, res.status, txt && txt.slice(0, 300));
      return false;
    }
    return true;
  } catch (err) {
    console.warn("redisSet error", key, err && err.message);
    return false;
  }
}

function normalizeCode(code) {
  return String(code || "").replace(/\.[A-Z0-9]{1,6}$/i, "").toUpperCase();
}

function isEtfName(name) {
  if (!name || typeof name !== "string") return false;
  const cleaned = name.replace(/[\u2013\u2014–—\-()]/g, " ").replace(/[\s\.,;:]+/g, " ").trim();
  return /\bETF$/i.test(cleaned);
}

function readUniverseSync() {
  const override = process.env.UNIVERSE_FILE;
  const candidates = override
    ? [ path.join(__dirname, override), path.join(process.cwd(), override), path.join(process.cwd(), "netlify", "functions", override) ]
    : [ path.join(__dirname, "asx-universe.txt"), path.join(process.cwd(), "asx-universe.txt"), path.join(process.cwd(), "netlify", "functions", "asx-universe.txt") ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw.split(/[\s,]+/).map(s => s.trim()).filter(Boolean);
        console.log(`[snapshot-asx-universe] using universe file: ${p} (entries=${parts.length})`);
        return Array.from(new Set(parts.map(c => c.toUpperCase())));
      }
    } catch (err) {
      console.warn(`[snapshot-asx-universe] read failure for ${p}: ${err && err.message}`);
    }
  }
  throw new Error("Failed to read universe list");
}

// Robust safe-get helper (replaces previous `get(...)` usage)
function safeGet(obj, path, fallback = null) {
  if (!obj || !path) return fallback;
  const parts = path.split('.');
  let cur = obj;
  for (const p of parts) {
    if (cur && Object.prototype.hasOwnProperty.call(cur, p)) {
      cur = cur[p];
    } else {
      return fallback;
    }
  }
  return cur;
}

exports.handler = async function (event) {
  const start = Date.now();

  if (!EODHD_TOKEN) return { statusCode: 500, body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }) };
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return { statusCode: 500, body: JSON.stringify({ error: "Missing Upstash env" }) };

  const qs = (event && event.queryStringParameters) || {};
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES ? process.env.TRY_SUFFIXES.split(",") : DEFAULT_TRY_SUFFIXES).map(s => s.trim()).filter(Boolean);
  const CONCURRENCY = Number(process.env.CONCURRENCY || DEFAULT_CONCURRENCY);
  const RETRIES = Number(process.env.RETRIES || DEFAULT_RETRIES);
  const BATCH_SIZE = Number(qs.limit || qs.size || process.env.BATCH_SIZE || 200);
  const EXCLUDE_ETF = String(process.env.EXCLUDE_ETF || "1") === "1";

  // Load universe & asx200
  let universeCodes, asx200Set;
  try {
    universeCodes = readUniverseSync();
    try {
      const asx200Raw = fs.existsSync(path.join(__dirname, "asx200.txt")) ? fs.readFileSync(path.join(__dirname, "asx200.txt"), "utf8") : "";
      const asxParts = asx200Raw ? asx200Raw.split(/[\s,]+/).map(s=>s.trim()).filter(Boolean) : [];
      asx200Set = new Set(asxParts.map(s => s.toUpperCase()));
    } catch (e) { asx200Set = new Set(); }
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message }) };
  }

  const universeTotal = universeCodes.length;
  console.log(`[snapshot-asx-universe] universeTotal=${universeTotal}`);

  // Prefer persisted offset from Upstash if qs.offset not provided
  let offset = Math.max(0, Number(qs.offset || qs.off || 0));
  if (!qs.offset) {
    const rawOffset = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:offset");
    if (rawOffset) {
      const parsed = Number(String(rawOffset));
      if (!Number.isNaN(parsed)) offset = Math.max(0, parsed);
    }
  }

  // Single-code dev override
  const singleCode = qs.code ? String(qs.code).trim().toUpperCase() : null;
  const batchStart = offset;
  const batchLimit = Math.max(1, Math.min(BATCH_SIZE, universeTotal - batchStart));

  // Prepare the slice
  const batchUniverse = singleCode ? [singleCode] : universeCodes.slice(batchStart, batchStart + batchLimit);
  console.log(`[snapshot-asx-universe] running batch start=${batchStart} limit=${batchLimit} actual=${batchUniverse.length} excludeETF=${EXCLUDE_ETF}`);

  // helper to fetch fundamentals with retries
  async function fetchFundamentals(fullCode) {
    const url = `https://eodhd.com/api/fundamentals/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;
    let attempt = 0;
    let lastText = null;
    while (attempt <= RETRIES) {
      try {
        const res = await fetchWithTimeout(url, {}, 18000);
        const text = await res.text().catch(()=>"");
        if (!res.ok) {
          lastText = text || lastText;
          if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
            const backoff = 400 * Math.pow(2, attempt);
            await sleep(backoff + Math.random() * 200);
            attempt++;
            continue;
          }
          return { ok: false, status: res.status, text };
        }
        try {
          const json = text ? JSON.parse(text) : null;
          return { ok: true, data: json || {} };
        } catch (e) {
          return { ok: false, status: res.status, text };
        }
      } catch (err) {
        lastText = String(err && err.message) || lastText;
        const backoff = 400 * Math.pow(2, attempt);
        await sleep(backoff + Math.random() * 200);
        attempt++;
      }
    }
    return { ok: false, status: 0, text: lastText };
  }

  async function getFundamentalsForBaseCode(baseCode) {
    const attempts = [];
    for (const sfx of TRY_SUFFIXES) {
      const fullCode = `${baseCode}.${sfx}`;
      attempts.push(fullCode);
      const res = await fetchFundamentals(fullCode);
      if (!res.ok || !res.data || !res.data.General) continue;
      return { baseCode, fullCode, data: res.data, attempts };
    }
    return { baseCode, fullCode: null, data: null, attempts };
  }

  // process batch with limited concurrency
  const items = [];
  const failures = [];
  let idx = 0;
  const total = batchUniverse.length;
  const workers = new Array(Math.min(CONCURRENCY, total)).fill(null).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= total) return;
      const rawCode = batchUniverse[i];
      const base = normalizeCode(rawCode);
      try {
        const result = await getFundamentalsForBaseCode(base);
        if (!result.data) {
          failures.push({ code: base, attempts: result.attempts, reason: "no-fundamentals" });
          continue;
        }
        const d = result.data;
        const general = d.General || {};
        const highlights = d.Highlights || {};
        const ratios = d.ValuationRatios || d.Valuation || {};
        const valuation = d.Valuation || {};
        const defaultStats = d.DefaultKeyStatistics || {};
        const price = Number(highlights.Close || highlights.LastClose || highlights.LatestClose);
        const num = (v) => v === null || typeof v === "undefined" || v === "" ? null : Number(v);

        const item = {
          code: base,
          name: general.Name || base,
          sector: general.Sector || null,
          industry: general.Industry || null,
          inAsx200: asx200Set.has(base) ? 1 : 0,
          price: Number.isFinite(price) ? price : null,
          marketCap: num(highlights.MarketCapitalization || general.MarketCapitalization),
          pctChange: num(highlights.ChangePercent || highlights.RelativePriceChange),
          ebitda: num(highlights.EBITDA),
          peRatio: num(ratios.PERatio || highlights.PERatio),
          pegRatio: num(ratios.PEGRatio || highlights.PEGRatio || defaultStats.PEGRatio),
          eps: num(highlights.EarningsPerShare || highlights.EarningsShare),
          bookValue: num(highlights.BookValue),
          dividendPerShare: num(highlights.DividendShare || safeGet(d, "SplitsDividends.LastAnnualDividend")),
          dividendYield: num(safeGet(d, "SplitsDividends.ForwardAnnualDividendYield", highlights.DividendYield)),
          profitMargin: num(highlights.ProfitMargin),
          operatingMargin: num(highlights.OperatingMargin || highlights.OperatingMarginTTM),
          returnOnAssets: num(highlights.ReturnOnAssets || highlights.ReturnOnAssetsTTM),
          returnOnEquity: num(highlights.ReturnOnEquity || highlights.ReturnOnEquityTTM),
          revenue: num(highlights.RevenueTTM || highlights.Revenue),
          revenuePerShare: num(highlights.RevenuePerShareTTM || highlights.RevenuePerShare),
          grossProfit: num(highlights.GrossProfitTTM || highlights.GrossProfit),
          dilutedEps: num(highlights.DilutedEpsTTM || highlights.DilutedEps),
          quarterlyRevenueGrowthYoy: num(highlights.QuarterlyRevenueGrowthYOY),
          quarterlyEarningsGrowthYoy: num(highlights.QuarterlyEarningsGrowthYOY),
          trailingPE: num(ratios.TrailingPE || valuation.TrailingPE || highlights.PERatio),
          forwardPE: num(ratios.ForwardPE || valuation.ForwardPE),
          priceToSales: num(ratios.PriceSalesTTM || ratios.PriceToSalesRatio || valuation.PriceSalesTTM),
          priceToBook: num(ratios.PriceBookMRQ || ratios.PriceToBookRatio || valuation.PriceBookMRQ),
          enterpriseValue: num(ratios.EnterpriseValue || valuation.EnterpriseValue),
          evToRevenue: num(ratios.EnterpriseValueRevenue || ratios.EnterpriseValueToRevenue || valuation.EnterpriseValueRevenue),
          evToEbitda: num(ratios.EnterpriseValueEbitda || ratios.EnterpriseValueToEBITDA || valuation.EnterpriseValueEbitda),
        };

        if (EXCLUDE_ETF && isEtfName(item.name)) {
          console.log(`[snapshot-asx-universe] skipping ETF ${item.code} - ${item.name}`);
          continue;
        }

        items.push(item);
      } catch (err) {
        console.warn("[snapshot-asx-universe] error for", rawCode, err && err.message);
        failures.push({ code: rawCode, reason: "exception", error: String(err && err.message) });
      }
    }
  });

  await Promise.all(workers);

  // Write per-part key and advance offset
  const partKey = `asx:universe:fundamentals:part:${batchStart}`;
  const payload = { generatedAt: new Date().toISOString(), universeTotal, batchStart, batchSize: batchUniverse.length, count: items.length, items };
  const okPart = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, partKey, payload);

  // Determine nextOffset (do not advance if singleCode used)
  const nextOffset = singleCode ? batchStart : Math.min(universeTotal, batchStart + batchLimit);
  const okOffset = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:offset", String(nextOffset));
  await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:lastRun", new Date().toISOString());

  const excludedInThisBatch = batchUniverse.length - items.length - failures.length;

  const responseBody = { ok: okPart && okOffset, partKey, requestedUniverseTotal: universeTotal, batchStart, batchRequested: batchLimit, batchProcessed: batchUniverse.length, collected: items.length, failures: failures.slice(0,40), excludedInThisBatch, nextOffset, elapsedMs: Date.now() - start };

  console.log("[snapshot-asx-universe] response:", JSON.stringify(responseBody));
  return { statusCode: okPart && okOffset ? 200 : 500, body: JSON.stringify(responseBody) };
};
