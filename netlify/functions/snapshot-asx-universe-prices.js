// netlify/functions/snapshot-asx-universe.js
//
// Automated batched snapshot for ASX universe.
// - Each invocation reads offset from Upstash, processes a batch, writes a per-part key,
//   updates offset, and exits. When finished (offset >= universeTotal) it merges parts
//   and writes the final asx:universe:fundamentals:latest blob.
//
// Environment required:
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env:
//   TRY_SUFFIXES   (default "AU,AX,ASX")
//   CONCURRENCY    (default 8)
//   RETRIES        (default 2)
//   BATCH_SIZE     (default 200)
// Notes:
//   - Schedule this function frequently (e.g. every 1-5 minutes) during the window you
//     want the job to run. Each invocation will do one batch and exit.

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_CONCURRENCY = 8;
const DEFAULT_RETRIES = 2;

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

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

// Upstash helpers: GET and SET (simple REST)
async function redisGet(urlBase, token, key) {
  if (!urlBase || !token) return null;
  try {
    const res = await fetchWithTimeout(`${urlBase}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${token}` },
    }, 10000);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisGet failed", key, res.status, txt && txt.slice(0, 200));
      return null;
    }
    const j = await res.json();
    return j.result || null;
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

// Read universe file (robust search locations)
function readUniverseSync() {
  const override = process.env.UNIVERSE_FILE;
  const candidates = override
    ? [
        path.join(__dirname, override),
        path.join(process.cwd(), override),
        path.join(process.cwd(), "netlify", "functions", override),
      ]
    : [
        path.join(__dirname, "asx-universe.txt"),
        path.join(process.cwd(), "asx-universe.txt"),
        path.join(process.cwd(), "netlify", "functions", "asx-universe.txt"),
        path.join(__dirname, "asx200.txt"),
        path.join(process.cwd(), "netlify", "functions", "asx200.txt"),
      ];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw
          .split(/[\s,]+/) // comma or whitespace separated
          .map((s) => s.trim())
          .filter(Boolean);
        console.log(`[snapshot-asx-universe] using universe file: ${p} (entries=${parts.length})`);
        return Array.from(new Set(parts.map((c) => c.toUpperCase())));
      }
    } catch (err) {
      console.warn(`[snapshot-asx-universe] read failure for ${p}: ${err && err.message}`);
    }
  }

  throw new Error("Failed to read universe list: asx-universe.txt not found in expected locations.");
}

function readAsx200Sync() {
  const candidates = [
    path.join(__dirname, "asx200.txt"),
    path.join(process.cwd(), "netlify", "functions", "asx200.txt"),
  ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw
          .split(/[\s,]+/)
          .map((s) => s.trim())
          .filter(Boolean);
        console.log(`[snapshot-asx-universe] using asx200 file: ${p} (entries=${parts.length})`);
        return new Set(parts.map((c) => c.toUpperCase()));
      }
    } catch (err) {
      console.warn(`[snapshot-asx-universe] asx200 read failure for ${p}: ${err && err.message}`);
    }
  }
  return new Set();
}

function get(obj, path, fallback = null) {
  return path.split(".").reduce((acc, key) => {
    if (acc && Object.prototype.hasOwnProperty.call(acc, key)) {
      return acc[key];
    }
    return fallback;
  }, obj);
}

// Merge parts into a final latest snapshot
async function mergePartsAndWriteLatest(universeTotal, batchSize) {
  const parts = [];
  for (let offset = 0; offset < universeTotal; offset += batchSize) {
    const partKey = `asx:universe:fundamentals:part:${offset}`;
    const raw = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, partKey);
    if (!raw) {
      console.warn("[merge] missing part", partKey);
      continue;
    }
    let parsed;
    try {
      parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (e) {
      parsed = raw;
    }
    if (parsed && Array.isArray(parsed.items)) parts.push(parsed);
  }

  // Dedupe by code, preserve order by partStart
  parts.sort((a, b) => (a.batchStart || 0) - (b.batchStart || 0));
  const map = new Map();
  for (const p of parts) {
    for (const it of p.items) {
      if (!map.has(it.code)) map.set(it.code, it);
    }
  }
  const merged = {
    generatedAt: new Date().toISOString(),
    universeTotal,
    partCount: parts.length,
    count: map.size,
    items: Array.from(map.values()),
  };

  const ok = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:fundamentals:latest", merged);
  if (!ok) throw new Error("failed to write latest merged snapshot");
  console.log("[merge] merged saved items=", merged.count);
  return merged.count;
}

exports.handler = async function (event) {
  const start = Date.now();

  if (!EODHD_TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }) };
  }
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: "Missing Upstash env" }) };
  }

  const qs = (event && event.queryStringParameters) || {};
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES ? process.env.TRY_SUFFIXES.split(",") : DEFAULT_TRY_SUFFIXES)
    .map((s) => s.trim())
    .filter(Boolean);
  const CONCURRENCY = Number(process.env.CONCURRENCY || DEFAULT_CONCURRENCY);
  const RETRIES = Number(process.env.RETRIES || DEFAULT_RETRIES);
  const BATCH_SIZE = Number(qs.limit || qs.size || process.env.BATCH_SIZE || 200);

  let universeCodes;
  let asx200Set;
  try {
    universeCodes = readUniverseSync();
    asx200Set = readAsx200Sync();
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }

  const universeTotal = universeCodes.length;
  console.log(`[snapshot-asx-universe] universeTotal=${universeTotal}`);

  // Read offset from Upstash (persisted progress). Key: asx:universe:offset
  const rawOffset = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:offset");
  let offset = 0;
  if (rawOffset) {
    try {
      offset = Number(String(rawOffset)) || 0;
    } catch (e) {
      offset = 0;
    }
  }

  // If query param offset provided, allow override (helps debugging)
  if (qs.offset) {
    offset = Math.max(0, Number(qs.offset));
  }

  // If offset already >= total, run merge and reset
  if (offset >= universeTotal) {
    console.log("[snapshot-asx-universe] offset >= universeTotal => running final merge");
    try {
      const mergedCount = await mergePartsAndWriteLatest(universeTotal, BATCH_SIZE);
      // reset offset to 0 to allow next daily snapshot cycle
      await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:offset", "0");
      await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:completedAt", new Date().toISOString());
      const resp = { ok: true, mergedCount, message: "merge completed and offset reset" };
      console.log("[snapshot-asx-universe] response:", JSON.stringify(resp));
      return { statusCode: 200, body: JSON.stringify(resp) };
    } catch (e) {
      console.error("[snapshot-asx-universe] merge failed", e && e.message);
      return { statusCode: 500, body: JSON.stringify({ error: "merge failed", detail: String(e && e.message) }) };
    }
  }

  // Determine slice for this run
  const batchStart = offset;
  const batchLimit = Math.max(1, Math.min(BATCH_SIZE, universeTotal - batchStart));
  const batchCodes = universeCodes.slice(batchStart, batchStart + batchLimit);
  console.log(`[snapshot-asx-universe] processing batch start=${batchStart} limit=${batchLimit} codes=${batchCodes.length}`);

  // Quick dev override: singleCode via ?code=AAA
  const singleCode = qs.code ? String(qs.code).trim().toUpperCase() : null;
  if (singleCode) {
    batchCodes.length = 0;
    batchCodes.push(singleCode);
  }

  // fetch fundamentals with retry helper
  async function fetchFundamentals(fullCode) {
    const url = `https://eodhd.com/api/fundamentals/${encodeURIComponent(fullCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;
    let attempt = 0;
    let lastText = null;
    while (attempt <= RETRIES) {
      try {
        const res = await fetchWithTimeout(url, {}, 18000);
        const text = await res.text().catch(() => "");
        if (!res.ok) {
          lastText = text || lastText;
          if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
            const backoff = 400 * Math.pow(2, attempt);
            await sleep(backoff + Math.random() * 250);
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
        await sleep(backoff + Math.random() * 250);
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

  const items = [];
  const failures = [];
  let idx = 0;
  const total = batchCodes.length;
  const workers = new Array(Math.min(CONCURRENCY, total)).fill(null).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= total) return;
      const rawCode = batchCodes[i];
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
        const num = (v) => (v === null || typeof v === "undefined" || v === "" ? null : Number(v));

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
          dividendPerShare: num(highlights.DividendShare || get(d, "SplitsDividends.LastAnnualDividend")),
          dividendYield: num(get(d, "SplitsDividends.ForwardAnnualDividendYield", highlights.DividendYield)),
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
        items.push(item);
      } catch (err) {
        console.warn("[snapshot-asx-universe] error for", rawCode, err && err.message);
        failures.push({ code: rawCode, reason: "exception", error: String(err && err.message) });
      }
    }
  });

  await Promise.all(workers);

  // Persist the batch as a part key to avoid clobbering
  const partKey = `asx:universe:fundamentals:part:${batchStart}`;
  const payload = {
    generatedAt: new Date().toISOString(),
    universeTotal,
    batchStart,
    batchSize: batchCodes.length,
    count: items.length,
    items,
  };

  const savedOk = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, partKey, payload);
  console.log(`[snapshot-asx-universe] saved part=${partKey} ok=${savedOk} items=${items.length} failures=${failures.length}`);

  // update offset to nextBatchStart (atomicity caveat: this is a simple update; avoid overlapping schedule windows)
  const nextOffset = batchStart + batchLimit;
  await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:offset", String(nextOffset));
  await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:lastRun", new Date().toISOString());

  const responseBody = {
    ok: savedOk,
    partKey,
    requestedUniverseTotal: universeTotal,
    batchStart,
    batchRequested: batchLimit,
    batchProcessed: batchCodes.length,
    collected: items.length,
    failures: failures.slice(0, 40),
    nextOffset,
    elapsedMs: Date.now() - start,
  };

  console.log("[snapshot-asx-universe] response:", JSON.stringify(responseBody));
  return { statusCode: savedOk ? 200 : 500, body: JSON.stringify(responseBody) };
};