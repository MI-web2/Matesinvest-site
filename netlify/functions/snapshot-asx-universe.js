// netlify/functions/snapshot-asx-universe.js
//
// Batched snapshot writer: writes each processed batch to asx:universe:fundamentals:part:<offset>
// and persists progress to asx:universe:offset. Does NOT overwrite the final "latest" key.
//
// Key behaviour change vs old version:
// - If a batch hits rate limits / is "bad", we DO NOT advance offset (so we don't permanently skip codes).
// - 429 handling respects Retry-After and uses longer backoff.
//
// Requirements (env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env:
//   TRY_SUFFIXES (default "AU,AX,ASX")
//   CONCURRENCY (default 2 if not set; recommend 1–2 with EODHD)
//   RETRIES (default 5; recommend 3–5)
//   BATCH_SIZE (default 50; recommend 25–50)
//   EXCLUDE_ETF  (default "1" to exclude ETFs at snapshot level)

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];
const DEFAULT_CONCURRENCY = 2; // safer default than 8 for rate-limited providers
const DEFAULT_RETRIES = 5;     // more forgiving

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

// -------------------------------
// Upstash helpers
// -------------------------------
async function redisGet(urlBase, token, key) {
  if (!urlBase || !token) return null;
  try {
    const res = await fetchWithTimeout(
      `${urlBase}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${token}` } },
      10000
    );
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

// -------------------------------
// Utilities
// -------------------------------
function normalizeCode(code) {
  return String(code || "").replace(/\.[A-Z0-9]{1,6}$/i, "").toUpperCase();
}

function isEtfName(name) {
  if (!name || typeof name !== "string") return false;
  const cleaned = name
    .replace(/[\u2013\u2014–—\-()]/g, " ")
    .replace(/[\s\.,;:]+/g, " ")
    .trim();
  return /\bETF$/i.test(cleaned);
}

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
      ];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw
          .split(/[\s,]+/)
          .map((s) => s.trim())
          .filter(Boolean);
        console.log(
          `[snapshot-asx-universe] using universe file: ${p} (entries=${parts.length})`
        );
        return Array.from(new Set(parts.map((c) => c.toUpperCase())));
      }
    } catch (err) {
      console.warn(
        `[snapshot-asx-universe] read failure for ${p}: ${err && err.message}`
      );
    }
  }
  throw new Error("Failed to read universe list");
}

function safeGet(obj, pathStr, fallback = null) {
  if (!obj || !pathStr) return fallback;
  const parts = pathStr.split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur && Object.prototype.hasOwnProperty.call(cur, p)) cur = cur[p];
    else return fallback;
  }
  return cur;
}

function parseRetryAfterToMs(retryAfterHeader) {
  if (!retryAfterHeader) return null;

  // Case 1: seconds (e.g. "30")
  const asNum = Number(String(retryAfterHeader).trim());
  if (Number.isFinite(asNum) && asNum >= 0) return Math.round(asNum * 1000);

  // Case 2: HTTP-date
  const dt = Date.parse(retryAfterHeader);
  if (!Number.isNaN(dt)) {
    const delta = dt - Date.now();
    return delta > 0 ? delta : 0;
  }
  return null;
}

function jitter(ms, maxJitter = 500) {
  return ms + Math.floor(Math.random() * maxJitter);
}

// -------------------------------
// Handler
// -------------------------------
exports.handler = async function (event) {
  const start = Date.now();

  if (!EODHD_TOKEN) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }),
    };
  }
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing Upstash env" }),
    };
  }
  const today = new Date().toISOString().slice(0, 10);
const lastRunDate = await redisGet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:lastRunDate");

if (lastRunDate !== today) {
  await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:offset", "0");
  await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:lastRunDate", today);
}

  const qs = (event && event.queryStringParameters) || {};
  const TRY_SUFFIXES = (
    process.env.TRY_SUFFIXES
      ? process.env.TRY_SUFFIXES.split(",")
      : DEFAULT_TRY_SUFFIXES
  )
    .map((s) => s.trim())
    .filter(Boolean);

  const CONCURRENCY = Number(process.env.CONCURRENCY || DEFAULT_CONCURRENCY);
  const RETRIES = Number(process.env.RETRIES || DEFAULT_RETRIES);
  const BATCH_SIZE = Number(qs.limit || qs.size || process.env.BATCH_SIZE || 50);
  const EXCLUDE_ETF = String(process.env.EXCLUDE_ETF || "1") === "1";

  // Load universe & asx200
  let universeCodes, asx200Set;
  try {
    universeCodes = readUniverseSync();
    try {
      const asx200Path = path.join(__dirname, "asx200.txt");
      const asx200Raw = fs.existsSync(asx200Path)
        ? fs.readFileSync(asx200Path, "utf8")
        : "";
      const asxParts = asx200Raw
        ? asx200Raw.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean)
        : [];
      asx200Set = new Set(asxParts.map((s) => s.toUpperCase()));
    } catch (e) {
      asx200Set = new Set();
    }
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

  // Prepare slice
  const batchUniverse = singleCode
    ? [singleCode]
    : universeCodes.slice(batchStart, batchStart + batchLimit);

  console.log(
    `[snapshot-asx-universe] running batch start=${batchStart} limit=${batchLimit} actual=${batchUniverse.length} excludeETF=${EXCLUDE_ETF} concurrency=${CONCURRENCY} retries=${RETRIES}`
  );

  // -------------------------------
  // EODHD fetch with robust 429 logic
  // -------------------------------
  async function fetchFundamentals(fullCode) {
    const url = `https://eodhd.com/api/fundamentals/${encodeURIComponent(
      fullCode
    )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;

    let attempt = 0;
    let lastText = null;

    while (attempt <= RETRIES) {
      try {
        const res = await fetchWithTimeout(url, {}, 18000);
        const text = await res.text().catch(() => "");
        if (!res.ok) {
          lastText = text || lastText;

          // 429: respect Retry-After if provided; otherwise exponential backoff
          if (res.status === 429) {
            const ra = parseRetryAfterToMs(res.headers.get("retry-after"));
            const base = ra != null ? ra : 2000 * Math.pow(2, attempt); // 2s, 4s, 8s, 16s...
            await sleep(jitter(base, 800));
            attempt++;
            continue;
          }

          // 5xx: retry with a moderate backoff
          if (res.status >= 500 && res.status < 600) {
            const base = 1000 * Math.pow(2, attempt); // 1s,2s,4s,8s...
            await sleep(jitter(base, 600));
            attempt++;
            continue;
          }

          // Anything else (4xx etc): don't retry
          return { ok: false, status: res.status, text };
        }

        // Parse JSON
        try {
          const json = text ? JSON.parse(text) : null;
          return { ok: true, status: res.status, data: json || {} };
        } catch (e) {
          return { ok: false, status: res.status, text };
        }
      } catch (err) {
        lastText = String(err && err.message) || lastText;
        const base = 1000 * Math.pow(2, attempt);
        await sleep(jitter(base, 600));
        attempt++;
      }
    }

    return { ok: false, status: 0, text: lastText };
  }

  async function getFundamentalsForBaseCode(baseCode) {
    const attempts = [];
    let saw429 = false;

    for (const sfx of TRY_SUFFIXES) {
      const fullCode = `${baseCode}.${sfx}`;
      attempts.push(fullCode);

      const res = await fetchFundamentals(fullCode);

      if (!res.ok) {
        if (res.status === 429) saw429 = true;
        continue;
      }

      if (!res.data || !res.data.General) continue;

      return { baseCode, fullCode, data: res.data, attempts, saw429 };
    }

    return { baseCode, fullCode: null, data: null, attempts, saw429 };
  }

  // -------------------------------
  // Process batch with limited concurrency
  // -------------------------------
  const items = [];
  const failures = [];

  let idx = 0;
  const total = batchUniverse.length;

  // Diagnostics (for "bad batch" detection)
  let rateLimitedCount = 0;
  let sawRateLimit = false;

  const workerCount = Math.max(1, Math.min(CONCURRENCY || 1, total));
  const workers = new Array(workerCount).fill(null).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= total) return;

      const rawCode = batchUniverse[i];
      const base = normalizeCode(rawCode);

      try {
        const result = await getFundamentalsForBaseCode(base);

        if (result.saw429) {
          sawRateLimit = true;
          rateLimitedCount++;
        }

        if (!result.data) {
          failures.push({
            code: base,
            attempts: result.attempts,
            reason: "no-fundamentals",
            rateLimited: result.saw429 ? 1 : 0,
          });
          continue;
        }

        const d = result.data;
        const general = d.General || {};
        const highlights = d.Highlights || {};
        const ratios = d.ValuationRatios || d.Valuation || {};
        const valuation = d.Valuation || {};
        const defaultStats = d.DefaultKeyStatistics || {};

        const price = Number(
          highlights.Close || highlights.LastClose || highlights.LatestClose
        );
        const num = (v) =>
          v === null || typeof v === "undefined" || v === "" ? null : Number(v);

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
          dividendPerShare: num(
            highlights.DividendShare || safeGet(d, "SplitsDividends.LastAnnualDividend")
          ),
          dividendYield: num(
            safeGet(d, "SplitsDividends.ForwardAnnualDividendYield", highlights.DividendYield)
          ),
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
          priceToSales: num(
            ratios.PriceSalesTTM || ratios.PriceToSalesRatio || valuation.PriceSalesTTM
          ),
          priceToBook: num(
            ratios.PriceBookMRQ || ratios.PriceToBookRatio || valuation.PriceBookMRQ
          ),
          enterpriseValue: num(ratios.EnterpriseValue || valuation.EnterpriseValue),
          evToRevenue: num(
            ratios.EnterpriseValueRevenue ||
              ratios.EnterpriseValueToRevenue ||
              valuation.EnterpriseValueRevenue
          ),
          evToEbitda: num(
            ratios.EnterpriseValueEbitda ||
              ratios.EnterpriseValueToEBITDA ||
              valuation.EnterpriseValueEbitda
          ),
        };

        if (EXCLUDE_ETF && isEtfName(item.name)) {
          console.log(`[snapshot-asx-universe] skipping ETF ${item.code} - ${item.name}`);
          continue;
        }

        items.push(item);
      } catch (err) {
        console.warn("[snapshot-asx-universe] error for", rawCode, err && err.message);
        failures.push({
          code: base,
          reason: "exception",
          error: String(err && err.message),
        });
      }
    }
  });

  await Promise.all(workers);

  // -------------------------------
  // Decide whether to commit batch
  // -------------------------------
  const attempted = batchUniverse.length;
  const failureRate = failures.length / Math.max(1, attempted);

  // "Bad batch" heuristic:
  // - any 429s observed (strong signal of provider throttling),
  // - OR we collected zero items but had failures,
  // - OR failureRate extremely high (treat as poisoned)
  const badBatch =
    !singleCode &&
    (sawRateLimit || (items.length === 0 && failures.length > 0) || failureRate > 0.8);

  const partKey = `asx:universe:fundamentals:part:${batchStart}`;

  // always write lastRun for observability
  await redisSet(UPSTASH_URL, UPSTASH_TOKEN, "asx:universe:lastRun", new Date().toISOString());

  // Write a small status key (TTL) every time so you can debug quickly
  const statusKey = `asx:universe:fundamentals:part:${batchStart}:status`;
  await redisSet(
    UPSTASH_URL,
    UPSTASH_TOKEN,
    statusKey,
    {
      generatedAt: new Date().toISOString(),
      universeTotal,
      batchStart,
      batchSize: attempted,
      collected: items.length,
      failures: failures.length,
      rateLimitedCount,
      sawRateLimit: sawRateLimit ? 1 : 0,
      badBatch: badBatch ? 1 : 0,
    },
    60 * 60 * 24 // 24h TTL
  );

  if (badBatch) {
    // DO NOT write the partKey payload (avoid overwriting a previously good batch)
    // DO NOT advance offset
    const responseBody = {
      ok: false,
      error: "Rate limited or failed batch; will retry same offset on next run",
      partKey,
      batchStart,
      batchProcessed: attempted,
      collected: items.length,
      failures: failures.slice(0, 40),
      rateLimitedCount,
      nextOffset: batchStart, // unchanged
      elapsedMs: Date.now() - start,
    };

    console.log("[snapshot-asx-universe] badBatch response:", JSON.stringify(responseBody));
    // Returning 429 makes Netlify logs obvious; scheduler will call again next interval
    return { statusCode: 429, body: JSON.stringify(responseBody) };
  }

  // -------------------------------
  // Commit part payload + advance offset
  // -------------------------------
  const payload = {
    generatedAt: new Date().toISOString(),
    universeTotal,
    batchStart,
    batchSize: attempted,
    count: items.length,
    items,
  };

  const okPart = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, partKey, payload);

  // Determine nextOffset (do not advance if singleCode used)
  const nextOffset = singleCode
    ? batchStart
    : Math.min(universeTotal, batchStart + batchLimit);

  const okOffset = await redisSet(
    UPSTASH_URL,
    UPSTASH_TOKEN,
    "asx:universe:offset",
    String(nextOffset)
  );

  const excludedInThisBatch = attempted - items.length - failures.length;

  const responseBody = {
    ok: okPart && okOffset,
    partKey,
    requestedUniverseTotal: universeTotal,
    batchStart,
    batchRequested: batchLimit,
    batchProcessed: attempted,
    collected: items.length,
    failures: failures.slice(0, 40),
    excludedInThisBatch,
    rateLimitedCount,
    nextOffset,
    elapsedMs: Date.now() - start,
  };

  console.log("[snapshot-asx-universe] response:", JSON.stringify(responseBody));
  return { statusCode: okPart && okOffset ? 200 : 500, body: JSON.stringify(responseBody) };
};
