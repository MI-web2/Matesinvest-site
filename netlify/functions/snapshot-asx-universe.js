// netlify/functions/snapshot-asx-universe.js
//
// Nightly snapshot of full ASX universe fundamentals for the screener.
//
// - Reads tickers from asx-universe.txt (CSV or newline-separated)
// - (Optional) also reads asx200.txt to mark which are in the ASX 200
// - Fetches fundamentals from EODHD once per symbol (with suffix fallbacks)
// - Stores a compact list into Upstash as:
//      asx:universe:fundamentals:latest
//
// Requirements (Netlify env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional env:
//   TRY_SUFFIXES   (default "AU,AX,ASX")
//   UNIVERSE_FILE  (override path to asx-universe.txt)
//   CONCURRENCY    (default 8)
//   RETRIES        (default 2)

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

async function redisSet(urlBase, token, key, value, ttlSeconds) {
  if (!urlBase || !token) return false;
  try {
    const payload =
      typeof value === "string" ? value : JSON.stringify(value);
    const ttl = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : "";
    const res = await fetchWithTimeout(
      `${urlBase}/set/${encodeURIComponent(key)}/${encodeURIComponent(
        payload
      )}${ttl}`,
      { method: "POST", headers: { Authorization: `Bearer ${token}` } },
      15000
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn(
        "redisSet failed",
        key,
        res.status,
        txt && txt.slice(0, 300)
      );
      return false;
    }
    return true;
  } catch (err) {
    console.warn("redisSet error", key, err && err.message);
    return false;
  }
}

function normalizeCode(code) {
  return String(code || "")
    .replace(/\.[A-Z0-9]{1,6}$/i, "")
    .toUpperCase();
}

// Read "big universe" from asx-universe.txt (or env override)
function readUniverseSync() {
  const override = process.env.UNIVERSE_FILE;
  const candidates = override
    ? [path.join(__dirname, override)]
    : [
        path.join(__dirname, "asx-universe.txt"),
        path.join(__dirname, "asx200.txt"), // fallback if you temporarily re-use it
      ];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, "utf8");
        const parts = raw
          .split(/[\s,]+/) // comma or whitespace separated
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

  throw new Error(
    "Failed to read universe list: asx-universe.txt not found in expected locations."
  );
}

// Optional: read ASX200 list to add a flag inAsx200
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
        console.log(
          `[snapshot-asx-universe] using asx200 file: ${p} (entries=${parts.length})`
        );
        return new Set(parts.map((c) => c.toUpperCase()));
      }
    } catch (err) {
      console.warn(
        `[snapshot-asx-universe] asx200 read failure for ${p}: ${
          err && err.message
        }`
      );
    }
  }
  return new Set();
}

// Safe getter
function get(obj, path, fallback = null) {
  return path.split(".").reduce((acc, key) => {
    if (acc && Object.prototype.hasOwnProperty.call(acc, key)) {
      return acc[key];
    }
    return fallback;
  }, obj);
}

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

  const qs = (event && event.queryStringParameters) || {};
  const TRY_SUFFIXES = (
    process.env.TRY_SUFFIXES
      ? process.env.TRY_SUFFIXES.split(",")
      : DEFAULT_TRY_SUFFIXES
  )
    .map((s) => s.trim())
    .filter(Boolean);

  const CONCURRENCY = Number(
    process.env.CONCURRENCY || DEFAULT_CONCURRENCY
  );
  const RETRIES = Number(process.env.RETRIES || DEFAULT_RETRIES);

  let universeCodes;
  let asx200Set;
  try {
    universeCodes = readUniverseSync();
    asx200Set = readAsx200Sync();
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message }),
    };
  }

  // Optional dev mode: &code=BHP to test a single ticker quickly
  const singleCode = qs.code
    ? String(qs.code).trim().toUpperCase()
    : null;
  if (singleCode) {
    universeCodes = [singleCode];
  }

  // Fundamentals fetch with retries
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
          // Retry on 429/5xx
          if (
            res.status === 429 ||
            (res.status >= 500 && res.status < 600)
          ) {
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
    // Try with existing suffixes; first one that returns ok JSON wins
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
  const total = universeCodes.length;
  const workers = new Array(Math.min(CONCURRENCY, total))
    .fill(null)
    .map(async () => {
      while (true) {
        const i = idx++;
        if (i >= total) return;
        const rawCode = universeCodes[i];
        const base = normalizeCode(rawCode);

        try {
          const result = await getFundamentalsForBaseCode(base);
          if (!result.data) {
            failures.push({
              code: base,
              attempts: result.attempts,
              reason: "no-fundamentals",
            });
            continue;
          }

          const d = result.data;

          // --- Map a compact item shape for the screener ---
          const general = d.General || {};
          const highlights = d.Highlights || {};
          const ratios =
            d.ValuationRatios || d.Valuation || {};
          const valuation = d.Valuation || {};
          const defaultStats = d.DefaultKeyStatistics || {};

          const price = Number(
            highlights.Close ||
              highlights.LastClose ||
              highlights.LatestClose
          );

          // helper to coerce to number but keep null if missing
          const num = (v) =>
            v === null || typeof v === "undefined" || v === ""
              ? null
              : Number(v);

          const item = {
            code: base,
            name: general.Name || base,
            sector: general.Sector || null,
            industry: general.Industry || null,
            inAsx200: asx200Set.has(base) ? 1 : 0,

            // Basic price & size
            price: Number.isFinite(price) ? price : null,
            marketCap: num(
              highlights.MarketCapitalization ||
                general.MarketCapitalization
            ),
            pctChange: num(
              highlights.ChangePercent ||
                highlights.RelativePriceChange
            ),

            // Key fundamentals
            ebitda: num(highlights.EBITDA),

            peRatio: num(
              ratios.PERatio || highlights.PERatio
            ),

            pegRatio: num(
              ratios.PEGRatio ||
                highlights.PEGRatio ||
                defaultStats.PEGRatio
            ),

            eps: num(
              highlights.EarningsPerShare ||
                highlights.EarningsShare
            ),

            bookValue: num(highlights.BookValue),

            dividendPerShare: num(
              highlights.DividendShare ||
                get(
                  d,
                  "SplitsDividends.LastAnnualDividend"
                )
            ),

            dividendYield: num(
              get(
                d,
                "SplitsDividends.ForwardAnnualDividendYield",
                highlights.DividendYield
              )
            ),

            profitMargin: num(highlights.ProfitMargin),

            operatingMargin: num(
              highlights.OperatingMargin ||
                highlights.OperatingMarginTTM
            ),

            returnOnAssets: num(
              highlights.ReturnOnAssets ||
                highlights.ReturnOnAssetsTTM
            ),

            returnOnEquity: num(
              highlights.ReturnOnEquity ||
                highlights.ReturnOnEquityTTM
            ),

            revenue: num(highlights.RevenueTTM || highlights.Revenue),

            revenuePerShare: num(
              highlights.RevenuePerShareTTM ||
                highlights.RevenuePerShare
            ),

            grossProfit: num(
              highlights.GrossProfitTTM ||
                highlights.GrossProfit
            ),

            dilutedEps: num(
              highlights.DilutedEpsTTM ||
                highlights.DilutedEps
            ),

            quarterlyRevenueGrowthYoy: num(
              highlights.QuarterlyRevenueGrowthYOY
            ),

            quarterlyEarningsGrowthYoy: num(
              highlights.QuarterlyEarningsGrowthYOY
            ),

            // Valuation metrics
            trailingPE: num(
              ratios.TrailingPE ||
                valuation.TrailingPE ||
                highlights.PERatio
            ),

            forwardPE: num(
              ratios.ForwardPE || valuation.ForwardPE
            ),

            priceToSales: num(
              ratios.PriceSalesTTM ||
                ratios.PriceToSalesRatio ||
                valuation.PriceSalesTTM
            ),

            priceToBook: num(
              ratios.PriceBookMRQ ||
                ratios.PriceToBookRatio ||
                valuation.PriceBookMRQ
            ),

            enterpriseValue: num(
              ratios.EnterpriseValue ||
                valuation.EnterpriseValue
            ),

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

          items.push(item);
        } catch (err) {
          console.warn(
            "[snapshot-asx-universe] error for",
            rawCode,
            err && err.message
          );
          failures.push({
            code: rawCode,
            reason: "exception",
            error: String(err && err.message),
          });
        }
      }
    });

  await Promise.all(workers);

  // Persist the result as a single blob
  const key = "asx:universe:fundamentals:latest";
  const payload = {
    generatedAt: new Date().toISOString(),
    universeSize: universeCodes.length,
    count: items.length,
    items,
  };

  const ok = await redisSet(UPSTASH_URL, UPSTASH_TOKEN, key, payload);

  const responseBody = {
    ok,
    key,
    requested: universeCodes.length,
    collected: items.length,
    failures: failures.slice(0, 40),
    elapsedMs: Date.now() - start,
  };

  return {
    statusCode: ok ? 200 : 500,
    body: JSON.stringify(responseBody),
  };
};
