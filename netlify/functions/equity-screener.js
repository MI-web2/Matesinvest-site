// netlify/functions/equity-screener.js
//
// Serves data to the ASX screener UI.
//
// Normal mode:
//   GET /.netlify/functions/equity-screener
//   - Reads price/sector rows from Upstash key:
//       asx200:latest  (or asx200:daily:YYYY-MM-DD if ?date= is provided)
//   - Reads fundamentals from Upstash key:
//       asx200:fundamentals:latest (or :YYYY-MM-DD)
//   - Joins them by code and returns a list of enriched items.
//
// Debug mode (single ticker live fundamentals):
//   GET /.netlify/functions/equity-screener?code=BHP
//
// Env:
//   EODHD_API_TOKEN             (for debug mode only)
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//

const fetch = (...args) => global.fetch(...args);

const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN || null;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

// ---------- Helpers ----------

function json(statusCode, bodyObj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify(bodyObj),
  };
}

function fetchWithTimeout(url, opts = {}, timeout = 12000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  return fetch(url, { ...opts, signal: controller.signal })
    .finally(() => clearTimeout(id));
}

async function redisGet(key, timeout = 8000) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  try {
    const res = await fetchWithTimeout(
      `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      timeout
    );
    if (!res.ok) {
      console.warn("redisGet not ok", key, res.status);
      return null;
    }
    const j = await res.json().catch(() => null);
    if (!j || typeof j.result === "undefined") return null;
    return j.result;
  } catch (err) {
    console.warn("redisGet error", key, err && err.message);
    return null;
  }
}

// Safe nested getter
const getVal = (obj, path, fallback = null) =>
  path.split(".").reduce((acc, key) => {
    if (acc && Object.prototype.hasOwnProperty.call(acc, key)) {
      return acc[key];
    }
    return fallback;
  }, obj);

// Live fundamentals fetch (only used in ?code=BHP debug mode)
async function fetchFundamentalsLive(code) {
  if (!EODHD_API_TOKEN) {
    throw new Error("EODHD_API_TOKEN not set for debug mode");
  }

  // Try a few common suffixes – same as elsewhere
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES || "AU,AX,ASX")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  let lastErr = null;

  for (const suffix of TRY_SUFFIXES) {
    const symbol = `${code}.${suffix}`;
    const url = `https://eodhd.com/api/fundamentals/${encodeURIComponent(
      symbol
    )}?api_token=${encodeURIComponent(EODHD_API_TOKEN)}&fmt=json`;

    try {
      const res = await fetchWithTimeout(url, {}, 15000);
      const text = await res.text().catch(() => "");
      if (!res.ok) {
        lastErr = `${res.status} ${text && text.slice(0, 120)}`;
        if (res.status !== 404) break;
        continue;
      }
      try {
        const json = text ? JSON.parse(text) : null;
        return json || null;
      } catch (e) {
        lastErr = e && e.message;
        break;
      }
    } catch (err) {
      lastErr = err && err.message;
      break;
    }
  }

  console.warn("live fundamentals failed for", code, lastErr);
  return null;
}

// ---------- Handler ----------

exports.handler = async function (event) {
  try {
    // CORS preflight
    if (event.httpMethod === "OPTIONS") {
      return {
        statusCode: 200,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "GET,OPTIONS",
        },
        body: "",
      };
    }

    const qs = event.queryStringParameters || {};
    const singleCode = qs.code ? String(qs.code).trim().toUpperCase() : null;
    const dateParam = qs.date ? String(qs.date).trim() : null;

    // --------------------------------------------------
    // 1) Debug mode: ?code=BHP -> live fundamentals only
    // --------------------------------------------------
    if (singleCode) {
      const fundamentals = await fetchFundamentalsLive(singleCode);
      if (!fundamentals) {
        return json(404, {
          error: "No fundamentals found for that code",
          code: singleCode,
        });
      }

      const h = fundamentals.Highlights || {};
      const v = fundamentals.Valuation || {};
      const g = fundamentals.General || {};

      const item = {
        code: singleCode,
        name: g.Name || singleCode,
        sector: g.Sector || "Unknown",
        industry: g.Industry || "Unknown",

        // Price-ish fields from fundamentals (not as clean as EOD snapshot)
        price: h.LatestClose != null ? Number(h.LatestClose) : null,

        // Core fundamentals (same schema as snapshot)
        marketCap:
          h.MarketCapitalization != null ? Number(h.MarketCapitalization) : null,
        ebitda: h.EBITDA != null ? Number(h.EBITDA) : null,
        peRatio: h.PERatio != null ? Number(h.PERatio) : null,
        pegRatio: h.PEGRatio != null ? Number(h.PEGRatio) : null,
        eps: h.EarningsShare != null ? Number(h.EarningsShare) : null,
        bookValue: h.BookValue != null ? Number(h.BookValue) : null,
        dividendPerShare:
          h.DividendShare != null ? Number(h.DividendShare) : null,
        dividendYield:
          h.DividendYield != null ? Number(h.DividendYield) : null,
        profitMargin:
          h.ProfitMargin != null ? Number(h.ProfitMargin) : null,
        operatingMargin:
          h.OperatingMarginTTM != null ? Number(h.OperatingMarginTTM) : null,
        returnOnAssets:
          h.ReturnOnAssetsTTM != null ? Number(h.ReturnOnAssetsTTM) : null,
        returnOnEquity:
          h.ReturnOnEquityTTM != null ? Number(h.ReturnOnEquityTTM) : null,
        revenue: h.RevenueTTM != null ? Number(h.RevenueTTM) : null,
        revenuePerShare:
          h.RevenuePerShareTTM != null ? Number(h.RevenuePerShareTTM) : null,
        grossProfit:
          h.GrossProfitTTM != null ? Number(h.GrossProfitTTM) : null,
        dilutedEps:
          h.DilutedEpsTTM != null ? Number(h.DilutedEpsTTM) : null,
        quarterlyRevenueGrowthYoy:
          h.QuarterlyRevenueGrowthYOY != null
            ? Number(h.QuarterlyRevenueGrowthYOY)
            : null,
        quarterlyEarningsGrowthYoy:
          h.QuarterlyEarningsGrowthYOY != null
            ? Number(h.QuarterlyEarningsGrowthYOY)
            : null,

        trailingPE: v.TrailingPE != null ? Number(v.TrailingPE) : null,
        forwardPE: v.ForwardPE != null ? Number(v.ForwardPE) : null,
        priceToSales:
          v.PriceSalesTTM != null ? Number(v.PriceSalesTTM) : null,
        priceToBook:
          v.PriceBookMRQ != null ? Number(v.PriceBookMRQ) : null,
        enterpriseValue:
          v.EnterpriseValue != null ? Number(v.EnterpriseValue) : null,
        evToRevenue:
          v.EnterpriseValueRevenue != null
            ? Number(v.EnterpriseValueRevenue)
            : null,
        evToEbitda:
          v.EnterpriseValueEbitda != null
            ? Number(v.EnterpriseValueEbitda)
            : null,
      };

      return json(200, {
        generatedAt: new Date().toISOString(),
        count: 1,
        items: [item],
        debug: { mode: "single-live" },
      });
    }

    // --------------------------------------------------
    // 2) Normal mode: join price snapshot + fundamentals
    // --------------------------------------------------

    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return json(500, {
        error: "Missing Upstash environment variables",
      });
    }

    const priceKey = dateParam
      ? `asx200:daily:${dateParam}`
      : "asx200:latest";
    const fundKey = dateParam
      ? `asx200:fundamentals:${dateParam}`
      : "asx200:fundamentals:latest";

    const [priceRaw, fundRaw] = await Promise.all([
      redisGet(priceKey),
      redisGet(fundKey),
    ]);

    if (!priceRaw) {
      return json(500, {
        error: "No ASX200 price snapshot found in cache",
        keyTried: priceKey,
      });
    }

    let priceRows;
    let fundMap;

    try {
      priceRows = typeof priceRaw === "string" ? JSON.parse(priceRaw) : priceRaw;
    } catch (e) {
      console.error("Failed to parse price snapshot JSON", e && e.message);
      return json(500, {
        error: "Failed to parse price snapshot",
      });
    }

    if (!Array.isArray(priceRows)) {
      return json(500, {
        error: "Price snapshot is not an array",
        keyUsed: priceKey,
      });
    }

    if (fundRaw) {
      try {
        fundMap = typeof fundRaw === "string" ? JSON.parse(fundRaw) : fundRaw;
      } catch (e) {
        console.warn(
          "Failed to parse fundamentals snapshot JSON",
          e && e.message
        );
        fundMap = {};
      }
    } else {
      fundMap = {};
    }

    const items = priceRows.map((row) => {
      const code = row.code;
      const f = (fundMap && fundMap[code]) || {};

      const sector =
        row.sector ||
        row.gicSector ||
        "Unknown";

      const industry =
        row.industry ||
        row.gicIndustry ||
        row.gicSubIndustry ||
        "Unknown";

      const lastPrice =
        typeof row.lastPrice === "number" ? row.lastPrice : row.lastPrice ?? null;
      const yesterdayPrice =
        typeof row.yesterdayPrice === "number"
          ? row.yesterdayPrice
          : row.yesterdayPrice ?? null;

      const displayPrice =
        yesterdayPrice != null
          ? yesterdayPrice
          : lastPrice != null
          ? lastPrice
          : null;

      return {
        // Identity
        code,
        name: row.name || code,
        sector,
        industry,

        // Price & moves
        price: displayPrice,              // what you'll show as "Yesterday close"
        lastPrice,                        // latest close from snapshot
        yesterdayPrice,
        pctChange:
          typeof row.pctChange === "number"
            ? row.pctChange
            : row.pctChange ?? null,
        lastDate: row.lastDate || null,
        yesterdayDate: row.yesterdayDate || null,

        // Core fundamentals from snapshot (flat)
        marketCap: f.marketCap ?? null,
        ebitda: f.ebitda ?? null,
        peRatio: f.peRatio ?? null,
        pegRatio: f.pegRatio ?? null,
        eps: f.eps ?? null,
        bookValue: f.bookValue ?? null,
        dividendPerShare: f.dividendPerShare ?? null,
        dividendYield: f.dividendYield ?? null,
        profitMargin: f.profitMargin ?? null,
        operatingMargin: f.operatingMargin ?? null,
        returnOnAssets: f.returnOnAssets ?? null,
        returnOnEquity: f.returnOnEquity ?? null,
        revenue: f.revenue ?? null,
        revenuePerShare: f.revenuePerShare ?? null,
        grossProfit: f.grossProfit ?? null,
        dilutedEps: f.dilutedEps ?? null,
        quarterlyRevenueGrowthYoy: f.quarterlyRevenueGrowthYoy ?? null,
        quarterlyEarningsGrowthYoy: f.quarterlyEarningsGrowthYoy ?? null,
        trailingPE: f.trailingPE ?? null,
        forwardPE: f.forwardPE ?? null,
        priceToSales: f.priceToSales ?? null,
        priceToBook: f.priceToBook ?? null,
        enterpriseValue: f.enterpriseValue ?? null,
        evToRevenue: f.evToRevenue ?? null,
        evToEbitda: f.evToEbitda ?? null,
      };
    });

    return json(200, {
      generatedAt: new Date().toISOString(),
      asOfDate: dateParam || null,
      count: items.length,
      items,
      debug: {
        mode: "cache-join",
        priceKeyUsed: priceKey,
        fundamentalsKeyUsed: fundKey,
        fundamentalsMissing: !fundRaw,
      },
    });
  } catch (err) {
    console.error("equity-screener error", err);
    return json(500, {
      error: "Failed to serve equity screener dataset",
      detail: String(err),
    });
  }
};
