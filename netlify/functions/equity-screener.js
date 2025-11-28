// netlify/functions/equity-screener.js
//
// Returns a cleaned list of equities + fundamentals for the MatesInvest screener.
//
// Features:
//  - Reads tickers from asx200.txt (CSV or newline-separated)
//  - Tries multiple exchange suffixes (AU, AX, ASX – via TRY_SUFFIXES env or default)
//  - Optional debug: ?code=BHP to fetch a single ticker
//  - Returns some debug info (tickersCount, failures) to help diagnose issues
//
// Env:
//   EODHD_API_TOKEN          (required)
//   TRY_SUFFIXES             (optional, e.g. "AU,AX,ASX")
//
// Files:
//   netlify/functions/asx200.txt   (tickers CSV or newline-separated)

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;
const UNIVERSE_FILE = path.join(__dirname, "asx200.txt");

// Matches your other code's pattern (instrument-details TRY_SUFFIXES)
const TRY_SUFFIXES = (process.env.TRY_SUFFIXES || "AU,AX,ASX")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

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

    if (!EODHD_API_TOKEN) {
      return json(
        500,
        {
          error: "EODHD_API_TOKEN is not set in Netlify environment",
        }
      );
    }

    const qs = event.queryStringParameters || {};
    const singleCode = qs.code ? String(qs.code).trim().toUpperCase() : null;

    let tickers = [];

    if (singleCode) {
      // Debug mode: single ticker, ignore universe file
      tickers = [singleCode];
    } else {
      // Normal mode: load ASX universe from file
      const raw = fs.readFileSync(UNIVERSE_FILE, "utf8");

      // Support both CSV and newline formats:
      // "BHP,CSL,FMG" or "BHP\nCSL\nFMG"
      tickers = raw
        .split(/[\s,]+/) // split on comma OR whitespace
        .map((t) => t.trim().toUpperCase())
        .filter(Boolean);

      // Deduplicate, just in case
      tickers = Array.from(new Set(tickers));
    }

    const items = [];
    const failures = [];

    // Safe nested getter
    const get = (obj, path, fallback = null) =>
      path.split(".").reduce((acc, key) => {
        if (acc && Object.prototype.hasOwnProperty.call(acc, key)) {
          return acc[key];
        }
        return fallback;
      }, obj);

    // Try multiple suffixes for each code until one works
    async function fetchFundamentals(code) {
      let lastStatus = null;
      let lastErrorBody = null;

      for (const suffix of TRY_SUFFIXES) {
        const symbol = `${code}.${suffix}`;
        const url = `https://eodhd.com/api/fundamentals/${symbol}?api_token=${EODHD_API_TOKEN}&fmt=json`;

        try {
          const res = await fetch(url);

          lastStatus = res.status;

          if (res.ok) {
            const data = await res.json();
            return { data, suffixUsed: suffix };
          }

          // If it's a 404 for this suffix, try the next suffix.
          // For other errors (401/403/500), record and break – no point retrying.
          if (res.status !== 404) {
            lastErrorBody = await res.text().catch(() => null);
            break;
          }
        } catch (err) {
          lastErrorBody = String(err);
          break;
        }
      }

      // No suffix worked
      failures.push({
        code,
        status: lastStatus,
        detail: lastErrorBody,
      });
      return null;
    }

    // Sequential for now – we’ll move to a cron + cache later
    for (const code of tickers) {
      const result = await fetchFundamentals(code);
      if (!result) continue;

      const { data } = result;

      const item = {
        code,
        name: get(data, "General.Name") || code,
        sector: get(data, "General.Sector") || "Unknown",
        industry: get(data, "General.Industry") || "Unknown",
        price: Number(get(data, "Highlights.LatestClose") ?? NaN),
        marketCap: Number(get(data, "Highlights.MarketCapitalization") ?? NaN),
        pe: Number(get(data, "ValuationRatios.PERatio") ?? NaN),
        dividendYield: Number(
          get(
            data,
            "SplitsDividends.ForwardAnnualDividendYield",
            get(data, "Highlights.DividendYield") ?? NaN
          )
        ),
      };

      items.push(item);
    }

    return json(200, {
      generatedAt: new Date().toISOString(),
      count: items.length,
      tickersCount: tickers.length,
      items,
      // slice failures so the response body doesn't explode
      debug: {
        suffixesTried: TRY_SUFFIXES,
        failures: failures.slice(0, 10),
      },
    });
  } catch (err) {
    console.error("equity-screener error", err);
    return json(500, {
      error: "Failed to build equity screener dataset",
      detail: String(err),
    });
  }
};

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
