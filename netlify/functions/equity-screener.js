// netlify/functions/equity-screener.js
//
// Returns a cleaned list of equities + fundamentals for use in the MatesInvest screener.
//
// For now, this:
//   - reads a static universe file (e.g. asx200.txt) next to this file
//   - for each ticker, calls EODHD Fundamentals API
//   - extracts a handful of fields (code, name, sector, price, marketCap, pe, dividendYield)
//   - returns JSON: { items: [...] }
//
// NOTE: For production you'll want to:
//   - cache results in Upstash (e.g. once a day)
//   - and have this function just read from cache.
//   But this version is fine to prove out the UI.
//
// Env:
//   EODHD_API_TOKEN

const fs = require("fs");
const path = require("path");

const fetch = (...args) => global.fetch(...args);

const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;
if (!EODHD_API_TOKEN) {
  console.warn("Warning: EODHD_API_TOKEN missing");
}

const UNIVERSE_FILE = path.join(__dirname, "asx200.txt");
const EXCHANGE_SUFFIX = "ASX"; // adjust if you use AX/ASX/etc.

exports.handler = async function (event) {
  try {
    // Simple CORS for your frontend
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

    const tickersRaw = fs.readFileSync(UNIVERSE_FILE, "utf8");
    const tickers = tickersRaw
      .split(/\r?\n/)
      .map((t) => t.trim())
      .filter(Boolean);

    const items = [];

    // Helper to safely get nested properties without blowing up
    const get = (obj, path, fallback = null) => {
      return path.split(".").reduce((acc, key) => {
        if (acc && Object.prototype.hasOwnProperty.call(acc, key)) {
          return acc[key];
        }
        return fallback;
      }, obj);
    };

    // Fetch fundamentals for each ticker (sequential for simplicity)
    for (const code of tickers) {
      try {
        const symbol = `${code}.${EXCHANGE_SUFFIX}`;
        const url = `https://eodhd.com/api/fundamentals/${symbol}?api_token=${EODHD_API_TOKEN}&fmt=json`;

        const res = await fetch(url, { timeout: 10000 });
        if (!res.ok) {
          console.warn("EODHD fundamentals error", code, res.status);
          continue;
        }

        const data = await res.json();

        // Map EODHD structure → our simplified shape
        // These paths are based on EODHD docs structure:
        //   General, Highlights, Valuation, SplitsDividends, etc.
        const item = {
          code,
          name: get(data, "General.Name") || code,
          sector: get(data, "General.Sector") || "Unknown",
          industry: get(data, "General.Industry") || "Unknown",
          // Some fundamentals
          price: Number(get(data, "Highlights.LatestClose") ?? NaN),
          marketCap: Number(
            get(data, "Highlights.MarketCapitalization") ?? NaN
          ),
          pe: Number(get(data, "ValuationRatios.PERatio") ?? NaN),
          dividendYield: Number(
            get(data, "SplitsDividends.ForwardAnnualDividendYield") ?? NaN
          ),
        };

        items.push(item);
      } catch (err) {
        console.error("Error fetching fundamentals for", code, err);
      }
    }

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({
        generatedAt: new Date().toISOString(),
        count: items.length,
        items,
      }),
    };
  } catch (err) {
    console.error("equity-screener error", err);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({
        error: "Failed to build equity screener dataset",
      }),
    };
  }
};
