// netlify/functions/stockOfTheDay.js
// Stock of the Day
// - Picks from today's ASX top performers (5-day window) as shown in morning-brief
// - If the frontend passes an explicit ticker/name (from the top performers row),
//   we use that directly.
// - Otherwise we call the morning-brief function and randomly choose one of its
//   `topPerformers`.
// - We cache the generated summary per day + ticker in Upstash so OpenAI is
//   only called once each morning for a given stock.

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

// Small helper: Node18+ global fetch is available on Netlify, but if not, fall back.
const fetchFn = (...args) => (global.fetch ? global.fetch(...args) : fetch(...args));

// -------------------------------
// Redis helpers
// -------------------------------
async function redisGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  try {
    const res = await fetchFn(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
    });
    if (!res.ok) return null;
    const data = await res.json().catch(() => null);
    return data && typeof data.result !== "undefined" ? data.result : null;
  } catch (err) {
    console.warn("stockOfTheDay redisGet error", err && err.message);
    return null;
  }
}

async function redisSetEx(key, value, ttlSeconds) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    await fetchFn(
      `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(
        value
      )}?EX=${ttlSeconds}`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
      }
    );
  } catch (err) {
    console.warn("stockOfTheDay redisSetEx error", err && err.message);
  }
}

// -------------------------------
// Date helpers
// -------------------------------
function getAussieDateString() {
  const now = new Date();
  const aussie = new Date(
    now.toLocaleString("en-US", { timeZone: "Australia/Sydney" })
  );
  const y = aussie.getFullYear();
  const m = String(aussie.getMonth() + 1).padStart(2, "0");
  const d = String(aussie.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

// -------------------------------
// Simple static pool as ultimate fallback
// -------------------------------
const STATIC_POOL = [
  {
    ticker: "CBA",
    name: "Commonwealth Bank of Australia",
    exchange: "ASX",
    blurb:
      "Australia's largest retail bank, with exposure to mortgages, deposits and business lending."
  },
  {
    ticker: "BHP",
    name: "BHP Group",
    exchange: "ASX",
    blurb:
      "A diversified resources major with key operations in iron ore, copper and coal."
  },
  {
    ticker: "CSL",
    name: "CSL",
    exchange: "ASX",
    blurb:
      "A global biotech leader focused on plasma therapies, vaccines and specialty medicines."
  },
  {
    ticker: "WES",
    name: "Wesfarmers",
    exchange: "ASX",
    blurb:
      "A diversified conglomerate with retail, industrials and resources exposure."
  }
];

function pickStockFromStaticPool(seedDate) {
  const idx = Math.abs(hashString(seedDate)) % STATIC_POOL.length;
  return STATIC_POOL[idx];
}

function hashString(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h * 31 + str.charCodeAt(i)) | 0;
  }
  return h;
}

// -------------------------------
// MAIN HANDLER
// -------------------------------
exports.handler = async function (event) {
  try {
    const qs = event.queryStringParameters || {};
    const region = (qs.region || "au").toLowerCase();
    const todayAEST = getAussieDateString();

    // If the frontend passes a top performer, prefer that
    const paramTicker = (qs.ticker || qs.symbol || "").toString().trim().toUpperCase();
    const paramName = (qs.name || "").toString().trim();
    const paramExchange = (qs.exchange || "ASX").toString().trim().toUpperCase();

    let baseStock = null;
    let debugSource = "";

    if (paramTicker && paramName) {
      baseStock = {
        ticker: paramTicker,
        name: paramName,
        exchange: paramExchange || "ASX",
        blurb: `${paramName} (${paramTicker}) has been one of the stronger ASX performers in recent sessions.`
      };
      debugSource = "query-param-top-performer";
    } else {
      // No explicit ticker: call morning-brief and pick from its topPerformers
      try {
        const host =
          event.headers["x-forwarded-host"] ||
          event.headers.host ||
          "localhost:8888";
        const proto = event.headers["x-forwarded-proto"] || "https";
        const baseUrl = process.env.URL || `${proto}://${host}`;
        const mbUrl = `${baseUrl.replace(/\/$/, "")}/.netlify/functions/morning-brief?region=${encodeURIComponent(
          region
        )}`;

        const mbRes = await fetchFn(mbUrl);
        if (mbRes.ok) {
          const mbJson = await mbRes.json().catch(() => null);
          const tps = Array.isArray(mbJson && mbJson.topPerformers)
            ? mbJson.topPerformers
            : [];

          if (tps.length > 0) {
            // deterministic-ish pick based on date so it doesn't change all day
            const idx = Math.abs(hashString(todayAEST)) % tps.length;
            const chosen = tps[idx];

            baseStock = {
              ticker: (chosen.symbol || chosen.code || "").toString().toUpperCase(),
              name:
                chosen.name ||
                chosen.CompanyName ||
                chosen.companyName ||
                "ASX stock",
              exchange: "ASX",
              blurb:
                "This company has been among the top percentage gainers on the ASX over the last five trading days."
            };
            debugSource = "morning-brief-top-performers";
          }
        }
      } catch (err) {
        console.warn("stockOfTheDay: failed to fetch morning-brief", err);
      }

      if (!baseStock) {
        baseStock = pickStockFromStaticPool(todayAEST);
        debugSource = "static-pool-fallback";
      }
    }

    const cacheKey = `stockOfTheDay:${region}:${todayAEST}:${baseStock.ticker}`;
    const cached = await redisGet(cacheKey);

    if (cached) {
      try {
        const parsed = JSON.parse(cached);
        return {
          statusCode: 200,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(parsed)
        };
      } catch (_) {
        // fall through if cache is corrupted
      }
    }

    // If no OpenAI key, build a very simple summary and stop here
    if (!OPENAI_API_KEY) {
      const fallbackSummary = `${baseStock.name} (${baseStock.ticker}) is ${baseStock.blurb} This profile is for general information only and is not a recommendation.`;

      const payload = {
        region,
        ticker: baseStock.ticker,
        name: baseStock.name,
        exchange: baseStock.exchange,
        summary: fallbackSummary,
        generatedAt: new Date().toISOString(),
        _debug: { usedFallback: true, source: debugSource }
      };

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      };
    }

    // -------------------------------
    // Call OpenAI for a short profile
    // -------------------------------
    const prompt = `
You are helping everyday Australian investors understand a single ASX stock.

Write a very short Stock of the Day note for:
- Ticker: ${baseStock.ticker}
- Name: ${baseStock.name}
- Exchange: ${baseStock.exchange}

Guidelines:
- 2–3 sentences max.
- Plain English, no buzzwords.
- Describe what the business does and one or two things investors tend to watch (earnings, dividends, exposure to a theme, etc.).
- Do NOT give direct advice or tell the reader to buy/sell/hold.
- No headings, no markdown, no bullet points.
`;

    const aiRes = await fetchFn("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "You write very short, neutral stock snapshots for Australian investors. You never give explicit investment advice."
          },
          { role: "user", content: prompt }
        ],
        temperature: 0.4,
        max_tokens: 220
      })
    });

    const aiJson = await aiRes.json().catch(() => null);

    let summary =
      (aiJson &&
        aiJson.choices &&
        aiJson.choices[0] &&
        aiJson.choices[0].message &&
        aiJson.choices[0].message.content) ||
      "";

    if (typeof summary !== "string") summary = "";

    summary = summary.replace(/\s+/g, " ").trim();

    if (!summary) {
      summary = `${baseStock.name} (${baseStock.ticker}) is ${baseStock.blurb} This summary is general information only.`;
    }

    const payload = {
      region,
      ticker: baseStock.ticker,
      name: baseStock.name,
      exchange: baseStock.exchange,
      summary,
      generatedAt: new Date().toISOString(),
      _debug: {
        usedFallback: false,
        source: debugSource
      }
    };

    // Cache for ~27 hours so it's always there for the next day as well
    await redisSetEx(cacheKey, JSON.stringify(payload), 27 * 60 * 60);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    };
  } catch (err) {
    console.error("stockOfTheDay handler failed", err);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        region: "au",
        ticker: "N/A",
        name: "Stock of the Day",
        exchange: "ASX",
        summary:
          "Unable to load today’s Stock of the Day. Keep an eye on the ASX top movers and major sectors at the open.",
        generatedAt: new Date().toISOString(),
        _debug: { error: err && err.message }
      })
    };
  }
};
