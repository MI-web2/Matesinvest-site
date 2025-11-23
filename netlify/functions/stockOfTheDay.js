// netlify/functions/stockOfTheDay.js

// -------------------------------
// Environment
// -------------------------------
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;


// -------------------------------
// Redis helpers
// -------------------------------
async function redisGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;

  const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
  });

  if (!res.ok) return null;

  const data = await res.json();
  return data.result || null;
}

async function redisSetEx(key, value, ttlSeconds) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;

  await fetch(
    `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(
      value
    )}?EX=${ttlSeconds}`,
    { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
  );
}


// -------------------------------
// Utilities
// -------------------------------

// Get YYYY-MM-DD in Australia/Sydney
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

// deterministic pick from date
function pickIndexForDate(dateStr, max) {
  const num = parseInt(dateStr.replace(/-/g, ""), 10) || 1;
  return num % max;
}


// -------------------------------
// Static ASX stock pool (fallback)
// -------------------------------
const STOCK_POOL = [
  {
    ticker: "BHP",
    name: "BHP Group",
    exchange: "ASX",
    blurb:
      "large-cap diversified miner with major exposure to iron ore, copper and coal, heavily linked to global growth and Chinese demand."
  },
  {
    ticker: "CBA",
    name: "Commonwealth Bank of Australia",
    exchange: "ASX",
    blurb:
      "Australia's largest retail bank, leveraged to the domestic housing market, interest-rate cycle and household spending."
  },
  {
    ticker: "CSL",
    name: "CSL Limited",
    exchange: "ASX",
    blurb:
      "global biotech focused on plasma therapies, vaccines and specialty medicines, with long-term R&D and currency exposure."
  },
  {
    ticker: "FMG",
    name: "Fortescue",
    exchange: "ASX",
    blurb:
      "pure-play iron ore producer with strong operating leverage to iron ore prices and an emerging green energy arm."
  },
  {
    ticker: "PLS",
    name: "Pilbara Minerals",
    exchange: "ASX",
    blurb:
      "lithium producer operating the Pilgangoora project in WA, exposed to lithium concentrate prices and EV demand cycles."
  },
  {
    ticker: "AKE",
    name: "Allkem",
    exchange: "ASX",
    blurb:
      "lithium chemicals producer with operations in Argentina and Canada, geared to lithium carbonate prices and project execution."
  },
  {
    ticker: "MIN",
    name: "Mineral Resources",
    exchange: "ASX",
    blurb:
      "diversified mining and mining services group with exposure to iron ore, lithium and contract crushing operations."
  },
  {
    ticker: "MQG",
    name: "Macquarie Group",
    exchange: "ASX",
    blurb:
      "global investment bank and asset manager, exposed to deal flow, asset management performance and infrastructure investment."
  },
  {
    ticker: "WOW",
    name: "Woolworths Group",
    exchange: "ASX",
    blurb:
      "supermarket and retail group, driven by consumer spending, competition in grocery and margins in food and liquor."
  },
  {
    ticker: "WES",
    name: "Wesfarmers",
    exchange: "ASX",
    blurb:
      "conglomerate with Bunnings, Kmart/Target and chemicals/fertilisers, leveraged to Australian retail and industrial activity."
  }
];

function pickStockForTodayFromPool(dateStr) {
  const index = pickIndexForDate(dateStr, STOCK_POOL.length);
  return STOCK_POOL[index];
}


// -------------------------------
// Prompt builder
// -------------------------------
function buildPrompt(stock) {
  return `
You are writing a very short, plain-English snapshot of one ASX company for everyday investors.

Stock:
- Ticker: ${stock.ticker}
- Name: ${stock.name}
- Exchange: ${stock.exchange}
- Description: ${stock.blurb}

Write 2–3 sentences explaining:
- what this business does in simple terms
- the main drivers that can help or hurt the share price over time
- one key risk investors should be aware of.

Do NOT give explicit recommendations (no "buy", "sell", "hold").
Do NOT mention specific valuation multiples or price targets.
Return your answer strictly as JSON like:
{"summary":"..."}
with no extra text, no markdown, no additional fields.
`;
}


// -------------------------------
// -------------------------------
// MAIN HANDLER (Updated)
// -------------------------------
exports.handler = async function (event) {
  try {
    const qs = event.queryStringParameters || {};
    const region = (qs.region || "au").toLowerCase();

    // Frontend-selected top performer
    const paramTicker = (qs.ticker || qs.symbol || "")
      .toString()
      .trim()
      .toUpperCase();

    const paramName = (qs.name || "").toString().trim();
    const paramExchange = (qs.exchange || "ASX")
      .toString()
      .trim()
      .toUpperCase();

    const todayAEST = getAussieDateString();

    // Cache key: different per region + day + ticker (so each chosen stock is cached)
    const cacheKeyBase = paramTicker || "pool";
    const cacheKey = `stockOfTheDay:${region}:${todayAEST}:${cacheKeyBase}`;

    // -------------------------------------------------------
    // 1) Use cached version if available
    // -------------------------------------------------------
    const cached = await redisGet(cacheKey);
    if (cached) {
      const parsed = JSON.parse(cached);
      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      };
    }

    // -------------------------------------------------------
    // 2) Build "selected stock" object
    // -------------------------------------------------------
    let stock;
    let source = "pool";

    if (paramTicker && paramName) {
      // Provided by frontend from top performers
      stock = {
        ticker: paramTicker,
        name: paramName,
        exchange: paramExchange,
        blurb: `${paramName} (${paramTicker}) is an ASX-listed company recently among the market’s top performers.`,
      };
      source = "top-performer";
    } else {
      // Fallback to deterministic pool selection
      stock = pickStockForTodayFromPool(todayAEST);
      source = "fallback-pool";
    }

    // -------------------------------------------------------
    // 3) Fallback summary if no OpenAI key
    // -------------------------------------------------------
    if (!OPENAI_API_KEY) {
      const fallbackSummary = `${stock.name} (${stock.ticker}) is ${stock.blurb} This is a general description only.`;

      const payload = {
        region,
        ticker: stock.ticker,
        name: stock.name,
        exchange: stock.exchange,
        summary: fallbackSummary,
        generatedAt: new Date().toISOString(),
        _debug: { usedFallback: true, source },
      };

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      };
    }

    // -------------------------------------------------------
    // 4) OpenAI Summary
    // -------------------------------------------------------
    const prompt = buildPromptForStock(stock);

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "You write short, clear company summaries for everyday Australian investors.",
          },
          { role: "user", content: prompt },
        ],
        temperature: 0.4,
        max_tokens: 180,
      }),
    });

    const aiJson = await aiRes.json();
    let summary =
      aiJson.choices?.[0]?.message?.content?.trim() ||
      `${stock.name} (${stock.ticker}) is an actively watched ASX company today.`;

    // Clean unwanted markdown
    summary = summary.replace(/^\*+|\*+$/g, "");
    summary = summary.trim();

    // -------------------------------------------------------
    // 5) Build & store payload
    // -------------------------------------------------------
    const payload = {
      region,
      ticker: stock.ticker,
      name: stock.name,
      exchange: stock.exchange,
      summary,
      generatedAt: new Date().toISOString(),
      _debug: { usedFallback: false, source },
    };

    // Cache for the full day (26 hours)
    await redisSetEx(cacheKey, JSON.stringify(payload), 26 * 60 * 60);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    };
  } catch (err) {
    console.error("stockOfTheDay ERROR:", err);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ticker: "N/A",
        name: "Unknown",
        exchange: "ASX",
        summary:
          "Unable to load today's Stock of the Day. Market data is temporarily unavailable.",
        generatedAt: new Date().toISOString(),
        _debug: { error: err.message || String(err) },
      }),
    };
  }
};


    const aiJson = await aiRes.json();
    let raw = aiJson.choices?.[0]?.message?.content?.trim() || "";

    let summary = raw;

    // Try to parse JSON as instructed
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed.summary === "string") {
        summary = parsed.summary;
      }
    } catch (err) {
      // fall back to raw text if JSON parsing fails
    }

    summary = summary.replace(/\s+/g, " ").trim();

    if (!summary) {
      summary = `${stock.name} (${stock.ticker}) is ${stock.blurb} This summary is general information only.`;
    }

    const payload = {
      region,
      ticker: stock.ticker,
      name: stock.name,
      exchange: stock.exchange,
      summary,
      generatedAt: new Date().toISOString(),
      _debug: {
        usedFallback: false,
        source
      }
    };

    // Cache for ~26 hours
    redisSetEx(cacheKey, JSON.stringify(payload), 26 * 60 * 60).catch((e) =>
      console.warn("stockOfTheDay: cache write failed", e)
    );

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
        region: "unknown",
        ticker: null,
        name: null,
        exchange: "ASX",
        summary:
          "Unable to load today’s Stock of the Day. This snapshot normally gives a quick overview of one ASX name to watch.",
        generatedAt: new Date().toISOString(),
        _debug: { error: err.message || String(err) }
      })
    };
  }
};
