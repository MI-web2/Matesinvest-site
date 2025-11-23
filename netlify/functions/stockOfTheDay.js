// netlify/functions/stockOfTheDay.js
// Stock of the Day: picks a stock (from query params, Upstash topPerformers, or a fallback pool),
// asks OpenAI for a short JSON summary, caches the result in Upstash for ~26 hours, and returns it.

const fetch = (...args) => global.fetch(...args);

// Env
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

// -------------------------------
// Redis helpers
// -------------------------------
async function redisGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  try {
    const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    if (!res.ok) return null;
    const data = await res.json().catch(() => null);
    return (data && typeof data.result !== "undefined") ? data.result : null;
  } catch (e) {
    console.warn("redisGet error", e && e.message);
    return null;
  }
}

async function redisSetEx(key, value, ttlSeconds) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    await fetch(
      `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(value)}?EX=${ttlSeconds}`,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
    );
  } catch (e) {
    console.warn("redisSetEx error", e && e.message);
  }
}

// -------------------------------
// Utilities
// -------------------------------
function getAussieDateString() {
  const now = new Date();
  const aussie = new Date(now.toLocaleString("en-US", { timeZone: "Australia/Sydney" }));
  const y = aussie.getFullYear();
  const m = String(aussie.getMonth() + 1).padStart(2, "0");
  const d = String(aussie.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}
function pickIndexForDate(dateStr, max) {
  const num = parseInt(dateStr.replace(/-/g, ""), 10) || 1;
  return num % max;
}

const STOCK_POOL = [
  { ticker: "BHP", name: "BHP Group", exchange: "ASX", blurb: "large-cap diversified miner..." },
  { ticker: "CBA", name: "Commonwealth Bank of Australia", exchange: "ASX", blurb: "Australia's largest retail bank..." },
  { ticker: "CSL", name: "CSL Limited", exchange: "ASX", blurb: "global biotech focused on plasma therapies..." },
  { ticker: "FMG", name: "Fortescue", exchange: "ASX", blurb: "pure-play iron ore producer..." },
  { ticker: "PLS", name: "Pilbara Minerals", exchange: "ASX", blurb: "lithium producer..." },
  { ticker: "AKE", name: "Allkem", exchange: "ASX", blurb: "lithium chemicals producer..." },
  { ticker: "MIN", name: "Mineral Resources", exchange: "ASX", blurb: "diversified mining and services..." },
  { ticker: "MQG", name: "Macquarie Group", exchange: "ASX", blurb: "global investment bank..." },
  { ticker: "WOW", name: "Woolworths Group", exchange: "ASX", blurb: "supermarket and retail group..." },
  { ticker: "WES", name: "Wesfarmers", exchange: "ASX", blurb: "conglomerate with Bunnings, Kmart..." }
];

function pickStockForTodayFromPool(dateStr) {
  const index = pickIndexForDate(dateStr, STOCK_POOL.length);
  return STOCK_POOL[index];
}

function buildPromptForStock(stock) {
  return `You are writing a very short, plain-English snapshot of one ASX company for everyday investors.

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
Return your answer strictly as JSON ONLY, for example:
{"summary":"..."} 
Do not include any other keys or surrounding text or markdown.`;
}

// -------------------------------
// MAIN HANDLER
// -------------------------------
exports.handler = async function (event) {
  try {
    const qs = (event && event.queryStringParameters) ? event.queryStringParameters : {};
    const region = (qs.region || "au").toLowerCase();

    // Accept either 'symbol' or 'ticker' for the ticker param
    let paramTicker = (qs.ticker || qs.symbol || "").toString().trim().toUpperCase();
    const paramName = (qs.name || "").toString().trim();
    const paramExchange = (qs.exchange || "ASX").toString().trim().toUpperCase();

    const todayAEST = getAussieDateString();
    const cacheKeyBase = paramTicker || "pool";
    const cacheKey = `stockOfTheDay:${region}:${todayAEST}:${cacheKeyBase}`;

    // 1) Return cached item if present
    try {
      const cached = await redisGet(cacheKey);
      if (cached) {
        const parsed = (typeof cached === "string") ? JSON.parse(cached) : cached;
        return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(parsed) };
      }
    } catch (e) {
      console.warn("stockOfTheDay: cache read failed", e && e.message);
    }

    // 2) Choose stock
    let stock;
    let source = "pool";

    // If no explicit ticker provided, try Upstash topPerformers:latest
    if (!paramTicker) {
      try {
        const tpRaw = await redisGet("topPerformers:latest");
        if (tpRaw) {
          const arr = (typeof tpRaw === "string") ? JSON.parse(tpRaw) : tpRaw;
          if (Array.isArray(arr) && arr.length) {
            // Prefer the top item; fallback you can change to random pick
            const cand = arr[0];
            const sym = (cand && (cand.symbol || cand.ticker)) ? String(cand.symbol || cand.ticker).replace(/\.AU$/i,"") : null;
            if (sym) {
              stock = { ticker: sym.toUpperCase(), name: cand.name || "", exchange: "ASX", blurb: cand.name ? `Top performer: ${cand.name}` : "Top performer from scan" };
              source = "top-performer-upstash";
              paramTicker = stock.ticker;
            }
          }
        }
      } catch (e) {
        console.warn("stockOfTheDay: topPerformers read failed", e && e.message);
      }
    }

    if (!stock) {
      if (paramTicker && paramName) {
        stock = { ticker: paramTicker, name: paramName, exchange: paramExchange, blurb: `${paramName} (${paramTicker}) is an ASX-listed company recently among the market’s top performers.` };
        source = "top-performer-param";
      } else if (paramTicker && !paramName) {
        // only ticker provided — best-effort name blank
        stock = { ticker: paramTicker, name: "", exchange: paramExchange, blurb: `${paramTicker} is an ASX-listed company.` };
        source = "ticker-only";
      } else {
        stock = pickStockForTodayFromPool(todayAEST);
        source = "fallback-pool";
      }
    }

    // 3) If no OpenAI key, return a simple fallback summary
    if (!OPENAI_API_KEY) {
      const fallbackSummary = `${stock.name || stock.ticker} (${stock.ticker}) is ${stock.blurb} This is a general description only.`;
      const payload = { region, ticker: stock.ticker, name: stock.name, exchange: stock.exchange, summary: fallbackSummary, generatedAt: new Date().toISOString(), _debug: { usedFallback: true, source } };
      // cache (best-effort)
      try { await redisSetEx(cacheKey, JSON.stringify(payload), 26 * 60 * 60); } catch (_) {}
      return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) };
    }

    // 4) Call OpenAI
    const prompt = buildPromptForStock(stock);

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You write short, clear company summaries for everyday Australian investors." },
          { role: "user", content: prompt }
        ],
        temperature: 0.3,
        max_tokens: 220
      })
    });

    const aiJson = await aiRes.json().catch(() => null);
    let raw = aiJson?.choices?.[0]?.message?.content?.trim?.() || "";

    // Try to parse JSON returned by the assistant
    let summary = "";
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed.summary === "string") summary = parsed.summary.trim();
      } catch (e) {
        // not valid JSON — fall back to using the raw text
        summary = raw.replace(/\n+/g, " ").trim();
      }
    }

    if (!summary) {
      summary = `${stock.name || stock.ticker} (${stock.ticker}) is ${stock.blurb} This summary is general information only.`;
    }

    const payload = {
      region,
      ticker: stock.ticker,
      name: stock.name,
      exchange: stock.exchange,
      summary,
      generatedAt: new Date().toISOString(),
      _debug: { usedFallback: false, source, aiRaw: raw ? raw.slice(0, 800) : null }
    };

    // Cache (best-effort)
    try {
      await redisSetEx(cacheKey, JSON.stringify(payload), 26 * 60 * 60);
    } catch (e) {
      console.warn("stockOfTheDay: cache write failed", e && e.message);
    }

    return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) };

  } catch (err) {
    console.error("stockOfTheDay handler failed", err && (err.stack || err.message || err));
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        region: "unknown",
        ticker: null,
        name: null,
        exchange: "ASX",
        summary: "Unable to load today’s Stock of the Day. This snapshot normally gives a quick overview of one ASX name to watch.",
        generatedAt: new Date().toISOString(),
        _debug: { error: err && (err.message || String(err)) }
      })
    };
  }
};