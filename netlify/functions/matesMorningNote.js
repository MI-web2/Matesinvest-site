// netlify/functions/matesMorningNote.js

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

// Get the YYYY-MM-DD date string in AEST/AEDT
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

// Determine our base URL (prod or local dev)
function getBaseUrl(event) {
  const envUrl = process.env.URL;
  if (envUrl) return envUrl.replace(/\/$/, "");

  const host = event.headers["x-forwarded-host"] || event.headers.host || "localhost:8888";
  const proto = event.headers["x-forwarded-proto"] || "http";
  return `${proto}://${host}`;
}


// -------------------------------
// Prompt builder
// -------------------------------
function buildPrompt(region, articles) {
  const regionLabel =
    region === "us" ? "the US"
    : region === "global" ? "global markets"
    : "Australia and the ASX";

  const topBits = articles
    .slice(0, 6)
    .map((a) => `• ${a.title || ""}${a.source ? ` (${a.source})` : ""}`)
    .join("\n");

  return `
You are writing a short pre-market note for everyday investors in ${regionLabel}.
Use plain English, no jargon, and keep it to 2–3 sentences max.

Here are some relevant recent headlines:

${topBits || "• No major headlines available."}

Write a concise "Mates Morning Note" that:
- starts directly with the market context (no greeting, no heading)
- uses no markdown (no **bold**, no # headings)
- does NOT repeat the phrase "Mates Morning Note" in the text
- does NOT include a sign-off
- maintains a calm, factual tone typical of morning market commentary
- focuses on what may influence Australian investors if applicable.
`;
}


// -------------------------------
// MAIN HANDLER
// -------------------------------
exports.handler = async function (event, context) {
  try {
    const region =
      (event.queryStringParameters && event.queryStringParameters.region) || "au";

    // -------------------------------------------------------
    // DAILY CACHE LOGIC (per region)
    // -------------------------------------------------------
    const todayAEST = getAussieDateString();
    const cacheKey = `matesMorningNote:${region}:${todayAEST}`;

    const cached = await redisGet(cacheKey);

    if (cached) {
      const parsed = JSON.parse(cached);
      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed)
      };
    }


    // -------------------------------------------------------
    // Fetch latest news from your own newsFeed function
    // -------------------------------------------------------
    const baseUrl = getBaseUrl(event);
    const newsUrl = `${baseUrl}/.netlify/functions/newsFeed?region=${region}`;

    let articles = [];
    try {
      const newsRes = await fetch(newsUrl);
      if (newsRes.ok) {
        const newsData = await newsRes.json();
        articles = newsData.articles || [];
      }
    } catch (err) {
      console.warn("matesMorningNote: newsFeed fetch failed", err);
    }


    // -------------------------------------------------------
    // Fallback if no OpenAI key
    // -------------------------------------------------------
    if (!OPENAI_API_KEY) {
      const note = articles.length
        ? `Markets are watching headlines today, particularly ${articles[0].title || "overnight moves"}. Keep an eye on how this flows through to key Australian sectors.`
        : `Headlines appear quiet so far. Watch major indices, banks and miners as trade begins today.`;

      const payload = {
        region,
        note,
        generatedAt: new Date().toISOString(),
        _debug: { usedFallback: true, articleCount: articles.length }
      };

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      };
    }


    // -------------------------------------------------------
    // Generate note using OpenAI
    // -------------------------------------------------------
    const prompt = buildPrompt(region, articles);

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You write very short, calm pre-market summaries for Australian investors." },
          { role: "user", content: prompt }
        ],
        temperature: 0.4,
        max_tokens: 200
      })
    });

    const aiJson = await aiRes.json();

    let note =
      aiJson.choices?.[0]?.message?.content?.trim() ||
      "Markets are open with mixed signals across indices and commodities today.";


    // -------------------------------------------------------
    // Clean up formatting (safe & simple)
    // -------------------------------------------------------
    note = note.replace(/\*\*/g, "");
    note = note.replace(/^#+\s*/g, "");
    note = note.replace(/^Mates Morning Note[:\- ]*/i, "");
    note = note.replace(/^Good\s+morning[,!.]?\s*/i, "");
    note = note.replace(/Take care\.?$/i, "");
    note = note.trim();


    // -------------------------------------------------------
    // Build final payload + store in cache
    // -------------------------------------------------------
    const payload = {
      region,
      note,
      generatedAt: new Date().toISOString(),
      _debug: {
        usedFallback: false,
        articleCount: articles.length
      }
    };

    // Cache for 26 hours (safe across DST)
    redisSetEx(cacheKey, JSON.stringify(payload), 26 * 60 * 60).catch((err) =>
      console.warn("matesMorningNote: cache write failed", err)
    );


    // -------------------------------------------------------
    // Return final result
    // -------------------------------------------------------
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    };


  } catch (err) {
    console.error("matesMorningNote handler failed", err);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        region: "unknown",
        note: "Unable to load today’s morning note. Watch headlines and major sectors as markets open.",
        generatedAt: new Date().toISOString(),
        _debug: { error: err.message || String(err) }
      })
    };
  }
};
