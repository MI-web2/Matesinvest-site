// netlify/functions/matesMorningNote.js

// -------------------------------
// Environment
// -------------------------------
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

// New: EODHD for US market snapshot
const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN || null;


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

// Helper: fetch JSON with a small wrapper so we can re-use it
async function safeFetchJson(url, timeoutMs = 9000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    const text = await res.text().catch(() => "");
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (e) {
      json = null;
    }
    return { ok: res.ok, status: res.status, json, text };
  } catch (err) {
    return { ok: false, status: 0, json: null, text: String(err && err.message || err) };
  } finally {
    clearTimeout(id);
  }
}


// -------------------------------
// US market snapshot via EODHD
// -------------------------------

// We use liquid US ETFs as proxies for "American markets":
//  - SPY.US  => S&P 500
//  - QQQ.US  => Nasdaq 100
const US_BENCHMARKS = [
  { symbol: "SPY.US", label: "S&P 500 (SPY)" },
  { symbol: "QQQ.US", label: "Nasdaq 100 (QQQ)" }
];

function formatPct(p) {
  if (typeof p !== "number" || !Number.isFinite(p)) return null;
  const v = Number(p.toFixed(2));
  const sign = v > 0 ? "+" : "";
  return `${sign}${v}%`;
}

async function fetchDailyChangePct(symbol, from, to) {
  if (!EODHD_API_TOKEN) return null;

  const url = `https://eodhd.com/api/eod/${encodeURIComponent(symbol)}?api_token=${encodeURIComponent(
    EODHD_API_TOKEN
  )}&period=d&from=${from}&to=${to}&fmt=json`;

  const r = await safeFetchJson(url, 10000);
  if (!r.ok || !Array.isArray(r.json) || r.json.length < 2) return null;

  const arr = r.json
    .slice()
    .sort((a, b) => new Date(a.date) - new Date(b.date));

  const prev = arr[arr.length - 2];
  const last = arr[arr.length - 1];

  if (
    !prev || !last ||
    typeof prev.close !== "number" ||
    typeof last.close !== "number" ||
    prev.close === 0
  ) {
    return null;
  }

  const raw = ((last.close - prev.close) / prev.close) * 100;
  return Number(raw.toFixed(2));
}

// Returns an object like:
// {
//   summaryLine: "S&P 500 (SPY): -1.2%, Nasdaq 100 (QQQ): -1.8%",
//   avgChange: -1.5,
//   components: [{ symbol, label, changePct }]
// }
async function getUSMarketsSnapshot() {
  if (!EODHD_API_TOKEN) return null;

  try {
    const today = new Date();
    const to = today.toISOString().slice(0, 10);
    const fromDate = new Date(today);
    fromDate.setDate(fromDate.getDate() - 7);
    const from = fromDate.toISOString().slice(0, 10);

    const components = [];
    for (const bench of US_BENCHMARKS) {
      try {
        const pct = await fetchDailyChangePct(bench.symbol, from, to);
        if (pct !== null) {
          components.push({
            symbol: bench.symbol,
            label: bench.label,
            changePct: pct
          });
        }
      } catch (err) {
        // swallow per-benchmark errors
        console.warn("matesMorningNote: EODHD fetch failed for", bench.symbol, err && err.message);
      }
    }

    if (!components.length) return null;

    const avgRaw =
      components.reduce((sum, x) => sum + (x.changePct || 0), 0) /
      components.length;
    const avgChange = Number(avgRaw.toFixed(2));

    const parts = components.map((c) => {
      const pctStr = formatPct(c.changePct);
      return `${c.label}: ${pctStr}`;
    });

    return {
      summaryLine: parts.join(", "),
      avgChange,
      components
    };
  } catch (err) {
    console.warn("matesMorningNote: getUSMarketsSnapshot error", err && err.message);
    return null;
  }
}


// -------------------------------
// Prompt builder
// -------------------------------
function buildPrompt(region, articles, usMarkets) {
  const regionLabel =
    region === "us" ? "the US"
    : region === "global" ? "global markets"
    : "Australia and the ASX";

  const topBits = articles
    .slice(0, 6)
    .map((a) => `• ${a.title || ""}${a.source ? ` (${a.source})` : ""}`)
    .join("\n");

  const usLine = usMarkets && usMarkets.summaryLine
    ? `Overnight US market performance (already calculated): ${usMarkets.summaryLine}.`
    : "Overnight US market moves were modest or mixed.";

  return `
You are writing a short pre-market note for everyday investors in ${regionLabel}.
Use plain English, no jargon, and keep it to 2–3 sentences max.

Here is a snapshot of key US market moves from the previous session:
${usLine}

Here are some relevant recent headlines:
${topBits || "• No major headlines available."}

Write a concise "Mates Morning Note" that:
- starts directly with the market context (no greeting, no heading)
- clearly reflects the direction and tone implied by the US market moves (weak session vs strong rally)
- links those moves back to what may influence Australian investors and the ASX today
- uses no markdown (no **bold**, no # headings)
- does NOT repeat the phrase "Mates Morning Note" in the text
- does NOT include a sign-off
- maintains a calm, factual tone typical of morning market commentary.
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
    // Fetch overnight US market snapshot
    // -------------------------------------------------------
    let usMarkets = null;
    try {
      usMarkets = await getUSMarketsSnapshot();
    } catch (err) {
      console.warn("matesMorningNote: US markets snapshot failed", err);
    }


    // -------------------------------------------------------
    // Fallback if no OpenAI key
    // -------------------------------------------------------
    if (!OPENAI_API_KEY) {
      const headlineBit = articles.length
        ? `Markets are watching headlines today, particularly ${articles[0].title || "overnight moves"}.`
        : `Headlines appear quiet so far. Watch major indices, banks and miners as trade begins today.`;

      const usBit = usMarkets && usMarkets.summaryLine
        ? `Overnight US markets moved as follows: ${usMarkets.summaryLine}. `
        : "";

      const note = `${usBit}${headlineBit}`;

      const payload = {
        region,
        note,
        generatedAt: new Date().toISOString(),
        _debug: {
          usedFallback: true,
          articleCount: articles.length,
          usMarkets
        }
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
    const prompt = buildPrompt(region, articles, usMarkets);

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
        articleCount: articles.length,
        usMarkets
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
