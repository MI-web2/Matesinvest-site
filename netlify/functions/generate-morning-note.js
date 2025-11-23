// netlify/functions/generate-morning-note.js
//
// Scheduled function: generates the "Mates Morning Note" once each day
// around 6–7am AEST/AEDT and stores it in Upstash Redis.
//
// Key written:
//   matesMorningNote:au:YYYY-MM-DD
//
// Env required:
//   OPENAI_API_KEY
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//   EODHD_API_TOKEN
//   URL  (Netlify injects the site URL in production)

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN || null;

// -------------------------------
// Redis helpers
// -------------------------------
async function redisGet(key) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;

  const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });

  if (!res.ok) return null;

  const data = await res.json().catch(() => null);
  return data && typeof data.result === "string" ? data.result : null;
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
// Time helpers
// -------------------------------

// YYYY-MM-DD in Australia/Sydney
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

// ISO string for 6:00am Australia/Sydney *today*
function getAussieSixAmISO() {
  const now = new Date();
  const aussie = new Date(
    now.toLocaleString("en-US", { timeZone: "Australia/Sydney" })
  );
  aussie.setHours(6, 0, 0, 0);
  return aussie.toISOString();
}

// -------------------------------
// Generic fetch helper
// -------------------------------
async function safeFetchJson(url, timeoutMs = 9000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    const text = await res.text().catch(() => "");
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return { ok: res.ok, status: res.status, json, text };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      json: null,
      text: String((err && err.message) || err),
    };
  } finally {
    clearTimeout(id);
  }
}

// -------------------------------
// US market snapshot via EODHD
// -------------------------------

const US_BENCHMARKS = [
  { symbol: "SPY.US", label: "S&P 500 (SPY)" },
  { symbol: "QQQ.US", label: "Nasdaq 100 (QQQ)" },
];

function formatPct(p) {
  if (typeof p !== "number" || !Number.isFinite(p)) return null;
  const v = Number(p.toFixed(2));
  const sign = v > 0 ? "+" : "";
  return `${sign}${v}%`;
}

async function fetchDailyChangePct(symbol, from, to) {
  if (!EODHD_API_TOKEN) return null;

  const url = `https://eodhd.com/api/eod/${encodeURIComponent(
    symbol
  )}?api_token=${encodeURIComponent(
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
    !prev ||
    !last ||
    typeof prev.close !== "number" ||
    typeof last.close !== "number" ||
    prev.close === 0
  ) {
    return null;
  }

  const raw = ((last.close - prev.close) / prev.close) * 100;
  return Number(raw.toFixed(2));
}

// Returns { summaryLine, avgChange, components } or null
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
            changePct: pct,
          });
        }
      } catch (err) {
        console.warn(
          "generate-morning-note: EODHD fetch failed for",
          bench.symbol,
          err && err.message
        );
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
      components,
    };
  } catch (err) {
    console.warn(
      "generate-morning-note: getUSMarketsSnapshot error",
      err && err.message
    );
    return null;
  }
}

// -------------------------------
// Prompt builder
// -------------------------------
function buildPrompt(region, articles, usMarkets) {
  const regionLabel =
    region === "us"
      ? "the US"
      : region === "global"
      ? "global markets"
      : "Australia and the ASX";

  const topBits = articles
    .slice(0, 6)
    .map((a) => `• ${a.title || ""}${a.source ? ` (${a.source})` : ""}`)
    .join("\n");

  const usLine =
    usMarkets && usMarkets.summaryLine
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
// MAIN HANDLER (scheduled)
// -------------------------------
exports.handler = async function () {
  try {
    const region = "au";
    const todayAEST = getAussieDateString();
    const cacheKey = `matesMorningNote:${region}:${todayAEST}`;

    // Idempotency: if we've already generated today's note, don't regenerate.
    const existing = await redisGet(cacheKey);
    if (existing) {
      console.log("generate-morning-note: cache already exists", cacheKey);
      return {
        statusCode: 200,
        body: JSON.stringify({
          ok: true,
          skipped: true,
          reason: "already-exists",
          cacheKey,
        }),
      };
    }

    if (!OPENAI_API_KEY) {
      console.error("generate-morning-note: missing OPENAI_API_KEY");
    }

    // -------- 1) Fetch AU news via your own newsFeed function --------
    const baseUrl =
      (process.env.URL && process.env.URL.replace(/\/$/, "")) ||
      "http://localhost:8888";
    const newsUrl = `${baseUrl}/.netlify/functions/newsFeed?region=au`;

    let articles = [];
    try {
      const newsRes = await fetch(newsUrl);
      if (newsRes.ok) {
        const newsData = await newsRes.json().catch(() => null);
        if (newsData && Array.isArray(newsData.articles)) {
          articles = newsData.articles;
        }
      } else {
        console.warn(
          "generate-morning-note: newsFeed HTTP",
          newsRes.status
        );
      }
    } catch (err) {
      console.warn("generate-morning-note: newsFeed fetch failed", err);
    }

    // -------- 2) US market snapshot via EODHD --------
    let usMarkets = null;
    try {
      usMarkets = await getUSMarketsSnapshot();
    } catch (err) {
      console.warn("generate-morning-note: US markets snapshot failed", err);
    }

    // -------- 3) Build note (OpenAI or fallback) --------
    let note;
    let usedFallback = false;

    if (!OPENAI_API_KEY) {
      usedFallback = true;
      const headlineBit = articles.length
        ? `Markets are watching headlines today, particularly ${articles[0].title ||
            "overnight moves"}.`
        : `Headlines appear quiet so far. Watch major indices, banks and miners as trade begins today.`;

      const usBit =
        usMarkets && usMarkets.summaryLine
          ? `Overnight US markets moved as follows: ${usMarkets.summaryLine}. `
          : "";

      note = `${usBit}${headlineBit}`;
    } else {
      const prompt = buildPrompt(region, articles, usMarkets);

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
                "You write very short, calm pre-market summaries for Australian investors.",
            },
            { role: "user", content: prompt },
          ],
          temperature: 0.4,
          max_tokens: 200,
        }),
      });

      const aiJson = await aiRes.json().catch(() => ({}));
      note =
        aiJson.choices?.[0]?.message?.content?.trim() ||
        "Markets are open with mixed signals across indices and commodities today.";
    }

    // Clean formatting
    note = note.replace(/\*\*/g, "");
    note = note.replace(/^#+\s*/g, "");
    note = note.replace(/^Mates Morning Note[:\- ]*/i, "");
    note = note.replace(/^Good\s+morning[,!.]?\s*/i, "");
    note = note.replace(/Take care\.?$/i, "");
    note = note.trim();

    // -------- 4) Build payload & write to Upstash --------
    const generatedAt = getAussieSixAmISO(); // pretend we generated exactly at 6:00am local

    const payload = {
      region,
      note,
      generatedAt,
      _debug: {
        usedFallback,
        articleCount: articles.length,
        usMarkets,
        source: "generate-morning-note",
      },
    };

    // keep around for a few days
    await redisSetEx(cacheKey, JSON.stringify(payload), 72 * 60 * 60);

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        cacheKey,
        generatedAt,
      }),
    };
  } catch (err) {
    console.error(
      "generate-morning-note handler failed",
      err && (err.stack || err.message || err)
    );
    return {
      statusCode: 500,
      body: JSON.stringify({
        ok: false,
        error: (err && err.message) || String(err),
      }),
    };
  }
};
