// netlify/functions/instrument-ai-summary.js
//
// Usage:
//   /.netlify/functions/instrument-ai-summary?type=equity&code=BHP
//
// Env vars needed:
//   OPENAI_API_KEY
//   MARKETAUX_API_TOKEN
//   UPSTASH_REDIS_REST_URL (optional but recommended)
//   UPSTASH_REDIS_REST_TOKEN (optional)

const fetch = (...args) => global.fetch(...args);

exports.handler = async (event) => {
  try {
    const qs = event.queryStringParameters || {};
    const type = (qs.type || "equity").toLowerCase();
    const code = (qs.code || "").toUpperCase().trim();

    if (!code) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Missing "code" query param' }),
      };
    }

    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
    const MARKETAUX_API_TOKEN = process.env.MARKETAUX_API_TOKEN;
    const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
    const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

    if (!OPENAI_API_KEY) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing OPENAI_API_KEY env" }),
      };
    }
    if (!MARKETAUX_API_TOKEN) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing MARKETAUX_API_TOKEN env" }),
      };
    }

    // ----- Simple Upstash helpers -----
    async function redisGet(key) {
      if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
      try {
        const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        });
        if (!res.ok) return null;
        const j = await res.json().catch(() => null);
        if (!j || typeof j.result === "undefined") return null;
        return j.result;
      } catch {
        return null;
      }
    }

    async function redisSetEx(key, value, ttlSeconds) {
      if (!UPSTASH_URL || !UPSTASH_TOKEN) return false;
      try {
        const valString = typeof value === "string" ? value : JSON.stringify(value);
        const res = await fetch(
          `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(
            valString
          )}?EX=${ttlSeconds}`,
          {
            method: "POST",
            headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
          }
        );
        return res.ok;
      } catch {
        return false;
      }
    }

    const cacheKey = `ai-summary:${type}:${code}`;

    // ----- Try cache first (e.g. 12h TTL) -----
    const cachedRaw = await redisGet(cacheKey);
    if (cachedRaw) {
      try {
        const cached = JSON.parse(cachedRaw);
        if (cached && cached.summary) {
          return {
            statusCode: 200,
            body: JSON.stringify({ ...cached, cached: true }),
          };
        }
      } catch {
        // ignore parse errors
      }
    }

    // ----- Fetch some news from Marketaux -----
    // We'll keep it simple: search by symbol + Australia context
    const newsUrl = new URL("https://api.marketaux.com/v1/news/all");
    newsUrl.searchParams.set("api_token", MARKETAUX_API_TOKEN);
    newsUrl.searchParams.set("language", "en");
    newsUrl.searchParams.set("countries", "au");
    newsUrl.searchParams.set("limit", "8");
    // search covers headline / content; use code to bias
    newsUrl.searchParams.set("search", code);

    const newsRes = await fetch(newsUrl.toString());
    const newsJson = await newsRes.json().catch(() => null);
    const articles = Array.isArray(newsJson?.data) ? newsJson.data : [];

    const trimmedArticles = articles.slice(0, 5).map((a) => ({
      headline: a.title || "",
      summary: a.description || a.snippet || "",
      source: a.source || "",
      published_at: a.published_at || "",
    }));

    // Build a compact news context string
    const newsContext =
      trimmedArticles.length === 0
        ? "No recent news articles were found for this company in the last few days."
        : trimmedArticles
            .map((a, idx) => {
              const date = a.published_at ? a.published_at.slice(0, 10) : "";
              return `${idx + 1}. [${date}] ${a.headline} — ${a.summary}`;
            })
            .join("\n");

    // ----- Call OpenAI for a 2-liner summary -----
    const systemPrompt =
      "You are an investment writer for a simple, jargon-light Aussie investing app. " +
      "You write clear, calm two-line summaries for everyday retail investors.";

    const userPrompt = `
Write a short, friendly two-line snapshot about this company for a retail investor.

Use:
- one line for what the company is / does
- one line for what's been happening recently in the news, if anything notable

If there is no meaningful recent news, say that the recent news flow has been quiet.

Company code: ${code}
Company type: ${type}

Recent news (most recent first):
${newsContext}

Write exactly 2 short sentences. No bullet points. No disclaimers.
`;

    const openaiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        temperature: 0.4,
        max_tokens: 140,
      }),
    });

    if (!openaiRes.ok) {
      const txt = await openaiRes.text().catch(() => "");
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "OpenAI request failed",
          status: openaiRes.status,
          body: txt.slice(0, 300),
        }),
      };
    }

    const openaiJson = await openaiRes.json();
    const summary =
      openaiJson?.choices?.[0]?.message?.content?.trim() ||
      "We couldn't generate a summary right now.";

    const payload = {
      type,
      code,
      summary,
      newsCount: trimmedArticles.length,
      generatedAt: new Date().toISOString(),
    };

    // cache for 12 hours
    await redisSetEx(cacheKey, payload, 60 * 60 * 12);

    return {
      statusCode: 200,
      body: JSON.stringify(payload),
    };
  } catch (err) {
    console.error("instrument-ai-summary error", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message || String(err) }),
    };
  }
};
