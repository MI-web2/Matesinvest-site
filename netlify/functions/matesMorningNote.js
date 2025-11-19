// netlify/functions/matesMorningNote.js

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Helper: figure out base URL so we can call our own newsFeed function
function getBaseUrl(event) {
  const envUrl = process.env.URL;
  if (envUrl) return envUrl.replace(/\/$/, "");

  const host = event.headers["x-forwarded-host"] || event.headers.host || "localhost:8888";
  const proto = (event.headers["x-forwarded-proto"] || "http");
  return `${proto}://${host}`;
}

// Build a short prompt for OpenAI from top headlines
function buildPrompt(region, articles) {
  const regionLabel =
    region === "us" ? "the US"
    : region === "global" ? "global markets"
    : "Australia and the ASX";

  const topBits = articles.slice(0, 6).map((a) => {
    const title = a.title || "";
    const src = a.source || "";
    return `• ${title}${src ? ` (${src})` : ""}`;
  }).join("\n");

  return `
return `
You are writing a short pre-market note for everyday investors in ${regionLabel}.
Use plain English, no jargon, and keep it to 2–3 sentences max.

Here are some of the latest headlines and market stories:

${topBits || "• No major headlines available."}

Write a concise "Mates Morning Note" that:
- starts directly with the market context (no greeting, no heading)
- does NOT include any markdown formatting (no **bold**, no bullet points)
- does NOT say "Mates Morning Note" in the text
- does NOT include a sign-off like "stay safe" or "take care"
- uses an Australian tone when talking about Australia and the ASX.
`;
}

exports.handler = async function (event, context) {
  try {
    const region = (event.queryStringParameters && event.queryStringParameters.region) || "au";

    const baseUrl = getBaseUrl(event);
    const newsUrl = `${baseUrl}/.netlify/functions/newsFeed${region ? `?region=${encodeURIComponent(region)}` : ""}`;

    // 1) Get latest headlines from your existing newsFeed lambda
    let articles = [];
    try {
      const newsRes = await fetch(newsUrl);   // ← global fetch
      if (newsRes.ok) {
        const newsData = await newsRes.json();
        articles = newsData.articles || [];
      }
    } catch (err) {
      console.warn("matesMorningNote: newsFeed fetch failed", err);
    }

    // 2) If no OpenAI key, just return a basic fallback so UI doesn't break
    if (!OPENAI_API_KEY) {
      const note = articles.length
        ? `Markets are watching the latest headlines, with a focus on ${articles[0].title || "overnight news"}. Keep an eye on how this flows through to Australian stocks today.`
        : `Markets are relatively quiet in the headlines we can see. Keep an eye on major indices, banks and resources as trade gets underway.`;

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          region,
          note,
          generatedAt: new Date().toISOString(),
          _debug: { usedFallback: true, articleCount: articles.length }
        })
      };
    }

// 3) Call OpenAI to generate the note
const prompt = buildPrompt(region, articles);

const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
  method: "POST",
  headers: {
    "Authorization": `Bearer ${OPENAI_API_KEY}`,
    "Content-Type": "application/json"
  },
  body: JSON.stringify({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: "You are an assistant that writes very short, calm pre-market notes for retail investors in Australia." },
      { role: "user", content: prompt }
    ],
    temperature: 0.4,
    max_tokens: 220
  })
});

const aiJson = await aiRes.json();

// -------------------
// CLEANUP BLOCK HERE
// -------------------
let note = aiJson.choices?.[0]?.message?.content?.trim()
  || "Markets are open with mixed signals across indices and commodities today.";

// Cleanup formatting
note = note.replace(/\*\*/g, "");
note = note.replace(/^#+\s*/g, "");
note = note.replace(/^Mates Morning Note[:\- ]*/i, "");
note = note.replace(/^Good\s+morning[,!.]?\s*/i, "");
note = note.replace(/Take care\.?$/i, "");
note = note.trim();

// return output…
return {
  statusCode: 200,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    region,
    note,
    generatedAt: new Date().toISOString(),
    _debug: {
      usedFallback: false,
      articleCount: articles.length
    }
  })
};
  } catch (err) {
    console.error("matesMorningNote handler failed", err);
    return {
      statusCode: 200, // still 200 so UI doesn't flash errors
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        region: "unknown",
        note: "Unable to load today’s morning note. Markets will still react to headlines, economic data and company news — keep an eye on your key names today.",
        generatedAt: new Date().toISOString(),
        _debug: { error: err.message || String(err) }
      })
    };
  }
};
