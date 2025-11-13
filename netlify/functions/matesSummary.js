// netlify/functions/matesSummary.js

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return { statusCode: 500, body: "Missing OPENAI_API_KEY" };
    }

    const body = JSON.parse(event.body || "{}");
    const text = body.text || "";
    const sourceType = body.sourceType || "news_article";
    const ticker = body.ticker || "";
    const headline = body.headline || "";

    if (!text.trim()) {
      return { statusCode: 400, body: "Missing 'text' in request body" };
    }

    const systemPrompt = `
You are MatesSummaries, the summarisation engine for MatesInvest.

Write in clear Australian plain English, friendly but not silly.
Never give financial advice. Never say “buy”, “sell” or “you should”.

Always respond as JSON with this exact shape:
{
  "tldrBullets": [ "...", "...", "..." ],
  "whatsHappening": "...",
  "whyItMatters": "...",
  "riskNote": "...",
  "disclaimer": "General information only, not financial advice."
}

Adapt tone slightly based on sourceType: "asx_announcement", "news_article" or "manual".
If a ticker is provided, reference it neutrally (e.g. "For holders of PRU,...").
`;

    const userPrompt = `
Source type: ${sourceType}
Ticker: ${ticker || "N/A"}
Headline: ${headline || "N/A"}

Original text:
"""${text}"""

Summarise following the JSON format above.
`;

    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt }
        ],
        temperature: 0.3,
      }),
    });

    if (!res.ok) {
      const textErr = await res.text();
      console.error("OpenAI error:", textErr);
      return { statusCode: 500, body: "Error from OpenAI" };
    }

    const data = await res.json();
    const content = data.choices?.[0]?.message?.content || "{}";

    let summary;
    try {
      summary = JSON.parse(content);
    } catch (e) {
      console.error("JSON parse error:", e, content);
      summary = {
        tldrBullets: ["Sorry, something went wrong parsing the summary."],
        whatsHappening: "",
        whyItMatters: "",
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify(summary),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: "Server error" };
  }
};