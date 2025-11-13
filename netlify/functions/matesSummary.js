// netlify/functions/matesSummary.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.error("Missing OPENAI_API_KEY");
      return { statusCode: 500, body: "Missing OPENAI_API_KEY" };
    }

<<<<<<< HEAD
    let body;
    try {
      body = JSON.parse(event.body || "{}");
    } catch (e) {
      console.error("Invalid JSON body:", event.body);
      return { statusCode: 400, body: "Invalid JSON body" };
    }

    const { text, headline, sourceType } = body;
    if (!text) {
      return { statusCode: 400, body: "Missing text to summarise" };
    }

    const prompt = `
You are helping Australian retail investors understand market news.

Return ONLY a JSON object with these keys:
- "tldrBullets": array of 3 concise dot points summarising the key points.
- "whatsHappening": one short paragraph.
- "whyItMatters": one short paragraph focusing on impact for investors.
- "riskNote": one short sentence about risks or uncertainties.
- "disclaimer": exactly "General information only, not financial advice."

Rules:
- Very plain English, no jargon.
- No financial advice.
- Do NOT add any extra keys.
- Respond with JSON only, no backticks.

Headline: ${headline || "n/a"}
Source type: ${sourceType || "news_article"}

Text to summarise:
"""${text.slice(0, 8000)}"""
    `.trim();

    const openAiRes = await fetch("https://api.openai.com/v1/chat/completions", {
=======
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
>>>>>>> parent of f1ac574 (ok)
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages: [
<<<<<<< HEAD
          { role: "system", content: "You respond only with strict JSON, no commentary." },
          { role: "user", content: prompt }
        ],
        temperature: 0.3,
        max_tokens: 450
      })
    });

    if (!openAiRes.ok) {
      const errText = await openAiRes.text();
      console.error("OpenAI HTTP error:", errText);
      // fall back to a very safe default so the UI never crashes
      return {
        statusCode: 200,
        body: JSON.stringify({
          tldrBullets: [],
          whatsHappening: "",
          whyItMatters: "",
          riskNote: "",
          disclaimer: "General information only, not financial advice."
        })
      };
    }

    const data = await openAiRes.json();
=======
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
>>>>>>> parent of f1ac574 (ok)
    const content = data.choices?.[0]?.message?.content || "{}";

    let summary;
    try {
      summary = JSON.parse(content);
    } catch (e) {
<<<<<<< HEAD
      console.error("Failed to parse JSON from OpenAI. Raw content:", content);
      parsed = {
        tldrBullets: [],
=======
      console.error("JSON parse error:", e, content);
      summary = {
        tldrBullets: ["Sorry, something went wrong parsing the summary."],
>>>>>>> parent of f1ac574 (ok)
        whatsHappening: "",
        whyItMatters: "",
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      };
    }

<<<<<<< HEAD
    // Ensure all expected keys exist so the front-end never explodes
    parsed.tldrBullets = parsed.tldrBullets || [];
    parsed.whatsHappening = parsed.whatsHappening || "";
    parsed.whyItMatters = parsed.whyItMatters || "";
    parsed.riskNote = parsed.riskNote || "";
    parsed.disclaimer = parsed.disclaimer || "General information only, not financial advice.";

    return {
      statusCode: 200,
      body: JSON.stringify(parsed)
=======
    return {
      statusCode: 200,
      body: JSON.stringify(summary),
>>>>>>> parent of f1ac574 (ok)
    };
  } catch (err) {
<<<<<<< HEAD
    console.error("matesSummary function error:", err);
    // Final safety fallback
    return {
      statusCode: 200,
      body: JSON.stringify({
        tldrBullets: [],
        whatsHappening: "",
        whyItMatters: "",
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      })
    };
=======
    console.error(err);
    return { statusCode: 500, body: "Server error" };
>>>>>>> parent of f1ac574 (ok)
  }
};
