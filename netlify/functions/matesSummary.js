// netlify/functions/matesSummary.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method not allowed" };
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.error("Missing OPENAI_API_KEY");
      return { statusCode: 500, body: "Missing OPENAI_API_KEY" };
    }

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
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
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
    const content = data.choices?.[0]?.message?.content || "{}";

    let parsed;
    try {
      parsed = JSON.parse(content);
    } catch (e) {
      console.error("Failed to parse JSON from OpenAI. Raw content:", content);
      parsed = {
        tldrBullets: [],
        whatsHappening: "",
        whyItMatters: "",
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      };
    }

    // Ensure all expected keys exist so the front-end never explodes
    parsed.tldrBullets = parsed.tldrBullets || [];
    parsed.whatsHappening = parsed.whatsHappening || "";
    parsed.whyItMatters = parsed.whyItMatters || "";
    parsed.riskNote = parsed.riskNote || "";
    parsed.disclaimer = parsed.disclaimer || "General information only, not financial advice.";

    return {
      statusCode: 200,
      body: JSON.stringify(parsed)
    };

  } catch (err) {
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
  }
};
