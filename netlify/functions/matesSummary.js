// netlify/functions/matesSummary.js

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method not allowed" };
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return { statusCode: 500, body: "Missing OPENAI_API_KEY" };
    }

    const { text, sourceType, headline } = JSON.parse(event.body || "{}");

    if (!text) {
      return { statusCode: 400, body: "Missing text to summarise" };
    }

    const prompt = `
You are helping Australian retail investors understand news in very plain English.

USER CONTEXT:
- Source type: ${sourceType || "unknown"}
- Headline: ${headline || "n/a"}

TASK:
Read the article / announcement text below and return a very compact JSON object with:
- "oneLiner": A single short sentence (max 18 words) that captures the core idea in everyday language.
- "tldrBullets": 3 concise bullet points summarising what is going on.
- "keyMetrics": 3–5 bullet points with key numbers, dates or concrete facts (e.g. revenue growth %, EPS, guidance, deal size).
- "riskNote": 1–2 sentences on risks or uncertainties. Keep this balanced and non-alarmist.
- "disclaimer": Always set to: "General information only, not financial advice."

STYLE:
- No jargon, no buzzwords, no hype.
- Write as if explaining to a smart friend over coffee.
- Do NOT include any additional keys or commentary outside the JSON.
- IMPORTANT: respond with JSON only, no backticks.

TEXT TO SUMMARISE:
"""${text.slice(0, 8000)}"""
    `.trim();

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You are a careful assistant that returns strict JSON only, no extra commentary." },
          { role: "user", content: prompt }
        ],
        temperature: 0.3,
        max_tokens: 400
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error("OpenAI error:", errText);
      return { statusCode: 500, body: "Error from OpenAI" };
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content || "{}";

    let parsed;
    try {
      parsed = JSON.parse(content);
    } catch (e) {
      console.error("JSON parse error, raw content:", content);
      // Fallback shape so frontend doesn't break
      parsed = {
        oneLiner: "Could not generate a clean summary for this item.",
        tldrBullets: [],
        keyMetrics: [],
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      };
    }

    // Safety: ensure required fields exist
    parsed.oneLiner = parsed.oneLiner || "";
    parsed.tldrBullets = parsed.tldrBullets || [];
    parsed.keyMetrics = parsed.keyMetrics || [];
    parsed.riskNote = parsed.riskNote || "";
    parsed.disclaimer = parsed.disclaimer || "General information only, not financial advice.";

    return {
      statusCode: 200,
      body: JSON.stringify(parsed),
    };

  } catch (err) {
    console.error("matesSummary function error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        oneLiner: "Sorry, something went wrong creating this summary.",
        tldrBullets: [],
        keyMetrics: [],
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      })
    };
  }
};
