// netlify/functions/matesSummary.js

exports.handler = async (event) => {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return { statusCode: 500, body: "Missing OPENAI_API_KEY" };
    }

    const body = JSON.parse(event.body || "{}");
    const { text, headline, sourceType } = body;

    if (!text) {
      return { statusCode: 400, body: "Missing text to summarise" };
    }

    const prompt = `
      Please summarise this ${sourceType || "news article"} for Australian retail investors in clear, simple English.

      Provide:
      - "tldrBullets": 3 concise dot points summarising the key points.
      - "whatsHappening": One short paragraph.
      - "whyItMatters": One short paragraph.
      - "riskNote": One short sentence about risks or uncertainty.
      - "disclaimer": Always set to "General information only, not financial advice."

      Respond in strict JSON only, no commentary or text outside JSON.

      Headline: ${headline || "n/a"}
      Text:
      """${text.slice(0, 8000)}"""
    `;

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "Respond only in JSON." },
          { role: "user", content: prompt }
        ],
        temperature: 0.3,
        max_tokens: 500
      })
    });

    if (!response.ok) {
      return { statusCode: 500, body: "OpenAI request failed" };
    }

    const data = await response.json();

    let parsed;
    try {
      parsed = JSON.parse(data.choices[0].message.content);
    } catch (e) {
      parsed = {
        tldrBullets: [],
        whatsHappening: "",
        whyItMatters: "",
        riskNote: "",
        disclaimer: "General information only, not financial advice."
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify(parsed)
    };

  } catch (err) {
    console.error("matesSummary error:", err);
    return {
      statusCode: 500,
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
