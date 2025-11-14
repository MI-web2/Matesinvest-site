// netlify/functions/matesSummary.js

exports.handler = async (event) => {
  try {
    // Only allow POST
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
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

    const text = (body.text || "").trim();
    const headline = body.headline || "";
    const sourceType = body.sourceType || "news_article";

    if (!text) {
      return { statusCode: 400, body: "Missing text to summarise" };
    }

    // --- Prompt for Option 1 style summaries ---
    const prompt = `
You help Australian retail investors quickly understand market news.

Return ONLY a JSON object (no backticks, no extra text) with these keys:

- "oneLiner": A single plain-English sentence (max 18 words) that captures the core idea.
- "tldrBullets": Array of exactly 3 concise bullet points summarising the main points.
- "keyMetrics": Array of 2–5 bullets with key numbers, dates or hard facts (e.g. % moves, guidance, deal size, EPS).
- "riskNote": 1–2 sentences on risks or uncertainty. Keep it balanced and non-alarmist.
- "disclaimer": Always exactly "General information only, not financial advice."

Rules:
- Very simple language, like explaining to a smart friend over coffee.
- No hype, no clickbait, no financial advice.
- Do NOT add any extra keys to the JSON.
- Respond with JSON only, no commentary.

Context:
- Source type: ${sourceType}
- Headline: ${headline || "n/a"}

Text to summarise (truncate if needed):
"""${text.slice(0, 8000)}"""
    `.trim();

    // Call OpenAI (built-in fetch in Netlify’s Node runtime)
    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages: [
          { role: "system", content: "You respond only with strict JSON, no extra text." },
          { role: "user", content: prompt },
        ],
        temperature: 0.3,
        max_tokens: 450,
      }),
    });

    // If OpenAI fails, fall back to a safe, empty summary but still status 200
    if (!aiRes.ok) {
      const errText = await aiRes.text();
      console.error("OpenAI HTTP error:", errText);
      const fallback = {
        oneLiner: "",
        tldrBullets: [],
        keyMetrics: [],
        riskNote: "",
        disclaimer: "General information only, not financial advice.",
      };
      return { statusCode: 200, body: JSON.stringify(fallback) };
    }

    const data = await aiRes.json();
    const content = data.choices?.[0]?.message?.content || "{}";

    let summary;
    try {
      summary = JSON.parse(content);
    } catch (e) {
      console.error("Failed to parse JSON from OpenAI. Raw content:", content);
      summary = {};
    }

    // Normalise shape so frontend never breaks
    const normalised = {
      oneLiner: summary.oneLiner || "",
      tldrBullets: Array.isArray(summary.tldrBullets) ? summary.tldrBullets : [],
      keyMetrics: Array.isArray(summary.keyMetrics) ? summary.keyMetrics : [],
      riskNote: summary.riskNote || "",
      disclaimer: summary.disclaimer || "General information only, not financial advice.",
    };

    return {
      statusCode: 200,
      body: JSON.stringify(normalised),
    };
  } catch (err) {
    console.error("matesSummary error:", err);
    // Final safety fallback – still return 200 so the UI shows *something*
    return {
      statusCode: 200,
      body: JSON.stringify({
        oneLiner: "",
        tldrBullets: [],
        keyMetrics: [],
        riskNote: "",
        disclaimer: "General information only, not financial advice.",
      }),
    };
  }
};
