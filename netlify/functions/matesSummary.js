// netlify/functions/matesSummary.js

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

    let body;
    try {
      body = JSON.parse(event.body || "{}");
    } catch (e) {
      console.error("Invalid JSON body:", event.body);
      return { statusCode: 400, body: "Invalid JSON body" };
    }

    const text = (body.text || "").trim();
    const sourceType = body.sourceType || "news_article";
    const headline = body.headline || "";

    if (!text) {
      return { statusCode: 400, body: "Missing text to summarise" };
    }

    // Prompt: matesy, analyst-style, numbers up front, still no advice
    const prompt = `
You are writing quick summaries for Australian retail investors.

Tone:
- Think "good sell-side / buy-side analyst chatting to a smart mate at the pub".
- Friendly, plain English, but not silly.
- Avoid jargon where possible, and explain any jargon in simple terms.
- No emojis, no exclamation marks, no hype.

Return ONLY a JSON object with exactly these keys:
- "tldrBullets": array of 3 concise dot points.
    • Bullet 1: What is happening in simple terms.
    • Bullet 2: Key numbers or metrics (prices, %, revenue/EPS changes, deal size, dates, guidance etc).
    • Bullet 3: Short takeaway in plain English.
- "whatsHappening": 1–2 sentences that describe the situation, like you'd explain it to a mate.
- "whyItMatters": 1–3 sentences on why this news matters for investors, in neutral language. No recommendations.
- "riskNote": 1–2 sentences that flag uncertainties, execution risks, regulatory risk, or "things that could go wrong".
- "disclaimer": Always exactly "General information only, not financial advice."

Rules:
- Do NOT give any financial advice, recommendations or price targets.
- Focus on facts and how they affect the business / theme, not "you should".
- Use numbers where they are present in the article (prices, moves, revenue/EPS/guidance, dates).
- Keep everything short and skimmable.
- Respond with pure JSON only, no backticks, no extra text outside the JSON.

Context:
- Source type: ${sourceType}
- Headline: ${headline || "n/a"}

Article / announcement text (may be truncated):
"""${text.slice(0, 8000)}"""
    `.trim();

    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages: [
          { role: "system", content: "You respond only with strict JSON. No extra commentary." },
          { role: "user", content: prompt }
        ],
        temperature: 0.4,
        max_tokens: 450
      })
    });

    if (!res.ok) {
      const textErr = await res.text();
      console.error("OpenAI error:", textErr);
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

    const data = await res.json();
    const content = data.choices?.[0]?.message?.content || "{}";

    let summary;
    try {
      summary = JSON.parse(content);
    } catch (e) {
      console.error("JSON parse error from OpenAI. Raw content:", content);
      summary = {};
    }

    // Normalise so the frontend never explodes
    const normalised = {
      tldrBullets: Array.isArray(summary.tldrBullets) ? summary.tldrBullets : [],
      whatsHappening: summary.whatsHappening || "",
      whyItMatters: summary.whyItMatters || "",
      riskNote: summary.riskNote || "",
      disclaimer: summary.disclaimer || "General information only, not financial advice."
    };

    return {
      statusCode: 200,
      body: JSON.stringify(normalised)
    };
  } catch (err) {
    console.error("matesSummary error:", err);
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
