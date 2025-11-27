// netlify/functions/matesWeeklyNote.js
// Generates a plain-English weekly wrap for the ASX + key commodities.

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
  const OPENAI_MODEL =
    process.env.OPENAI_MODEL || "gpt-4.1-mini"; // or whatever you're using elsewhere

  if (!OPENAI_API_KEY) {
    console.error("OPENAI_API_KEY missing");
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "OpenAI not configured" }),
    };
  }

  let aggregates;
  try {
    const body = event && event.body ? JSON.parse(event.body) : {};
    aggregates = body.aggregates || {};
  } catch (err) {
    console.error("Failed to parse request body for matesWeeklyNote:", err.message);
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "Invalid request body" }),
    };
  }

  // Basic sanity check
  const hasAnyData =
    (aggregates.weeklyTop && aggregates.weeklyTop.length) ||
    (aggregates.weeklyBottom && aggregates.weeklyBottom.length) ||
    (aggregates.metalsWeekly &&
      Object.keys(aggregates.metalsWeekly).length > 0);

  if (!hasAnyData) {
    console.warn("matesWeeklyNote called with empty aggregates");
    return {
      statusCode: 200,
      body: JSON.stringify({
        note:
          "A quiet week on the ASX overall. No major moves stood out in the top gainers, decliners or key commodities based on our data snapshot.",
      }),
    };
  }

  // Build prompt
  const systemMessage = {
    role: "system",
    content: [
      "You are an investment writer for MatesInvest, speaking to everyday Australian retail investors.",
      "Write in short, clear, plain-English. Assume the reader has basic ASX knowledge but is not a professional.",
      "Summarise what happened on the ASX over the last 5 trading days, using the JSON data provided.",
      "Highlight key winners and losers by code, and what sectors or themes they represent.",
      "Explain what happened in major commodities like gold, iron ore, lithium, nickel and uranium.",
      "You MUST NOT give personal financial advice or tell the reader what to buy or sell.",
      "Avoid predictions, price targets and specific trade recommendations.",
      "Tone: friendly, confident, calm, and slightly conversational.",
    ].join(" "),
  };

  const userMessage = {
    role: "user",
    content:
      [
        "Here is the weekly ASX and commodities data for the last five trading days as JSON.",
        "",
        "Field meanings:",
        "- weeklyTop: array of top movers across the week (sumPct is total percentage move over the week).",
        "- weeklyBottom: array of worst performers across the week.",
        "- metalsWeekly: object keyed by symbol (XAU, XAG, IRON, LITH-CAR, NI, URANIUM, etc) with first/last price in AUD and weeklyPct move.",
        "",
        "Use this data to write a short weekly wrap called 'Weekly Wrap'.",
        "Structure it like this (but DO NOT include bullet labels, just headings/paragraphs):",
        "1) A one-sentence opener summarising the week for the ASX overall.",
        "2) A short 'Equities' paragraph or two highlighting notable winners and losers (mention a few tickers like A2M, BHP, etc).",
        "3) A short 'Commodities' paragraph noting which commodities moved the most and how that might relate to sectors (gold miners, iron ore names, lithium, uranium, etc).",
        "4) A short 'What to watch next week' paragraph that is very general — focus on themes (volatility, commodity sensitivity), not specific predictions.",
        "",
        "Do not exceed about 250–300 words.",
        "Keep it Australian in spelling and context.",
        "",
        "Here is the JSON data:\n",
        JSON.stringify(aggregates, null, 2),
      ].join("\n"),
  };

  try {
    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        temperature: 0.6,
        max_tokens: 450,
        messages: [systemMessage, userMessage],
      }),
    });

    if (!aiRes.ok) {
      const txt = await aiRes.text().catch(() => "");
      console.error("OpenAI weekly note error:", aiRes.status, txt);
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Failed to generate weekly note" }),
      };
    }

    const data = await aiRes.json().catch(() => null);
    const note =
      (data &&
        data.choices &&
        data.choices[0] &&
        data.choices[0].message &&
        data.choices[0].message.content &&
        data.choices[0].message.content.trim()) ||
      "";

    if (!note) {
      console.warn("OpenAI weekly note returned empty content");
      return {
        statusCode: 200,
        body: JSON.stringify({
          note:
            "It was a fairly mixed week on the ASX, with no clear trend across the major indices or commodities based on our data snapshot.",
        }),
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ note }),
    };
  } catch (err) {
    console.error("matesWeeklyNote unexpected error:", err && err.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal error generating weekly note" }),
    };
  }
};
