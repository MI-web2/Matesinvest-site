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
  (aggregates.weeklyTopSectors &&
    aggregates.weeklyTopSectors.length) ||
  (aggregates.weeklyBottomSectors &&
    aggregates.weeklyBottomSectors.length) ||
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
    "Your focus is on SECTORS, not individual stock tips.",
    "Use the JSON data to explain which sectors had the strongest and weakest average moves over the week, and how that links to key commodities.",
    "You can mention a couple of example tickers in passing, but the main story should be sector-level themes (e.g. gold miners, tech, energy, materials).",
    "Explain what happened in major commodities like gold, iron ore, lithium, nickel and uranium.",
    "You MUST NOT give personal financial advice or tell the reader what to buy or sell.",
    "Avoid predictions, price targets and specific trade recommendations.",
    "Tone: friendly, confident, calm, slightly conversational, with Australian spelling.",
  ].join(" "),
};


const userMessage = {
  role: "user",
  content: [
    "Here is the weekly ASX and commodities data for the last five trading days as JSON.",
    "",
    "Field meanings:",
    "- weeklyTopSectors: array of best-performing sectors across the week (avgPct is the average daily percentage move across the sector).",
    "- weeklyBottomSectors: array of worst-performing sectors.",
    "- metalsWeekly: object keyed by symbol (XAU, XAG, IRON, LITH-CAR, NI, URANIUM, etc) with first/last price in AUD and weeklyPct move.",
    "",
    "Use this data to write a short weekly wrap called 'Weekly Wrap'.",
    "Structure it like this (but DO NOT include bullet labels, just headings/paragraphs):",
    "1) A one-sentence opener summarising the week for the ASX overall.",
    "2) A short 'Equities by sector' section explaining which sectors led and lagged, and why that might be (e.g. rate-sensitive names, defensives, resources, tech).",
    "3) A short 'Commodities' section noting which commodities moved the most and which sectors that tends to impact (gold miners, iron ore producers, lithium names, uranium plays, etc).",
    "4) A short 'What to watch next week' paragraph that is very general — focus on themes (e.g. whether resource-heavy sectors stay in focus), not specific predictions.",
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
