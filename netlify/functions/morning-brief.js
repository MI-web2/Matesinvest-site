// netlify/functions/morning-brief.js
// Morning brief (OpenAI-driven): ask OpenAI for the spot gold price in AUD and return strict JSON.
// Pattern mirrors netlify/functions/matesSummary.js (system + user, same model family).
//
// NOTE: This asks OpenAI for a numeric market value. That can hallucinate — safer approach is to
// fetch numeric data from market APIs and use OpenAI only to phrase results. But per your request
// this will call OpenAI directly.
//
// Requires env var: OPENAI_API_KEY

exports.handler = async (event) => {
  try {
    // Allow GET or POST (client uses GET)
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
    if (!OPENAI_API_KEY) {
      console.error("Missing OPENAI_API_KEY");
      return { statusCode: 500, body: "Missing OPENAI_API_KEY" };
    }

    // Build the prompt: ask OpenAI to return strict JSON with numeric price in AUD
    const prompt = `
You are a concise financial reporter. Using live market knowledge, return a JSON object ONLY (no commentary, no code fences) with exactly these keys:
- "priceAUD": numeric (the current spot gold price in Australian dollars per troy ounce).
- "narrative": a single short sentence in this exact form: "The spot gold price is currently $X AUD per ounce." where X is the priceAUD rounded to 2 decimals.
- "priceTimestamp": ISO timestamp if you can provide it (otherwise null).
- "generatedAt": ISO timestamp string for when this response is generated.

Answer with only the JSON object. Do not include any surrounding text.
Question: What is the spot gold price in AUD terms right now?
`.trim();

    // Call OpenAI (same style as matesSummary)
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages: [
          { role: "system", content: "You respond only with strict JSON. No extra commentary." },
          { role: "user", content: prompt }
        ],
        temperature: 0.0,
        max_tokens: 120
      })
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "<no-body>");
      console.error("OpenAI error:", res.status, txt);
      // Return graceful fallback so UI doesn't break
      return {
        statusCode: 200,
        body: JSON.stringify({
          priceAUD: null,
          narrative: "The spot gold price is currently unavailable.",
          priceTimestamp: null,
          generatedAt: new Date().toISOString(),
          // debug: include OpenAI status/text in logs only (not exposing secret)
          _debug: { openai_status: res.status, openai_body_preview: txt.slice(0, 1000) }
        })
      };
    }

    const data = await res.json().catch(() => null);
    const rawContent = data?.choices?.[0]?.message?.content || "";

    // Attempt to parse JSON robustly: try direct JSON.parse, then balanced-object extraction
    function extractBalancedJSON(text) {
      if (!text || typeof text !== 'string') return null;
      const first = text.indexOf('{');
      if (first === -1) return null;
      let inString = false;
      let escape = false;
      let depth = 0;
      let start = -1;
      for (let i = first; i < text.length; i++) {
        const ch = text[i];
        if (!inString) {
          if (ch === '{') { if (start === -1) start = i; depth++; }
          else if (ch === '}') { depth--; if (depth === 0 && start !== -1) return text.slice(start, i + 1); }
          if (ch === '"') { inString = true; escape = false; }
        } else {
          if (escape) { escape = false; continue; }
          if (ch === '\\') { escape = true; continue; }
          if (ch === '"') { inString = false; }
        }
      }
      return null;
    }

    let parsed = null;
    try {
      parsed = JSON.parse(rawContent.trim());
    } catch (e) {
      const candidate = extractBalancedJSON(rawContent);
      if (candidate) {
        try { parsed = JSON.parse(candidate); } catch (e2) { parsed = null; }
      }
    }

    // If we parsed a valid object, normalise and return
    if (parsed && typeof parsed === 'object') {
      // Ensure numeric coercion for priceAUD
      const priceAUD = (() => {
        const v = parsed.priceAUD;
        if (v === null || v === undefined || v === '') return null;
        const num = Number(v);
        return Number.isFinite(num) ? Number(num.toFixed(2)) : null;
      })();

      const narrative = (typeof parsed.narrative === 'string' && parsed.narrative.trim()) ?
        parsed.narrative.trim() :
        (priceAUD !== null ? `The spot gold price is currently $${priceAUD} AUD per ounce.` : 'The spot gold price is currently unavailable.');

      const out = {
        priceAUD,
        narrative,
        priceTimestamp: parsed.priceTimestamp || null,
        generatedAt: parsed.generatedAt || new Date().toISOString()
      };

      return { statusCode: 200, body: JSON.stringify(out) };
    }

    // If parsing failed, log raw and return fallback
    console.error("OpenAI returned unparsable content for morning-brief. Raw:", rawContent.slice(0, 2000));
    return {
      statusCode: 200,
      body: JSON.stringify({
        priceAUD: null,
        narrative: "The spot gold price is currently unavailable.",
        priceTimestamp: null,
        generatedAt: new Date().toISOString(),
        _debug: { rawOpenAI: rawContent.slice(0, 2000) }
      })
    };

  } catch (err) {
    console.error("morning-brief error:", err && (err.stack || err.message || err));
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err && (err.message || "Server error") })
    };
  }
};