// netlify/functions/morning-brief.js
// Morning brief: authoritative numeric fetch -> OpenAI for phrasing -> validation.
// Flow:
// 1) Fetch XAU/USD (Yahoo XAUUSD=X, fallback GC=F).
// 2) Fetch USD->AUD (exchangerate.host, fallback er-api).
// 3) Compute priceAUD deterministically.
// 4) Call OpenAI (same pattern as matesSummary) with an explicit instruction: use the supplied numbers only.
// 5) Parse OpenAI response, extract numeric value and validate it's within tolerance of computed priceAUD.
// 6) If validated, return OpenAI's phrasing; otherwise return deterministic phrasing (server as single source of truth).
//
// This prevents the model hallucinating live prices while still using the model for natural phrasing.
//
// Requires env var: OPENAI_API_KEY

exports.handler = async (event) => {
  try {
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY || null;

    // Helper: fetch with timeout
    async function fetchWithTimeout(url, opts = {}, timeout = 8000) {
      const controller = new AbortController();
      const id = setTimeout(() => controller.abort(), timeout);
      try {
        const res = await fetch(url, { ...opts, signal: controller.signal });
        clearTimeout(id);
        return res;
      } catch (err) {
        clearTimeout(id);
        throw err;
      }
    }

    // Helper: format numeric or null
    const fmtNumber = (n) => (typeof n === 'number' && !isNaN(n)) ? Number(n.toFixed(2)) : null;

    // 1) Try Yahoo XAUUSD=X
    let priceUSD = null;
    let priceTimestamp = null;
    try {
      const yf = await fetchWithTimeout('https://query1.finance.yahoo.com/v7/finance/quote?symbols=XAUUSD=X', {}, 7000);
      if (yf.ok) {
        const j = await yf.json();
        const r = j?.quoteResponse?.result?.[0];
        if (r && typeof r.regularMarketPrice === 'number') {
          priceUSD = Number(r.regularMarketPrice);
          if (r.regularMarketTime) priceTimestamp = new Date(r.regularMarketTime * 1000).toISOString();
        }
      } else {
        console.warn('Yahoo XAUUSD request non-ok', yf.status);
      }
    } catch (e) {
      console.warn('Yahoo XAUUSD fetch error:', e && e.message);
    }

    // fallback to GC=F
    if (priceUSD === null) {
      try {
        const gc = await fetchWithTimeout('https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC=F', {}, 7000);
        if (gc.ok) {
          const j = await gc.json();
          const r = j?.quoteResponse?.result?.[0];
          if (r && typeof r.regularMarketPrice === 'number') {
            priceUSD = Number(r.regularMarketPrice);
            if (r.regularMarketTime) priceTimestamp = new Date(r.regularMarketTime * 1000).toISOString();
          }
        } else {
          console.warn('Yahoo GC=F request non-ok', gc.status);
        }
      } catch (e) {
        console.warn('Yahoo GC=F fetch error:', e && e.message);
      }
    }

    // 2) Fetch USD -> AUD
    let usdToAud = null;
    try {
      const fx = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 6000);
      if (fx.ok) {
        const j = await fx.json();
        if (j && j.rates && typeof j.rates.AUD === 'number') usdToAud = Number(j.rates.AUD);
      } else {
        console.warn('exchangerate.host non-ok', fx.status);
      }
    } catch (e) {
      console.warn('exchangerate.host fetch error:', e && e.message);
    }

    // fallback FX
    if (usdToAud === null) {
      try {
        const fx2 = await fetchWithTimeout('https://open.er-api.com/v6/latest/USD', {}, 6000);
        if (fx2.ok) {
          const j = await fx2.json();
          if (j && j.rates && typeof j.rates.AUD === 'number') usdToAud = Number(j.rates.AUD);
        } else {
          console.warn('er-api non-ok', fx2.status);
        }
      } catch (e) {
        console.warn('er-api fetch error:', e && e.message);
      }
    }

    // Compute AUD price
    const outPriceUSD = fmtNumber(priceUSD);
    const outFx = fmtNumber(usdToAud);
    const outPriceAUD = (outPriceUSD !== null && outFx !== null) ? fmtNumber(outPriceUSD * outFx) : null;
    const nowIso = new Date().toISOString();

    // Prepare deterministic fallback narrative
    const deterministicNarrative = outPriceAUD !== null
      ? `The spot gold price is currently $${outPriceAUD} AUD per ounce.`
      : `The spot gold price is currently unavailable.`;

    // If we don't have numeric data or no API key, skip OpenAI and return deterministic result
    if (!OPENAI_API_KEY) {
      console.warn('OPENAI_API_KEY not set or empty; returning deterministic narrative.');
      return {
        statusCode: 200,
        body: JSON.stringify({
          narrative: deterministicNarrative,
          priceUSD: outPriceUSD,
          usdToAud: outFx,
          priceAUD: outPriceAUD,
          priceTimestamp: priceTimestamp || null,
          generatedAt: nowIso
        })
      };
    }

    if (outPriceAUD === null) {
      console.warn('Numeric data incomplete; skipping OpenAI phrasing.');
      return {
        statusCode: 200,
        body: JSON.stringify({
          narrative: deterministicNarrative,
          priceUSD: outPriceUSD,
          usdToAud: outFx,
          priceAUD: outPriceAUD,
          priceTimestamp: priceTimestamp || null,
          generatedAt: nowIso
        })
      };
    }

    // 3) Build strict prompt that forces the model to use provided numbers only.
    const prompt = `
You are a concise reporter. Use only the numeric values provided below — do NOT change, guess, or hallucinate numbers.
Return EXACTLY one JSON object and nothing else (no commentary, no code fences) with these keys:
- "priceAUD": numeric (the AUD price you used, rounded to 2 decimals)
- "narrative": string, exactly one sentence in this form: "The spot gold price is currently $X AUD per ounce." with X matching priceAUD
- "priceTimestamp": ISO timestamp or null
- "generatedAt": ISO timestamp for when this object is produced

Numbers to use (do NOT change them):
priceUSD=${outPriceUSD}
usdToAud=${outFx}
priceAUD=${outPriceAUD}
priceTimestamp=${priceTimestamp || nowIso}

Return only the JSON object.
`.trim();

    // 4) Call OpenAI (same pattern as matesSummary)
    let openaiRaw = null;
    try {
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
        console.error('OpenAI non-ok status', res.status, txt);
        // Return deterministic fallback (do not expose raw OpenAI body)
        return {
          statusCode: 200,
          body: JSON.stringify({
            narrative: deterministicNarrative,
            priceUSD: outPriceUSD,
            usdToAud: outFx,
            priceAUD: outPriceAUD,
            priceTimestamp: priceTimestamp || null,
            generatedAt: nowIso
          })
        };
      }

      const data = await res.json().catch(() => null);
      openaiRaw = data?.choices?.[0]?.message?.content || "";
    } catch (e) {
      console.error('OpenAI call error:', e && e.message);
      return {
        statusCode: 200,
        body: JSON.stringify({
          narrative: deterministicNarrative,
          priceUSD: outPriceUSD,
          usdToAud: outFx,
          priceAUD: outPriceAUD,
          priceTimestamp: priceTimestamp || null,
          generatedAt: nowIso
        })
      };
    }

    // 5) Parse OpenAI content robustly and validate number matches computed priceAUD
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
      parsed = JSON.parse(openaiRaw.trim());
    } catch (e) {
      const candidate = extractBalancedJSON(openaiRaw);
      if (candidate) {
        try { parsed = JSON.parse(candidate); } catch (e2) { parsed = null; }
      }
    }

    // If parsed, coerce and validate
    if (parsed && typeof parsed === 'object') {
      // Coerce numeric
      const parsedPriceAUD = (parsed.priceAUD !== undefined && parsed.priceAUD !== null) ? Number(parsed.priceAUD) : null;
      const roundedParsed = Number.isFinite(parsedPriceAUD) ? Number(parsedPriceAUD.toFixed(2)) : null;

      // Validation tolerance: accept if difference < 0.03 AUD (3 cents) to allow rounding noise
      const tolerance = 0.03;
      const isValid = (roundedParsed !== null && outPriceAUD !== null && Math.abs(roundedParsed - outPriceAUD) <= tolerance);

      if (isValid && typeof parsed.narrative === 'string' && parsed.narrative.trim()) {
        // Good: return OpenAI phrasing but ensure numeric fields are canonical
        const out = {
          narrative: parsed.narrative.trim(),
          priceUSD: outPriceUSD,
          usdToAud: outFx,
          priceAUD: outPriceAUD,
          priceTimestamp: parsed.priceTimestamp || priceTimestamp || null,
          generatedAt: parsed.generatedAt || nowIso
        };
        return { statusCode: 200, body: JSON.stringify(out) };
      } else {
        // Validation failed — log details and fall back to deterministic narrative
        console.warn('OpenAI response failed numeric validation. computed=', outPriceAUD, 'openai_price=', roundedParsed, 'valid=', isValid);
        console.warn('OpenAI raw content (first 1000 chars):', openaiRaw.slice(0, 1000));
        return {
          statusCode: 200,
          body: JSON.stringify({
            narrative: deterministicNarrative,
            priceUSD: outPriceUSD,
            usdToAud: outFx,
            priceAUD: outPriceAUD,
            priceTimestamp: priceTimestamp || null,
            generatedAt: nowIso
          })
        };
      }
    }

    // If we couldn't parse, log and return deterministic
    console.error('OpenAI returned unparsable content for morning-brief. Raw (truncated):', openaiRaw.slice(0, 2000));
    return {
      statusCode: 200,
      body: JSON.stringify({
        narrative: deterministicNarrative,
        priceUSD: outPriceUSD,
        usdToAud: outFx,
        priceAUD: outPriceAUD,
        priceTimestamp: priceTimestamp || null,
        generatedAt: nowIso
      })
    };

  } catch (err) {
    console.error('morning-brief error:', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && (err.message || 'Server error') }) };
  }
};