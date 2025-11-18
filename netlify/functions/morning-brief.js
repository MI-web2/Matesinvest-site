// netlify/functions/morning-brief.js
// Morning brief: fetch live gold (XAU) USD price and USD->AUD FX rate, then ask OpenAI
// to return a short JSON payload (narrative + numeric fields) using the same pattern
// as the working matesSummary function.
//
// This function is designed to be invoked via GET from the client (no method check).
// It follows the matesSummary OpenAI usage style (system message + user prompt, model, etc).
//
// Requires env var: OPENAI_API_KEY (and optional NEWSAPI_KEY if you later want headlines)
// Returns JSON: { narrative, priceUSD, usdToAud, priceAUD, priceTimestamp, generatedAt }
// If OpenAI fails or returns invalid JSON, a deterministic fallback is returned.

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

    // 1) Get XAU -> USD price (spot) from Yahoo Finance (XAUUSD=X)
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
        console.warn('Yahoo XAU request non-ok', yf.status);
      }
    } catch (e) {
      console.warn('Yahoo XAU fetch error:', e && e.message);
    }

    // 1b) Fallback: GC=F if XAUUSD didn't return a price
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

    // 2) Get USD -> AUD rate using exchangerate.host (fallbacks possible)
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

    // 2b) Fallback FX: open.er-api.com
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

    // Compute AUD price if both numbers present
    const fmtNumber = (n) => (typeof n === 'number' && !isNaN(n)) ? Number(n.toFixed(2)) : null;
    const outPriceUSD = fmtNumber(priceUSD);
    const outFx = fmtNumber(usdToAud);
    const outPriceAUD = (outPriceUSD !== null && outFx !== null) ? fmtNumber(outPriceUSD * outFx) : null;
    const nowIso = new Date().toISOString();

    // If we have numeric data and OPENAI_API_KEY is present, call OpenAI using the same pattern as matesSummary
    let openaiPayload = null;
    if (OPENAI_API_KEY && outPriceAUD !== null) {
      const prompt = `
You are a concise financial reporter. Given the numeric values provided, return a single JSON object (no commentary, no surrounding text, no code fences) with exactly these keys:
- "narrative": a single short sentence stating the spot gold price in AUD, matching this exact form: "The spot gold price is currently $X AUD per ounce." where X is the AUD price rounded to 2 decimals.
- "priceUSD": numeric (the USD price used)
- "usdToAud": numeric (the FX rate used)
- "priceAUD": numeric (the AUD price used)
- "priceTimestamp": ISO timestamp string if available or null
- "generatedAt": ISO timestamp string for when the brief was generated

Values to use (do not guess or change the numbers):
priceUSD=${outPriceUSD}
usdToAud=${outFx}
priceAUD=${outPriceAUD}
priceTimestamp=${priceTimestamp || nowIso}

Return ONLY the JSON object.
`.trim();

      try {
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${OPENAI_API_KEY}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            model: 'gpt-4.1-mini',
            messages: [
              { role: 'system', content: 'You respond only with strict JSON. No extra commentary.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.4,
            max_tokens: 150
          })
        });

        if (!res.ok) {
          const txt = await res.text().catch(() => '<no-body>');
          console.error('OpenAI non-ok:', res.status, txt);
        } else {
          const data = await res.json().catch(() => null);
          const content = data?.choices?.[0]?.message?.content || null;
          if (content) {
            try {
              // Parse content; matesSummary pattern expects strict JSON so try JSON.parse
              const parsed = JSON.parse(content);
              openaiPayload = parsed;
            } catch (e) {
              console.error('OpenAI content parse error. Raw content:', content);
            }
          } else {
            console.warn('OpenAI returned no content');
          }
        }
      } catch (e) {
        console.error('OpenAI call error:', e && e.message);
      }
    } else {
      if (!OPENAI_API_KEY) console.warn('OPENAI_API_KEY missing; skipping OpenAI phrasing');
      if (outPriceAUD === null) console.warn('Numeric price not available; skipping OpenAI phrasing');
    }

    // If OpenAI returned a valid payload, normalise it to expected shape; otherwise produce deterministic fallback
    if (openaiPayload && typeof openaiPayload === 'object') {
      // Ensure numeric fields are numbers (try to coerce)
      const normalised = {
        narrative: typeof openaiPayload.narrative === 'string' ? openaiPayload.narrative : (outPriceAUD !== null ? `The spot gold price is currently $${outPriceAUD} AUD per ounce.` : 'The spot gold price is currently unavailable.'),
        priceUSD: Number(openaiPayload.priceUSD) || outPriceUSD,
        usdToAud: Number(openaiPayload.usdToAud) || outFx,
        priceAUD: Number(openaiPayload.priceAUD) || outPriceAUD,
        priceTimestamp: openaiPayload.priceTimestamp || priceTimestamp || null,
        generatedAt: openaiPayload.generatedAt || nowIso
      };
      return {
        statusCode: 200,
        body: JSON.stringify(normalised)
      };
    }

    // Fallback deterministic payload (no OpenAI or OpenAI failed)
    const fallback = {
      narrative: outPriceAUD !== null ? `The spot gold price is currently $${outPriceAUD} AUD per ounce.` : `The spot gold price is currently unavailable.`,
      priceUSD: outPriceUSD,
      usdToAud: outFx,
      priceAUD: outPriceAUD,
      priceTimestamp: priceTimestamp || null,
      generatedAt: nowIso
    };

    return {
      statusCode: 200,
      body: JSON.stringify(fallback)
    };

  } catch (err) {
    console.error('morning-brief error:', err && (err.stack || err.message || err));
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err && (err.message || 'Server error') })
    };
  }
};