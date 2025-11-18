// netlify/functions/morning-brief.js
// Simple morning brief: fetch live gold (XAU) USD price and USD->AUD FX rate, compute AUD price,
// ask OpenAI to render a one-line narrative, return JSON.
//
// Requires env var: OPENAI_API_KEY
export async function handler(event) {
  try {
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
    if (!OPENAI_API_KEY) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: 'Missing OPENAI_API_KEY in environment.' })
      };
    }

    // Helper fetch with timeout
    async function fetchWithTimeout(url, opts = {}, timeout = 6000) {
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

    // 1) Get XAU -> USD price (spot) from Yahoo Finance
    // Symbol used: XAUUSD=X
    let priceUSD = null;
    let priceTimestamp = null;
    try {
      const yf = await fetchWithTimeout('https://query1.finance.yahoo.com/v7/finance/quote?symbols=XAUUSD=X', {}, 6000);
      if (yf.ok) {
        const j = await yf.json();
        const r = j?.quoteResponse?.result?.[0];
        if (r && typeof r.regularMarketPrice === 'number') {
          priceUSD = Number(r.regularMarketPrice);
          // Yahoo provides market time in regularMarketTime (unix)
          if (r.regularMarketTime) {
            priceTimestamp = new Date(r.regularMarketTime * 1000).toISOString();
          }
        }
      }
    } catch (e) {
      console.warn('Yahoo price fetch failed:', e && e.message);
    }

    // 2) Get USD -> AUD rate using exchangerate.host (free)
    let usdToAud = null;
    try {
      const fxRes = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 5000);
      if (fxRes.ok) {
        const fx = await fxRes.json();
        if (fx && fx.rates && typeof fx.rates.AUD === 'number') {
          usdToAud = Number(fx.rates.AUD);
        }
      }
    } catch (e) {
      console.warn('FX fetch failed:', e && e.message);
    }

    // If we have both values compute price; else we will return fallback
    let priceAUD = null;
    if (priceUSD !== null && usdToAud !== null) {
      priceAUD = priceUSD * usdToAud;
    }

    // Format values (if present)
    const fmtNumber = (n) => (typeof n === 'number' && !isNaN(n)) ? Number(n.toFixed(2)) : null;
    const outPriceUSD = fmtNumber(priceUSD);
    const outFx = fmtNumber(usdToAud);
    const outPriceAUD = fmtNumber(priceAUD);
    const nowIso = new Date().toISOString();

    // Build a short prompt telling OpenAI to only craft a ONE-LINER, using the numeric values we supply.
    // We do NOT ask it to guess numbers — pass the numbers we fetched.
    let narrative = null;
    try {
      // Build the system/user messages
      const system = { role: 'system', content: 'You are a concise reporter. Return a single sentence (one line) that states the spot gold price in AUD using the numeric value provided. Do not add extra commentary.' };
      const userContent = (outPriceAUD !== null)
        ? `Using the following numeric values, output one short sentence exactly of the form "The spot gold price is currently $X AUD per ounce." where X is the AUD price rounded to 2 decimals. Values: priceUSD=${outPriceUSD}, usdToAud=${outFx}, priceAUD=${outPriceAUD}, timestamp=${priceTimestamp || nowIso}.`
        : `We could not fetch live price data. If you do not have a numeric price, output the single sentence: "The spot gold price is currently unavailable." and nothing else.`;
      const user = { role: 'user', content: userContent };

      // Call OpenAI
      const resp = await fetchWithTimeout('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: [system, user],
          max_tokens: 60,
          temperature: 0.0
        })
      }, 7000);

      if (resp.ok) {
        const data = await resp.json();
        const content = data?.choices?.[0]?.message?.content;
        if (typeof content === 'string') {
          narrative = content.trim().replace(/\s+/g, ' ');
        }
      } else {
        console.warn('OpenAI returned non-ok status', resp.status);
      }
    } catch (e) {
      console.warn('OpenAI call failed:', e && e.message);
    }

    // If narrative still empty, fallback to a deterministic string using the computed number (or unavailable)
    if (!narrative) {
      if (outPriceAUD !== null) {
        narrative = `The spot gold price is currently $${outPriceAUD} AUD per ounce.`;
      } else {
        narrative = `The spot gold price is currently unavailable.`;
      }
    }

    // Return structured JSON to client
    const payload = {
      narrative,
      priceUSD: outPriceUSD,
      usdToAud: outFx,
      priceAUD: outPriceAUD,
      priceTimestamp: priceTimestamp || null,
      generatedAt: nowIso
    };

    return {
      statusCode: 200,
      body: JSON.stringify(payload)
    };

  } catch (err) {
    console.error('morning-brief handler error', err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
}