// netlify/functions/morning-brief.js
// Simple deterministic morning brief: fetch live gold (XAU) USD price and USD->AUD FX rate,
// compute AUD price and return a one-line narrative WITHOUT calling OpenAI (reliable).
//
// No env var required for OpenAI. Requires only NEWSAPI_KEY if you later use headlines (not required here).

export async function handler(event) {
  try {
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

    // 1) Get XAU -> USD price (spot) from Yahoo Finance
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
        console.warn('Yahoo returned non-ok', yf.status);
      }
    } catch (e) {
      console.warn('Yahoo price fetch failed:', e && e.message);
    }

    // 2) Get USD -> AUD rate using exchangerate.host
    let usdToAud = null;
    try {
      const fxRes = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 6000);
      if (fxRes.ok) {
        const fx = await fxRes.json();
        if (fx && fx.rates && typeof fx.rates.AUD === 'number') usdToAud = Number(fx.rates.AUD);
      } else {
        console.warn('FX provider returned non-ok', fxRes.status);
      }
    } catch (e) {
      console.warn('FX fetch failed:', e && e.message);
    }

    // Compute AUD price if possible
    let priceAUD = null;
    if (priceUSD !== null && usdToAud !== null) priceAUD = priceUSD * usdToAud;

    // Format utility
    const fmtNumber = (n) => (typeof n === 'number' && !isNaN(n)) ? Number(n.toFixed(2)) : null;
    const outPriceUSD = fmtNumber(priceUSD);
    const outFx = fmtNumber(usdToAud);
    const outPriceAUD = fmtNumber(priceAUD);
    const nowIso = new Date().toISOString();

    // Build deterministic narrative (no OpenAI)
    let narrative;
    if (outPriceAUD !== null) {
      narrative = `The spot gold price is currently $${outPriceAUD} AUD per ounce.`;
    } else {
      narrative = `The spot gold price is currently unavailable.`;
    }

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
    console.error('morning-brief handler error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err.message || 'Server error' }) };
  }
}