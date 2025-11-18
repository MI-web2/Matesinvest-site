// netlify/functions/morning-brief.js
// Minimal morning brief: use Metals-API to get XAU/USD then converter to AUD and return JSON.
// Requires env var: METALS_API_KEY
//
// Returns:
// { priceAUD, priceUSD, usdToAud, priceTimestamp, generatedAt, narrative }
//
// Notes:
// - metals-api may return rates.XAU as XAU per USD (fraction). We handle both common variants.
// - Uses exchangerate.host for USD->AUD (no key required).
// - Deterministic fallback if providers fail.

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

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

  // Numeric formatting helper
  const fmt = (n) => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    let priceUSD = null;
    let priceTimestamp = null;

    if (METALS_API_KEY) {
      try {
        const url = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU`;
        const r = await fetchWithTimeout(url, {}, 9000);
        if (r.ok) {
          const j = await r.json();
          // metals-api commonly returns rates.XAU. Depending on plan it may be XAU per USD (small fraction)
          // or USD per XAU (large number). Handle both cases.
          if (j && j.rates && typeof j.rates.XAU === 'number') {
            const v = Number(j.rates.XAU);
            // If value is a small fraction (<1), treat as XAU per USD -> priceUSD = 1 / v
            if (v > 0 && v < 1) {
              priceUSD = 1 / v;
            } else if (v >= 1) {
              // If it's >= 1 (e.g., 2000), assume it's USD per XAU already
              priceUSD = v;
            }
            if (j.timestamp) {
              priceTimestamp = new Date(j.timestamp * 1000).toISOString();
            }
          } else {
            console.warn('metals-api: unexpected response shape', Object.keys(j || {}).slice(0,10));
          }
        } else {
          const body = await r.text().catch(() => '');
          console.warn('metals-api non-ok', r.status, body.slice(0,300));
        }
      } catch (e) {
        console.warn('metals-api fetch error', e && e.message);
      }
    } else {
      console.warn('METALS_API_KEY not set');
    }

    // If metals-api did not yield a price, try a quick fallback to Yahoo GC=F (best-effort, unofficial)
    if (priceUSD === null) {
      try {
        const yf = await fetchWithTimeout('https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC=F', {}, 7000);
        if (yf.ok) {
          const j = await yf.json();
          const r = j?.quoteResponse?.result?.[0];
          if (r && typeof r.regularMarketPrice === 'number') {
            priceUSD = Number(r.regularMarketPrice);
            if (r.regularMarketTime) priceTimestamp = new Date(r.regularMarketTime * 1000).toISOString();
          } else {
            console.warn('Yahoo GC=F returned unexpected payload');
          }
        } else {
          console.warn('Yahoo GC=F non-ok', yf.status);
        }
      } catch (e) {
        console.warn('Yahoo GC=F fetch error', e && e.message);
      }
    }

    // FX: USD -> AUD
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
      console.warn('exchangerate.host fetch error', e && e.message);
    }

    const outPriceUSD = fmt(priceUSD);
    const outFx = fmt(usdToAud);
    const outPriceAUD = (outPriceUSD !== null && outFx !== null) ? fmt(outPriceUSD * outFx) : null;

    const narrative = outPriceAUD !== null
      ? `The spot gold price is currently $${outPriceAUD} AUD per ounce.`
      : 'The spot gold price is currently unavailable.';

    const payload = {
      priceAUD: outPriceAUD,
      priceUSD: outPriceUSD,
      usdToAud: outFx,
      priceTimestamp: priceTimestamp || null,
      generatedAt: nowIso,
      narrative
    };

    return { statusCode: 200, body: JSON.stringify(payload) };
  } catch (err) {
    console.error('morning-brief error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && (err.message || 'Server error') }) };
  }
};