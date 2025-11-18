// netlify/functions/morning-brief.js
// Morning brief: Metals-API (symbols=XAU only) -> FX fallbacks -> Yahoo fallback.
// Returns: { narrative, priceUSD, usdToAud, priceAUD, priceTimestamp, generatedAt, _debug }
// Requires METALS_API_KEY env var.
exports.handler = async function (event) {
  const nowIso = new Date().toISOString();
  const CACHE_TTL_MS = 60 * 1000;
  const CACHE_PATH = '/tmp/morning-brief-cache.json';

  async function fetchDebug(url, opts = {}, timeout = 9000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(id);
      const text = await res.text().catch(() => '');
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch (e) { json = null; }
      return { ok: res.ok, status: res.status, text: text.slice(0, 2000), json };
    } catch (err) {
      clearTimeout(id);
      return { ok: false, status: null, error: err && err.message ? err.message : String(err) };
    }
  }

  function readCache() {
    try {
      const fs = require('fs');
      if (!fs.existsSync(CACHE_PATH)) return null;
      const raw = fs.readFileSync(CACHE_PATH, 'utf8');
      const parsed = JSON.parse(raw);
      if (!parsed || !parsed.ts) return null;
      if ((Date.now() - parsed.ts) > CACHE_TTL_MS) return null;
      return parsed.data;
    } catch (e) {
      return null;
    }
  }
  function writeCache(data) {
    try {
      const fs = require('fs');
      fs.writeFileSync(CACHE_PATH, JSON.stringify({ ts: Date.now(), data }), 'utf8');
    } catch (e) { /* ignore */ }
  }

  const fmt = (n) => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  try {
    // quick cache
    const cached = readCache();
    if (cached) {
      cached._debug = cached._debug || {};
      cached._debug.cached = true;
      return { statusCode: 200, body: JSON.stringify(cached) };
    }

    const debug = { steps: [] };
    let priceUSD = null;
    let priceTimestamp = null;

    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (METALS_API_KEY) {
      // Request only XAU (avoid asking for derived/special symbols)
      const metaUrl = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU`;
      const mres = await fetchDebug(metaUrl, {}, 10000);
      debug.steps.push({ source: 'metals-api', ok: !!mres.ok, status: mres.status });
      if (mres.ok && mres.json && mres.json.rates) {
        const rates = mres.json.rates;
        // rates.XAU may be either:
        // - a small fraction (XAU per USD) => priceUSD = 1 / rates.XAU
        // - or directly USD per XAU (large number) => priceUSD = rates.XAU
        const v = rates.XAU;
        if (typeof v === 'number' && v > 0) {
          if (v < 1) priceUSD = 1 / v;
          else priceUSD = v;
        }
        if (mres.json.timestamp) priceTimestamp = new Date(mres.json.timestamp * 1000).toISOString();
        debug.ratesPreview = rates;
      } else {
        // Save body preview and error (helps diagnose invalid_symbol)
        debug.steps.push({ source: 'metals-api-body', status: mres.status, textPreview: mres.text ? mres.text.slice(0,400) : null, error: mres.error || null });
      }
    } else {
      debug.steps.push({ source: 'metals-api', ok: false, note: 'METALS_API_KEY missing' });
    }

    // If Metals-API didn't produce a price, fallback to Yahoo GC=F
    if (priceUSD === null) {
      try {
        const yf = await fetchDebug('https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC=F', {}, 7000);
        debug.steps.push({ source: 'yahoo_gc_f', ok: !!yf.ok, status: yf.status });
        if (yf.ok && yf.json && yf.json.quoteResponse && Array.isArray(yf.json.quoteResponse.result) && yf.json.quoteResponse.result.length) {
          const r = yf.json.quoteResponse.result[0];
          if (r && typeof r.regularMarketPrice === 'number') {
            priceUSD = Number(r.regularMarketPrice);
            if (r.regularMarketTime) priceTimestamp = new Date(r.regularMarketTime * 1000).toISOString();
          }
        } else {
          debug.steps.push({ source: 'yahoo_body_preview', preview: yf.text ? yf.text.slice(0,500) : null });
        }
      } catch (e) {
        debug.steps.push({ source: 'yahoo_error', error: e && e.message ? e.message : String(e) });
      }
    }

    // FX: prefer open.er-api.com then exchangerate.host then exchangerate-api.com
    let usdToAud = null;
    const fxCandidates = [
      { name: 'open.er-api.com', url: 'https://open.er-api.com/v6/latest/USD' },
      { name: 'exchangerate.host', url: 'https://api.exchangerate.host/latest?base=USD&symbols=AUD' },
      { name: 'exchangerate-api.com', url: 'https://api.exchangerate-api.com/v4/latest/USD' }
    ];
    for (const c of fxCandidates) {
      try {
        const fres = await fetchDebug(c.url, {}, 8000);
        debug.steps.push({ source: c.name, ok: !!fres.ok, status: fres.status });
        if (fres.ok && fres.json && fres.json.rates && typeof fres.json.rates.AUD === 'number') {
          usdToAud = Number(fres.json.rates.AUD);
          debug.fxSource = c.name;
          break;
        } else {
          debug.steps.push({ source: c.name + '-body', preview: fres.text ? fres.text.slice(0,400) : null });
        }
      } catch (e) {
        debug.steps.push({ source: c.name + '-error', error: e && e.message ? e.message : String(e) });
      }
    }

    const outPriceUSD = fmt(priceUSD);
    const outFx = fmt(usdToAud);
    const outPriceAUD = (outPriceUSD !== null && outFx !== null) ? fmt(outPriceUSD * outFx) : null;

    const narrative = outPriceAUD !== null
      ? `The spot gold price is currently $${outPriceAUD} AUD per ounce.`
      : `The spot gold price is currently unavailable.`;

    const payload = {
      narrative,
      priceUSD: outPriceUSD,
      usdToAud: outFx,
      priceAUD: outPriceAUD,
      priceTimestamp: priceTimestamp || null,
      generatedAt: nowIso,
      _debug: debug
    };

    if (outPriceAUD !== null) writeCache(payload);
    console.log('morning-brief:', { priceUSD: outPriceUSD, usdToAud: outFx, priceAUD: outPriceAUD, fxSource: payload._debug.fxSource || null });

    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    console.error('morning-brief error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && (err.message || 'Server error') }) };
  }
};