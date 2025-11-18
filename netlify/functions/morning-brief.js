// netlify/functions/morning-brief.js
// Morning brief: Metals-API for XAU/USD + robust FX sourcing (open.er-api.com primary),
// with quick /tmp caching to smooth transient provider failures.
// Requires: METALS_API_KEY env var
// Returns: { narrative, priceUSD, usdToAud, priceAUD, priceTimestamp, generatedAt, _debug }

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();
  const CACHE_TTL_MS = 60 * 1000; // 60s
  const CACHE_PATH = '/tmp/morning-brief-cache.json';

  // helper: fetch with timeout & simple parsed result
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

  // cache helpers (very small /tmp cache)
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
      console.warn('cache read failed', e && e.message);
      return null;
    }
  }
  function writeCache(data) {
    try {
      const fs = require('fs');
      fs.writeFileSync(CACHE_PATH, JSON.stringify({ ts: Date.now(), data }), 'utf8');
    } catch (e) {
      console.warn('cache write failed', e && e.message);
    }
  }

  const fmt = (n) => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  try {
    // Return cached if fresh
    const cached = readCache();
    if (cached) {
      cached._debug = cached._debug || {};
      cached._debug.cached = true;
      return { statusCode: 200, body: JSON.stringify(cached) };
    }

    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    const debug = { steps: [] };

    // 1) Fetch metals price (Metals-API)
    let priceUSD = null;
    let priceTimestamp = null;
    if (!METALS_API_KEY) {
      debug.steps.push({ source: 'metals-api', ok: false, note: 'METALS_API_KEY missing' });
    } else {
      const metaUrl = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU,USDXAU`;
      const mres = await fetchDebug(metaUrl, {}, 10000);
      debug.steps.push({ source: 'metals-api', ok: !!mres.ok, status: mres.status });
      if (mres.ok && mres.json && mres.json.rates) {
        const rates = mres.json.rates;
        // common shapes: rates.XAU either small fraction (XAU per USD) or large (USD per XAU).
        if (typeof rates.XAU === 'number') {
          const v = rates.XAU;
          if (v > 0 && v < 1) priceUSD = 1 / v;
          else if (v >= 1) priceUSD = v;
        }
        // explicit USDXAU (USD per XAU) if provided
        if ((!priceUSD || !isFinite(priceUSD)) && typeof rates.USDXAU === 'number' && rates.USDXAU > 0) {
          priceUSD = Number(rates.USDXAU);
        }
        if (mres.json.timestamp) priceTimestamp = new Date(mres.json.timestamp * 1000).toISOString();
        if (mres.json.rates) debug.ratesPreview = mres.json.rates;
      } else {
        debug.steps.push({ source: 'metals-api-body', textPreview: mres.text ? mres.text.slice(0,300) : null, error: mres.error || null });
      }
    }

    // 2) FX: try primary open.er-api.com, then exchangerate.host, then exchangerate-api.com
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
          // include body preview for diagnosis
          debug.steps.push({ source: c.name + '-body-preview', preview: fres.text ? fres.text.slice(0,300) : null });
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
      : 'The spot gold price is currently unavailable.';

    const payload = {
      narrative,
      priceUSD: outPriceUSD,
      usdToAud: outFx,
      priceAUD: outPriceAUD,
      priceTimestamp: priceTimestamp || null,
      generatedAt: nowIso,
      _debug: debug
    };

    // cache successful computed payload briefly
    if (outPriceAUD !== null) writeCache(payload);

    console.log('morning-brief summary:', { priceUSD: outPriceUSD, usdToAud: outFx, priceAUD: outPriceAUD, fxSource: payload._debug.fxSource });
    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    console.error('morning-brief error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && (err.message || 'Server error') }) };
  }
};