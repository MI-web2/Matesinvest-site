// netlify/functions/morning-brief.js
// Morning brief (Metals-API + robust FX fallbacks + caching).
// Requires METALS_API_KEY env var. Uses exchangerate.host and open.er-api.com as FX sources.
// Returns: { narrative, priceUSD, usdToAud, priceAUD, priceTimestamp, generatedAt, _debug }
exports.handler = async function (event) {
  const nowIso = new Date().toISOString();
  const CACHE_TTL_MS = 60 * 1000; // 60s cache to smooth transient failures (adjust as needed)
  const CACHE_PATH = '/tmp/morning-brief-cache.json';

  // helper: fetch with timeout and return { ok, status, text, json, error }
  async function fetchDebug(url, opts = {}, timeout = 8000) {
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

  // Write/read short cache in /tmp
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

  // Numeric formatter
  const fmt = (n) => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  try {
    // If cached and fresh, return it
    const cached = readCache();
    if (cached) {
      // indicate we returned cached value
      const outCached = { ...cached, _debug: { cached: true } };
      return { statusCode: 200, body: JSON.stringify(outCached) };
    }

    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    let priceUSD = null;
    let priceTimestamp = null;
    const debug = { steps: [] };

    // 1) Metals-API price
    if (METALS_API_KEY) {
      const metaUrl = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU,USDXAU`;
      const mres = await fetchDebug(metaUrl, {}, 9000);
      debug.steps.push({ source: 'metals-api', result: mres.status || null, ok: !!mres.ok });
      if (mres.ok && mres.json && mres.json.rates) {
        const rates = mres.json.rates;
        // Two common shapes:
        // - rates.XAU is XAU per USD (small fraction) => priceUSD = 1 / rates.XAU
        // - rates.XAU is USD per XAU (large number) => priceUSD = rates.XAU
        const v = rates.XAU;
        if (typeof v === 'number' && v > 0) {
          if (v < 1) priceUSD = 1 / v;
          else priceUSD = v;
        }
        // Some accounts include explicit USDXAU or similar; prefer explicit if present and sensible
        if ((!priceUSD || !isFinite(priceUSD)) && typeof rates.USDXAU === 'number' && rates.USDXAU > 0) {
          priceUSD = rates.USDXAU;
        }
        if (mres.json.timestamp) priceTimestamp = new Date(mres.json.timestamp * 1000).toISOString();
      } else {
        debug.steps.push({ note: 'metals-api-missing-or-nonok', status: mres.status, error: mres.error || null });
      }
    } else {
      debug.steps.push({ note: 'METALS_API_KEY_missing' });
    }

    // 2) FX - try exchangerate.host, then open.er-api.com
    let usdToAud = null;
    // First attempt
    const fx1 = await fetchDebug('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 7000);
    debug.steps.push({ source: 'exchangerate.host', ok: !!fx1.ok, status: fx1.status });
    if (fx1.ok && fx1.json && fx1.json.rates && typeof fx1.json.rates.AUD === 'number') {
      usdToAud = Number(fx1.json.rates.AUD);
      debug.fxSource = 'exchangerate.host';
    } else {
      // fallback attempt
      const fx2 = await fetchDebug('https://open.er-api.com/v6/latest/USD', {}, 7000);
      debug.steps.push({ source: 'open.er-api.com', ok: !!fx2.ok, status: fx2.status });
      if (fx2.ok && fx2.json && fx2.json.rates && typeof fx2.json.rates.AUD === 'number') {
        usdToAud = Number(fx2.json.rates.AUD);
        debug.fxSource = 'open.er-api.com';
      } else {
        // additional fallback: try exchangerate-api.com (public v4 endpoint)
        const fx3 = await fetchDebug('https://api.exchangerate-api.com/v4/latest/USD', {}, 7000);
        debug.steps.push({ source: 'exchangerate-api.com', ok: !!fx3.ok, status: fx3.status });
        if (fx3.ok && fx3.json && fx3.json.rates && typeof fx3.json.rates.AUD === 'number') {
          usdToAud = Number(fx3.json.rates.AUD);
          debug.fxSource = 'exchangerate-api.com';
        } else {
          debug.fxSource = 'none';
        }
      }
    }

    const outPriceUSD = fmt(priceUSD);
    const outFx = fmt(usdToAud);
    const outPriceAUD = (outPriceUSD !== null && outFx !== null) ? fmt(outPriceUSD * outFx) : null;

    // Deterministic narrative
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
      _debug: {
        steps: debug.steps,
        fxSource: debug.fxSource || null
      }
    };

    // If we successfully computed priceAUD, cache it briefly
    if (outPriceAUD !== null) {
      writeCache(payload);
    }

    // Log short summary server-side
    console.log('morning-brief:', { priceUSD: outPriceUSD, usdToAud: outFx, priceAUD: outPriceAUD, fxSource: payload._debug.fxSource });

    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    console.error('morning-brief handler error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && (err.message || 'Server error') }) };
  }
};