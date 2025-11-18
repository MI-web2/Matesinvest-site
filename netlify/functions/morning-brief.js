// netlify/functions/morning-brief.js
// Morning brief for multiple metals: fetch live prices, get yesterday snapshot from Upstash,
// compute pct change per symbol and return structured JSON.
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Symbols used: XAU, XAG, IRON, LITHIUM, NI, URANIUM
// The response contains a `metals` object keyed by symbol with priceUSD, priceAUD, yesterdayPriceAUD, pctChange.

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  async function fetchWithTimeout(url, opts = {}, timeout = 9000) {
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

  const fmt = n => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  // Upstash helpers
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;
  async function redisGet(key) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
    try {
      const res = await fetchWithTimeout(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
        headers: { 'Authorization': `Bearer ${UPSTASH_TOKEN}` }
      }, 7000);
      if (!res.ok) return null;
      const j = await res.json().catch(()=>null);
      if (!j || typeof j.result === 'undefined') return null;
      return j.result;
    } catch (e) {
      console.warn('redisGet error', e && e.message);
      return null;
    }
  }

  // symbols list
  const symbols = ['XAU','XAG','IRON','LITHIUM','NI','URANIUM'];
  const debug = { steps: [] };

  try {
    // 1) fetch current metals rates from Metals-API
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    let currentUsd = {}; // symbol -> USD price (number|null)
    let priceTimestamp = null;
    if (METALS_API_KEY) {
      try {
        const metaUrl = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=${encodeURIComponent(symbols.join(','))}`;
        const mres = await fetchWithTimeout(metaUrl, {}, 10000);
        const mtxt = await mres.text().catch(()=>'');
        let mj = null;
        try { mj = mtxt ? JSON.parse(mtxt) : null; } catch(e){ mj = null; }
        debug.steps.push({ source: 'metals-api', ok: !!mres.ok, status: mres.status });
        if (mres.ok && mj && mj.rates) {
          const rates = mj.rates;
          // parse price logic (handles fraction vs direct)
          for (const s of symbols) {
            const rv = rates[s];
            if (typeof rv === 'number') {
              if (rv > 0 && rv < 1) currentUsd[s] = 1 / rv;
              else currentUsd[s] = rv;
            } else {
              currentUsd[s] = null;
            }
          }
          if (mj.timestamp) priceTimestamp = new Date(mj.timestamp * 1000).toISOString();
          debug.ratesPreview = mj.rates;
        } else {
          debug.steps.push({ source: 'metals-api-body', preview: mtxt.slice(0,400) });
        }
      } catch (e) {
        debug.steps.push({ source: 'metals-api-error', error: e && e.message });
      }
    } else {
      debug.steps.push({ source: 'metals-api', note: 'METALS_API_KEY missing' });
      for (const s of symbols) currentUsd[s] = null;
    }

    // 2) FX USD -> AUD (try open.er-api.com then exchangerate.host)
    let usdToAud = null;
    try {
      let fRes = await fetchWithTimeout('https://open.er-api.com/v6/latest/USD', {}, 7000);
      let ftxt = await fRes.text().catch(()=>'');
      let fj = null;
      try { fj = ftxt ? JSON.parse(ftxt) : null; } catch(e){ fj = null; }
      if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') {
        usdToAud = Number(fj.rates.AUD);
        debug.fxSource = 'open.er-api.com';
      } else {
        // fallback
        fRes = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 7000);
        ftxt = await fRes.text().catch(()=>'');
        try { fj = ftxt ? JSON.parse(ftxt) : null; } catch(e){ fj = null; }
        if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') {
          usdToAud = Number(fj.rates.AUD);
          debug.fxSource = 'exchangerate.host';
        } else {
          debug.steps.push({ source: 'fx-bodies', preview: ftxt.slice(0,300) });
        }
      }
    } catch (e) {
      debug.steps.push({ source: 'fx-error', error: e && e.message });
    }

    // compute current AUD prices per symbol
    const currentAud = {};
    for (const s of symbols) {
      const pUsd = currentUsd[s];
      currentAud[s] = (typeof pUsd === 'number' && typeof usdToAud === 'number') ? Number((pUsd * usdToAud).toFixed(2)) : null;
    }

    // 3) read yesterday's snapshot from Upstash
    let yesterdayData = null;
    try {
      const d = new Date();
      const yd = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - 1));
      const key = `metals:${yd.toISOString().slice(0,10)}`; // metals:YYYY-MM-DD
      const val = await redisGet(key);
      if (val) {
        try { yesterdayData = JSON.parse(val); } catch (e) { yesterdayData = null; }
      }
      debug.steps.push({ source: 'redis-get', key, found: !!yesterdayData });
    } catch (e) {
      debug.steps.push({ source: 'redis-get-error', error: e && e.message });
    }

    // 4) assemble per-symbol result: priceUSD, priceAUD, yesterdayPriceAUD, pctChange
    const metals = {};
    for (const s of symbols) {
      const todayAUD = currentAud[s];
      let yesterdayAUD = null;
      if (yesterdayData && yesterdayData.symbols && typeof yesterdayData.symbols[s] !== 'undefined') {
        const p = yesterdayData.symbols[s] && typeof yesterdayData.symbols[s].priceAUD !== 'undefined' ? yesterdayData.symbols[s].priceAUD : null;
        if (p !== null) yesterdayAUD = typeof p === 'number' ? p : Number(p);
      }
      let pctChange = null;
      if (todayAUD !== null && yesterdayAUD !== null && yesterdayAUD !== 0) {
        pctChange = Number(((todayAUD - yesterdayAUD) / yesterdayAUD * 100).toFixed(2));
      }
      metals[s] = {
        priceUSD: fmt(currentUsd[s]),
        priceAUD: fmt(todayAUD),
        yesterdayPriceAUD: yesterdayAUD !== null ? fmt(yesterdayAUD) : null,
        pctChange,
        priceTimestamp: priceTimestamp || null
      };
    }

    // 5) create short narratives (one-liners) and top-level summary
    const narratives = {};
    for (const s of symbols) {
      const m = metals[s];
      if (m.priceAUD === null) {
        narratives[s] = `The ${s} price is currently unavailable.`;
      } else {
        const upDown = (m.pctChange === null) ? '' : (m.pctChange > 0 ? ` — up ${Math.abs(m.pctChange)}% vs yesterday` : (m.pctChange < 0 ? ` — down ${Math.abs(m.pctChange)}% vs yesterday` : ' — unchanged vs yesterday'));
        narratives[s] = `${s} is currently $${m.priceAUD} AUD per unit${upDown}.`;
      }
    }

    const payload = {
      generatedAt: nowIso,
      usdToAud: fmt(usdToAud),
      metals,
      narratives,
      _debug: debug
    };

    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    console.error('morning-brief multi error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message || String(err) }) };
  }
};