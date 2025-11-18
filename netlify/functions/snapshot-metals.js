// netlify/functions/snapshot-metals.js
// Snapshot multiple metals and write to Upstash: metals:YYYY-MM-DD and metals:latest
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Symbols stored: XAU, XAG, IRON, LITHIUM, NI, URANIUM
// (Assumes these symbols are supported in your Metals-API plan.)

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

  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;
  async function redisSet(key, value) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return false;
    try {
      const encoded = encodeURIComponent(JSON.stringify(value));
      const res = await fetchWithTimeout(`${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encoded}`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${UPSTASH_TOKEN}` }
      }, 8000);
      return res.ok;
    } catch (e) {
      console.warn('redisSet error', e && e.message);
      return false;
    }
  }

  // formatting helpers
  const fmt = n => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) return { statusCode: 500, body: 'Missing METALS_API_KEY' };
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return { statusCode: 500, body: 'Missing Upstash env vars' };

    // symbols to snapshot
    const symbols = ['XAU','XAG','IRON','LITHIUM','NI','URANIUM'];
    const metaUrl = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=${encodeURIComponent(symbols.join(','))}`;

    // 1) fetch metals rates
    const mRes = await fetchWithTimeout(metaUrl, {}, 10000);
    const mText = await mRes.text().catch(()=>'');
    let mJson = null;
    try { mJson = mText ? JSON.parse(mText) : null; } catch (e) { mJson = null; }

    // helper to convert API rate -> USD price
    function parseUsdFromRate(v) {
      if (typeof v !== 'number') return null;
      if (v > 0 && v < 1) return 1 / v; // API returned units per USD
      return v; // assume USD per unit
    }

    const rates = (mJson && mJson.rates) ? mJson.rates : null;
    const priceUsdMap = {};
    if (rates) {
      for (const s of symbols) {
        const rv = rates[s];
        priceUsdMap[s] = (typeof rv === 'number') ? parseUsdFromRate(rv) : null;
      }
    } else {
      // If metals-api failed, leave prices null — snapshot may still try FX for completeness.
    }

    // 2) FX to AUD (open.er-api.com primary, fallback exchangerate.host)
    let usdToAud = null;
    try {
      let fRes = await fetchWithTimeout('https://open.er-api.com/v6/latest/USD', {}, 7000);
      let ftxt = await fRes.text().catch(()=>'');
      let fj = null;
      try { fj = ftxt ? JSON.parse(ftxt) : null; } catch(e) { fj = null; }
      if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') {
        usdToAud = Number(fj.rates.AUD);
      } else {
        // fallback
        fRes = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 7000);
        ftxt = await fRes.text().catch(()=>'');
        try { fj = ftxt ? JSON.parse(ftxt) : null; } catch(e){ fj = null; }
        if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') usdToAud = Number(fj.rates.AUD);
      }
    } catch (e) {
      // ignore; usdToAud may remain null
      console.warn('fx fetch error', e && e.message);
    }

    // 3) build snapshot payload per symbol
    const snapshot = { snappedAt: nowIso, usdToAud: usdToAud, symbols: {} };
    const timestampFromMetals = (mJson && mJson.timestamp) ? new Date(mJson.timestamp * 1000).toISOString() : null;
    for (const s of symbols) {
      const pUsd = (typeof priceUsdMap[s] === 'number') ? Number(priceUsdMap[s]) : null;
      const pAud = (pUsd !== null && usdToAud !== null) ? Number((pUsd * usdToAud).toFixed(2)) : null;
      snapshot.symbols[s] = {
        priceUSD: pUsd === null ? null : Number(pUsd.toFixed(2)),
        priceAUD: pAud,
        usdToAud: usdToAud === null ? null : Number(usdToAud.toFixed(6)),
        priceTimestamp: timestampFromMetals
      };
    }

    // 4) persist under metals:YYYY-MM-DD and metals:latest (UTC date)
    const d = new Date();
    const key = `metals:${d.toISOString().slice(0,10)}`; // metals:YYYY-MM-DD
    const okToday = await redisSet(key, snapshot);
    const okLatest = await redisSet('metals:latest', snapshot);

    return {
      statusCode: 200,
      body: JSON.stringify({ key, okToday, okLatest, payload: snapshot })
    };

  } catch (err) {
    console.error('snapshot-metals error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message || String(err) }) };
  }
};