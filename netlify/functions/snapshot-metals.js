// netlify/functions/snapshot-metals.js
// Snapshot multiple metals and write to Upstash: metals:YYYY-MM-DD and metals:latest
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
// Optional env:
// - IRON_NORMALISATION_FACTOR  (e.g. 0.329) -- map Metals-API IRON -> USD/tonne
// - LITHIUM_UNIT (e.g. "LCE/t") -- unit label to store for LITH-CAR (default "LCE/t")
//
// Symbols stored: XAU, XAG, IRON, LITH-CAR, NI, URANIUM
// The code stores both the API raw USD (apiPriceUSD) and the final converted priceUSD.
// Lithium (LITH-CAR) is taken directly from Metals-API and converted from per-ounce -> per-tonne (oz -> tonne).

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

  // formatting helper
  const fmt = n => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) return { statusCode: 500, body: 'Missing METALS_API_KEY' };
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return { statusCode: 500, body: 'Missing Upstash env vars' };

    // Configurable items
    const NORMALISATION_FACTOR_IRON = parseFloat(process.env.IRON_NORMALISATION_FACTOR) || 0.329;
    const LITHIUM_UNIT = process.env.LITHIUM_UNIT || 'LCE/t'; // label stored for LITH-CAR

    // constants
    const OUNCES_PER_TONNE = 32150.7466; // troy ounces per metric tonne

    // fetch Metals-API using the LITH-CAR symbol (not LITHIUM)
    const fetchSymbols = ['XAU','XAG','IRON','LITH-CAR','NI','URANIUM'];
    const metaUrl = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=${encodeURIComponent(fetchSymbols.join(','))}`;

    const mRes = await fetchWithTimeout(metaUrl, {}, 10000);
    const mText = await mRes.text().catch(()=>'');

    let mJson = null;
    try { mJson = mText ? JSON.parse(mText) : null; } catch(e) { mJson = null; }

    function parseUsdFromRate(v) {
      if (typeof v !== 'number') return null;
      if (v > 0 && v < 1) return 1 / v; // API returned units per USD
      return v; // assume USD per unit
    }

    const rates = (mJson && mJson.rates) ? mJson.rates : null;
    const priceUsdMap = {};
    if (rates) {
      for (const s of fetchSymbols) {
        const rv = rates[s];
        priceUsdMap[s] = (typeof rv === 'number') ? parseUsdFromRate(rv) : null;
      }
    }

    // IRON normalisation (map to USD/tonne using NORMALISATION_FACTOR_IRON)
    if (typeof priceUsdMap['IRON'] === 'number') {
      const apiIronUsd = priceUsdMap['IRON'];
      const normalisedIronUsd = Number(apiIronUsd * NORMALISATION_FACTOR_IRON);
      priceUsdMap['IRON_apiRaw'] = apiIronUsd;
      priceUsdMap['IRON'] = normalisedIronUsd;
    }

    // LITH-CAR handling: take API LITH-CAR raw USD value (assumed per-ounce) and convert oz -> tonne
    if (typeof priceUsdMap['LITH-CAR'] === 'number') {
      const apiLithUsd = priceUsdMap['LITH-CAR'];
      priceUsdMap['LITH-CAR_apiRaw'] = apiLithUsd;
      // convert from USD per ounce -> USD per tonne
      const finalLithUsd = Number(apiLithUsd * OUNCES_PER_TONNE);
      priceUsdMap['LITH-CAR'] = finalLithUsd;
    }

    // FX to AUD
    let usdToAud = null;
    try {
      let fRes = await fetchWithTimeout('https://open.er-api.com/v6/latest/USD', {}, 7000);
      let ftxt = await fRes.text().catch(()=>'' );
      let fj = null;
      try { fj = ftxt ? JSON.parse(ftxt) : null; } catch(e){ fj = null; }
      if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') {
        usdToAud = Number(fj.rates.AUD);
      } else {
        fRes = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 7000);
        ftxt = await fRes.text().catch(()=>'' );
        try { fj = ftxt ? JSON.parse(ftxt) : null; } catch(e){ fj = null; }
        if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') usdToAud = Number(fj.rates.AUD);
      }
    } catch (e) {
      console.warn('fx fetch error', e && e.message);
    }

    // Build snapshot payload keys: XAU, XAG, IRON, LITH-CAR, NI, URANIUM
    const outputSymbols = ['XAU','XAG','IRON','LITH-CAR','NI','URANIUM'];
    const snapshot = { snappedAt: nowIso, usdToAud: usdToAud, symbols: {} };
    const timestampFromMetals = (mJson && mJson.timestamp) ? new Date(mJson.timestamp * 1000).toISOString() : null;

    for (const s of outputSymbols) {
      let pUsdApiRaw = null;
      let pUsdFinal = null;

      if (s === 'LITH-CAR') {
        pUsdApiRaw = (typeof priceUsdMap['LITH-CAR_apiRaw'] === 'number') ? priceUsdMap['LITH-CAR_apiRaw'] : null;
        pUsdFinal = (typeof priceUsdMap['LITH-CAR'] === 'number') ? priceUsdMap['LITH-CAR'] : null;
      } else {
        pUsdApiRaw = (typeof priceUsdMap[`${s}_apiRaw`] === 'number') ? Number(priceUsdMap[`${s}_apiRaw`]) : null;
        pUsdFinal = (typeof priceUsdMap[s] === 'number') ? Number(priceUsdMap[s]) : null;
      }

      const pAud = (pUsdFinal !== null && usdToAud !== null) ? Number((pUsdFinal * usdToAud).toFixed(2)) : null;

      const unit = (s === 'XAU' || s === 'XAG') ? 'oz' : (s === 'IRON' ? 'tonne' : (s === 'LITH-CAR' ? LITHIUM_UNIT : 'unit'));

      snapshot.symbols[s] = {
        apiPriceUSD: pUsdApiRaw === null ? null : Number(Number(pUsdApiRaw).toFixed(6)),
        priceUSD: pUsdFinal === null ? null : Number(Number(pUsdFinal).toFixed(2)),
        priceAUD: pAud,
        usdToAud: usdToAud === null ? null : Number(usdToAud.toFixed(6)),
        priceTimestamp: timestampFromMetals,
        unit
      };
    }

    // Backwards compatibility
    snapshot.metals = snapshot.symbols;

    // Persist under metals:YYYY-MM-DD and metals:latest (UTC date)
    const d = new Date();
    const key = `metals:${d.toISOString().slice(0,10)}`; // metals:YYYY-MM-DD
    const okToday = await redisSet(key, snapshot);
    const okLatest = await redisSet('metals:latest', snapshot);

    const debugPayload = {
      key,
      okToday,
      okLatest,
      normalisationFactorIron: NORMALISATION_FACTOR_IRON,
      lithiumUnit: LITHIUM_UNIT,
      payload: snapshot,
      metalsApiPreview: (mText ? mText.slice(0,2000) : null),
      metalsApiStatus: mRes.status
    };

    return {
      statusCode: 200,
      body: JSON.stringify(debugPayload)
    };

  } catch (err) {
    console.error('snapshot-metals error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message || String(err) }) };
  }
};