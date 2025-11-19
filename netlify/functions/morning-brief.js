// netlify/functions/morning-brief.js
// Morning brief for multiple metals: prefer the latest Upstash snapshot (normalized IRON etc),
// fall back to live Metals-API if snapshot missing, compute pct change vs yesterday snapshot,
// and return structured JSON for the front-end.
//
// Env required:
// - METALS_API_KEY (optional — used only if snapshot missing)
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Symbols used: XAU, XAG, IRON, LITH-CAR, NI, URANIUM
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
  const symbols = ['XAU','XAG','IRON','LITH-CAR','NI','URANIUM'];
  const debug = { steps: [] };

  try {
    // 0) Attempt to read the latest snapshot from Upstash first (preferred)
    let latestSnapshot = null;
    try {
      const rawLatest = await redisGet('metals:latest');
      if (rawLatest) {
        // rawLatest may be a JSON string or already an object depending on how it was stored
        if (typeof rawLatest === 'string') {
          try { latestSnapshot = JSON.parse(rawLatest); }
          catch (e) { latestSnapshot = null; debug.steps.push({ source: 'parse-latest-failed', error: e && e.message }); }
        } else if (typeof rawLatest === 'object') {
          latestSnapshot = rawLatest;
        }
        debug.steps.push({ source: 'upstash-latest', found: !!latestSnapshot });
      } else {
        debug.steps.push({ source: 'upstash-latest', found: false });
      }
    } catch (e) {
      debug.steps.push({ source: 'upstash-latest-error', error: e && e.message });
    }

    // We'll populate currentUsd/currentAud/priceTimestamp either from snapshot or from live fetch.
    const currentUsd = {}; // symbol -> USD price (number|null)
    const currentAud = {}; // symbol -> AUD price (number|null)
    let priceTimestamp = null;
    let usdToAud = null;
    let dataSource = 'live-metals-api';

    if (latestSnapshot && latestSnapshot.symbols) {
      // Use snapshot values (these should already be normalised by snapshot-metals)
      dataSource = 'upstash-latest';
      for (const s of symbols) {
        const entry = latestSnapshot.symbols[s] || latestSnapshot.metals && latestSnapshot.metals[s] ? (latestSnapshot.symbols[s] || latestSnapshot.metals[s]) : null;
        // Handle either object or string representations
        if (entry && typeof entry === 'object') {
          currentUsd[s] = (typeof entry.priceUSD === 'number') ? entry.priceUSD : (typeof entry.apiPriceUSD === 'number' ? entry.apiPriceUSD : null);
          currentAud[s] = (typeof entry.priceAUD === 'number') ? entry.priceAUD : null;
          // prefer symbol-level timestamp, fallback to snapshot top-level
          priceTimestamp = priceTimestamp || entry.priceTimestamp || latestSnapshot.priceTimestamp || latestSnapshot.snappedAt || null;
        } else {
          currentUsd[s] = null;
          currentAud[s] = null;
        }
      }
      usdToAud = latestSnapshot.usdToAud || null;
      debug.snapshotDate = latestSnapshot.snappedAt || null;
    } else {
      // Snapshot not available: fetch live from Metals-API and FX services
      const METALS_API_KEY = process.env.METALS_API_KEY || null;
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
            for (const s of symbols) currentUsd[s] = null;
          }
        } catch (e) {
          debug.steps.push({ source: 'metals-api-error', error: e && e.message });
          for (const s of symbols) currentUsd[s] = null;
        }
      } else {
        debug.steps.push({ source: 'metals-api', note: 'METALS_API_KEY missing' });
        for (const s of symbols) currentUsd[s] = null;
      }

      // FX USD -> AUD (try open.er-api.com then exchangerate.host)
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

      // compute current AUD prices per symbol (from live USD)
      for (const s of symbols) {
        const pUsd = currentUsd[s];
        currentAud[s] = (typeof pUsd === 'number' && typeof usdToAud === 'number') ? Number((pUsd * usdToAud).toFixed(2)) : null;
      }
    }

    // 3) read yesterday's snapshot from Upstash (to compute pct change)
    let yesterdayData = null;
    try {
      const d = new Date();
      // build UTC date for yesterday
      const yd = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - 1));
      const key = `metals:${yd.toISOString().slice(0,10)}`; // metals:YYYY-MM-DD
      const val = await redisGet(key);
      if (val) {
        if (typeof val === 'string') {
          try { yesterdayData = JSON.parse(val); } catch (e) { yesterdayData = null; debug.steps.push({ source: 'parse-yesterday-failed', error: e && e.message }); }
        } else if (typeof val === 'object') {
          yesterdayData = val;
        }
      }
      debug.steps.push({ source: 'redis-get-yesterday', key, found: !!yesterdayData });
    } catch (e) {
      debug.steps.push({ source: 'redis-get-error', error: e && e.message });
    }

    // 4) assemble per-symbol result: priceUSD, priceAUD, yesterdayPriceAUD, pctChange
    const metals = {};
    for (const s of symbols) {
      const todayUSD = (typeof currentUsd[s] === 'number') ? currentUsd[s] : null;
      const todayAUD = (typeof currentAud[s] === 'number') ? currentAud[s] : null;

      // try to pull yesterday AUD from yesterdayData snapshot shape
      let yesterdayAUD = null;
      if (yesterdayData && yesterdayData.symbols && typeof yesterdayData.symbols[s] !== 'undefined') {
        const p = yesterdayData.symbols[s] && typeof yesterdayData.symbols[s].priceAUD !== 'undefined' ? yesterdayData.symbols[s].priceAUD : null;
        if (p !== null) yesterdayAUD = (typeof p === 'number') ? p : Number(p);
      }

      let pctChange = null;
      if (todayAUD !== null && yesterdayAUD !== null && yesterdayAUD !== 0) {
        pctChange = Number(((todayAUD - yesterdayAUD) / yesterdayAUD * 100).toFixed(2));
      }

      metals[s] = {
        priceUSD: fmt(todayUSD),
        priceAUD: fmt(todayAUD),
        yesterdayPriceAUD: yesterdayAUD !== null ? fmt(yesterdayAUD) : null,
        pctChange,
        priceTimestamp: priceTimestamp || null
      };
    }

    // 5) narratives (optional)
    const narratives = {};
    for (const s of symbols) {
      const m = metals[s];
      if (m.priceAUD === null) {
        narratives[s] = `The ${s} price is currently unavailable.`;
      } else {
        const upDown = (m.pctChange === null) ? '' : (m.pctChange > 0 ? ` — up ${Math.abs(m.pctChange)}% vs yesterday` : (m.pctChange < 0 ? ` — down ${Math.abs(m.pctChange)}% vs yesterday` : ' — unchanged vs yesterday'));
        narratives[s] = `${s} is currently $${m.priceAUD} AUD per ${s === 'IRON' ? 'tonne' : 'unit'}${upDown}.`;
      }
    }

    const payload = {
      generatedAt: nowIso,
      usdToAud: fmt(usdToAud),
      metals,
      narratives,
      _debug: {
        ...debug,
        dataSource
      }
    };

    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    console.error('morning-brief multi error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message || String(err) }) };
  }
};