// netlify/functions/snapshot-gold.js
// Snapshot the authoritative gold price and write to Upstash Redis under gold:YYYY-MM-DD and gold:latest.
// Intended to be run daily near market close (manually or scheduled).
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Response: { key, okToday, okLatest, payload } or error message.

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

  // Upstash REST helpers
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  async function redisSet(key, value) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return false;
    try {
      const encoded = encodeURIComponent(JSON.stringify(value));
      const res = await fetchWithTimeout(`${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encoded}`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${UPSTASH_TOKEN}` }
      }, 7000);
      return res.ok;
    } catch (e) {
      console.warn('redisSet error', e && e.message);
      return false;
    }
  }

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) return { statusCode: 500, body: 'Missing METALS_API_KEY' };
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return { statusCode: 500, body: 'Missing Upstash env vars' };

    // 1) fetch priceUSD from Metals-API (XAU only)
    let priceUSD = null;
    let priceTimestamp = null;
    try {
      const url = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU`;
      const r = await fetchWithTimeout(url, {}, 9000);
      const text = await r.text().catch(() => '');
      let j = null;
      try { j = text ? JSON.parse(text) : null; } catch (e) { j = null; }
      if (r.ok && j && j.rates && typeof j.rates.XAU === 'number') {
        const v = j.rates.XAU;
        if (v > 0 && v < 1) priceUSD = 1 / v;
        else if (v >= 1) priceUSD = v;
        if (j.timestamp) priceTimestamp = new Date(j.timestamp * 1000).toISOString();
      }
    } catch (e) {
      console.warn('metals API fetch failed', e && e.message);
    }

    // 2) FX (open.er-api fallback chain)
    let usdToAud = null;
    try {
      // try open.er-api.com
      let fres = await fetchWithTimeout('https://open.er-api.com/v6/latest/USD', {}, 7000);
      let ftxt = await fres.text().catch(() => '');
      let fj = null;
      try { fj = ftxt ? JSON.parse(ftxt) : null; } catch (e) { fj = null; }
      if (fres.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') usdToAud = Number(fj.rates.AUD);
      else {
        // fallback exchangerate.host
        fres = await fetchWithTimeout('https://api.exchangerate.host/latest?base=USD&symbols=AUD', {}, 7000);
        ftxt = await fres.text().catch(() => '');
        try { fj = ftxt ? JSON.parse(ftxt) : null; } catch (e) { fj = null; }
        if (fres.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') usdToAud = Number(fj.rates.AUD);
      }
    } catch (e) {
      // ignore; price may be null
    }

    const priceAUD = (typeof priceUSD === 'number' && typeof usdToAud === 'number') ? Number((priceUSD * usdToAud).toFixed(2)) : null;

    // persist to Upstash under today's date (UTC YYYY-MM-DD)
    const d = new Date();
    const key = `gold:${d.toISOString().slice(0,10)}`;
    const payload = {
      priceAUD,
      priceUSD: priceUSD === null ? null : Number(priceUSD.toFixed(2)),
      usdToAud: usdToAud === null ? null : Number(usdToAud.toFixed(6)),
      priceTimestamp,
      snappedAt: nowIso
    };

    const okToday = await redisSet(key, payload);
    const okLatest = await redisSet('gold:latest', payload);

    return { statusCode: 200, body: JSON.stringify({ key, okToday, okLatest, payload }) };
  } catch (err) {
    console.error('snapshot-gold error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && (err.message || 'Server error') }) };
  }
};