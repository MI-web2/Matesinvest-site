// netlify/functions/morning-brief.js
// Morning brief: fetch live gold (Metals-API primary, Yahoo fallback) + FX, compare to yesterday's snapshot in Upstash Redis.
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Returns:
// {
//   priceAUD, priceUSD, usdToAud, yesterdayPriceAUD, pctChange, narrative, priceTimestamp, generatedAt, _debug
// }

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // --- helpers ---
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

  const fmt = (n) => (typeof n === 'number' && Number.isFinite(n)) ? Number(n.toFixed(2)) : null;

  // Upstash REST helpers
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  async function redisGet(key) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
    try {
      const res = await fetchWithTimeout(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
        headers: { 'Authorization': `Bearer ${UPSTASH_TOKEN}` }
      }, 7000);
      if (!res.ok) return null;
      const j = await res.json().catch(() => null);
      if (!j || typeof j.result === 'undefined') return null;
      return j.result; // may be string or null
    } catch (e) {
      console.warn('redisGet error', e && e.message);
      return null;
    }
  }

  // --- main ---
  const debug = { steps: [] };
  let priceUSD = null;
  let priceTimestamp = null;

  // 1) Metals-API (primary) - request only XAU (avoid account symbol restrictions)
  const METALS_API_KEY = process.env.METALS_API_KEY || null;
  if (METALS_API_KEY) {
    try {
      const url = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(METALS_API_KEY)}&base=USD&symbols=XAU`;
      const r = await fetchWithTimeout(url, {}, 9000);
      const text = await r.text().catch(() => '');
      let j = null;
      try { j = text ? JSON.parse(text) : null; } catch (e) { j = null; }
      debug.steps.push({ source: 'metals-api', ok: !!r.ok, status: r.status });
      if (r.ok && j && j.rates && typeof j.rates.XAU === 'number') {
        const v = j.rates.XAU;
        // metals-api returns either XAU per USD (fraction) or USD per XAU (large number)
        if (v > 0 && v < 1) priceUSD = 1 / v;
        else if (v >= 1) priceUSD = v;
        if (j.timestamp) priceTimestamp = new Date(j.timestamp * 1000).toISOString();
        debug.ratesPreview = j.rates;
      } else {
        debug.steps.push({ source: 'metals-api-body', preview: text.slice(0, 400) });
      }
    } catch (e) {
      debug.steps.push({ source: 'metals-api-error', error: e && e.message });
    }
  } else {
    debug.steps.push({ source: 'metals-api', note: 'METALS_API_KEY missing' });
  }

  // 2) Fallback: Yahoo GC=F if metals-api didn't produce priceUSD
  if (priceUSD === null) {
    try {
      const yf = await fetchWithTimeout('https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC=F', {}, 7000);
      const t = await yf.text().catch(() => '');
      let j = null;
      try { j = t ? JSON.parse(t) : null; } catch (e) { j = null; }
      debug.steps.push({ source: 'yahoo_gc_f', ok: !!yf.ok, status: yf.status });
      if (yf.ok && j && j.quoteResponse && Array.isArray(j.quoteResponse.result) && j.quoteResponse.result.length) {
        const r = j.quoteResponse.result[0];
        if (r && typeof r.regularMarketPrice === 'number') {
          priceUSD = Number(r.regularMarketPrice);
          if (r.regularMarketTime) priceTimestamp = new Date(r.regularMarketTime * 1000).toISOString();
          debug.ratesPreview = { yahoo_price: priceUSD };
        }
      } else {
        debug.steps.push({ source: 'yahoo_body_preview', preview: t.slice(0, 400) });
      }
    } catch (e) {
      debug.steps.push({ source: 'yahoo_error', error: e && e.message });
    }
  }

  // 3) FX: try open.er-api.com -> exchangerate.host -> exchangerate-api.com
  let usdToAud = null;
  const fxCandidates = [
    { name: 'open.er-api.com', url: 'https://open.er-api.com/v6/latest/USD' },
    { name: 'exchangerate.host', url: 'https://api.exchangerate.host/latest?base=USD&symbols=AUD' },
    { name: 'exchangerate-api.com', url: 'https://api.exchangerate-api.com/v4/latest/USD' }
  ];
  for (const c of fxCandidates) {
    try {
      const fres = await fetchWithTimeout(c.url, {}, 7000);
      const ftext = await fres.text().catch(() => '');
      let fj = null;
      try { fj = ftext ? JSON.parse(ftext) : null; } catch (e) { fj = null; }
      debug.steps.push({ source: c.name, ok: !!fres.ok, status: fres.status });
      if (fres.ok && fj && fj.rates && typeof fj.rates.AUD === 'number') {
        usdToAud = Number(fj.rates.AUD);
        debug.fxSource = c.name;
        break;
      } else {
        debug.steps.push({ source: c.name + '-body-preview', preview: ftext.slice(0, 300) });
      }
    } catch (e) {
      debug.steps.push({ source: c.name + '-error', error: e && e.message });
    }
  }

  const outPriceUSD = fmt(priceUSD);
  const outFx = fmt(usdToAud);
  const outPriceAUD = (outPriceUSD !== null && outFx !== null) ? fmt(outPriceUSD * outFx) : null;

  // 4) Read yesterday's snapshot from Upstash
  let yesterdayPriceAUD = null;
  try {
    // compute yesterday date in UTC YYYY-MM-DD
    const d = new Date();
    const yd = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - 1));
    const key = `gold:${yd.toISOString().slice(0, 10)}`; // e.g. gold:2025-11-17
    const val = await redisGet(key);
    if (val !== null && typeof val !== 'undefined' && val !== '') {
      try {
        const parsed = JSON.parse(val);
        if (parsed && typeof parsed === 'object' && typeof parsed.priceAUD !== 'undefined') {
          yesterdayPriceAUD = Number(parsed.priceAUD);
        } else {
          const asNum = Number(val);
          if (!Number.isNaN(asNum)) yesterdayPriceAUD = asNum;
        }
      } catch (e) {
        const asNum = Number(val);
        if (!Number.isNaN(asNum)) yesterdayPriceAUD = asNum;
      }
    }
  } catch (e) {
    debug.steps.push({ source: 'redis-get-error', error: e && e.message });
  }

  // 5) pct change
  let pctChange = null;
  if (outPriceAUD !== null && yesterdayPriceAUD !== null && yesterdayPriceAUD !== 0) {
    pctChange = Number(((outPriceAUD - yesterdayPriceAUD) / yesterdayPriceAUD * 100).toFixed(2));
  }

  // 6) narrative
  let narrative;
  if (outPriceAUD === null) {
    narrative = 'The spot gold price is currently unavailable.';
  } else {
    const upDown = (pctChange === null) ? '' : (pctChange > 0 ? ` — up ${Math.abs(pctChange)}% vs yesterday` : (pctChange < 0 ? ` — down ${Math.abs(pctChange)}% vs yesterday` : ' — unchanged vs yesterday'));
    narrative = `The spot gold price is currently $${outPriceAUD} AUD per ounce${upDown}.`;
  }

  const payload = {
    priceAUD: outPriceAUD,
    priceUSD: outPriceUSD,
    usdToAud: outFx,
    yesterdayPriceAUD: yesterdayPriceAUD !== null ? fmt(yesterdayPriceAUD) : null,
    pctChange,
    narrative,
    priceTimestamp: priceTimestamp || null,
    generatedAt: nowIso,
    _debug: debug
  };

  return { statusCode: 200, body: JSON.stringify(payload) };
};