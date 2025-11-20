// netlify/functions/morning-brief.js
// Morning brief for multiple metals + top performers across US + ASX using EODHD.
// - Preserves your working metals snapshot logic (Upstash + Metals-API fallback)
// - Adds an EODHD-backed computation of top 5 performers across US and ASX
// Env required for metals part (existing):
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
// - METALS_API_KEY (optional fallback)
// Env required for EODHD part (new):
// - EODHD_API_TOKEN
//
// Notes
// - EODHD scanning of all symbols is potentially heavy. Defaults are conservative (MAX_PER_EXCHANGE).
// - You can request specific symbols by passing ?symbols=AAPL,BHP or restrict region with ?region=au or ?region=us
// - Response includes existing `metals` and `narratives` keys plus `topPerformers` (array) and `_debug.eodhd` metadata.

const fetch = global.fetch || require('node-fetch');

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

  // ---------- Upstash helpers (existing) ----------
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

  // symbols to show for metals
  const symbols = ['XAU','XAG','IRON','LITH-CAR','NI','URANIUM'];
  const debug = { steps: [] };

  try {
    // ------------------------------
    // Existing metals snapshot logic
    // ------------------------------
    let latestSnapshot = null;
    try {
      const rawLatest = await redisGet('metals:latest');
      if (rawLatest) {
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

    const currentUsd = {}; // symbol -> USD price (number|null)
    const currentAud = {}; // symbol -> AUD price (number|null)
    let priceTimestamp = null;
    let usdToAud = null;
    let metalsDataSource = 'live-metals-api';

    if (latestSnapshot && latestSnapshot.symbols) {
      metalsDataSource = 'upstash-latest';
      for (const s of symbols) {
        const entry = latestSnapshot.symbols[s] || (latestSnapshot.metals && latestSnapshot.metals[s]) || null;
        if (entry && typeof entry === 'object') {
          currentUsd[s] = (typeof entry.priceUSD === 'number') ? entry.priceUSD : (typeof entry.apiPriceUSD === 'number' ? entry.apiPriceUSD : null);
          currentAud[s] = (typeof entry.priceAUD === 'number') ? entry.priceAUD : null;
          priceTimestamp = priceTimestamp || entry.priceTimestamp || latestSnapshot.priceTimestamp || latestSnapshot.snappedAt || null;
        } else {
          currentUsd[s] = null;
          currentAud[s] = null;
        }
      }
      usdToAud = latestSnapshot.usdToAud || null;
      debug.snapshotDate = latestSnapshot.snappedAt || null;
    } else {
      // Fallback live fetch path (tries Metals-API then FX)
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
                // metals-api may return rates as units per USD or USD per unit depending on base; handle heuristics
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

    // ------------------------------
    // Read yesterday snapshot (Upstash) to compute pct change for metals
    // ------------------------------
    let yesterdayData = null;
    try {
      const d = new Date();
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

      let yesterdayAUD = null;
      if (yesterdayData && yesterdayData.symbols && typeof yesterdayData.symbols[s] !== 'undefined') {
        const p = (yesterdayData.symbols[s] && typeof yesterdayData.symbols[s].priceAUD !== 'undefined') ? yesterdayData.symbols[s].priceAUD : null;
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

    // ------------------------------
    // NEW: EODHD top performers logic
    // ------------------------------
    // Environment and defaults
    const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
    const MAX_PER_EXCHANGE = Number(process.env.EODHD_MAX_SYMBOLS_PER_EXCHANGE || 500); // conservative default
    const EODHD_CONCURRENCY = Number(process.env.EODHD_CONCURRENCY || 8);
    const FIVE_DAYS = 5;

    const eodhdDebug = { active: !!EODHD_TOKEN, steps: [] };

    // Utility: return last N business days (ascending oldest->newest), inclusive of today if business day
    function getLastBusinessDays(n) {
      const days = [];
      let d = new Date(); // local timezone is fine for dates; EODHD expects YYYY-MM-DD
      while (days.length < n) {
        const dow = d.getDay(); // 0 Sun, 6 Sat
        if (dow !== 0 && dow !== 6) {
          // push a copy
          days.push(new Date(d));
        }
        d.setDate(d.getDate() - 1);
      }
      return days.reverse().map(dt => dt.toISOString().slice(0,10));
    }

    // Generic JSON fetch w/ timeout (used for EODHD)
    async function fetchJson(url, opts = {}, timeout = 12000) {
      try {
        const res = await fetchWithTimeout(url, opts, timeout);
        const text = await res.text().catch(()=>'');
        try {
          return { ok: res.ok, status: res.status, json: text ? JSON.parse(text) : null, text };
        } catch (e) {
          return { ok: res.ok, status: res.status, json: null, text };
        }
      } catch (err) {
        return { ok: false, status: 0, json: null, text: String(err && err.message || err) };
      }
    }

    // EODHD: list symbols for exchange
    async function listSymbolsForExchange(exchangeCode) {
      const url = `https://eodhd.com/api/exchange-symbol-list/${encodeURIComponent(exchangeCode)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;
      const r = await fetchJson(url, {}, 12000);
      if (!r.ok || !Array.isArray(r.json)) {
        return { ok:false, data: [], error: r.text || `HTTP ${r.status}` };
      }
      return { ok:true, data: r.json };
    }

    // EODHD: fetch eod for symbol.exchange from->to
    async function fetchEodForSymbol(symbol, exchange, from, to) {
      const url = `https://eodhd.com/api/eod/${encodeURIComponent(symbol)}.${encodeURIComponent(exchange)}?api_token=${encodeURIComponent(EODHD_TOKEN)}&period=d&from=${from}&to=${to}&fmt=json`;
      const r = await fetchJson(url, {}, 12000);
      if (!r.ok || !Array.isArray(r.json)) {
        return { ok:false, data: null, error: r.text || `HTTP ${r.status}` };
      }
      const arr = r.json.slice().sort((a,b)=> new Date(a.date) - new Date(b.date));
      return { ok:true, data: arr };
    }

    function pctGainFromPrices(prices) {
      if (!Array.isArray(prices) || prices.length < 2) return null;
      const first = prices[0].close;
      const last = prices[prices.length - 1].close;
      if (typeof first !== 'number' || typeof last !== 'number' || first === 0) return null;
      return ((last - first) / first) * 100;
    }

    async function mapWithConcurrency(items, fn, concurrency = EODHD_CONCURRENCY) {
      const results = new Array(items.length);
      let idx = 0;
      const workers = new Array(Math.min(concurrency, items.length)).fill(null).map(async () => {
        while (true) {
          const i = idx++;
          if (i >= items.length) return;
          try {
            results[i] = await fn(items[i], i);
          } catch (err) {
            results[i] = { error: err.message || String(err) };
          }
        }
      });
      await Promise.all(workers);
      return results;
    }

    // Compose EODHD work only if token present
    let topPerformers = [];
    if (EODHD_TOKEN) {
      try {
        const qs = (event && event.queryStringParameters) ? event.queryStringParameters : {};
        const requestedSymbolsParam = qs.symbols && String(qs.symbols).trim();
        const regionParam = (qs.region || 'both').toLowerCase();

        const days = getLastBusinessDays(FIVE_DAYS);
        const from = days[0];
        const to = days[days.length - 1];

        let symbolRequests = [];

        if (requestedSymbolsParam) {
          const sarr = requestedSymbolsParam.split(',').map(x=>x.trim()).filter(Boolean);
          sarr.forEach(sym => {
            const parts = sym.split('.');
            if (parts.length === 1) {
              const exch = (regionParam === 'au') ? 'ASX' : 'US';
              symbolRequests.push({ symbol: parts[0].toUpperCase(), exchange: exch });
            } else {
              symbolRequests.push({ symbol: parts[0].toUpperCase(), exchange: parts.slice(1).join('.') });
            }
          });
          eodhdDebug.steps.push({ source: 'symbols-param', count: symbolRequests.length });
        } else {
          // fetch exchange symbol lists
          const exchanges = [];
          if (regionParam === 'us') exchanges.push('US');
          else if (regionParam === 'au') exchanges.push('ASX');
          else exchanges.push('US','ASX');

          for (const ex of exchanges) {
            const res = await listSymbolsForExchange(ex);
            if (!res.ok) {
              eodhdDebug.steps.push({ source: 'list-symbols-failed', exchange: ex, error: res.error || 'unknown' });
              continue;
            }
            const items = res.data;
            // Normalize
            const normalized = items.map(it => {
              if (!it) return null;
              if (typeof it === 'string') return { code: it };
              return { code: it.code || it.symbol || (it[0]||''), name: it.name || it.companyName || (it[1]||'') };
            }).filter(Boolean).filter(x => x.code && !x.code.includes('^') && !x.code.includes('/'));
            const limited = normalized.slice(0, MAX_PER_EXCHANGE);
            limited.forEach(it => symbolRequests.push({ symbol: it.code.toUpperCase(), exchange: ex, name: it.name || '' }));
            // polite pause
            await new Promise(r=>setTimeout(r, 200));
            eodhdDebug.steps.push({ source: 'list-symbols', exchange: ex, totalFound: normalized.length, used: limited.length });
          }
        }

        if (symbolRequests.length > 0) {
          // fetch EOD data per symbol with concurrency limit
          const results = await mapWithConcurrency(symbolRequests, async (req) => {
            const sym = req.symbol;
            const exch = req.exchange;
            const r = await fetchEodForSymbol(sym, exch, from, to);
            if (!r.ok || !Array.isArray(r.data) || r.data.length < FIVE_DAYS) {
              return null;
            }
            const pct = pctGainFromPrices(r.data);
            if (pct === null || isNaN(pct)) return null;
            return {
              symbol: sym,
              exchange: exch,
              name: req.name || '',
              pctGain: Number(pct.toFixed(2)),
              firstClose: r.data[0].close,
              lastClose: r.data[r.data.length - 1].close,
              pricesCount: r.data.length
            };
          }, EODHD_CONCURRENCY);

          const cleaned = results.filter(Boolean);
          cleaned.sort((a,b) => b.pctGain - a.pctGain);
          topPerformers = cleaned.slice(0,5);
          eodhdDebug.steps.push({ source: 'computed', evaluated: cleaned.length, top5: topPerformers.map(x=>({ symbol:x.symbol, pct:x.pctGain })) });
        } else {
          eodhdDebug.steps.push({ source: 'no-symbols' });
        }

        // attach eodhd metadata
        debug.eodhd = eodhdDebug;
        debug.eodhd.window = { from: (new Date(from)).toISOString().slice(0,10), to: (new Date(to)).toISOString().slice(0,10) };

      } catch (err) {
        debug.eodhd = debug.eodhd || {};
        debug.eodhd.error = err && err.message || String(err);
      }
    } else {
      debug.eodhd = { active: false, note: 'EODHD_API_TOKEN missing' };
    }

    // ------------------------------
    // Final payload (combined)
    // ------------------------------
    const payload = {
      generatedAt: nowIso,
      usdToAud: fmt(usdToAud),
      metals,
      narratives,
      topPerformers, // array of {symbol, exchange, pctGain, ...}
      _debug: {
        ...debug,
        metalsDataSource,
      }
    };

    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    console.error('morning-brief multi error', err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: err && err.message || String(err) }) };
  }
};