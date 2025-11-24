// scripts/generate-top-performers.js
// Fetch asx200:latest from Upstash, compute top N gainers (by pctChange) and save topPerformers:latest and topPerformers:YYYY-MM-DD
// Environment variables (set as GitHub secrets for the workflow):
//   UPSTASH_REDIS_REST_URL  - Upstash REST URL (e.g. https://us1-xxxxx.upstash.io)
//   UPSTASH_REDIS_REST_TOKEN - Upstash REST token
//   TOP_N (optional) - number of top performers to keep (default 6)

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const TOP_N = Number(process.env.TOP_N || 6);

if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error('Missing UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN environment variables.');
  process.exit(2);
}

async function fetchWithTimeout(url, opts = {}, timeout = 10000) {
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

async function redisGet(key) {
  const res = await fetchWithTimeout(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
  }, 10000);
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`Upstash GET ${key} failed: ${res.status} ${txt}`);
  }
  const j = await res.json().catch(() => null);
  return j && typeof j.result !== 'undefined' ? j.result : null;
}

async function redisSet(key, value, ttlSeconds) {
  const payload = typeof value === 'string' ? value : JSON.stringify(value);
  const ttl = ttlSeconds ? `?EX=${Number(ttlSeconds)}` : '';
  const res = await fetchWithTimeout(`${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(payload)}${ttl}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
  }, 10000);
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`Upstash SET ${key} failed: ${res.status} ${txt}`);
  }
  return true;
}

function normalizeCode(code) {
  return String(code || '').replace(/\.[A-Z0-9]{1,6}$/i, '').toUpperCase();
}

(async () => {
  try {
    console.log('Fetching asx200:latest from Upstash...');
    const raw = await redisGet('asx200:latest');
    if (!raw) {
      console.log('No asx200:latest key found in Upstash; nothing to do.');
      process.exit(0);
    }

    let rows = raw;
    if (typeof raw === 'string') {
      try { rows = JSON.parse(raw); } catch (e) { /* leave as string */ }
    }

    if (!Array.isArray(rows)) {
      console.error('asx200:latest is not an array; aborting.');
      process.exit(2);
    }

    // Normalize and compute pct if not present
    const cleaned = rows.map(r => {
      const last = (typeof r.lastPrice === 'number') ? r.lastPrice : (r.lastPrice ? Number(r.lastPrice) : null);
      const prev = (typeof r.yesterdayPrice === 'number') ? r.yesterdayPrice : (r.yesterdayPrice ? Number(r.yesterdayPrice) : null);
      let pct = typeof r.pctChange === 'number' ? r.pctChange : null;
      if ((pct === null || typeof pct === 'undefined') && last !== null && prev !== null && prev !== 0) {
        pct = ((last - prev) / prev) * 100;
      }
      return {
        code: normalizeCode(r.code || r.fullCode || r.symbol || ''),
        fullCode: r.fullCode || r.full || r.code || null,
        name: r.name || (r.companyName || '') || '',
        lastDate: r.lastDate || null,
        lastPrice: (typeof last === 'number' && !Number.isNaN(last)) ? Number(last) : null,
        yesterdayDate: r.yesterdayDate || null,
        yesterdayPrice: (typeof prev === 'number' && !Number.isNaN(prev)) ? Number(prev) : null,
        pctChange: (typeof pct === 'number' && Number.isFinite(pct)) ? Number(pct) : null,
        raw: r
      };
    }).filter(x => x && x.lastPrice !== null && x.yesterdayPrice !== null && x.pctChange !== null);

    if (!cleaned.length) {
      console.log('No valid rows with lastPrice & yesterdayPrice & pctChange; nothing to save.');
      process.exit(0);
    }

    cleaned.sort((a,b) => b.pctChange - a.pctChange);

    const top = cleaned.slice(0, TOP_N).map(x => ({
      symbol: x.code || null,
      name: x.name || '',
      lastClose: x.lastPrice !== null ? Number(x.lastPrice) : null,
      pctGain: x.pctChange !== null ? Number(Number(x.pctChange).toFixed(2)) : null,
      // keep snapshot-style too
      code: x.code || null,
      fullCode: x.fullCode || null,
      lastPrice: x.lastPrice,
      yesterdayPrice: x.yesterdayPrice,
      pctChange: x.pctChange !== null ? Number(Number(x.pctChange).toFixed(2)) : null
    }));

    console.log(`Saving top ${top.length} performers to Upstash...`);
    const todayKey = `topPerformers:latest`;
    const datedKey = `topPerformers:${new Date().toISOString().slice(0,10)}`;

    await redisSet(todayKey, top);
    // keep dated key with TTL optional (set to forever); change ttlSeconds argument if you want TTL
    await redisSet(datedKey, top);

    console.log('Top performers saved successfully.');
    console.log(JSON.stringify({ top, savedAt: new Date().toISOString() }, null, 2));
    process.exit(0);
  } catch (err) {
    console.error('generate-top-performers failed:', (err && err.stack) || err);
    process.exit(3);
  }
})();