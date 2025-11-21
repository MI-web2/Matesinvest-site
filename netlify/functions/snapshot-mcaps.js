// netlify/functions/snapshot-mcaps.js
// Daily snapshot of all ASX symbols + their market caps from EODHD
// Saved as: mcaps:YYYY-MM-DD and mcaps:latest

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;

async function redisSet(key, value) {
  await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${UPSTASH_TOKEN}` },
    body: JSON.stringify({ value })
  });
}

async function fetchJson(url) {
  const res = await fetch(url);
  const txt = await res.text();
  try { return JSON.parse(txt); } catch { return null; }
}

exports.handler = async () => {
  try {
    const today = new Date().toISOString().slice(0,10);

    // 1) Load all ASX symbols
    const listUrl = `https://eodhd.com/api/exchange-symbol-list/AU?api_token=${EODHD_API_TOKEN}&fmt=json`;
    const list = await fetchJson(listUrl);

    if (!Array.isArray(list)) {
      return { statusCode: 500, body: "Failed to load ASX symbol list" };
    }

    // 2) For each symbol fetch fundamentals
    const snapshot = [];
    for (const item of list) {
      const symbol = item.Code || item.code || item.symbol;
      if (!symbol) continue;

      const full = `${symbol}.AU`;
      const fundamentalsUrl =
        `https://eodhd.com/api/fundamentals/${full}?api_token=${EODHD_API_TOKEN}&fmt=json`;

      const fundamentals = await fetchJson(fundamentalsUrl);
      const mcap = fundamentals?.Highlights?.MarketCapitalization || null;

      snapshot.push({
        code: full,
        mcap,
        name: item.Name || item.name || "",
        sector: fundamentals?.General?.Sector || "",
      });

      await new Promise(r => setTimeout(r, 300)); // Rate-limit safety
    }

    // 3) Write snapshots
    await redisSet(`mcaps:${today}`, snapshot);
    await redisSet("mcaps:latest", snapshot);

    return {
      statusCode: 200,
      body: JSON.stringify({
        saved: snapshot.length,
        key: `mcaps:${today}`
      })
    };

  } catch (err) {
    return { statusCode: 500, body: err.message };
  }
};
