// netlify/functions/snapshot-mcaps.js
// Daily snapshot of all ASX symbols + their market caps from EODHD
// Saved as: mcaps:YYYY-MM-DD and mcaps:latest
//
// This version includes a debug mode (?debug=1) that returns a small sample
// of symbols + fundamentals and does NOT write to Upstash. Use that to
// quickly verify EODHD responses and environment variables.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;

async function redisSet(key, value) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    throw new Error("Upstash env variables missing (UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN)");
  }
  const res = await fetch(`${UPSTASH_URL}/set/${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify({ value })
  });
  if (!res.ok) {
    const text = await res.text().catch(()=>"");
    throw new Error(`Upstash set failed ${res.status} ${text}`);
  }
  const j = await res.json().catch(()=>null);
  return j;
}

async function fetchJson(url) {
  const res = await fetch(url);
  const txt = await res.text().catch(()=>"");
  try { return JSON.parse(txt); } catch { return { _rawText: txt }; }
}

exports.handler = async (event) => {
  try {
    // --- Quick checks & debug mode ---
    const qs = (event && event.queryStringParameters) ? event.queryStringParameters : {};
    const isDebug = qs.debug === '1' || qs.debug === 'true';

    if (isDebug) {
      // Diagnostic path: fetch list and sample fundamentals (no writes)
      if (!EODHD_API_TOKEN) {
        return { statusCode: 500, body: JSON.stringify({ error: "EODHD_API_TOKEN missing in environment" }, null, 2) };
      }

      const listUrl = `https://eodhd.com/api/exchange-symbol-list/AU?api_token=${EODHD_API_TOKEN}&fmt=json`;
      const list = await fetchJson(listUrl);

      if (!Array.isArray(list)) {
        return { statusCode: 500, body: JSON.stringify({ error: "Failed to load ASX symbol list", list }, null, 2) };
      }

      const sample = list.slice(0, 10);
      const out = [];
      for (const item of sample) {
        const symbol = item.Code || item.code || item.symbol;
        const name = item.Name || item.name || "";
        if (!symbol) {
          out.push({ raw: item, note: "no symbol field" });
          continue;
        }
        const full = `${symbol}.AU`;
        const fundamentalsUrl = `https://eodhd.com/api/fundamentals/${encodeURIComponent(full)}?api_token=${EODHD_API_TOKEN}&fmt=json`;
        const fundamentals = await fetchJson(fundamentalsUrl);
        // heuristics for market cap fields
        let mcap = null;
        if (fundamentals && typeof fundamentals === "object") {
          mcap = fundamentals?.Highlights?.MarketCapitalization ?? fundamentals?.marketCap ?? fundamentals?.MarketCap ?? fundamentals?.mktCap ?? null;
        }
        out.push({ code: full, name, mcap: mcap ?? null, fundamentalsPreview: fundamentals && typeof fundamentals === "object" ? Object.keys(fundamentals).slice(0,6) : fundamentals });
        // polite pause (avoid bursting EODHD)
        await new Promise(r => setTimeout(r, 200));
      }

      return { statusCode: 200, body: JSON.stringify({ debug: true, sample: out, note: "debug mode: no Upstash writes performed" }, null, 2) };
    }

    // --- Normal scheduled run path ---
    // Basic env checks
    if (!EODHD_API_TOKEN) {
      const msg = "EODHD_API_TOKEN missing in environment";
      console.error(msg);
      return { statusCode: 500, body: JSON.stringify({ error: msg }) };
    }
    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      const msg = "Upstash env variables missing (UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN)";
      console.error(msg);
      return { statusCode: 500, body: JSON.stringify({ error: msg }) };
    }

    const today = new Date().toISOString().slice(0,10);
    console.log("snapshot-mcaps: starting snapshot for", today);

    // 1) Load all ASX symbols
    const listUrl = `https://eodhd.com/api/exchange-symbol-list/AU?api_token=${EODHD_API_TOKEN}&fmt=json`;
    const list = await fetchJson(listUrl);
    if (!Array.isArray(list)) {
      console.error("Failed to fetch ASX list", list);
      return { statusCode: 500, body: JSON.stringify({ error: "Failed to load ASX symbol list", list }) };
    }
    console.log("Loaded ASX symbol list length:", list.length);

    // 2) For each symbol fetch fundamentals (serial - rate-limited)
    const snapshot = [];
    let processed = 0;
    for (const item of list) {
      const symbol = item.Code || item.code || item.symbol;
      if (!symbol) continue;

      const full = `${symbol}.AU`;
      const fundamentalsUrl = `https://eodhd.com/api/fundamentals/${encodeURIComponent(full)}?api_token=${EODHD_API_TOKEN}&fmt=json`;
      const fundamentals = await fetchJson(fundamentalsUrl);
      // attempt a few likely fields for market cap
      let mcap = null;
      if (fundamentals && typeof fundamentals === "object") {
        mcap = fundamentals?.Highlights?.MarketCapitalization ?? fundamentals?.marketCap ?? fundamentals?.MarketCap ?? fundamentals?.mktCap ?? null;
      }
      snapshot.push({
        code: full,
        mcap: (typeof mcap === "number" && Number.isFinite(mcap)) ? mcap : (typeof mcap === "string" ? Number(String(mcap).replace(/[^\d.-]/g,'')) : null),
        name: item.Name || item.name || "",
        sector: (fundamentals && fundamentals?.General && fundamentals.General.Sector) ? fundamentals.General.Sector : (item.sector || "")
      });

      processed++;
      if (processed % 50 === 0) console.log(`Processed ${processed} symbols...`);
      // Rate-limit safety pause; adjust if your plan allows higher rates
      await new Promise(r => setTimeout(r, 300));
    }

    console.log("Fetched fundamentals for", snapshot.length, "symbols. Writing to Upstash...");

    // 3) Write snapshots to Upstash
    try {
      await redisSet(`mcaps:${today}`, snapshot);
      await redisSet("mcaps:latest", snapshot);
      console.log("Saved snapshots to Upstash:", `mcaps:${today}`, "mcaps:latest");
    } catch (e) {
      console.error("Failed to write to Upstash:", e && e.message);
      return { statusCode: 500, body: JSON.stringify({ error: "Upstash write failed", detail: String(e && e.message || e) }) };
    }

    return { statusCode: 200, body: JSON.stringify({ saved: snapshot.length, key: `mcaps:${today}` }, null, 2) };

  } catch (err) {
    console.error("snapshot-mcaps error:", err && (err.stack || err.message || err));
    return { statusCode: 500, body: JSON.stringify({ error: (err && err.message) || String(err) }) };
  }
};