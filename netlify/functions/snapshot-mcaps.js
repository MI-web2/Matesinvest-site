// netlify/functions/snapshot-mcaps.js
// Daily snapshot of all ASX symbols + their market caps from EODHD Screener.
// Saved as: mcaps:YYYY-MM-DD and mcaps:latest
//
// Debug mode:
//   /.netlify/functions/snapshot-mcaps?debug=1
//   -> fetches data but does NOT write to Upstash, returns a sample payload.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;

// ---------- Simple helpers ----------

async function fetchJson(url, opts = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    const text = await res.text().catch(() => "");
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (e) {
      json = null;
    }
    return { ok: res.ok, status: res.status, json, text };
  } catch (err) {
    return { ok: false, status: 0, json: null, text: String(err && err.message || err) };
  } finally {
    clearTimeout(id);
  }
}

async function redisSet(key, value) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.warn("redisSet skipped, Upstash env vars missing");
    return false;
  }

  const payload = encodeURIComponent(JSON.stringify(value));
  const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${payload}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    console.error("redisSet failed:", key, res.status, txt);
    return false;
  }
  return true;
}

// ---------- Main handler ----------

exports.handler = async (event) => {
  try {
    const qs = (event && event.queryStringParameters) || {};
    const isDebug = qs.debug === "1" || qs.debug === "true";

    if (!EODHD_API_TOKEN) {
      const msg = "EODHD_API_TOKEN missing in environment";
      console.error(msg);
      return { statusCode: 500, body: JSON.stringify({ error: msg }) };
    }

    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      const msg = "Upstash env variables missing (UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN)";
      console.error(msg);
      // In debug we still allow reading to inspect payload
      if (!isDebug) {
        return { statusCode: 500, body: JSON.stringify({ error: msg }) };
      }
    }

    const today = new Date().toISOString().slice(0, 10);
    console.log("snapshot-mcaps: starting snapshot for", today);

    // -------- 1) Page through EODHD Screener for ASX --------
    const LIMIT = 100; // Screener max per call
    let offset = 0;
    const all = [];

    while (true) {
      const filters = '[["exchange","=","AU"]]';
      const url =
        `https://eodhd.com/api/screener?` +
        `api_token=${encodeURIComponent(EODHD_API_TOKEN)}` +
        `&filters=${encodeURIComponent(filters)}` +
        `&sort=market_capitalization.desc` +
        `&limit=${LIMIT}` +
        `&offset=${offset}`;

      const { ok, status, json, text } = await fetchJson(url, {}, 15000);

      if (!ok) {
        console.error("Screener HTTP error", { status, text });
        return {
          statusCode: 500,
          body: JSON.stringify(
            { error: "Failed to load screener data", status, text },
            null,
            2
          )
        };
      }

      // EODHD screener can return either an array or { data: [...] }
      const rows = Array.isArray(json)
        ? json
        : (json && Array.isArray(json.data) ? json.data : null);

      if (!rows) {
        console.error("Unexpected screener JSON shape", { status, text });
        return {
          statusCode: 500,
          body: JSON.stringify(
            { error: "Unexpected screener JSON shape", status, text },
            null,
            2
          )
        };
      }

      if (rows.length === 0) {
        console.log("No more screener rows at offset", offset);
        break;
      }

      console.log(`Fetched ${rows.length} screener rows at offset ${offset}`);

      for (const row of rows) {
        const code =
          (row.code || row.Code || row.symbol || row.Symbol || "").toString().trim();
        if (!code) continue;

        const name =
          (row.name || row.Name || row.companyName || row.CompanyName || "").toString().trim();
        const exchange =
          (row.exchange || row.Exchange || "AU").toString().trim().toUpperCase();

        const rawMcap =
          row.market_capitalization ??
          row.MarketCapitalization ??
          row.market_cap ??
          row.MarketCap ??
          null;

        const marketCap = rawMcap !== null ? Number(rawMcap) : null;

        all.push({
          code: code.toUpperCase(),   // e.g. "CBA"
          name,
          exchange,                   // e.g. "AU"
          marketCap                   // usually in local currency (A$ for AU)
        });
      }

      if (rows.length < LIMIT) {
        // last page
        break;
      }

      offset += LIMIT;

      // Safety stop so we don’t loop forever if something weird happens
      if (offset > 5000) {
        console.warn("Stopping screener pagination early at offset", offset);
        break;
      }
    }

    console.log("Total rows collected:", all.length);

    // -------- 2) Debug mode: return sample, no writes --------
    if (isDebug) {
      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            date: today,
            count: all.length,
            sample: all.slice(0, 25)
          },
          null,
          2
        )
      };
    }

    // -------- 3) Persist to Upstash --------
    try {
      const keyToday = `mcaps:${today}`;
      await redisSet(keyToday, all);
      await redisSet("mcaps:latest", all);
      console.log("Saved snapshots to Upstash:", keyToday, "and mcaps:latest");

      return {
        statusCode: 200,
        body: JSON.stringify({ saved: all.length, key: keyToday }, null, 2)
      };
    } catch (e) {
      console.error("Failed to write to Upstash:", e && e.message);
      return {
        statusCode: 500,
        body: JSON.stringify(
          { error: "Upstash write failed", detail: String(e && e.message || e) },
          null,
          2
        )
      };
    }
  } catch (err) {
    console.error("snapshot-mcaps error:", err && (err.stack || err.message || err));
    return {
      statusCode: 500,
      body: JSON.stringify({ error: (err && err.message) || String(err) })
    };
  }
};
