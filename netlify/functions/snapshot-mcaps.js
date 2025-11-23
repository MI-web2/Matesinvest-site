// netlify/functions/snapshot-mcaps.js
//
// Nightly snapshot of ASX market caps using EODHD Screener API.
// Stores into Upstash as:
//   mcaps:YYYY-MM-DD  (daily)
//   mcaps:latest      (alias for today's snapshot)
//
// Env required:
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// This function is intended to run on a Netlify schedule
// (e.g. 0 20 * * *  =>  6am AEST next day for you).

const LIMIT = 100;        // screener max per docs
const MAX_OFFSET = 900;   // <= 999 to avoid 422 error

// ---------- Small helpers ----------

const fetchWithTimeout = (url, opts = {}, timeoutMs = 12000) => {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...opts, signal: controller.signal })
    .finally(() => clearTimeout(id));
};

const aussieDateString = () => {
  const now = new Date();
  const syd = new Date(
    now.toLocaleString("en-US", { timeZone: "Australia/Sydney" })
  );
  const y = syd.getFullYear();
  const m = String(syd.getMonth() + 1).padStart(2, "0");
  const d = String(syd.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
};

// ---------- Upstash helpers ----------

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

async function redisSet(key, value) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.warn("snapshot-mcaps: missing Upstash env");
    return false;
  }

  try {
    const payload = encodeURIComponent(JSON.stringify(value));
    const res = await fetchWithTimeout(
      `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${payload}`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
      },
      8000
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("snapshot-mcaps: redisSet failed", key, res.status, txt);
      return false;
    }
    return true;
  } catch (err) {
    console.warn("snapshot-mcaps: redisSet error", key, err && err.message);
    return false;
  }
}

// ---------- Screener fetch ----------

async function fetchScreenerPage(token, offset) {
  // Filters: AU exchange only
  const filters = encodeURIComponent(
    JSON.stringify([["exchange", "=", "AU"]])
  );

  const url = `https://eodhd.com/api/screener` +
    `?api_token=${encodeURIComponent(token)}` +
    `&sort=market_capitalization.desc` +
    `&filters=${filters}` +
    `&limit=${LIMIT}` +
    `&offset=${offset}` +
    `&fmt=json`;

  const res = await fetchWithTimeout(url, {}, 15000);
  const text = await res.text().catch(() => "");

  if (!res.ok) {
    console.error("Screener HTTP error", { status: res.status, text });
    return { ok: false, data: null };
  }

  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (err) {
    console.error("Screener JSON parse error", err && err.message);
    return { ok: false, data: null };
  }

  if (!json || !Array.isArray(json.data)) {
    console.error("Screener unexpected body", json);
    return { ok: false, data: null };
  }

  return { ok: true, data: json.data };
}

// ---------- MAIN HANDLER ----------

exports.handler = async function () {
  const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;

  if (!EODHD_TOKEN) {
    console.error("snapshot-mcaps: missing EODHD_API_TOKEN");
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" })
    };
  }

  const today = aussieDateString();
  console.info(`snapshot-mcaps: starting snapshot for ${today}`);

  const allRows = [];
  let offset = 0;

  while (offset <= MAX_OFFSET) {
    const { ok, data } = await fetchScreenerPage(EODHD_TOKEN, offset);
    if (!ok) {
      // Stop on first error – don’t partially snapshot
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Screener call failed" })
      };
    }

    console.info(`Fetched ${data.length} screener rows at offset ${offset}`);
    if (!data.length) break;

    allRows.push(...data);

    if (data.length < LIMIT) {
      // No more pages
      break;
    }

    offset += LIMIT;
  }

  // Normalise to a compact structure we’ll use later
  const normalized = allRows.map((row) => ({
    code: row.code,
    name: row.name || "",
    exchange: row.exchange || "AU",
    currency_symbol: row.currency_symbol || "",
    last_day_data_date: row.last_day_data_date || null,
    market_cap: typeof row.market_capitalization === "number"
      ? row.market_capitalization
      : null,
    sector: row.sector || "",
    industry: row.industry || "",
    avgvol_1d: typeof row.avgvol_1d === "number" ? row.avgvol_1d : null,
    avgvol_200d: typeof row.avgvol_200d === "number" ? row.avgvol_200d : null,
  }));

  const snapshot = {
    snappedAt: new Date().toISOString(),
    exchange: "AU",
    rows: normalized
  };

  const dailyKey = `mcaps:${today}`;
  const okDaily = await redisSet(dailyKey, snapshot);
  const okLatest = await redisSet("mcaps:latest", snapshot);

  console.info("snapshot-mcaps: finished", {
    count: normalized.length,
    okDaily,
    okLatest
  });

  return {
    statusCode: 200,
    body: JSON.stringify({
      ok: okDaily && okLatest,
      count: normalized.length,
      key: dailyKey
    })
  };
};
