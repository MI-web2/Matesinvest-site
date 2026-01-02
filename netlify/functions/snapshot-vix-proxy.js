// netlify/functions/snapshot-vix-proxy.js
// Standalone snapshot for VIX proxy (VXX.US) via EODHD.
// Writes to Upstash:
//   market:vixproxy:eod:latest
//   market:vixproxy:eod:YYYY-MM-DD
//   market:vixproxy:latestDate
//
// Query params:
//   dryrun=1   -> fetch only, do not write to Upstash
//   asOf=YYYY-MM-DD -> override "today" (UTC date) for backtests/debug

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;

// -------------------------------
// Helpers
// -------------------------------
function assertEnv() {
  const missing = [];
  if (!UPSTASH_URL) missing.push("UPSTASH_REDIS_REST_URL");
  if (!UPSTASH_TOKEN) missing.push("UPSTASH_REDIS_REST_TOKEN");
  if (!EODHD_API_TOKEN) missing.push("EODHD_API_TOKEN");
  if (missing.length) throw new Error(`Missing env vars: ${missing.join(", ")}`);
}

function jsonResponse(statusCode, bodyObj) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(bodyObj),
  };
}

async function redisCmd(cmdArray) {
  const res = await fetch(`${UPSTASH_URL}/${cmdArray.map(encodeURIComponent).join("/")}`, {
    headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Upstash ${res.status}: ${text}`);
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

async function redisPipeline(cmds) {
  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(cmds),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Upstash pipeline ${res.status}: ${text}`);
  return JSON.parse(text);
}

async function fetchJson(url) {
  const res = await fetch(url);
  const text = await res.text();
  if (!res.ok) throw new Error(`EODHD ${res.status}: ${text}`);
  return JSON.parse(text);
}

function isoDateUTC(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

function shiftIsoDate(iso, days) {
  const t = Date.parse(iso + "T00:00:00Z");
  return new Date(t + days * 86400000).toISOString().slice(0, 10);
}

// -------------------------------
// Main
// -------------------------------
exports.handler = async function (event) {
  try {
    assertEnv();

    const qs = event.queryStringParameters || {};
    const dryrun = String(qs.dryrun || "") === "1";
    const asOf = (qs.asOf && /^\d{4}-\d{2}-\d{2}$/.test(qs.asOf)) ? qs.asOf : isoDateUTC();

    // Pull a small window so we always catch the latest US trading day
    const from = shiftIsoDate(asOf, -12);
    const to = asOf;

    const symbol = "VXX.US";
    const url =
      `https://eodhd.com/api/eod/${symbol}` +
      `?api_token=${encodeURIComponent(EODHD_API_TOKEN)}` +
      `&fmt=json&period=d&from=${from}&to=${to}`;

    const rows = await fetchJson(url);

    if (!Array.isArray(rows) || rows.length === 0) {
      return jsonResponse(200, {
        ok: false,
        dryrun,
        symbol,
        from,
        to,
        error: "No data returned from EODHD (empty array).",
      });
    }

    // EODHD returns ascending by date; take last element as most recent bar
    const last = rows[rows.length - 1];
    if (!last || !last.date) throw new Error("EODHD response missing last.date");

    const payload = {
      code: symbol,
      name: "VIX Proxy (VXX) — Short-Term VIX Futures ETN",
      date: last.date,
      open: last.open,
      high: last.high,
      low: last.low,
      close: last.close,
      adj_close: last.adjusted_close ?? null,
      volume: last.volume ?? null,
      updatedAt: new Date().toISOString(),
      source: "eodhd",
      note: "VXX is a tradable proxy tied to short-term VIX futures; can decay in calm markets.",
    };

    if (!dryrun) {
      const latestKey = "market:vixproxy:eod:latest";
      const datedKey = `market:vixproxy:eod:${payload.date}`;
      const dateKey = "market:vixproxy:latestDate";

      await redisPipeline([
        ["SET", latestKey, JSON.stringify(payload)],
        ["SET", datedKey, JSON.stringify(payload)],
        ["SET", dateKey, payload.date],
      ]);
    }

    return jsonResponse(200, {
      ok: true,
      dryrun,
      symbol,
      from,
      to,
      fetchedRows: rows.length,
      latestDate: payload.date,
      payload,
      writtenKeys: dryrun
        ? []
        : [
            "market:vixproxy:eod:latest",
            `market:vixproxy:eod:${payload.date}`,
            "market:vixproxy:latestDate",
          ],
    });
  } catch (err) {
    return jsonResponse(500, {
      ok: false,
      error: err?.message || String(err),
    });
  }
};
