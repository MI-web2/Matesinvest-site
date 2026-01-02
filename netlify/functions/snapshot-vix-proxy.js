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

    // backfill=30  (days)
    const backfillDays = Number(qs.backfill || 0);
    const doBackfill = Number.isFinite(backfillDays) && backfillDays > 0;

    // Optional override for debugging
    const asOf =
      qs.asOf && /^\d{4}-\d{2}-\d{2}$/.test(qs.asOf) ? qs.asOf : isoDateUTC();

    // Range selection:
    // - Normal run: last ~12 days (to guarantee last US trading day)
    // - Backfill: pull N trading days worth of calendar days
    //   Since EODHD only returns trading days (no weekends/holidays), we need to fetch
    //   more calendar days to get N trading days. Using 1.6x multiplier which accounts for
    //   weekends (theoretical 7/5 = 1.4) plus additional buffer for holidays and market closures.
    const bufferDays = 5;
    const calendarDaysNeeded = doBackfill ? Math.ceil(backfillDays * 1.6) + bufferDays : 12;
    const from = shiftIsoDate(asOf, -calendarDaysNeeded);
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

    // Filter rows if backfill requested (keep last N trading days worth of bars)
    // EODHD returns trading days only, so we take the last N rows (each row = 1 trading day).
    const selected = doBackfill ? rows.slice(-backfillDays) : [rows[rows.length - 1]];

    // Build payloads
    const payloads = selected
      .filter(r => r && r.date)
      .map(r => ({
        code: symbol,
        name: "VIX Proxy (VXX) — Short-Term VIX Futures ETN",
        date: r.date,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        adj_close: r.adjusted_close ?? null,
        volume: r.volume ?? null,
        updatedAt: new Date().toISOString(),
        source: "eodhd",
        note: "VXX is a tradable proxy tied to short-term VIX futures; can decay in calm markets.",
      }));

    if (payloads.length === 0) throw new Error("No valid dated bars to write.");

    // Latest = most recent by date (array should already be chronological)
    const latest = payloads[payloads.length - 1];

    const writtenKeys = [];
    if (!dryrun) {
      const cmds = [];

      // Write each dated key
      for (const p of payloads) {
        const datedKey = `market:vixproxy:eod:${p.date}`;
        cmds.push(["SET", datedKey, JSON.stringify(p)]);
        writtenKeys.push(datedKey);
      }

      // Write latest keys
      cmds.push(["SET", "market:vixproxy:eod:latest", JSON.stringify(latest)]);
      cmds.push(["SET", "market:vixproxy:latestDate", latest.date]);
      writtenKeys.push("market:vixproxy:eod:latest", "market:vixproxy:latestDate");

      // Upstash pipeline can be large; chunk to be safe
      const CHUNK = 250;
      for (let i = 0; i < cmds.length; i += CHUNK) {
        await redisPipeline(cmds.slice(i, i + CHUNK));
      }
    }

    return jsonResponse(200, {
      ok: true,
      dryrun,
      symbol,
      from,
      to,
      mode: doBackfill ? `backfill:${backfillDays}` : "latest-only",
      fetchedRows: rows.length,
      writtenDays: payloads.length,
      latestDate: latest.date,
      writtenKeys: dryrun ? [] : writtenKeys.slice(0, 10).concat(writtenKeys.length > 10 ? ["..."] : []),
    });
  } catch (err) {
    return jsonResponse(500, {
      ok: false,
      error: err?.message || String(err),
    });
  }
};
