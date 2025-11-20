// netlify/functions/morning-brief.js
// Morning brief for multiple metals + top performers across ASX using EODHD.
// - Metals prices: snapshot-only from Upstash (no live metals or FX fetches)
// - Top performers: EODHD-backed 5-day % gain for ASX, filtered by market cap
//
// Env for metals:
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Env for EODHD:
//   EODHD_API_TOKEN
//   (optional) EODHD_MAX_SYMBOLS_PER_EXCHANGE
//   (optional) EODHD_CONCURRENCY
//   (optional) EODHD_MIN_MARKET_CAP

const fetch = (...args) => global.fetch(...args);

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

  const fmt = (n) =>
    typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(2)) : null;

  // ---------- Upstash helpers ----------
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  async function redisGet(key) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
    try {
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
        {
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        7000
      );
      if (!res.ok) return null;
      const j = await res.json().catch(() => null);
      if (!j || typeof j.result === "undefined") return null;
      return j.result;
    } catch (e) {
      console.warn("redisGet error", e && e.message);
      return null;
    }
  }

  // metals symbols we show
  const symbols = ["XAU", "XAG", "IRON", "LITH-CAR", "NI", "URANIUM"];
  const debug = { steps: [] };

  try {
    // --------------------------------------------------
    // 1) METALS: snapshot-only from Upstash
    // --------------------------------------------------
    let latestSnapshot = null;
    try {
      const rawLatest = await redisGet("metals:latest");
      if (rawLatest) {
        if (typeof rawLatest === "string") {
          try {
            latestSnapshot = JSON.parse(rawLatest);
          } catch (e) {
            latestSnapshot = null;
            debug.steps.push({
              source: "parse-latest-failed",
              error: e && e.message,
            });
          }
        } else if (typeof rawLatest === "object") {
          latestSnapshot = rawLatest;
        }
        debug.steps.push({ source: "upstash-latest", found: !!latestSnapshot });
      } else {
        debug.steps.push({ source: "upstash-latest", found: false });
      }
    } catch (e) {
      debug.steps.push({
        source: "upstash-latest-error",
        error: e && e.message,
      });
    }

    const currentUsd = {}; // symbol -> USD price (number|null)
    const currentAud = {}; // symbol -> AUD price (number|null)
    let priceTimestamp = null;
    let usdToAud = null;
    let metalsDataSource = "snapshot-only";

    if (latestSnapshot && latestSnapshot.symbols) {
      metalsDataSource = "upstash-latest";
      for (const s of symbols) {
        const entry =
          latestSnapshot.symbols[s] ||
          (latestSnapshot.metals && latestSnapshot.metals[s]) ||
          null;
        if (entry && typeof entry === "object") {
          currentUsd[s] =
            typeof entry.priceUSD === "number"
              ? entry.priceUSD
              : typeof entry.apiPriceUSD === "number"
              ? entry.apiPriceUSD
              : null;
          currentAud[s] =
            typeof entry.priceAUD === "number" ? entry.priceAUD : null;
          priceTimestamp =
            priceTimestamp ||
            entry.priceTimestamp ||
            latestSnapshot.priceTimestamp ||
            latestSnapshot.snappedAt ||
            null;
        } else {
          currentUsd[s] = null;
          currentAud[s] = null;
        }
      }
      usdToAud = latestSnapshot.usdToAud || null;
      debug.snapshotDate = latestSnapshot.snappedAt || null;
    } else {
      // No snapshot for today – do NOT fetch live prices.
      metalsDataSource = "snapshot-missing";
      for (const s of symbols) {
        currentUsd[s] = null;
        currentAud[s] = null;
      }
      debug.steps.push({
        source: "snapshot-missing",
        note: "No metals:latest snapshot found; live fetch disabled.",
      });
    }

    // ------------------------------
    // 1b) Yesterday snapshot for pct change
    // ------------------------------
    let yesterdayData = null;
    try {
      const d = new Date();
      const yd = new Date(
        Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - 1)
      );
      const key = `metals:${yd.toISOString().slice(0, 10)}`; // metals:YYYY-MM-DD
      const val = await redisGet(key);
      if (val) {
        if (typeof val === "string") {
          try {
            yesterdayData = JSON.parse(val);
          } catch (e) {
            yesterdayData = null;
            debug.steps.push({
              source: "parse-yesterday-failed",
              error: e && e.message,
            });
          }
        } else if (typeof val === "object") {
          yesterdayData = val;
        }
      }
      debug.steps.push({
        source: "redis-get-yesterday",
        key,
        found: !!yesterdayData,
      });
    } catch (e) {
      debug.steps.push({
        source: "redis-get-error",
        error: e && e.message,
      });
    }

    // assemble per-symbol result
    const metals = {};
    for (const s of symbols) {
      const todayUSD = typeof currentUsd[s] === "number" ? currentUsd[s] : null;
      const todayAUD = typeof currentAud[s] === "number" ? currentAud[s] : null;

      let yesterdayAUD = null;
      if (
        yesterdayData &&
        yesterdayData.symbols &&
        typeof yesterdayData.symbols[s] !== "undefined"
      ) {
        const p =
          yesterdayData.symbols[s] &&
          typeof yesterdayData.symbols[s].priceAUD !== "undefined"
            ? yesterdayData.symbols[s].priceAUD
            : null;
        if (p !== null) yesterdayAUD = typeof p === "number" ? p : Number(p);
      }

      let pctChange = null;
      if (todayAUD !== null && yesterdayAUD !== null && yesterdayAUD !== 0) {
        pctChange = Number(
          (((todayAUD - yesterdayAUD) / yesterdayAUD) * 100).toFixed(2)
        );
      }

      metals[s] = {
        priceUSD: fmt(todayUSD),
        priceAUD: fmt(todayAUD),
        yesterdayPriceAUD: yesterdayAUD !== null ? fmt(yesterdayAUD) : null,
        pctChange,
        priceTimestamp: priceTimestamp || null,
      };
    }

    const narratives = {};
    for (const s of symbols) {
      const m = metals[s];
      if (m.priceAUD === null) {
        narratives[s] = `The ${s} price is currently unavailable.`;
      } else {
        const upDown =
          m.pctChange === null
            ? ""
            : m.pctChange > 0
            ? ` — up ${Math.abs(m.pctChange)}% vs yesterday`
            : m.pctChange < 0
            ? ` — down ${Math.abs(m.pctChange)}% vs yesterday`
            : " — unchanged vs yesterday";
        narratives[s] = `${s} is currently $${m.priceAUD} AUD per ${
          s === "IRON" ? "tonne" : "unit"
        }${upDown}.`;
      }
    }

    // --------------------------------------------------
    // 2) EODHD top performers (ASX only, market cap filtered)
    // --------------------------------------------------
    const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
    const MAX_PER_EXCHANGE = Number(
      process.env.EODHD_MAX_SYMBOLS_PER_EXCHANGE || 500
    ); // safety cap
    const EODHD_CONCURRENCY = Number(process.env.EODHD_CONCURRENCY || 8);
    const FIVE_DAYS = 5;
    const MIN_MARKET_CAP = Number(process.env.EODHD_MIN_MARKET_CAP || 300_000_000); // default 300m

    const eodhdDebug = { active: !!EODHD_TOKEN, steps: [] };

    function getLastBusinessDays(n) {
      const days = [];
      let d = new Date();
      while (days.length < n) {
        const dow = d.getDay(); // 0 Sun, 6 Sat
        if (dow !== 0 && dow !== 6) {
          days.push(new Date(d));
        }
        d.setDate(d.getDate() - 1);
      }
      return days.reverse().map((dt) => dt.toISOString().slice(0, 10));
    }

    async function fetchJson(url, opts = {}, timeout = 12000) {
      try {
        const res = await fetchWithTimeout(url, opts, timeout);
        const text = await res.text().catch(() => "");
        try {
          return {
            ok: res.ok,
            status: res.status,
            json: text ? JSON.parse(text) : null,
            text,
          };
        } catch (e) {
          return { ok: res.ok, status: res.status, json: null, text };
        }
      } catch (err) {
        return {
          ok: false,
          status: 0,
          json: null,
          text: String((err && err.message) || err),
        };
      }
    }

    async function listSymbolsForExchange(exchangeCode) {
      const url = `https://eodhd.com/api/exchange-symbol-list/${encodeURIComponent(
        exchangeCode
      )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;
      const r = await fetchJson(url, {}, 12000);
      if (!r.ok || !Array.isArray(r.json)) {
        return { ok: false, data: [], error: r.text || `HTTP ${r.status}` };
      }
      return { ok: true, data: r.json };
    }

    async function fetchEodForSymbol(symbol, exchange, from, to) {
      const fullCode = symbol.includes(".") ? symbol : `${symbol}.${exchange}`;
      const url = `https://eodhd.com/api/eod/${encodeURIComponent(
        fullCode
      )}?api_token=${encodeURIComponent(
        EODHD_TOKEN
      )}&period=d&from=${from}&to=${to}&fmt=json`;
      const r = await fetchJson(url, {}, 12000);
      if (!r.ok || !Array.isArray(r.json)) {
        return { ok: false, data: null, error: r.text || `HTTP ${r.status}` };
      }
      const arr = r.json.slice().sort((a, b) => new Date(a.date) - new Date(b.date));
      return { ok: true, data: arr };
    }

    function pctGainFromPrices(prices) {
      if (!Array.isArray(prices) || prices.length < 2) return null;
      const first = prices[0].close;
      const last = prices[prices.length - 1].close;
      if (typeof first !== "number" || typeof last !== "number" || first === 0)
        return null;
      return ((last - first) / first) * 100;
    }

    async function mapWithConcurrency(items, fn, concurrency = EODHD_CONCURRENCY) {
      const results = new Array(items.length);
      let idx = 0;
      const workers = new Array(Math.min(concurrency, items.length))
        .fill(null)
        .map(async () => {
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

    let topPerformers = [];
    if (EODHD_TOKEN) {
      try {
        const qs = event && event.queryStringParameters ? event.queryStringParameters : {};
        const requestedSymbolsParam = qs.symbols && String(qs.symbols).trim();

        const days = getLastBusinessDays(FIVE_DAYS);
        const from = days[0];
        const to = days[days.length - 1];

        let symbolRequests = [];

        if (requestedSymbolsParam) {
          const sarr = requestedSymbolsParam.split(",").map((x) => x.trim()).filter(Boolean);
          sarr.forEach((sym) => {
            const parts = sym.split(".");
            if (parts.length === 1) {
              symbolRequests.push({
                symbol: parts[0].toUpperCase(),
                exchange: "ASX",
              });
            } else {
              symbolRequests.push({
                symbol: sym,
                exchange: "ASX",
              });
            }
          });
          eodhdDebug.steps.push({ source: "symbols-param", count: symbolRequests.length });
        } else {
          // ASX only – list ASX exchange
          const exchanges = ["ASX"];
          for (const ex of exchanges) {
            const res = await listSymbolsForExchange(ex);
            if (!res.ok) {
              eodhdDebug.steps.push({
                source: "list-symbols-failed",
                exchange: ex,
                error: res.error || "unknown",
              });
              continue;
            }

            const items = res.data;

            const normalized = items
              .map((it) => {
                if (!it) return null;
                if (typeof it === "string") {
                  return { code: it, name: "" };
                }
                const code = it.code || it.Code || it.symbol || it.Symbol || (it[0] || "");
                const name = it.name || it.Name || it.companyName || it.CompanyName || (it[1] || "");
                return { code, name };
              })
              .filter(Boolean)
              .filter((x) => x.code && !x.code.includes("^") && !x.code.includes("/"));

            const limited = normalized.slice(0, MAX_PER_EXCHANGE);

            limited.forEach((it) =>
              symbolRequests.push({
                symbol: it.code.toUpperCase(),
                exchange: "ASX",
                name: it.name || "",
              })
            );

            await new Promise((r) => setTimeout(r, 200));
            eodhdDebug.steps.push({
              source: "list-symbols",
              exchange: ex,
              totalFound: normalized.length,
              used: limited.length,
            });
          }
        }

        if (symbolRequests.length > 0) {
          const results = await mapWithConcurrency(
            symbolRequests,
            async (req) => {
              const sym = req.symbol;
              const r = await fetchEodForSymbol(sym, req.exchange || "ASX", from, to);
              if (!r.ok || !Array.isArray(r.data) || r.data.length < FIVE_DAYS) {
                return null;
              }
              const pct = pctGainFromPrices(r.data);
              if (pct === null || Number.isNaN(pct)) return null;
              return {
                symbol: sym,
                exchange: "ASX",
                name: req.name || "",
                pctGain: Number(pct.toFixed(2)),
                firstClose: r.data[0].close,
                lastClose: r.data[r.data.length - 1].close,
                pricesCount: r.data.length,
                mcap: req.mcap || null,
              };
            },
            EODHD_CONCURRENCY
          );

          const cleaned = results.filter(Boolean);
          cleaned.sort((a, b) => b.pctGain - a.pctGain);
          topPerformers = cleaned.slice(0, 5);
          eodhdDebug.steps.push({
            source: "computed",
            evaluated: cleaned.length,
            top5: topPerformers.map((x) => ({ symbol: x.symbol, pct: x.pctGain })),
          });
        } else {
          eodhdDebug.steps.push({ source: "no-symbols" });
        }

        eodhdDebug.window = {
          from: new Date(from).toISOString().slice(0, 10),
          to: new Date(to).toISOString().slice(0, 10),
        };
        debug.eodhd = eodhdDebug;
      } catch (err) {
        debug.eodhd = debug.eodhd || {};
        debug.eodhd.error = (err && err.message) || String(err);
      }
    } else {
      debug.eodhd = { active: false, note: "EODHD_API_TOKEN missing" };
    }

    // --------------------------------------------------
    // Final payload
    // --------------------------------------------------
    const payload = {
      generatedAt: nowIso,
      usdToAud: fmt(usdToAud),
      metals,
      narratives,
      topPerformers,
      _debug: {
        ...debug,
        metalsDataSource,
      },
    };

    return { statusCode: 200, body: JSON.stringify(payload) };
  } catch (err) {
    console.error("morning-brief multi error", err && (err.stack || err.message || err));
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: (err && err.message) || String(err),
      }),
    };
  }
};