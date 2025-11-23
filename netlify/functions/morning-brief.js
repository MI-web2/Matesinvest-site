// netlify/functions/morning-brief.js
// Morning brief for multiple metals + top performers across ASX (AU) using EODHD.
// - Metals prices: snapshot-only from Upstash (no live metals/FX fetches)
// - Top performers: EODHD-backed 5-business-day % gain for AU exchange,
//   filtered by market cap using Upstash "mcaps:latest".
//
// Required env:
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//   EODHD_API_TOKEN
// Optional env:
//   EODHD_MAX_SYMBOLS_PER_EXCHANGE (default 500)
//   EODHD_CONCURRENCY (default 8)
//   EODHD_MIN_MARKET_CAP (default 300_000_000)

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // -------------------------------
  // Helpers
  // -------------------------------
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

  async function redisSet(key, value) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return false;
    try {
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/set/${encodeURIComponent(key)}`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "application/json" },
          body: JSON.stringify({ value }),
        },
        8000
      );
      return !!res.ok;
    } catch (e) {
      console.warn("redisSet error", e && e.message);
      return false;
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
      // Work out "yesterday" relative to the time the latest snapshot was taken,
      // not the server's current clock. This avoids UTC vs AEST date mismatches.
      let keyDate;

      if (latestSnapshot && (latestSnapshot.snappedAt || latestSnapshot.priceTimestamp)) {
        const base = new Date(
          latestSnapshot.snappedAt || latestSnapshot.priceTimestamp
        );
        base.setUTCDate(base.getUTCDate() - 1);
        keyDate = base.toISOString().slice(0, 10);
      } else {
        // Fallback: one calendar day before current UTC date
        const d = new Date();
        const yd = new Date(
          Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - 1)
        );
        keyDate = yd.toISOString().slice(0, 10);
      }

      const key = `metals:${keyDate}`; // metals:YYYY-MM-DD
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
    // 2) EODHD top performers (AU exchange) + market cap filter
    // --------------------------------------------------
    const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;
    const MAX_PER_EXCHANGE = Number(
      process.env.EODHD_MAX_SYMBOLS_PER_EXCHANGE || 500
    ); // safety cap
    const EODHD_CONCURRENCY = Number(process.env.EODHD_CONCURRENCY || 8);
    const MIN_MCAP = Number(process.env.EODHD_MIN_MARKET_CAP || 300000000);
    const FIVE_DAYS = 5;

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
      const arr = r.json
        .slice()
        .sort((a, b) => new Date(a.date) - new Date(b.date));
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
        const qs =
          (event && event.queryStringParameters) || {};
        const requestedSymbolsParam =
          qs.symbols && String(qs.symbols).trim();

        const days = getLastBusinessDays(FIVE_DAYS);
        const from = days[0];
        const to = days[days.length - 1];

        let symbolRequests = [];

        if (requestedSymbolsParam) {
          // Explicit list of symbols (dev / debugging)
          const sarr = requestedSymbolsParam
            .split(",")
            .map((x) => x.trim())
            .filter(Boolean);
          sarr.forEach((sym) => {
            const parts = sym.split(".");
            if (parts.length === 1) {
              symbolRequests.push({
                symbol: parts[0].toUpperCase(),
                exchange: "AU",
              });
            } else {
              symbolRequests.push({
                symbol: sym,
                exchange: "AU",
              });
            }
          });
          eodhdDebug.steps.push({
            source: "symbols-param",
            count: symbolRequests.length,
          });
        } else {
          // AU only – list AU exchange fully, then trim to MAX_PER_EXCHANGE
          const exchanges = ["AU"];
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
                const code =
                  it.code ||
                  it.Code ||
                  it.symbol ||
                  it.Symbol ||
                  it[0] ||
                  "";
                const name =
                  it.name ||
                  it.Name ||
                  it.companyName ||
                  it.CompanyName ||
                  it[1] ||
                  "";
                return { code, name };
              })
              .filter(Boolean)
              .filter(
                (x) => x.code && !x.code.includes("^") && !x.code.includes("/")
              );

            const limited = normalized.slice(0, MAX_PER_EXCHANGE);

            limited.forEach((it) =>
              symbolRequests.push({
                symbol: it.code.toUpperCase(),
                exchange: "AU",
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
              const r = await fetchEodForSymbol(
                sym,
                req.exchange || "AU",
                from,
                to
              );
              if (!r.ok || !Array.isArray(r.data) || r.data.length < FIVE_DAYS) {
                return null;
              }
              const pct = pctGainFromPrices(r.data);
              if (pct === null || Number.isNaN(pct)) return null;
              return {
                symbol: sym,
                exchange: "AU",
                name: req.name || "",
                pctGain: Number(pct.toFixed(2)),
                firstClose: r.data[0].close,
                lastClose: r.data[r.data.length - 1].close,
                pricesCount: r.data.length,
              };
            },
            EODHD_CONCURRENCY
          );

          const cleaned = results.filter(Boolean);
          cleaned.sort((a, b) => b.pctGain - a.pctGain);

          // -----------------------------
          // Load today's market caps from Upstash
          // -----------------------------
          let mcapMap = {};
          try {
            const rawMcap = await redisGet("mcaps:latest");
            if (rawMcap) {
              const arr =
                typeof rawMcap === "string" ? JSON.parse(rawMcap) : rawMcap;
              if (Array.isArray(arr)) {
                for (const item of arr) {
                  if (!item || !item.code) continue;
                  const base = String(item.code).replace(/\.AU$/i, "");
                  const mc =
                    typeof item.mcap === "number"
                      ? item.mcap
                      : Number(item.mcap);
                  if (!Number.isNaN(mc)) mcapMap[base] = mc;
                }
              }
            }
            eodhdDebug.steps.push({
              source: "mcaps-loaded",
              count: Object.keys(mcapMap).length,
              minMcap: MIN_MCAP,
            });
          } catch (err) {
            console.warn("Failed to load mcaps", err);
            eodhdDebug.steps.push({
              source: "mcaps-error",
              error: err && err.message,
            });
          }

          // -----------------------------
          // Apply minimum market cap filter BEFORE slicing
          // -----------------------------
          let filtered = cleaned;
          if (Object.keys(mcapMap).length > 0) {
            filtered = cleaned.filter((tp) => {
              const base = String(tp.symbol).replace(/\.AU$/i, "");
              const mc = mcapMap[base];
              return typeof mc === "number" && mc >= MIN_MCAP;
            });
          }

          // If filter nukes everything (eg. missing mcaps), fall back to cleaned
          const sourceArray = filtered.length ? filtered : cleaned;

          topPerformers = sourceArray.slice(0, 5);

          eodhdDebug.steps.push({
            source: "computed-with-mcap",
            evaluated: cleaned.length,
            afterMcapFilter: filtered.length,
            top5: topPerformers.map((x) => ({
              symbol: x.symbol,
              pct: x.pctGain,
            })),
          });

          // -----------------------------
          // Persist topPerformers to Upstash (best-effort)
          // -----------------------------
          try {
            const today = new Date().toISOString().slice(0,10);
            await redisSet("topPerformers:latest", JSON.stringify(topPerformers));
            await redisSet(`topPerformers:${today}`, JSON.stringify(topPerformers));
            eodhdDebug.steps.push({ source: "topperformers-saved", count: topPerformers.length });
          } catch (e) {
            eodhdDebug.steps.push({ source: "topperformers-save-failed", error: e && e.message });
          }

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
    console.error(
      "morning-brief multi error",
      err && (err.stack || err.message || err)
    );
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: (err && err.message) || String(err),
      }),
    };
  }
};