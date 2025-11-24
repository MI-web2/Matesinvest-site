// netlify/functions/morning-brief.js
// Morning brief for multiple metals + top performers across ASX (AU).
// - Metals prices: snapshot-only from Upstash (no live metals/FX fetches)
// - Top performers: now read from Upstash key `asx200:latest` and pick the TOP_N
//   largest percent gain from the most recent business day snapshot.
//   (This avoids EODHD calls and uses the asx200 snapshot you already produce.)

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

  // Normalize symbol / code (strip dot-suffix and uppercase)
  function normalizeCode(code) {
    return String(code || "").replace(/\.[A-Z0-9]{1,6}$/i, "").toUpperCase();
  }

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
      const valString =
        typeof value === "string" ? value : JSON.stringify(value);

      const url =
        `${UPSTASH_URL}/set/` +
        `${encodeURIComponent(key)}/` +
        `${encodeURIComponent(valString)}`;

      const res = await fetchWithTimeout(
        url,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` }
        },
        8000
      );

      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        console.warn("redisSet failed", key, res.status, txt);
        return false;
      }
      return true;
    } catch (e) {
      console.warn("redisSet error", key, e && e.message);
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

    const currentUsd = {};
    const currentAud = {};
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
      let keyDate;
      if (latestSnapshot && (latestSnapshot.snappedAt || latestSnapshot.priceTimestamp)) {
        const base = new Date(
          latestSnapshot.snappedAt || latestSnapshot.priceTimestamp
        );
        base.setUTCDate(base.getUTCDate() - 1);
        keyDate = base.toISOString().slice(0, 10);
      } else {
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

    // assemble per-symbol result for metals
    const metals = {};
    for (const s of symbols) {
      const todayUSD = typeof currentUsd[s] === "number" ? currentUsd[s] : null;
      const todayAUD = typeof currentAud[s] === "number" ? currentAud[s] : null;

      // today's unit (from latestSnapshot.symbols[s].unit)
      let unitLabel = "unit";
      if (
        latestSnapshot &&
        latestSnapshot.symbols &&
        latestSnapshot.symbols[s] &&
        typeof latestSnapshot.symbols[s].unit === "string"
      ) {
        unitLabel = latestSnapshot.symbols[s].unit;
      }

      // yesterday's price + unit
      let yesterdayAUD = null;
      let yesterdayUnit = unitLabel;

      if (
        yesterdayData &&
        yesterdayData.symbols &&
        typeof yesterdayData.symbols[s] !== "undefined"
      ) {
        const yEntry = yesterdayData.symbols[s];
        const p =
          yEntry && typeof yEntry.priceAUD !== "undefined"
            ? yEntry.priceAUD
            : null;
        if (p !== null) yesterdayAUD = typeof p === "number" ? p : Number(p);

        if (yEntry && typeof yEntry.unit === "string") {
          yesterdayUnit = yEntry.unit;
        }
      }

      let pctChange = null;
      // Only compute pct change if units match and yesterday price is sensible
      if (
        todayAUD !== null &&
        yesterdayAUD !== null &&
        yesterdayAUD !== 0 &&
        unitLabel === yesterdayUnit
      ) {
        const rawPct = ((todayAUD - yesterdayAUD) / yesterdayAUD) * 100;
        // guardrail: ignore absurd moves (e.g. from old bugged snapshots)
        if (Number.isFinite(rawPct) && Math.abs(rawPct) <= 1000) {
          pctChange = Number(rawPct.toFixed(2));
        }
      }

      metals[s] = {
        priceUSD: fmt(todayUSD),
        priceAUD: fmt(todayAUD),
        yesterdayPriceAUD: yesterdayAUD !== null ? fmt(yesterdayAUD) : null,
        pctChange,
        priceTimestamp: priceTimestamp || null,
        unit: unitLabel,
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
          m.unit || (s === "IRON" ? "tonne" : "unit")
        }${upDown}.`;

      }
    }

    // --------------------------------------------------
    // 2) TOP PERFORMERS: use asx200:latest from Upstash and pick top N gainers for last business day
    // --------------------------------------------------
    const TOP_N = Number(process.env.TOP_N || 6); // default 6
    let topPerformers = [];
    try {
      const raw = await redisGet("asx200:latest");
      let asxRows = null;
      if (raw) {
        if (typeof raw === "string") {
          try {
            asxRows = JSON.parse(raw);
          } catch (e) {
            // if parse fails, try to continue treating as null
            debug.steps.push({ source: "parse-asx200-latest-failed", error: e && e.message });
            asxRows = null;
          }
        } else if (Array.isArray(raw)) {
          asxRows = raw;
        } else if (typeof raw === "object" && Array.isArray(raw.result)) {
          // defensive: some Upstash returns wrap in { result: [...] }
          asxRows = raw.result;
        } else {
          asxRows = raw;
        }
      }

      if (!Array.isArray(asxRows)) {
        debug.steps.push({ source: "asx200-latest-missing-or-invalid", found: !!asxRows });
      } else {
        // compute pctChange if not present and filter valid rows
        const cleaned = asxRows
          .map((r) => {
            const last = typeof r.lastPrice === "number" ? r.lastPrice : (r.lastPrice ? Number(r.lastPrice) : null);
            const prev = typeof r.yesterdayPrice === "number" ? r.yesterdayPrice : (r.yesterdayPrice ? Number(r.yesterdayPrice) : null);
            let pct = null;
            if (last !== null && prev !== null && prev !== 0) {
              pct = ((last - prev) / prev) * 100;
            } else if (typeof r.pctChange === "number") {
              pct = r.pctChange;
            }
            return {
              code: normalizeCode(r.code || r.fullCode || r.symbol || ""),
              fullCode: r.fullCode || r.full_code || r.full || r.code || null,
              name: r.name || r.companyName || "",
              lastDate: r.lastDate || r.date || null,
              lastPrice: typeof last === "number" && !Number.isNaN(last) ? Number(last) : null,
              yesterdayDate: r.yesterdayDate || null,
              yesterdayPrice: typeof prev === "number" && !Number.isNaN(prev) ? Number(prev) : null,
              pctChange: typeof pct === "number" && Number.isFinite(pct) ? Number(pct) : null,
              raw: r
            };
          })
          .filter((x) => x && x.lastPrice !== null && x.yesterdayPrice !== null && x.pctChange !== null);

        cleaned.sort((a, b) => b.pctChange - a.pctChange);

        // Create mapping that includes both UI-expected keys (symbol, lastClose, pctGain)
        // and the snapshot-style keys (code, lastPrice, pctChange) so existing client HTML works.
        topPerformers = cleaned.slice(0, TOP_N).map((x) => ({
          // UI-friendly names used by mates-summaries.html
          symbol: x.code || null,
          lastClose: typeof x.lastPrice === "number" ? x.lastPrice : null,
          pctGain: typeof x.pctChange === "number" ? Number(x.pctChange.toFixed(2)) : null,
          name: x.name || "",

          // snapshot-style names preserved for downstream consumers
          code: x.code || null,
          fullCode: x.fullCode || null,
          lastDate: x.lastDate || null,
          lastPrice: typeof x.lastPrice === "number" ? x.lastPrice : null,
          yesterdayDate: x.yesterdayDate || null,
          yesterdayPrice: typeof x.yesterdayPrice === "number" ? x.yesterdayPrice : null,
          pctChange: typeof x.pctChange === "number" ? Number(x.pctChange.toFixed(2)) : null,
        }));

        debug.steps.push({
          source: "computed-top-performers-from-asx200-latest",
          available: cleaned.length,
          topN: TOP_N,
          topSample: topPerformers.map((t) => ({ symbol: t.symbol, pct: t.pctGain })),
        });

        // persist topPerformers to Upstash (best-effort)
        try {
          const today = new Date().toISOString().slice(0, 10);
          await redisSet("topPerformers:latest", topPerformers);
          await redisSet(`topPerformers:${today}`, topPerformers);
          debug.steps.push({ source: "top-performers-saved", count: topPerformers.length });
        } catch (e) {
          debug.steps.push({ source: "top-performers-save-failed", error: e && e.message });
        }
      }
    } catch (e) {
      debug.steps.push({ source: "top-performers-error", error: e && e.message });
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
        topSource: "asx200:latest"
      },
    };

    // small helpful console log for browser debug when opening brief
    try { console.log("[morning-brief] payload sample:", { topPerformers: payload.topPerformers && payload.topPerformers.slice(0,6), debug: payload._debug }); } catch (e) {}

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