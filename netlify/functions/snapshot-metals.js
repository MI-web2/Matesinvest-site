// netlify/functions/snapshot-metals.js
// Snapshot multiple metals and write to Upstash: metals:YYYY-MM-DD and metals:latest
//
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Symbols stored: XAU, XAG, IRON, LITH-CAR, NI, URANIUM
//
// How this works:
// - We call Metals-API /latest with base=USD and the correct unit parameter:
//     XAU, XAG              -> unit=Troy Ounce  (USD per oz target)
//     IRON, LITH-CAR, NI    -> unit=Ton         (USD per tonne target)
//     URANIUM               -> unit=Pound       (USD per lb target)
//
// - Metals-API response example (for IRON):
//   {
//     "base": "USD",
//     "rates": {
//       "IRON": 10826469.433475018,
//       "USD": 1,
//       "USDIRON": 114.9270710242
//     }
//   }
//
//   In this shape:
//   - USDIRON is the direct "USD per unit" price we want.
//   - IRON is a normalization rate (metal per USD) that we generally ignore.
//
// - For each symbol we therefore:
//   1) Prefer rates[`USD${symbol}`] if present and sane (e.g. USDIRON).
//   2) If that is missing, fall back to interpreting rates[symbol] via
//      direct vs inverse + sanity ranges.
//
// - We convert USD -> AUD via FX and store priceAUD alongside priceUSD.
// - We attach a "sanity" section with any symbols that failed checks.

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // Return YYYY-MM-DD string using AEST (Australia/Brisbane, UTC+10, no DST)
  function getTodayAestDateString(baseDate = new Date()) {
    const AEST_OFFSET_MINUTES = 10 * 60; // Brisbane is UTC+10 all year
    const aestTime = new Date(baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
    return aestTime.toISOString().slice(0, 10);
  }

  // -----------------------------
  // Helpers
  // -----------------------------
  async function fetchWithTimeout(url, opts = {}, timeout = 9000) {
    ...


  // -----------------------------
  // Helpers
  // -----------------------------
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

  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || null;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || null;

  async function redisSet(key, value) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return false;
    try {
      const encoded = encodeURIComponent(JSON.stringify(value));
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/set/${encodeURIComponent(key)}/${encoded}`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        console.warn("redisSet failed", key, res.status, txt.slice(0, 300));
      }
      return res.ok;
    } catch (e) {
      console.warn("redisSet error", key, e && e.message);
      return false;
    }
  }

  const fmt = (n) =>
    typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(2)) : null;

  // Map Metals-API unit -> human label for UI
  const UNIT_LABELS = {
    troy_ounce: "oz",
    ounce: "oz",
    Ounce: "oz",
    "Troy Ounce": "oz",
    pound: "lb",
    Pound: "lb",
    gram: "g",
    Gram: "g",
    kilogram: "kg",
    KG: "kg",
    kilogramme: "kg",
    ton: "tonne",
    Ton: "tonne",
  };

  // Rough sanity ranges in USD per unit for each symbol.
  // Intentionally wide – just enough to filter out absurd values.
  const VALID_RANGES = {
    XAU: { min: 800, max: 7000 },            // gold per oz
    XAG: { min: 5, max: 150 },               // silver per oz
    IRON: { min: 30, max: 500 },             // iron ore per tonne
    "LITH-CAR": { min: 3000, max: 150000 },  // lithium carbonate per tonne
    NI: { min: 8000, max: 150000 },          // nickel per tonne
    URANIUM: { min: 10, max: 500 },          // uranium per lb
  };

  // Given a candidate price and a symbol, check if it's within our sanity window
  function isInRange(symbol, price) {
    const range = VALID_RANGES[symbol];
    if (!range || typeof price !== "number" || !Number.isFinite(price)) return false;
    return price >= range.min && price <= range.max;
  }

  // Fallback interpreter for when USD{SYMBOL} is missing:
  // Try treating rate as USD per unit or unit per USD and see what fits.
  function deriveFromRawRate(symbol, rateRaw) {
    if (typeof rateRaw !== "number" || !Number.isFinite(rateRaw) || rateRaw <= 0) {
      return { priceUSD: null, mode: "invalid_rate" };
    }

    const range = VALID_RANGES[symbol];
    const candDirect = rateRaw;      // treat as USD per unit
    const candInverse = 1 / rateRaw; // treat as unit price inverted

    if (!range) {
      // No range defined: default to inverse (Metals-API base=USD docs behaviour)
      return { priceUSD: candInverse, mode: "inverse_no_range" };
    }

    const directOK = isInRange(symbol, candDirect);
    const inverseOK = isInRange(symbol, candInverse);

    if (directOK && !inverseOK) return { priceUSD: candDirect, mode: "direct" };
    if (!directOK && inverseOK) return { priceUSD: candInverse, mode: "inverse" };

    if (directOK && inverseOK) {
      // Pick whichever is closer to the middle of the range
      const mid = (range.min + range.max) / 2;
      const dDist = Math.abs(candDirect - mid);
      const iDist = Math.abs(candInverse - mid);
      return dDist <= iDist
        ? { priceUSD: candDirect, mode: "direct_both_ok" }
        : { priceUSD: candInverse, mode: "inverse_both_ok" };
    }

    // Neither in range – treat as unusable.
    return {
      priceUSD: null,
      mode: "out_of_range",
      debug: { candDirect, candInverse, range },
    };
  }

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) {
      return { statusCode: 500, body: "Missing METALS_API_KEY" };
    }
    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return { statusCode: 500, body: "Missing Upstash env vars" };
    }

    // -------------------------------
    // 1) Fetch metals prices in realistic units
    // -------------------------------
    const groups = [
      {
        unitParam: "Troy Ounce", // XAU, XAG per oz
        symbols: ["XAU", "XAG"],
      },
      {
        unitParam: "Ton", // IRON, LITH-CAR, NI per tonne
        symbols: ["IRON", "LITH-CAR", "NI"],
      },
      {
        unitParam: "Pound", // URANIUM per lb
        symbols: ["URANIUM"],
      },
    ];

    const allRates = {}; // symbol -> { ratesObj, unitParam }
    let anyTimestamp = null;
    const previews = [];

    for (const group of groups) {
      const { unitParam, symbols } = group;
      const url =
        `https://metals-api.com/api/latest` +
        `?access_key=${encodeURIComponent(METALS_API_KEY)}` +
        `&base=USD` +
        `&symbols=${encodeURIComponent(symbols.join(","))}` +
        `&unit=${encodeURIComponent(unitParam)}`;

      const res = await fetchWithTimeout(url, {}, 10000);
      const txt = await res.text().catch(() => "");
      previews.push({
        url,
        status: res.status,
        bodyPreview: txt.slice(0, 1200),
      });

      let json = null;
      try {
        json = txt ? JSON.parse(txt) : null;
      } catch (e) {
        json = null;
      }

      if (!json || json.success === false) {
        console.warn("metals-api group failure", unitParam, json && json.error);
        continue;
      }

      const rates = json && json.rates ? json.rates : null;
      if (!rates) continue;

      if (typeof json.timestamp === "number") {
        const ts = new Date(json.timestamp * 1000).toISOString();
        if (!anyTimestamp || ts > anyTimestamp) anyTimestamp = ts;
      }

      for (const s of symbols) {
        allRates[s] = {
          rates,
          unitParam,
        };
      }
    }

    // -------------------------------
    // 2) FX to AUD
    // -------------------------------
    let usdToAud = null;
    try {
      // Primary FX source
      let fRes = await fetchWithTimeout(
        "https://open.er-api.com/v6/latest/USD",
        {},
        7000
      );
      let ftxt = await fRes.text().catch(() => "");
      let fj = null;
      try {
        fj = ftxt ? JSON.parse(ftxt) : null;
      } catch (e) {
        fj = null;
      }
      if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === "number") {
        usdToAud = Number(fj.rates.AUD);
      } else {
        // Fallback FX source
        fRes = await fetchWithTimeout(
          "https://api.exchangerate.host/latest?base=USD&symbols=AUD",
          {},
          7000
        );
        ftxt = await fRes.text().catch(() => "");
        try {
          fj = ftxt ? JSON.parse(ftxt) : null;
        } catch (e) {
          fj = null;
        }
        if (
          fRes.ok &&
          fj &&
          fj.rates &&
          typeof fj.rates.AUD === "number"
        ) {
          usdToAud = Number(fj.rates.AUD);
        }
      }
    } catch (e) {
      console.warn("fx fetch error", e && e.message);
    }

    // -------------------------------
    // 3) Build snapshot payload (with USD{symbol} priority)
    // -------------------------------
    const outputSymbols = ["XAU", "XAG", "IRON", "LITH-CAR", "NI", "URANIUM"];

    const snapshot = {
      snappedAt: nowIso,
      usdToAud,
      symbols: {},
      sanity: {
        issues: [],
      },
    };

    const priceTimestamp = anyTimestamp;

    for (const s of outputSymbols) {
      const entry = allRates[s] || {};
      const rates = entry.rates || {};
      const unitParam = entry.unitParam || "Troy Ounce";
      const unitLabel = UNIT_LABELS[unitParam] || unitParam;

      const usdKey = `USD${s}`;
      const directUsd =
        typeof rates[usdKey] === "number" && rates[usdKey] > 0
          ? rates[usdKey]
          : null;

      const rawRate =
        typeof rates[s] === "number" && rates[s] > 0 ? rates[s] : null;

      let priceUSD = null;
      let mode = null;
      let debugInfo = null;

      // 1) Prefer the USD{symbol} field if it exists and is sane
      if (directUsd !== null && isInRange(s, directUsd)) {
        priceUSD = directUsd;
        mode = "direct_usd_key";
      } else if (directUsd !== null && !isInRange(s, directUsd)) {
        // USD key exists but is crazy – treat as issue, try raw fallback
        debugInfo = { usdKey, directUsd };
        const derived = deriveFromRawRate(s, rawRate);
        priceUSD = derived.priceUSD;
        mode = derived.mode;
        if (derived.debug) debugInfo = { ...debugInfo, ...derived.debug };
      } else {
        // 2) No USD{symbol} field; derive from rawRate only
        const derived = deriveFromRawRate(s, rawRate);
        priceUSD = derived.priceUSD;
        mode = derived.mode;
        debugInfo = derived.debug || null;
      }

      let priceAUD =
        priceUSD !== null && usdToAud !== null
          ? fmt(priceUSD * usdToAud)
          : null;

      if (priceUSD === null && mode && mode !== "invalid_rate") {
        snapshot.sanity.issues.push({
          symbol: s,
          mode,
          ...(debugInfo || {}),
          usdKey,
          directUsd,
          rawRate,
        });
      }

      snapshot.symbols[s] = {
        apiPriceUSD: priceUSD,
        priceUSD: priceUSD === null ? null : fmt(priceUSD),
        priceAUD,
        usdToAud: usdToAud === null ? null : Number(usdToAud.toFixed(6)),
        priceTimestamp,
        unit: unitLabel,
      };
    }

    // Backwards compatibility alias
    snapshot.metals = snapshot.symbols;

    // Persist under metals:YYYY-MM-DD (AEST date) and metals:latest
    const todayDateAest = getTodayAestDateString();
    const key = `metals:${todayDateAest}`; // metals:YYYY-MM-DD (AEST)
    const okToday = await redisSet(key, snapshot);
    const okLatest = await redisSet("metals:latest", snapshot);


    const debugPayload = {
      key,
      okToday,
      okLatest,
      payload: snapshot,
      previews,
    };

    return {
      statusCode: 200,
      body: JSON.stringify(debugPayload),
    };
  } catch (err) {
    console.error(
      "snapshot-metals error",
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
