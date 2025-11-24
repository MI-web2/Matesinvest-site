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
//     XAU, XAG              -> unit=troy_ounce  (USD per oz target)
//     IRON, LITH-CAR, NI    -> unit=ton         (USD per tonne target)
//     URANIUM               -> unit=pound       (USD per lb target)
// - Metals-API returns a "rate" that may represent either:
//     * metal units per 1 USD   (e.g. 0.00049 XAU for 1 USD)
//     * or USD per 1 metal unit
//   The docs + examples are inconsistent, so we interpret both possibilities.
// - For each symbol we compute TWO candidates:
//     candDirect  = rate (as USD per unit)
//     candInverse = 1 / rate (USD per unit)
//   Then we pick whichever candidate falls into a realistic USD range for that symbol.
//   If neither looks realistic, we treat the price as unavailable (null).
// - We convert USD -> AUD via FX and store priceAUD for the app.

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

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
  // These are intentionally wide – we're just filtering out absurd values.
  const VALID_RANGES = {
    XAU: { min: 800, max: 7000 }, // per oz
    XAG: { min: 5, max: 150 }, // per oz
    IRON: { min: 30, max: 500 }, // per tonne
    "LITH-CAR": { min: 3000, max: 150000 }, // per tonne
    NI: { min: 8000, max: 150000 }, // per tonne
    URANIUM: { min: 10, max: 500 }, // per lb
  };

  // Given a raw Metals-API "rate" and a symbol, choose a realistic USD-per-unit price
  function chooseUsdPerUnitFromRate(symbol, rateRaw) {
    if (typeof rateRaw !== "number" || !Number.isFinite(rateRaw) || rateRaw <= 0)
      return { priceUSD: null, mode: "invalid_rate" };

    const range = VALID_RANGES[symbol];
    const candDirect = rateRaw; // interpret as USD per unit
    const candInverse = 1 / rateRaw; // interpret as unit price inverted

    if (!range) {
      // If we don't have a range, default to the inverse behaviour Metals-API documents for base=USD
      return { priceUSD: candInverse, mode: "inverse_no_range" };
    }

    const inRange = (v) => v >= range.min && v <= range.max;

    const directOK = inRange(candDirect);
    const inverseOK = inRange(candInverse);

    if (directOK && !inverseOK) {
      return { priceUSD: candDirect, mode: "direct" };
    }
    if (inverseOK && !directOK) {
      return { priceUSD: candInverse, mode: "inverse" };
    }
    if (directOK && inverseOK) {
      // Both somehow in range – pick the one closer to the midpoint
      const mid = (range.min + range.max) / 2;
      const distDirect = Math.abs(candDirect - mid);
      const distInverse = Math.abs(candInverse - mid);
      return distDirect <= distInverse
        ? { priceUSD: candDirect, mode: "direct_both_ok" }
        : { priceUSD: candInverse, mode: "inverse_both_ok" };
    }

    // Neither candidate looks realistic – drop it.
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
        unitParam: "Troy Ounce", // docs accept title-cased as well
        symbols: ["XAU", "XAG"],
      },
      {
        unitParam: "Ton",
        symbols: ["IRON", "LITH-CAR", "NI"],
      },
      {
        unitParam: "Pound",
        symbols: ["URANIUM"],
      },
    ];

    const allRates = {}; // symbol -> { rawRate, unitParam }
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
        const rv = rates[s];
        allRates[s] = {
          rawRate: typeof rv === "number" ? rv : null,
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
    // 3) Build snapshot payload (with sanity checks)
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
      const rawRate = entry.rawRate;
      const unitParam = entry.unitParam || "Troy Ounce";
      const unitLabel = UNIT_LABELS[unitParam] || unitParam;

      const interpretation = chooseUsdPerUnitFromRate(s, rawRate);
      let priceUSD = interpretation.priceUSD;
      let priceAUD =
        priceUSD !== null && usdToAud !== null
          ? fmt(priceUSD * usdToAud)
          : null;

      if (priceUSD === null && interpretation.mode !== "invalid_rate") {
        snapshot.sanity.issues.push({
          symbol: s,
          mode: interpretation.mode,
          ...(interpretation.debug || {}),
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

    // Persist under metals:YYYY-MM-DD and metals:latest (UTC date)
    const d = new Date();
    const key = `metals:${d.toISOString().slice(0, 10)}`; // metals:YYYY-MM-DD
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
