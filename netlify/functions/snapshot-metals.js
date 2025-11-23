// netlify/functions/snapshot-metals.js
// Snapshot multiple metals and write to Upstash: metals:YYYY-MM-DD and metals:latest
//
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Symbols stored: XAU, XAG, IRON, LITH-CAR, NI, URANIUM
// Notes:
// - We now use Metals-API's unit parameter so prices are returned in realistic market units:
//     XAU, XAG      -> USD per troy_ounce
//     IRON, LITH-CAR, NI -> USD per ton (metric tonne)
//     URANIUM       -> USD per pound
// - We convert to AUD using FX and store priceAUD, but keep priceUSD for reference.

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
      return res.ok;
    } catch (e) {
      console.warn("redisSet error", e && e.message);
      return false;
    }
  }

  // formatting helper
  const fmt = (n) =>
    typeof n === "number" && Number.isFinite(n) ? Number(n.toFixed(2)) : null;

  // Map Metals-API "unit" parameter -> human-facing unit label
  const UNIT_LABELS = {
    troy_ounce: "oz",
    ounce: "oz",
    pound: "lb",
    gram: "g",
    kilogram: "kg",
    ton: "tonne",
  };

  // Metals-API sometimes returns "units per USD" if the rate < 1
  function parseUsdFromRate(v) {
    if (typeof v !== "number") return null;
    if (v > 0 && v < 1) return 1 / v; // API returned units per USD
    return v; // assume USD per unit
  }

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY)
      return { statusCode: 500, body: "Missing METALS_API_KEY" };
    if (!UPSTASH_URL || !UPSTASH_TOKEN)
      return { statusCode: 500, body: "Missing Upstash env vars" };

    // -------------------------------
    // 1) Fetch metals prices in realistic units
    // -------------------------------
    const groups = [
      {
        unitParam: "troy_ounce",
        symbols: ["XAU", "XAG"],
      },
      {
        unitParam: "ton",
        symbols: ["IRON", "LITH-CAR", "NI"],
      },
      {
        unitParam: "pound",
        symbols: ["URANIUM"],
      },
    ];

    const allRates = {};
    let anyTimestamp = null;
    const previews = [];

    for (const group of groups) {
      const { unitParam, symbols } = group;
      const url = `https://metals-api.com/api/latest?access_key=${encodeURIComponent(
        METALS_API_KEY
      )}&base=USD&symbols=${encodeURIComponent(
        symbols.join(",")
      )}&unit=${encodeURIComponent(unitParam)}`;

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

      const rates = json && json.rates ? json.rates : null;
      if (!rates) continue;

      if (json && typeof json.timestamp === "number") {
        const ts = new Date(json.timestamp * 1000).toISOString();
        if (!anyTimestamp || ts > anyTimestamp) anyTimestamp = ts;
      }

      for (const s of symbols) {
        const rv = rates[s];
        const usdPerUnit = parseUsdFromRate(rv);
        allRates[s] = {
          usdPerUnit: typeof usdPerUnit === "number" ? usdPerUnit : null,
          unitParam,
        };
      }
    }

    // -------------------------------
    // 2) FX to AUD
    // -------------------------------
    let usdToAud = null;
    try {
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
        )
          usdToAud = Number(fj.rates.AUD);
      }
    } catch (e) {
      console.warn("fx fetch error", e && e.message);
    }

    // -------------------------------
    // 3) Build snapshot payload
    // -------------------------------
    const outputSymbols = ["XAU", "XAG", "IRON", "LITH-CAR", "NI", "URANIUM"];

    const snapshot = {
      snappedAt: nowIso,
      usdToAud: usdToAud,
      symbols: {},
    };

    const priceTimestamp = anyTimestamp;

    for (const s of outputSymbols) {
      const entry = allRates[s] || {};
      const usdPerUnit =
        typeof entry.usdPerUnit === "number" ? entry.usdPerUnit : null;
      const unitParam = entry.unitParam || "troy_ounce";
      const unitLabel = UNIT_LABELS[unitParam] || unitParam;

      const priceUSD =
        usdPerUnit !== null ? Number(usdPerUnit.toFixed(2)) : null;
      const priceAUD =
        priceUSD !== null && usdToAud !== null
          ? fmt(priceUSD * usdToAud)
          : null;

      snapshot.symbols[s] = {
        apiPriceUSD: priceUSD,
        priceUSD,
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
