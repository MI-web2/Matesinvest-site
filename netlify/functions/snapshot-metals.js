// netlify/functions/snapshot-metals.js
// Snapshot multiple metals and write to Upstash:
//   - metals:YYYY-MM-DD (AEST)
//   - metals:latest
//   - history:metal:daily:<SYMBOL> (rolling history for charts) - DAILY APPEND only (no backfill)
//
// Env required:
// - METALS_API_KEY
// - UPSTASH_REDIS_REST_URL
// - UPSTASH_REDIS_REST_TOKEN
//
// Symbols stored: XAU, XAG, IRON, LITH-CAR, NI, URANIUM
//
// NOTE: This version NO LONGER backfills. Each run:
//  - fetches unit-aware latests (for your daily cards)
//  - fetches raw no-unit latests (for history)
//  - appends the raw no-unit USD number into history:metal:daily:<SYMBOL>

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // Return YYYY-MM-DD string using AEST (Australia/Brisbane, UTC+10, no DST)
  function getTodayAestDateString(baseDate = new Date()) {
    const AEST_OFFSET_MINUTES = 10 * 60; // Brisbane is UTC+10 all year
    const aestTime = new Date(
      baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000
    );
    return aestTime.toISOString().slice(0, 10);
  }

  function monthsAgoDateStringAest(months, baseDate = new Date()) {
    const AEST_OFFSET_MIN = 10 * 60;
    const aest = new Date(baseDate.getTime() + AEST_OFFSET_MIN * 60 * 1000);
    aest.setMonth(aest.getMonth() - months);
    return aest.toISOString().slice(0, 10);
  }

  function addDays(dateStr, days) {
    const d = new Date(dateStr + "T00:00:00Z");
    d.setDate(d.getDate() + days);
    return d.toISOString().slice(0, 10);
  }

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
      const encoded =
        typeof value === "string" ? value : JSON.stringify(value);
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/set/${encodeURIComponent(
          key
        )}/${encodeURIComponent(encoded)}`,
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

  async function redisGet(key) {
    if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
    try {
      const res = await fetchWithTimeout(
        `${UPSTASH_URL}/get/${encodeURIComponent(key)}`,
        {
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );
      if (!res.ok) return null;
      const j = await res.json().catch(() => null);
      if (!j || typeof j.result === "undefined") return null;
      return j.result;
    } catch (e) {
      console.warn("redisGet error", key, e && e.message);
      return null;
    }
  }

  async function redisGetJson(key) {
    const raw = await redisGet(key);
    if (!raw) return null;
    if (typeof raw === "string") {
      try {
        return JSON.parse(raw);
      } catch (e) {
        console.warn("redisGetJson parse error", key, e && e.message);
        return null;
      }
    }
    if (typeof raw === "object") return raw;
    return null;
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
  const VALID_RANGES = {
    XAU: { min: 800, max: 7000 }, // gold per oz
    XAG: { min: 5, max: 150 }, // silver per oz
    IRON: { min: 30, max: 500 }, // iron ore per tonne
    "LITH-CAR": { min: 3000, max: 150000 }, // lithium carbonate per tonne
    NI: { min: 8000, max: 150000 }, // nickel per tonne
    URANIUM: { min: 10, max: 500 }, // uranium per lb
  };

  function isInRange(symbol, price) {
    const range = VALID_RANGES[symbol];
    if (!range || typeof price !== "number" || !Number.isFinite(price)) {
      return false;
    }
    return price >= range.min && price <= range.max;
  }

  // Fallback interpreter for when USD{SYMBOL} is missing:
  function deriveFromRawRate(symbol, rateRaw) {
    if (
      typeof rateRaw !== "number" ||
      !Number.isFinite(rateRaw) ||
      rateRaw <= 0
    ) {
      return { priceUSD: null, mode: "invalid_rate" };
    }

    const range = VALID_RANGES[symbol];
    const candDirect = rateRaw; // treat as USD per unit
    const candInverse = 1 / rateRaw;

    if (!range) {
      // No range defined: default to inverse (Metals-API base=USD docs behaviour)
      return { priceUSD: candInverse, mode: "inverse_no_range" };
    }

    const directOK = isInRange(symbol, candDirect);
    const inverseOK = isInRange(symbol, candInverse);

    if (directOK && !inverseOK) return { priceUSD: candDirect, mode: "direct" };
    if (!directOK && inverseOK) return { priceUSD: candInverse, mode: "inverse" };

    if (directOK && inverseOK) {
      const mid = (range.min + range.max) / 2;
      const dDist = Math.abs(candDirect - mid);
      const iDist = Math.abs(candInverse - mid);
      return dDist <= iDist
        ? { priceUSD: candDirect, mode: "direct_both_ok" }
        : { priceUSD: candInverse, mode: "inverse_both_ok" };
    }

    return {
      priceUSD: null,
      mode: "out_of_range",
      debug: { candDirect, candInverse, range },
    };
  }

  const HISTORY_MONTHS = Number(process.env.HISTORY_MONTHS || 6);

  // Metals-API unit mapping (for unit-aware latests)
  const METAL_UNIT_PARAMS = {
    XAU: "Troy Ounce",
    XAG: "Troy Ounce",
    IRON: "Ton",
    "LITH-CAR": "Ton",
    NI: "Ton",
    URANIUM: "Pound",
  };

  try {
    const METALS_API_KEY = process.env.METALS_API_KEY || null;
    if (!METALS_API_KEY) {
      return { statusCode: 500, body: "Missing METALS_API_KEY" };
    }
    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return { statusCode: 500, body: "Missing Upstash env vars" };
    }

    const qs = (event && event.queryStringParameters) || {};
    // backfill removed: ignore qs.backfill

    // -------------------------------
    // 1) Fetch metals prices in realistic units (latest) — for daily cards
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
        bodyPreview: txt.slice(0, 600),
      });

      let json = null;
      try {
        json = txt ? JSON.parse(txt) : null;
      } catch (e) {
        json = null;
      }

      if (!json || json.success === false) {
        console.warn(
          "metals-api latest group failure",
          unitParam,
          json && json.error
        );
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
    // 2) FX to AUD (today's USD->AUD) — still fetched for snapshot.priceAUD
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
      } catch {
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
        } catch {
          fj = null;
        }
        if (fRes.ok && fj && fj.rates && typeof fj.rates.AUD === "number") {
          usdToAud = Number(fj.rates.AUD);
        }
      }
    } catch (e) {
      console.warn("fx fetch error", e && e.message);
    }

    // -------------------------------
    // 3) Fetch raw latest rates (no unit param) for history storage
    // -------------------------------
    const outputSymbols = ["XAU", "XAG", "IRON", "LITH-CAR", "NI", "URANIUM"];
    try {
      const rawUrl =
        `https://metals-api.com/api/latest` +
        `?access_key=${encodeURIComponent(METALS_API_KEY)}` +
        `&base=USD` +
        `&symbols=${encodeURIComponent(outputSymbols.join(","))}`;
      const rawRes = await fetchWithTimeout(rawUrl, {}, 10000);
      const rawTxt = await rawRes.text().catch(() => "");
      let rawJson = null;
      try {
        rawJson = rawTxt ? JSON.parse(rawTxt) : null;
      } catch {
        rawJson = null;
      }
      previews.push({ url: rawUrl, status: rawRes.status, bodyPreview: rawTxt.slice(0, 600) });
      const rawRates = rawJson && rawJson.rates ? rawJson.rates : null;

      // Attach apiRawPrice to allRates entries for later snapshot use
      for (const s of outputSymbols) {
        const usdKey = `USD${s}`;
        const directUsd =
          rawRates && typeof rawRates[usdKey] === "number" && rawRates[usdKey] > 0
            ? rawRates[usdKey]
            : null;
        const rawRate =
          rawRates && typeof rawRates[s] === "number" && rawRates[s] > 0
            ? rawRates[s]
            : null;

        let apiRawPrice = null;

        if (s === "NI" || s === "LITH-CAR") {
          // *** SPECIAL CASE ***
          // For NI & LITH-CAR, keep apiPriceRaw equal to the raw MetalsAPI rate,
          // so it lines up with your manual timeseries backfill.
          apiRawPrice =
            typeof rawRate === "number" && Number.isFinite(rawRate)
              ? rawRate
              : null;

          // If for some reason rawRate is missing, fall back to the old behaviour.
          if (apiRawPrice === null) {
            if (directUsd !== null && isInRange(s, directUsd)) {
              apiRawPrice = directUsd;
            } else {
              const derived = deriveFromRawRate(s, rawRate);
              apiRawPrice = derived.priceUSD;
            }
          }
        } else {
          // Existing behaviour for other metals
          if (directUsd !== null && isInRange(s, directUsd)) {
            apiRawPrice = directUsd;
          } else {
            const derived = deriveFromRawRate(s, rawRate);
            apiRawPrice = derived.priceUSD;
          }
        }

        if (!allRates[s]) {
          allRates[s] = {
            rates: {},
            unitParam: METAL_UNIT_PARAMS[s] || "Troy Ounce",
          };
        }
        allRates[s].apiRawPrice = apiRawPrice;
      }

    } catch (e) {
      console.warn("raw latest fetch failed", e && e.message);
    }

    // -------------------------------
    // 4) Build snapshot payload (with USD{symbol} priority) — include apiPriceRaw
    // -------------------------------
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

      if (directUsd !== null && isInRange(s, directUsd)) {
        priceUSD = directUsd;
        mode = "direct_usd_key";
      } else if (directUsd !== null && !isInRange(s, directUsd)) {
        debugInfo = { usdKey, directUsd };
        const derived = deriveFromRawRate(s, rawRate);
        priceUSD = derived.priceUSD;
        mode = derived.mode;
        if (derived.debug) debugInfo = { ...debugInfo, ...derived.debug };
      } else {
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

      // Attach both unit-aware derived USD (apiPriceUSD) and the raw no-unit USD (apiPriceRaw)
      snapshot.symbols[s] = {
        apiPriceUSD: priceUSD,
        apiPriceRaw: entry && typeof entry.apiRawPrice === "number" ? entry.apiRawPrice : null,
        priceUSD: priceUSD === null ? null : fmt(priceUSD),
        priceAUD,
        usdToAud: usdToAud === null ? null : Number(usdToAud.toFixed(6)),
        priceTimestamp,
        unit: unitLabel,
      };
    }

    snapshot.metals = snapshot.symbols; // alias for convenience

    const todayDateAest = getTodayAestDateString();

    // -------------------------------
    // -------------------------------
    // 5) Update rolling history with today's snapshot point (append raw USD/AUD per symbol rules)
    // -------------------------------
    async function updateMetalHistoryWithToday(symbols, snapshot, todayDateAest) {
      const coll = snapshot.metals || snapshot.symbols || {};
      const fromStr = monthsAgoDateStringAest(HISTORY_MONTHS);

      function numOrNull(v) {
        return typeof v === "number" && Number.isFinite(v) ? v : null;
      }

      for (const s of symbols) {
        const m = coll[s];
        if (!m) continue;

        const historyKey = `history:metal:daily:${s}`;
        const existing = (await redisGetJson(historyKey)) || {
          symbol: s,
          startDate: todayDateAest,
          endDate: todayDateAest,
          lastUpdated: nowIso,
          points: [],
        };

        const points = Array.isArray(existing.points)
          ? existing.points.filter((p) => p && p[0] !== todayDateAest)
          : [];

        let value = null;

        // -------- per-metal rules --------
        if (s === "XAU" || s === "XAG") {
          // AUD per oz
          value = numOrNull(m.priceAUD);
} else if (s === "URANIUM") {
  // For URANIUM history:
  // Convert USD/lb → AUD/lb
  const usd =
    numOrNull(m.apiPriceRaw) ??
    numOrNull(m.apiPriceUSD) ??
    numOrNull(m.priceUSD);
  const fx = numOrNull(m.usdToAud);

  if (usd != null && fx != null) {
    value = Number((usd * fx).toFixed(2));   // AUD per lb
  } else {
    // fallback (rare)
    value =
      numOrNull(m.priceAUD) ??
      numOrNull(m.priceUSD) ??
      numOrNull(m.apiPriceUSD);
  }
}

        } else if (s === "IRON") {
          // apiPriceRaw (USD per tonne) -> AUD per tonne for history
          const rawUsd = numOrNull(m.apiPriceRaw);
          const fx = numOrNull(m.usdToAud);
          if (rawUsd != null && fx != null) {
            value = Number((rawUsd * fx).toFixed(2)); // apiPriceRaw AUD
          } else {
            value = numOrNull(m.priceAUD);
          }
} else if (s === "NI" || s === "LITH-CAR") {
  // For NI & LITH-CAR history:
  // Use apiPriceRaw (USD/unit) converted to AUD (USD->AUD fx)
  const rawUsd = numOrNull(m.apiPriceRaw);
  const fx = numOrNull(m.usdToAud);
  if (rawUsd != null && fx != null) {
    value = Number((rawUsd * fx).toFixed(2));   // AUD per tonne (or unit)
  } else {
    // fallback (very rare)
    value =
      numOrNull(m.priceAUD) ??
      numOrNull(m.priceUSD) ??
      numOrNull(m.apiPriceUSD);
  }
}

        if (value == null) continue;

        points.push([todayDateAest, value]);

        // Trim to last ~HISTORY_MONTHS
        const trimmed = points.filter((p) => p[0] >= fromStr);
        const newStart = trimmed.length > 0 ? trimmed[0][0] : fromStr;
        const newEnd =
          trimmed.length > 0
            ? trimmed[trimmed.length - 1][0]
            : todayDateAest;

        const updated = {
          symbol: s,
          startDate: newStart,
          endDate: newEnd,
          lastUpdated: nowIso,
          points: trimmed,
        };

        await redisSet(historyKey, updated);
      }
    }

    await updateMetalHistoryWithToday(outputSymbols, snapshot, todayDateAest);

    // -------------------------------
    // 6) Persist daily snapshot & latest
    // -------------------------------
    const key = `metals:${todayDateAest}`;
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
