// netlify/functions/instrument-details.js
//
// Returns instrument details for use in the MatesInvest UI.
//
// Usage:
//   Equities:
//     /.netlify/functions/instrument-details?type=equity&code=BHP
//   Metals:
//     /.netlify/functions/instrument-details?type=metal&code=IRON
//
// Requirements (equities):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional:
//   HISTORY_MONTHS (default 6)
//   TRY_SUFFIXES (default "AU,AX,ASX")

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const nowIso = new Date().toISOString();

  // -------------------------------
  // Helpers
  // -------------------------------

  async function fetchWithTimeout(url, opts = {}, timeout = 10000) {
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

  const fmt = (n, digits = 2) =>
    typeof n === "number" && Number.isFinite(n)
      ? Number(n.toFixed(digits))
      : null;

  function normalizeCode(code) {
    return String(code || "")
      .replace(/\.[A-Z0-9]{1,6}$/i, "")
      .toUpperCase();
  }

  function toDateString(d) {
    return new Date(d).toISOString().slice(0, 10);
  }

  function monthsAgoDateString(months) {
    const d = new Date();
    d.setMonth(d.getMonth() - months);
    return toDateString(d);
  }

  // -------------------------------
  // Upstash helpers
  // -------------------------------
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
      console.warn("redisGet error", key, e && e.message);
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
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );

      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        console.warn(
          "redisSet failed",
          key,
          res.status,
          txt && txt.slice(0, 300)
        );
        return false;
      }
      return true;
    } catch (e) {
      console.warn("redisSet error", key, e && e.message);
      return false;
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
async function getUniverseFundamentalsLatestManifest() {
  return await redisGetJson("asx:universe:fundamentals:latest");
}

async function findUniverseFundamentalsByCode(baseCode) {
  const manifest = await getUniverseFundamentalsLatestManifest();
  if (!manifest) return null;

  const norm = normalizeCode(baseCode);

  // Case 1: Direct items array (merged object)
  if (Array.isArray(manifest.items)) {
    const hit = manifest.items.find((x) => normalizeCode(x && x.code) === norm);
    if (hit) return hit;
  }

  // Case 2: Manifest with parts (fallback mode for large datasets)
  // Support both 'parts' and 'partKeys' property names for flexibility
  let partKeys = null;
  if (Array.isArray(manifest.parts)) {
    partKeys = manifest.parts;
  } else if (Array.isArray(manifest.partKeys)) {
    partKeys = manifest.partKeys;
  }

  if (!partKeys || partKeys.length === 0) return null;

  // Pull each part and search for the code
  for (const partKey of partKeys) {
    const rawPart = await redisGetJson(partKey);
    if (!rawPart) continue;

    // Support both: objects with .items property AND raw arrays
    let arr;
    if (Array.isArray(rawPart.items)) {
      arr = rawPart.items;
    } else if (Array.isArray(rawPart)) {
      arr = rawPart;
    } else {
      continue;
    }

    const hit = arr.find((x) => normalizeCode(x && x.code) === norm);
    if (hit) return hit;
  }

  return null;
}
  // -------------------------------
  // EODHD helpers (equities only)
  // -------------------------------
  const EODHD_TOKEN = process.env.EODHD_API_TOKEN || null;

  async function fetchEodHistory(fullCode, from, to) {
    if (!EODHD_TOKEN) {
      throw new Error("Missing EODHD_API_TOKEN");
    }
    const url = `https://eodhd.com/api/eod/${encodeURIComponent(
      fullCode
    )}?api_token=${encodeURIComponent(
      EODHD_TOKEN
    )}&period=d&from=${from}&to=${to}&fmt=json`;

    const res = await fetchWithTimeout(url, {}, 12000);
    const txt = await res.text().catch(() => "");
    if (!res.ok) {
      throw new Error(
        `EODHD eod error ${res.status}: ${txt.slice(0, 300) || "no body"}`
      );
    }
    let json;
    try {
      json = txt ? JSON.parse(txt) : null;
    } catch (e) {
      throw new Error("Failed to parse EODHD eod JSON");
    }
    if (!Array.isArray(json) || json.length === 0) {
      return [];
    }
    return json;
  }

  // Robust fundamentals fetch – try /fundamental, /fundamentals, /company
  async function fetchFundamentals(fullCode) {
    if (!EODHD_TOKEN) {
      throw new Error("Missing EODHD_API_TOKEN");
    }

    const endpoints = [
      `https://eodhd.com/api/fundamental/${encodeURIComponent(
        fullCode
      )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
      `https://eodhd.com/api/fundamentals/${encodeURIComponent(
        fullCode
      )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
      `https://eodhd.com/api/company/${encodeURIComponent(
        fullCode
      )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`,
    ];

    let lastErr = null;

    for (const url of endpoints) {
      try {
        const res = await fetchWithTimeout(url, {}, 12000);
        const txt = await res.text().catch(() => "");
        if (!res.ok) {
          if (res.status === 404) {
            lastErr =
              new Error(
                `EODHD fundamentals 404 for ${fullCode} at ${url}`
              ) || lastErr;
            continue;
          }
          lastErr = new Error(
            `EODHD fundamentals error ${res.status}: ${
              txt.slice(0, 300) || "no body"
            }`
          );
          continue;
        }
        let json;
        try {
          json = txt ? JSON.parse(txt) : null;
        } catch (e) {
          lastErr = new Error("Failed to parse EODHD fundamentals JSON");
          continue;
        }
        return json || null;
      } catch (e) {
        lastErr = e;
      }
    }

    if (lastErr) throw lastErr;
    return null;
  }

  // ---- Fundamentals extraction helpers ----

  function extractEquityFundamentals(f) {
    if (!f || typeof f !== "object") return {};

    const general = f.General || {};
    const highlights = f.Highlights || {};
    const financials = f.Financials || {};
    const ratios = f.ValuationRatios || f.Valuation || {};
    const valuation = f.Valuation || {};
    const incomeYearly =
      financials.Income_Statement && financials.Income_Statement.yearly
        ? financials.Income_Statement.yearly
        : null;

    const splitsDiv = f.SplitsDividends || {};

    const pickNumber = (...vals) => {
      for (const v of vals) {
        if (typeof v === "number" && Number.isFinite(v)) return v;
      }
      return null;
    };

    // --- Market cap ---
    let marketCap =
      typeof highlights.MarketCapitalization === "number"
        ? highlights.MarketCapitalization
        : null;
    if (
      marketCap === null &&
      general &&
      typeof general.MarketCapitalization === "number"
    ) {
      marketCap = general.MarketCapitalization;
    }

    // --- Revenue & net income last year ---
    let revenueLastYear = null;
    let netIncomeLastYear = null;

    if (incomeYearly && typeof incomeYearly === "object") {
      const entries = Object.entries(incomeYearly);
      if (entries.length > 0) {
        entries.sort((a, b) => (a[0] > b[0] ? 1 : a[0] < b[0] ? -1 : 0));
        const latest = entries[entries.length - 1][1] || {};

        revenueLastYear = pickNumber(
          latest.totalRevenue,
          latest.TotalRevenue,
          latest.Revenue,
          latest.Revenues
        );

        netIncomeLastYear = pickNumber(
          latest.netIncome,
          latest.NetIncome,
          latest.Net_Income
        );
      }
    }

    if (revenueLastYear === null) {
      revenueLastYear = pickNumber(
        highlights.RevenueTTM,
        highlights.Revenue,
        highlights.TotalRevenue
      );
    }
    if (netIncomeLastYear === null) {
      netIncomeLastYear = pickNumber(
        highlights.NetIncomeTTM,
        highlights.NetIncome,
        highlights.Net_Income
      );
    }

    // --- Dividends ---
    let dividendsPerShareLastYear = null;
    let paysDividend = false;

    if (splitsDiv.Dividends && typeof splitsDiv.Dividends === "object") {
      const d = splitsDiv.Dividends;
      const lastDiv = pickNumber(d.LastDiv, d.lastDiv);
      if (lastDiv !== null) {
        dividendsPerShareLastYear = lastDiv;
        if (lastDiv > 0) paysDividend = true;
      }
    }

    if (
      !paysDividend &&
      typeof highlights.DividendYield === "number" &&
      highlights.DividendYield > 0
    ) {
      paysDividend = true;
    }

    // --- Size bucket ---
    let sizeBucket = null;
    if (typeof marketCap === "number") {
      if (marketCap >= 10_000_000_000) sizeBucket = "mega";
      else if (marketCap >= 2_000_000_000) sizeBucket = "large";
      else if (marketCap >= 500_000_000) sizeBucket = "mid";
      else sizeBucket = "small";
    }

    // --- Leverage bucket (simple) ---
    let leverageBucket = "unknown";
    const debtToEquity = pickNumber(
      highlights.TotalDebtToEquity,
      highlights.DebtToEquity,
      highlights.Debt_to_Equity
    );
    if (debtToEquity !== null) {
      if (debtToEquity < 0.25) leverageBucket = "low";
      else if (debtToEquity < 0.75) leverageBucket = "medium";
      else leverageBucket = "high";
    }

    // --- Gross Profit ---
    let grossProfitLastYear = null;
    if (incomeYearly && typeof incomeYearly === "object") {
      const entries = Object.entries(incomeYearly);
      if (entries.length > 0) {
        entries.sort((a, b) => (a[0] > b[0] ? 1 : a[0] < b[0] ? -1 : 0));
        const latest = entries[entries.length - 1][1] || {};
        
        grossProfitLastYear = pickNumber(
          latest.grossProfit,
          latest.GrossProfit,
          latest.Gross_Profit
        );
      }
    }
    if (grossProfitLastYear === null) {
      grossProfitLastYear = pickNumber(
        highlights.GrossProfitTTM,
        highlights.GrossProfit,
        highlights.Gross_Profit
      );
    }

    return {
      name: general.Name || null,
      sector: general.Sector || null,
      industry: general.Industry || null,
      currency: general.CurrencyCode || null,

      marketCap,
      sizeBucket,

      revenueLastYear,
      grossProfitLastYear,
      netIncomeLastYear,

      dividendYield:
        typeof highlights.DividendYield === "number"
          ? fmt(highlights.DividendYield * 100, 2)
          : null,
      dividendsPerShareLastYear,
      paysDividend,

      pe:
        typeof highlights.PERatio === "number"
          ? fmt(highlights.PERatio, 2)
          : null,
      priceToBook: (() => {
        const val = pickNumber(
          ratios.PriceBookMRQ,
          ratios.PriceToBookRatio,
          valuation.PriceBookMRQ,
          highlights.PriceBookMRQ
        );
        return val !== null ? fmt(val, 2) : null;
      })(),
      priceToSales: (() => {
        const val = pickNumber(
          ratios.PriceSalesTTM,
          ratios.PriceToSalesRatio,
          valuation.PriceSalesTTM,
          highlights.PriceSalesTTM
        );
        return val !== null ? fmt(val, 2) : null;
      })(),
      eps:
        typeof highlights.EarningsShare === "number"
          ? fmt(highlights.EarningsShare, 2)
          : null,

      netDebt: null,
      leverageBucket,
    };
  }

  // -------------------------------
  // History caching helpers (equities)
  // -------------------------------
  const DEFAULT_HISTORY_MONTHS = 6;
  const HISTORY_MONTHS = Number(
    process.env.HISTORY_MONTHS || DEFAULT_HISTORY_MONTHS
  );

  async function getOrBuildEquityHistory(fullCode, months) {
    const key = `history:equity:daily:${fullCode}`;
    const cached = await redisGetJson(key);

    const todayStr = toDateString(new Date());
    const fromStr = monthsAgoDateString(months);

    if (
      cached &&
      cached.startDate &&
      cached.endDate &&
      cached.startDate <= fromStr &&
      cached.endDate >= todayStr &&
      Array.isArray(cached.points) &&
      cached.points.length > 0
    ) {
      return cached;
    }

    const eod = await fetchEodHistory(fullCode, fromStr, todayStr);
    const points = eod
      .map((bar) => {
        const date = bar.date || bar.Date || null;
        const close =
          typeof bar.adjusted_close === "number"
            ? bar.adjusted_close
            : typeof bar.close === "number"
            ? bar.close
            : typeof bar.Close === "number"
            ? bar.Close
            : null;
        if (!date || typeof close !== "number" || !Number.isFinite(close)) {
          return null;
        }
        return [date, Number(close)];
      })
      .filter(Boolean);

    const history = {
      symbol: fullCode,
      startDate: fromStr,
      endDate: todayStr,
      lastUpdated: new Date().toISOString(),
      points,
    };

    await redisSet(key, history);
    return history;
  }

  // -------------------------------
  // ASX200 latest snapshot helper (equities)
  // -------------------------------
  async function getAsx200LatestRows() {
    const snapshot = await redisGetJson("asx200:latest");
    if (Array.isArray(snapshot)) return snapshot;
    return null;
  }

  function findAsxRowForCode(rows, code) {
    if (!Array.isArray(rows)) return null;
    const norm = normalizeCode(code);
    let best = null;
    for (const r of rows) {
      const rCode = normalizeCode(r.code || r.fullCode || "");
      if (rCode === norm) {
        best = r;
        break;
      }
    }
    return best;
  }

  // -------------------------------
  // Full code resolver (equities)
  // -------------------------------
  const DEFAULT_TRY_SUFFIXES = ["AU", "AX", "ASX"];
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES ||
    DEFAULT_TRY_SUFFIXES.join(","))
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  function inferFullCodeFromBase(baseCode) {
    const norm = normalizeCode(baseCode);
    const preferred = ["AU", "AX", "ASX"];
    for (const p of preferred) {
      if (TRY_SUFFIXES.includes(p)) {
        return `${norm}.${p}`;
      }
    }
    if (TRY_SUFFIXES.length > 0) {
      return `${norm}.${TRY_SUFFIXES[0]}`;
    }
    return norm;
  }

  // -------------------------------
  // Equity handler
  // -------------------------------
  async function handleEquity(codeRaw) {
    if (!codeRaw) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code for equity" }),
      };
    }

    // Check if we have at least one data source available
    const hasUpstash = !!(UPSTASH_URL && UPSTASH_TOKEN);
    const hasEodhd = !!EODHD_TOKEN;

    if (!hasUpstash && !hasEodhd) {
      return {
        statusCode: 500,
        body: JSON.stringify({ 
          error: "No data sources available. Missing both Upstash credentials and EODHD_API_TOKEN",
          debug: {
            hasUpstash: false,
            hasEodhd: false,
          }
        }),
      };
    }

    // We can proceed with at least one data source
    // If only EODHD is available, we'll skip Upstash lookups

    const debug = { steps: [], hasUpstash, hasEodhd };
    const baseCode = normalizeCode(codeRaw);
    // A) Pull the same fundamentals row the screener uses (manifest + parts)
let universeRow = null;
if (hasUpstash) {
  try {
    universeRow = await findUniverseFundamentalsByCode(baseCode);
    debug.steps.push({
      step: "universe-fundamentals-latest",
      found: !!universeRow,
    });
  } catch (e) {
    debug.steps.push({
      step: "universe-fundamentals-latest-error",
      error: e && e.message,
    });
  }
} else {
  debug.steps.push({
    step: "universe-fundamentals-latest-skipped",
    reason: "Upstash not available",
  });
}

    // 1) Try to resolve from asx200:latest
    let asxRow = null;
    if (hasUpstash) {
      try {
        const rows = await getAsx200LatestRows();
        if (rows) {
          asxRow = findAsxRowForCode(rows, baseCode);
          debug.steps.push({
            step: "asx200-latest",
            found: !!asxRow,
            rows: Array.isArray(rows) ? rows.length : 0,
          });
        } else {
          debug.steps.push({ step: "asx200-latest", found: false });
        }
      } catch (e) {
        debug.steps.push({
          step: "asx200-latest-error",
          error: e && e.message,
        });
      }
    } else {
      debug.steps.push({
        step: "asx200-latest-skipped",
        reason: "Upstash not available",
      });
    }

    // 2) Decide what symbol to send to EODHD
    let asxSymbolCode = null;
    if (asxRow) {
      asxSymbolCode = asxRow.fullCode || asxRow.code || baseCode;
    }

    let eodSymbol;
    if (asxSymbolCode) {
      if (asxSymbolCode.includes(".")) {
        eodSymbol = asxSymbolCode.toUpperCase();
      } else {
        eodSymbol = inferFullCodeFromBase(asxSymbolCode);
      }
    } else if (codeRaw.includes(".")) {
      eodSymbol = codeRaw.toUpperCase();
    } else {
      eodSymbol = inferFullCodeFromBase(baseCode);
    }

    debug.steps.push({
      step: "symbol-resolution",
      baseCode,
      asxSymbolCode,
      eodSymbol,
    });

    // 3) History (cached)
    let history = null;
    if (hasUpstash) {
      try {
        history = await getOrBuildEquityHistory(eodSymbol, HISTORY_MONTHS);
        debug.steps.push({
          step: "history",
          symbol: eodSymbol,
          points:
            history && Array.isArray(history.points)
              ? history.points.length
              : 0,
        });
      } catch (e) {
        debug.steps.push({
          step: "history-error",
          symbol: eodSymbol,
          error: e && e.message,
        });
      }
    } else {
      debug.steps.push({
        step: "history-skipped",
        reason: "Upstash not available",
      });
    }

// 4) Fundamentals (SOURCE OF TRUTH = Upstash screener fundamentals if present)
let fundamentalsRaw = null;
let fundamentals = {};

if (universeRow && typeof universeRow === "object") {
  fundamentals = {
    // Keep only what your UI expects under fundamentals:
    name: universeRow.name || null,
    sector: universeRow.sector || null,
    industry: universeRow.industry || null,
    currency: "AUD",

    marketCap: typeof universeRow.marketCap === "number" ? universeRow.marketCap : null,

    // Ratios / per-share
    pe: typeof universeRow.peRatio === "number" ? fmt(universeRow.peRatio, 2) : null,
    priceToBook: typeof universeRow.priceToBook === "number" ? fmt(universeRow.priceToBook, 2) : null,
    priceToSales: typeof universeRow.priceToSales === "number" ? fmt(universeRow.priceToSales, 2) : null,
    eps: typeof universeRow.eps === "number" ? fmt(universeRow.eps, 2) : null,
    dividendYield: typeof universeRow.dividendYield === "number" ? fmt(universeRow.dividendYield, 2) : null,

    // Optional extras if your slip uses them later
    revenueLastYear: typeof universeRow.revenue === "number" ? universeRow.revenue : null,
    grossProfitLastYear: typeof universeRow.grossProfit === "number" ? universeRow.grossProfit : null,
    netIncomeLastYear: null,

    netDebt: null,
    sizeBucket: null,
    leverageBucket: "unknown",
    paysDividend: (typeof universeRow.dividendYield === "number" && universeRow.dividendYield > 0) || false,
    dividendsPerShareLastYear: typeof universeRow.dividendPerShare === "number" ? universeRow.dividendPerShare : null,
  };

  debug.steps.push({ step: "fundamentals-from-upstash", ok: true });
} else if (hasEodhd) {
  // Fallback to EODHD if Upstash row missing
  try {
    fundamentalsRaw = await fetchFundamentals(eodSymbol);
    fundamentals = extractEquityFundamentals(fundamentalsRaw);
    debug.steps.push({ step: "fundamentals-from-eodhd", ok: true });
  } catch (e) {
    debug.steps.push({
      step: "fundamentals-error",
      symbol: eodSymbol,
      error: e && e.message,
    });
  }
} else {
  debug.steps.push({
    step: "fundamentals-unavailable",
    reason: "No Upstash row and EODHD not available",
  });
}
    // 5) Latest price (prefer ASX snapshot)
    let latest = {
      date: null,
      price: null,
      yesterdayDate: null,
      yesterdayPrice: null,
      pctChange: null,
      marketCap: null,
    };

    if (asxRow) {
      latest = {
        date: asxRow.lastDate || null,
        price:
          typeof asxRow.lastPrice === "number"
            ? asxRow.lastPrice
            : asxRow.lastPrice !== null
            ? Number(asxRow.lastPrice)
            : null,
        yesterdayDate: asxRow.yesterdayDate || null,
        yesterdayPrice:
          typeof asxRow.yesterdayPrice === "number"
            ? asxRow.yesterdayPrice
            : asxRow.yesterdayPrice !== null
            ? Number(asxRow.yesterdayPrice)
            : null,
        pctChange:
          typeof asxRow.pctChange === "number"
            ? fmt(asxRow.pctChange, 4)
            : asxRow.pctChange !== null
            ? Number(asxRow.pctChange)
            : null,
        marketCap:
          typeof asxRow.marketCap === "number"
            ? asxRow.marketCap
            : asxRow.marketCap !== null
            ? Number(asxRow.marketCap)
            : fundamentals.marketCap || null,
      };
      debug.steps.push({ step: "latest-from-asx200", ok: true });
    } else if (history && Array.isArray(history.points)) {
      const points = history.points;
      const n = points.length;
      if (n >= 1) {
        const [lastDate, lastClose] = points[n - 1];
        let yesterdayDate = null;
        let yesterdayPrice = null;
        let pctChange = null;
        if (n >= 2) {
          const [prevDate, prevClose] = points[n - 2];
          yesterdayDate = prevDate;
          yesterdayPrice = prevClose;
          if (
            typeof lastClose === "number" &&
            typeof prevClose === "number" &&
            prevClose !== 0
          ) {
            pctChange = ((lastClose - prevClose) / prevClose) * 100;
          }
        }
        latest = {
          date: lastDate,
          price: lastClose,
          yesterdayDate,
          yesterdayPrice,
          pctChange: pctChange !== null ? fmt(pctChange, 4) : null,
          marketCap: fundamentals.marketCap || null,
        };
      }
      debug.steps.push({ step: "latest-from-history", ok: true });
    }

    // 6) News (stub for now)
    const news = [];

    const payload = {
      type: "equity",
      code: baseCode,
      fullCode: eodSymbol,
name: (fundamentals.name || (universeRow && universeRow.name) || null),
sector: (fundamentals.sector || (universeRow && universeRow.sector) || null),
industry: (fundamentals.industry || (universeRow && universeRow.industry) || null),
      latest,
      history: history || null,
      fundamentals,
      news,
      debug,
      generatedAt: nowIso,
    };

    return {
      statusCode: 200,
      body: JSON.stringify(payload),
    };
  }

  // -------------------------------
  // Metal handler (Upstash-only; no Metals-API calls)
  // -------------------------------
  async function handleMetal(codeRaw) {
    if (!codeRaw) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code for metal" }),
      };
    }

    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing Upstash env" }),
      };
    }

    const debug = { steps: [] };
    const symbol = String(codeRaw).toUpperCase().trim();

    // Supported metals in our snapshot
    const SUPPORTED_METALS = ["XAU", "XAG", "IRON", "LITH-CAR", "NI", "URANIUM"];
    if (!SUPPORTED_METALS.includes(symbol)) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: `Unsupported metal "${symbol}". Use one of: ${SUPPORTED_METALS.join(
            ", "
          )}`,
        }),
      };
    }

    // Friendly names for UI
    const METAL_NAMES = {
      XAU: "Gold",
      XAG: "Silver",
      IRON: "Iron Ore 62% Fe",
      "LITH-CAR": "Lithium Carbonate (Battery Grade)",
      NI: "Nickel",
      URANIUM: "Uranium (U3O8)",
    };

    // 1) Latest snapshot from Upstash
    const latestSnapshot = await redisGetJson("metals:latest");
    if (!latestSnapshot) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "No metals:latest snapshot found in Upstash",
        }),
      };
    }

    const coll = latestSnapshot.metals || latestSnapshot.symbols || {};
    const m = coll[symbol];
    if (!m) {
      return {
        statusCode: 404,
        body: JSON.stringify({
          error: `No latest metal data found for ${symbol} in metals:latest`,
        }),
      };
    }

    debug.steps.push({ step: "latest-metal", symbol, ok: true });

    // 2) History from Upstash (built by snapshot-metals)
    const histKey = `history:metal:daily:${symbol}`;
    const hist = await redisGetJson(histKey);
    let history = null;
    let yesterdayDate = null;
    let yesterdayPrice = null;
    let pctChange = null;

    if (hist && Array.isArray(hist.points) && hist.points.length > 0) {
      history = hist;
      debug.steps.push({
        step: "metal-history",
        symbol,
        points: hist.points.length,
      });

      const pts = hist.points;
      const n = pts.length;
      const [lastDate, lastVal] = pts[n - 1];

      if (n >= 2) {
        const [prevDate, prevVal] = pts[n - 2];
        yesterdayDate = prevDate;
        yesterdayPrice =
          typeof prevVal === "number" && Number.isFinite(prevVal)
            ? Number(prevVal)
            : null;

        if (
          typeof lastVal === "number" &&
          Number.isFinite(lastVal) &&
          typeof prevVal === "number" &&
          Number.isFinite(prevVal) &&
          prevVal !== 0
        ) {
          const rawPct = ((lastVal - prevVal) / prevVal) * 100;
          pctChange = Number(rawPct.toFixed(4));
        }
      }
    } else {
      debug.steps.push({ step: "metal-history-missing", symbol });
    }

    // 3) Build latest block
    const latestDate =
      (history && history.endDate) ||
      (m.priceTimestamp && m.priceTimestamp.slice(0, 10)) ||
      (latestSnapshot.snappedAt &&
        latestSnapshot.snappedAt.slice(0, 10)) ||
      null;

    const latest = {
      date: latestDate,
      priceAUD:
        typeof m.priceAUD === "number" && Number.isFinite(m.priceAUD)
          ? m.priceAUD
          : null,
      priceUSD:
        typeof m.priceUSD === "number" && Number.isFinite(m.priceUSD)
          ? m.priceUSD
          : null,
      yesterdayDate,
      yesterdayPrice,
      pctChange,
      unit: m.unit || null,
    };

    const payload = {
      type: "metal",
      code: symbol,
      name: METAL_NAMES[symbol] || symbol,
      unit: latest.unit,
      latest,
      history, // { symbol, startDate, endDate, points } or null
      fundamentals: null,
      news: [],
      debug,
      generatedAt: nowIso,
    };

    return {
      statusCode: 200,
      body: JSON.stringify(payload),
    };
  }

  // -------------------------------
  // Main handler
  // -------------------------------
  try {
    const qs = (event && event.queryStringParameters) || {};
    const type = (qs.type || "equity").toLowerCase();
    const code = qs.code || qs.symbol || qs.ticker || null;

    if (!code) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: 'Missing "code" query param (e.g. ?type=equity&code=BHP)',
        }),
      };
    }

    if (type === "equity") {
      return await handleEquity(code);
    }

    if (type === "metal") {
      return await handleMetal(code);
    }

    return {
      statusCode: 400,
      body: JSON.stringify({
        error: `Unsupported type "${type}". Use "equity" or "metal".`,
      }),
    };
  } catch (err) {
    console.error(
      "instrument-details fatal error",
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
