// netlify/functions/instrument-details.js
//
// Returns instrument details for use in the MatesInvest UI.
//
// Usage (equities for now):
//   /.netlify/functions/instrument-details?type=equity&code=BHP
//
// Response (equity example):
// {
//   "type": "equity",
//   "code": "BHP",
//   "fullCode": "BHP.AX",
//   "name": "BHP Group Limited",
//   "sector": "Materials",
//   "industry": "Other Industrial Metals & Mining",
//   "latest": {
//     "date": "2025-11-25",
//     "price": 45.12,
//     "yesterdayDate": "2025-11-22",
//     "yesterdayPrice": 43.70,
//     "pctChange": 3.24,
//     "marketCap": 210000000000
//   },
//   "history": {
//     "symbol": "BHP.AX",
//     "startDate": "2025-05-25",
//     "endDate": "2025-11-25",
//     "lastUpdated": "2025-11-25T07:05:00.123Z",
//     "points": [
//       ["2025-05-25", 38.12],
//       ...
//     ]
//   },
//   "fundamentals": {
//     "marketCap": 210000000000,
//     "pe": 15.2,
//     "dividendYield": 4.1,
//     "eps": 2.35
//   },
//   "news": [] // TODO: Marketaux integration
// }
//
// Requirements (Netlify env):
//   EODHD_API_TOKEN
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//
// Optional:
//   HISTORY_MONTHS (default 6) - number of months of daily history to fetch/cache
//   TRY_SUFFIXES (default "AX,AU,ASX") - fallback suffixes when resolving full symbol codes

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

  // Normalize symbol / code (strip dot-suffix and uppercase)
  function normalizeCode(code) {
    return String(code || "")
      .replace(/\.[A-Z0-9]{1,6}$/i, "")
      .toUpperCase();
  }

  // Date helpers: YYYY-MM-DD
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
        console.warn("redisSet failed", key, res.status, txt && txt.slice(0, 300));
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

  // -------------------------------
  // EODHD helpers
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

  async function fetchFundamentals(fullCode) {
    if (!EODHD_TOKEN) {
      throw new Error("Missing EODHD_API_TOKEN");
    }
    const url = `https://eodhd.com/api/fundamental/${encodeURIComponent(
      fullCode
    )}?api_token=${encodeURIComponent(EODHD_TOKEN)}&fmt=json`;

    const res = await fetchWithTimeout(url, {}, 12000);
    const txt = await res.text().catch(() => "");
    if (!res.ok) {
      throw new Error(
        `EODHD fundamentals error ${res.status}: ${
          txt.slice(0, 300) || "no body"
        }`
      );
    }
    let json;
    try {
      json = txt ? JSON.parse(txt) : null;
    } catch (e) {
      throw new Error("Failed to parse EODHD fundamentals JSON");
    }
    return json || null;
  }

  function extractEquityFundamentals(f) {
    if (!f || typeof f !== "object") return {};

    const general = f.General || {};
    const highlights = f.Highlights || {};

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

    return {
      name: general.Name || null,
      sector: general.Sector || null,
      industry: general.Industry || null,
      marketCap: marketCap,
      pe:
        typeof highlights.PERatio === "number"
          ? fmt(highlights.PERatio, 2)
          : null,
      dividendYield:
        typeof highlights.DividendYield === "number"
          ? fmt(highlights.DividendYield * 100, 2)
          : null,
      eps:
        typeof highlights.EarningsShare === "number"
          ? fmt(highlights.EarningsShare, 2)
          : null,
      currency: general.CurrencyCode || null,
    };
  }

  // -------------------------------
  // History caching helpers
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

    // If cache exists and covers our window, just return it
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

    // Otherwise fetch from EODHD
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
  // ASX200 latest snapshot helper
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
  const DEFAULT_TRY_SUFFIXES = ["AX", "AU", "ASX"];
  const TRY_SUFFIXES = (process.env.TRY_SUFFIXES ||
    DEFAULT_TRY_SUFFIXES.join(","))
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  function inferFullCodeFromBase(baseCode) {
    // Simple heuristic: prefer .AX, then .AU, then .ASX
    const norm = normalizeCode(baseCode);
    const preferred = ["AX", "AU", "ASX"];
    for (const p of preferred) {
      if (TRY_SUFFIXES.includes(p)) {
        return `${norm}.${p}`;
      }
    }
    // fallback: first suffix in TRY_SUFFIXES
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

    if (!UPSTASH_URL || !UPSTASH_TOKEN) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing Upstash env" }),
      };
    }

    if (!EODHD_TOKEN) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing EODHD_API_TOKEN" }),
      };
    }

    const debug = { steps: [] };
    const baseCode = normalizeCode(codeRaw);

    // 1) Try to resolve from asx200:latest
    let asxRow = null;
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

    let fullCode = null;

    if (asxRow && asxRow.fullCode) {
      fullCode = asxRow.fullCode;
    } else if (codeRaw.includes(".")) {
      // Caller passed a full symbol like BHP.AX
      fullCode = codeRaw.toUpperCase();
    } else {
      fullCode = inferFullCodeFromBase(baseCode);
    }

    // 2) Build / retrieve history (cached)
    let history = null;
    try {
      history = await getOrBuildEquityHistory(fullCode, HISTORY_MONTHS);
      debug.steps.push({
        step: "history",
        symbol: fullCode,
        points: history && Array.isArray(history.points)
          ? history.points.length
          : 0,
      });
    } catch (e) {
      debug.steps.push({
        step: "history-error",
        symbol: fullCode,
        error: e && e.message,
      });
    }

    // 3) Fundamentals
    let fundamentalsRaw = null;
    let fundamentals = {};
    try {
      fundamentalsRaw = await fetchFundamentals(fullCode);
      fundamentals = extractEquityFundamentals(fundamentalsRaw);
      debug.steps.push({ step: "fundamentals", ok: true });
    } catch (e) {
      debug.steps.push({
        step: "fundamentals-error",
        error: e && e.message,
      });
    }

    // 4) Latest price snapshot (prefer asx200 snapshot)
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
            : null,
      };
      debug.steps.push({ step: "latest-from-asx200", ok: true });
    } else if (history && Array.isArray(history.points)) {
      // Fallback: derive from history
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

    // 5) News (stub for now)
    const news = []; // TODO: integrate Marketaux here

    const payload = {
      type: "equity",
      code: baseCode,
      fullCode,
      name: fundamentals.name || null,
      sector: fundamentals.sector || null,
      industry: fundamentals.industry || null,
      latest,
      history: history || null,
      fundamentals,
      news,
      debug, // keep for now; you can remove in production if you like
      generatedAt: nowIso,
    };

    return {
      statusCode: 200,
      body: JSON.stringify(payload),
    };
  }

  // -------------------------------
  // Metal handler (stub for now)
  // -------------------------------
  async function handleMetal(codeRaw) {
    return {
      statusCode: 501,
      body: JSON.stringify({
        error: "type=metal not implemented yet",
        code: codeRaw || null,
      }),
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
