// netlify/functions/week-ahead.js
// Generates and caches Week Ahead payload (Monday email):
// 1) AU macro bullets (manual, stored in Upstash)
// 2) Sector ETF proxy trends (6M / 3M / 1M) from EODHD EOD prices
// 3) Optional chart: AU CPI vs chosen sector proxy (5y, rebased) via QuickChart URL
//
// Env required:
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//   EODHD_API_TOKEN
//
// Optional env:
//   WEEK_AHEAD_CHART_SECTOR   (default: OZF.AU)
//   WEEK_AHEAD_DISABLE_CHART  ("1" to disable)

const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  const EODHD_API_TOKEN = process.env.EODHD_API_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, body: "Upstash not configured" };
  }
  if (!EODHD_API_TOKEN) {
    return { statusCode: 500, body: "EODHD_API_TOKEN missing" };
  }

  // ---------------------------------
  // Time / date helpers (AEST fixed)
  // ---------------------------------
  const AEST_OFFSET_MINUTES = 10 * 60; // Brisbane UTC+10

  function getAestDate(base = new Date()) {
    return new Date(base.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
  }

  function startOfAestDay(base = new Date()) {
    const d = getAestDate(base);
    d.setUTCHours(0, 0, 0, 0);
    return d;
  }

  function toYmdFromAestDate(aestDate) {
    const y = aestDate.getUTCFullYear();
    const m = String(aestDate.getUTCMonth() + 1).padStart(2, "0");
    const d = String(aestDate.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  function ymdUtc(d) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  }

  function getThisOrNextMondayAest(base = new Date()) {
    // If today (AEST) is Mon-Fri, use this week's Monday.
    // If Sat/Sun, use next Monday.
    const d = startOfAestDay(base);
    const day = d.getUTCDay(); // 0 Sun .. 6 Sat

    const monday = new Date(d.getTime());

    if (day >= 1 && day <= 5) {
      // go back to Monday
      monday.setUTCDate(monday.getUTCDate() - (day - 1));
      return monday;
    }

    // Sat/Sun -> next Monday
    const add = (8 - day) % 7 || 7;
    monday.setUTCDate(monday.getUTCDate() + add);
    return monday;
  }

  function niceWeekLabel(monAest, friAest) {
    const fmt = (d) =>
      d.toLocaleDateString("en-AU", {
        weekday: "short",
        day: "2-digit",
        month: "short",
      });
    return `${fmt(monAest)} → ${fmt(friAest)} (AEST)`;
  }

  // ---------------------------------
  // HTTP / Redis helpers
  // ---------------------------------
  async function fetchWithTimeout(url, opts = {}, timeout = 12000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(id);
      return res;
    } catch (e) {
      clearTimeout(id);
      throw e;
    }
  }

  async function redisGet(key) {
    const url = `${UPSTASH_URL}/get/` + encodeURIComponent(key);
    const res = await fetchWithTimeout(
      url,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      8000
    );
    if (!res.ok) return null;
    const j = await res.json().catch(() => null);
    return j ? j.result : null;
  }

  async function redisSet(key, value, ttlSeconds) {
    let url =
      `${UPSTASH_URL}/set/` +
      encodeURIComponent(key) +
      "/" +
      encodeURIComponent(value);

    if (ttlSeconds && Number.isFinite(ttlSeconds)) url += `?EX=${ttlSeconds}`;

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
      throw new Error(`redisSet failed ${res.status}: ${txt}`);
    }
  }

  // ---------------------------------
  // Sector proxies (AU ETFs)
  // ---------------------------------
  // v1 set (as discussed):
  // - Tech: ATEC.AU
  // - Financials: QFN.AU
  // - Financials ex-REITs: OZF.AU
  // - Resources: QRE.AU or OZR.AU (we include both and you can keep/remove)
  // - Property / A-REITs: SLF.AU
  const SECTOR_PROXIES = [
    { key: "financials", label: "Financials", ticker: "QFN.AU" },
    { key: "financialsExReit", label: "Financials (ex-REIT)", ticker: "OZF.AU" },
    { key: "resources", label: "Resources", ticker: "OZR.AU" }, // SPDR resources
    { key: "resourcesAlt", label: "Resources (alt)", ticker: "QRE.AU" }, // BetaShares resources
    { key: "tech", label: "Tech", ticker: "ATEC.AU" },
    { key: "property", label: "A-REITs", ticker: "SLF.AU" },
  ];

  // ---------------------------------
  // Week keys
  // ---------------------------------
  const mondayAest = getThisOrNextMondayAest(new Date());
  const fridayAest = new Date(mondayAest.getTime());
  fridayAest.setUTCDate(fridayAest.getUTCDate() + 4);

  const weekStart = toYmdFromAestDate(mondayAest);
  const weekEnd = toYmdFromAestDate(fridayAest);
  const weekLabel = niceWeekLabel(mondayAest, fridayAest);

  const payloadKey = `weekAhead:au:${weekStart}`;
  const macroKey = `weekAhead:au:${weekStart}:macro`; // manual bullets

  // ---------------------------------
  // 1) Macro bullets (manual)
  // ---------------------------------
  let macro = { title: "Important AU macro this week", bullets: [] };

  const macroRaw = await redisGet(macroKey);
  if (macroRaw) {
    try {
      const parsed = JSON.parse(macroRaw);
      if (parsed && Array.isArray(parsed.bullets)) macro = parsed;
    } catch {
      // allow simple text format: one bullet per line
      macro.bullets = String(macroRaw)
        .split("\n")
        .map((s) => s.trim())
        .filter(Boolean)
        .slice(0, 8);
    }
  }

  // ---------------------------------
  // 2) Sector trends (6/3/1 month)
  // ---------------------------------
  function isoDate(d) {
    return d.toISOString().slice(0, 10);
  }

  // Target dates (calendar offsets, then we snap to nearest trading day on/before)
  const today = new Date();
  const toISO = isoDate(today);

  const from = new Date(today.getTime());
  from.setUTCDate(from.getUTCDate() - 320); // enough history to cover 6M windows
  const fromISO = isoDate(from);

  const t1m = new Date(today.getTime());
  t1m.setUTCDate(t1m.getUTCDate() - 31);

  const t3m = new Date(today.getTime());
  t3m.setUTCDate(t3m.getUTCDate() - 93);

  const t6m = new Date(today.getTime());
  t6m.setUTCDate(t6m.getUTCDate() - 186);

  const t1mISO = isoDate(t1m);
  const t3mISO = isoDate(t3m);
  const t6mISO = isoDate(t6m);

  async function fetchEodSeries(ticker) {
    const url =
      `https://eodhd.com/api/eod/${encodeURIComponent(ticker)}` +
      `?from=${fromISO}&to=${toISO}&fmt=json&api_token=${EODHD_API_TOKEN}`;

    const res = await fetchWithTimeout(url, {}, 15000);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`EODHD eod failed for ${ticker}: ${res.status} ${txt}`);
    }
    const data = await res.json().catch(() => []);
    return Array.isArray(data) ? data : [];
  }

  function closeOnOrBefore(seriesAsc, targetISO) {
    // seriesAsc sorted ascending by date
    for (let i = seriesAsc.length - 1; i >= 0; i--) {
      const row = seriesAsc[i];
      if (row && row.date && row.date <= targetISO) {
        const c = row.close;
        if (typeof c === "number" && Number.isFinite(c)) return { close: c, date: row.date };
      }
    }
    return { close: null, date: null };
  }

  function latestClose(seriesAsc) {
    for (let i = seriesAsc.length - 1; i >= 0; i--) {
      const row = seriesAsc[i];
      const c = row && row.close;
      if (typeof c === "number" && Number.isFinite(c)) return { close: c, date: row.date };
    }
    return { close: null, date: null };
  }

  function pctReturn(now, then) {
    if (!Number.isFinite(now) || !Number.isFinite(then) || then === 0) return null;
    return ((now / then) - 1) * 100;
  }

  const sectorRows = [];

  for (const p of SECTOR_PROXIES) {
    try {
      const s = await fetchEodSeries(p.ticker);
      s.sort((a, b) => String(a.date).localeCompare(String(b.date)));

      const last = latestClose(s);
      const c1 = closeOnOrBefore(s, t1mISO);
      const c3 = closeOnOrBefore(s, t3mISO);
      const c6 = closeOnOrBefore(s, t6mISO);

      const r1 = pctReturn(last.close, c1.close);
      const r3 = pctReturn(last.close, c3.close);
      const r6 = pctReturn(last.close, c6.close);

      const trendLabel =
        (r1 != null && r3 != null && r1 > 0 && r3 > 0) ? "Bullish" :
        (r1 != null && r3 != null && r1 < 0 && r3 < 0) ? "Weak" :
        "Mixed";

      sectorRows.push({
        key: p.key,
        label: p.label,
        ticker: p.ticker,
        asOf: last.date,
        close: last.close,
        returnsPct: { m1: r1, m3: r3, m6: r6 },
        trendLabel,
      });
    } catch (err) {
      sectorRows.push({
        key: p.key,
        label: p.label,
        ticker: p.ticker,
        asOf: null,
        close: null,
        returnsPct: { m1: null, m3: null, m6: null },
        trendLabel: "—",
        error: err && err.message ? err.message : "fetch failed",
      });
    }
  }

  // Sort by 3M return desc (generally best “trend” read)
  sectorRows.sort((a, b) => (b.returnsPct.m3 ?? -Infinity) - (a.returnsPct.m3 ?? -Infinity));

  // ---------------------------------
  // 3) Chart (CPI vs sector, ~5y rebased) via QuickChart
  // ---------------------------------
  const disableChart = String(process.env.WEEK_AHEAD_DISABLE_CHART || "").trim() === "1";
  const chartSector = String(process.env.WEEK_AHEAD_CHART_SECTOR || "OZF.AU").trim();

  function isoMonth(ymd) {
    return String(ymd || "").slice(0, 7);
  }

  function rebaseTo100(values) {
    const first = values.find((v) => typeof v === "number" && Number.isFinite(v));
    if (!first) return values.map(() => null);
    return values.map((v) =>
      typeof v === "number" && Number.isFinite(v) ? (v / first) * 100 : null
    );
  }

  function buildQuickChartUrl({ title, labels, a, b, labelA, labelB }) {
    const cfg = {
      type: "line",
      data: {
        labels,
        datasets: [
          { label: labelA, data: a, fill: false, borderWidth: 2, pointRadius: 0 },
          { label: labelB, data: b, fill: false, borderWidth: 2, pointRadius: 0 },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          title: { display: true, text: title },
          legend: { display: true },
        },
        scales: {
          x: { ticks: { maxRotation: 0 } },
          y: { ticks: { callback: (v) => (typeof v === "number" ? v.toFixed(0) : v) } },
        },
      },
    };
    const encoded = encodeURIComponent(JSON.stringify(cfg));
    return `https://quickchart.io/chart?width=640&height=320&c=${encoded}`;
  }

  async function buildCpiVsSectorChartUrl(sectorTicker) {
    // CPI macro indicator (AU)
    const cpiUrl =
      `https://eodhd.com/api/macro-indicator/AUS` +
      `?indicator=consumer_price_index&api_token=${EODHD_API_TOKEN}&fmt=json`;

    const cpiRes = await fetchWithTimeout(cpiUrl, {}, 15000);
    if (!cpiRes.ok) return null;

    const cpiRaw = await cpiRes.json().catch(() => []);
    const cpiByMonth = new Map();

    // CPI is usually monthly/quarterly; we map to YYYY-MM
    for (const r of Array.isArray(cpiRaw) ? cpiRaw : []) {
      const m = isoMonth(r.date || r.period || r.datetime);
      const v =
        typeof r.value === "number" && Number.isFinite(r.value)
          ? r.value
          : null;
      if (m) cpiByMonth.set(m, v);
    }

    // Sector series: get daily last 5y, then keep last close in each month
    const to = new Date();
    const from5 = new Date(to.getTime());
    from5.setUTCFullYear(from5.getUTCFullYear() - 5);

    const from5ISO = isoDate(from5);
    const to5ISO = isoDate(to);

    const secUrl =
      `https://eodhd.com/api/eod/${encodeURIComponent(sectorTicker)}` +
      `?from=${from5ISO}&to=${to5ISO}&fmt=json&api_token=${EODHD_API_TOKEN}`;

    const secRes = await fetchWithTimeout(secUrl, {}, 15000);
    if (!secRes.ok) return null;

    const secRaw = await secRes.json().catch(() => []);
    const secByMonth = new Map();
    for (const r of Array.isArray(secRaw) ? secRaw : []) {
      const m = isoMonth(r.date);
      const v =
        typeof r.close === "number" && Number.isFinite(r.close)
          ? r.close
          : null;
      if (m) secByMonth.set(m, v); // overwrite -> last close in month
    }

    const months = Array.from(new Set([...cpiByMonth.keys(), ...secByMonth.keys()]))
      .sort()
      .slice(-60); // ~60 months

    const cpiVals = months.map((m) => cpiByMonth.get(m) ?? null);
    const secVals = months.map((m) => secByMonth.get(m) ?? null);

    const cpiRe = rebaseTo100(cpiVals);
    const secRe = rebaseTo100(secVals);

    const title = `AU CPI vs ${sectorTicker} (rebased, ~5y)`;
    return buildQuickChartUrl({
      title,
      labels: months,
      a: cpiRe,
      b: secRe,
      labelA: "AU CPI (rebased)",
      labelB: `${sectorTicker} (rebased)`,
    });
  }

  let chart = { enabled: false, sector: chartSector, url: null };
  if (!disableChart) {
    try {
      const url = await buildCpiVsSectorChartUrl(chartSector);
      if (url) chart = { enabled: true, sector: chartSector, url };
    } catch {
      // ignore chart failures (email can still send without it)
    }
  }

  // ---------------------------------
  // Final payload + cache
  // ---------------------------------
  const payload = {
    meta: {
      id: "week_ahead_v1",
      region: "au",
      timezone: "Australia/Brisbane",
      generatedAtAEST: getAestDate(new Date()).toISOString().replace("Z", "+10:00"),
    },
    week: {
      weekStartAEST: weekStart,
      weekEndAEST: weekEnd,
      label: weekLabel,
    },
    macro,
    sectors: {
      title: "Sector trends (6M / 3M / 1M)",
      sort: "3m_desc",
      asOf: ymdUtc(getAestDate(new Date())),
      proxies: SECTOR_PROXIES,
      results: sectorRows,
    },
    chart,
  };

  // Cache for ~10 days (covers the week)
  await redisSet(payloadKey, JSON.stringify(payload), 60 * 60 * 24 * 10);

  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ key: payloadKey, payload }),
  };
};
