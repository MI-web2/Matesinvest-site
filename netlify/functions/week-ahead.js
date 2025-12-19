// netlify/functions/week-ahead.js
// Week Ahead generator (Monday email):
// 1) AU macro bullets auto-picked from local text file (bundled with functions)
// 2) Sector ETF proxy trends (6M / 3M / 1M) from EODHD EOD prices
// 3) Chart: AU CPI vs chosen sector proxy (5y rebased) via QuickChart URL
// 4) Cache final payload to Upstash: weekAhead:au:YYYY-MM-DD
//
// Env required:
//   UPSTASH_REDIS_REST_URL
//   UPSTASH_REDIS_REST_TOKEN
//   EODHD_API_TOKEN
//
// Optional env:
//   WEEK_AHEAD_CHART_SECTOR   (default: OZR.AU)
//   WEEK_AHEAD_DISABLE_CHART  ("1" to disable)
//
// Local file required (place next to this JS in netlify/functions/):
//   au-macro-key-dates-2026.txt

const fetch = (...args) => global.fetch(...args);
const fs = require("fs");
const path = require("path");

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
  // AEST helpers (Brisbane UTC+10)
  // ---------------------------------
  const AEST_OFFSET_MINUTES = 10 * 60;

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

  function getThisOrNextMondayAest(base = new Date()) {
    const d = startOfAestDay(base);
    const day = d.getUTCDay(); // 0 Sun..6 Sat
    const monday = new Date(d.getTime());

    if (day >= 1 && day <= 5) {
      monday.setUTCDate(monday.getUTCDate() - (day - 1));
      return monday;
    }

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
  // HTTP + Upstash helpers
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
  // Week window + payload key
  // ---------------------------------
  const mondayAest = getThisOrNextMondayAest(new Date());
  const fridayAest = new Date(mondayAest.getTime());
  fridayAest.setUTCDate(fridayAest.getUTCDate() + 4);

  const weekStart = toYmdFromAestDate(mondayAest);
  const weekEnd = toYmdFromAestDate(fridayAest);
  const weekLabel = niceWeekLabel(mondayAest, fridayAest);

  const payloadKey = `weekAhead:au:${weekStart}`;

  // ---------------------------------
  // 1) Macro bullets (from local txt file)
  // ---------------------------------
  function parseMacroFile(txt) {
    const lines = String(txt || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    const items = [];

    for (const l of lines) {
      if (!l.startsWith("•")) continue;

      // Normalize dash types to " - "
      const line = l.replace(/\s+–\s+/, " - ").replace(/\s+—\s+/, " - ");

      // Expect: "• Tue 3 Feb 2026 - RBA Cash Rate Decision"
      const m = line.match(
        /^•\s+[A-Za-z]{3}\s+(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+-\s+(.+)$/
      );
      if (!m) continue;

      const day = parseInt(m[1], 10);
      const monStr = m[2];
      const year = parseInt(m[3], 10);
      const title = m[4].trim();

      const monthMap = {
        Jan: 0,
        Feb: 1,
        Mar: 2,
        Apr: 3,
        May: 4,
        Jun: 5,
        Jul: 6,
        Aug: 7,
        Sep: 8,
        Oct: 9,
        Nov: 10,
        Dec: 11,
      };
      const month = monthMap[monStr];
      if (month === undefined) continue;

      const ymd = `${year}-${String(month + 1).padStart(2, "0")}-${String(
        day
      ).padStart(2, "0")}`;

      items.push({ ymd, title });
    }

    return items;
  }

  function macroPriorityScore(title) {
    const t = String(title || "").toLowerCase();
    if (t.includes("rba") || t.includes("cash rate")) return 100;
    if (t.includes("cpi") || t.includes("inflation")) return 90;
    if (
      t.includes("labour") ||
      t.includes("labor") ||
      t.includes("employment") ||
      t.includes("unemployment")
    )
      return 80;
    return 10;
  }

  function formatMacroBullet(ymd, title) {
    // Pretty bullet: "Tue: RBA Cash Rate Decision (3 Feb)"
    const y = parseInt(ymd.slice(0, 4), 10);
    const m = parseInt(ymd.slice(5, 7), 10) - 1;
    const d = parseInt(ymd.slice(8, 10), 10);
    const dt = new Date(Date.UTC(y, m, d, 0, 0, 0));

    const dow = dt.toLocaleDateString("en-AU", {
      weekday: "short",
      timeZone: "Australia/Brisbane",
    });

    const dayMonth = dt.toLocaleDateString("en-AU", {
      day: "numeric",
      month: "short",
      timeZone: "Australia/Brisbane",
    });

    return `${dow}: ${title} (${dayMonth})`;
  }

  function pickMacroBullets(items, weekStartYmd, weekEndYmd, maxBullets = 6) {
    const inWeek = items.filter(
      (it) => it.ymd >= weekStartYmd && it.ymd <= weekEndYmd
    );

    inWeek.sort(
      (a, b) =>
        macroPriorityScore(b.title) - macroPriorityScore(a.title) ||
        a.ymd.localeCompare(b.ymd)
    );

    return inWeek
      .slice(0, maxBullets)
      .map((it) => formatMacroBullet(it.ymd, it.title));
  }

  let macro = { title: "Important AU macro this week", bullets: [] };

  try {
    // File lives next to this function file (bundled in Netlify deploy)
    const macroFilePath = path.join(__dirname, "au-macro-key-dates-2026.txt");
    const txt = fs.readFileSync(macroFilePath, "utf8");
    const items = parseMacroFile(txt);
    macro.bullets = pickMacroBullets(items, weekStart, weekEnd, 6);
    macro._source = "local_function_file";
  } catch (e) {
    macro.bullets = [];
    macro.error = e && e.message ? e.message : "macro file read failed";
  }

  // ---------------------------------
  // 2) Sector trends (ETFs, from EODHD)
  // ---------------------------------
  const SECTOR_PROXIES = [
    { key: "financials", label: "Financials", ticker: "QFN.AU" },
    { key: "financialsExReit", label: "Financials (ex-REIT)", ticker: "OZF.AU" },
    { key: "resources", label: "Resources", ticker: "OZR.AU" },
    { key: "resourcesAlt", label: "Resources (alt)", ticker: "QRE.AU" },
    { key: "tech", label: "Tech", ticker: "ATEC.AU" },
    { key: "property", label: "A-REITs", ticker: "SLF.AU" },
  ];

  function isoDate(d) {
    return d.toISOString().slice(0, 10);
  }

  const today = new Date();
  const toISO = isoDate(today);

  const from = new Date(today.getTime());
  from.setUTCDate(from.getUTCDate() - 320);
  const fromISO = isoDate(from);

  // Calendar offsets; we snap to trading day on/before
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
    for (let i = seriesAsc.length - 1; i >= 0; i--) {
      const row = seriesAsc[i];
      if (row && row.date && row.date <= targetISO) {
        const c = row.close;
        if (typeof c === "number" && Number.isFinite(c))
          return { close: c, date: row.date };
      }
    }
    return { close: null, date: null };
  }

  function latestClose(seriesAsc) {
    for (let i = seriesAsc.length - 1; i >= 0; i--) {
      const row = seriesAsc[i];
      const c = row && row.close;
      if (typeof c === "number" && Number.isFinite(c))
        return { close: c, date: row.date };
    }
    return { close: null, date: null };
  }

  function pctReturn(now, then) {
    if (!Number.isFinite(now) || !Number.isFinite(then) || then === 0)
      return null;
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
        r1 != null && r3 != null && r1 > 0 && r3 > 0
          ? "Bullish"
          : r1 != null && r3 != null && r1 < 0 && r3 < 0
          ? "Weak"
          : "Mixed";

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

  // Sort by 3M return desc
  sectorRows.sort((a, b) => (b.returnsPct.m3 ?? -Infinity) - (a.returnsPct.m3 ?? -Infinity));

  // ---------------------------------
  // 3) Chart: CPI vs sector (~5y rebased)
  // ---------------------------------
  const disableChart =
    String(process.env.WEEK_AHEAD_DISABLE_CHART || "").trim() === "1";
  const chartSector =
    String(process.env.WEEK_AHEAD_CHART_SECTOR || "OZR.AU").trim();

  function isoMonth(ymd) {
    return String(ymd || "").slice(0, 7);
  }

  function toNum(v) {
    const n = typeof v === "string" ? Number(v) : v;
    return typeof n === "number" && Number.isFinite(n) ? n : null;
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

    for (const r of Array.isArray(cpiRaw) ? cpiRaw : []) {
      const m = isoMonth(r.date || r.period || r.datetime);
      const v =
        toNum(r.value) ??
        toNum(r.close) ??
        toNum(r.adjusted_close) ??
        toNum(r.cpi) ??
        null;

      if (m) cpiByMonth.set(m, v);
    }

    // Sector monthly closes (last close each month)
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
      const v = toNum(r.close);
      if (m) secByMonth.set(m, v);
    }

    const months = Array.from(new Set([...cpiByMonth.keys(), ...secByMonth.keys()]))
      .sort()
      .slice(-60);

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
      // optional
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
      proxies: SECTOR_PROXIES,
      results: sectorRows,
    },
    chart,
  };

  // Cache for ~10 days
  await redisSet(payloadKey, JSON.stringify(payload), 60 * 60 * 24 * 10);

  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ key: payloadKey, payload }),
  };
};
