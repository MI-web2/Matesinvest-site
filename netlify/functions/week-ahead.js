// netlify/functions/week-ahead.js
// Week Ahead generator (Monday email)

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

    // If it's Mon–Fri, use THIS week's Monday
    if (day >= 1 && day <= 5) {
      monday.setUTCDate(monday.getUTCDate() - (day - 1));
      return monday;
    }

    // If Sat/Sun, use NEXT Monday
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

  async function redisGet(key) {
    const url = `${UPSTASH_URL}/get/` + encodeURIComponent(key);
    const res = await fetchWithTimeout(
      url,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      8000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`redisGet failed ${res.status}: ${txt}`);
    }

    const j = await res.json().catch(() => null);
    return j ? j.result : null; // string or null
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

      const line = l.replace(/\s+–\s+/, " - ").replace(/\s+—\s+/, " - ");
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

  function toNum(v) {
    const n = typeof v === "string" ? Number(v) : v;
    return typeof n === "number" && Number.isFinite(n) ? n : null;
  }

  const today = new Date();
  const toISO = isoDate(today);

  const from = new Date(today.getTime());
  from.setUTCDate(from.getUTCDate() - 320);
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

  async function fetchEodSeries(ticker, fromISOArg, toISOArg) {
    const url =
      `https://eodhd.com/api/eod/${encodeURIComponent(ticker)}` +
      `?from=${fromISOArg}&to=${toISOArg}&fmt=json&api_token=${EODHD_API_TOKEN}`;

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
    return (now / then - 1) * 100;
  }

  const sectorRows = [];
  for (const p of SECTOR_PROXIES) {
    try {
      const s = await fetchEodSeries(p.ticker, fromISO, toISO);
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

  sectorRows.sort(
    (a, b) => (b?.returnsPct?.m3 ?? -Infinity) - (a?.returnsPct?.m3 ?? -Infinity)
  );

  // ---------------------------------
  // 3) Charts
  // ---------------------------------
  const disableCharts =
    String(process.env.WEEK_AHEAD_DISABLE_CHARTS || "").trim() === "1";

  function isoMonth(d) {
    return String(d || "").slice(0, 7);
  }

  function isoMonthFromYmd(ymd) {
    return String(ymd || "").slice(0, 7);
  }

  function rebaseTo100(values) {
    const first = values.find(
      (v) => typeof v === "number" && Number.isFinite(v)
    );
    if (!first) return values.map(() => null);
    return values.map((v) =>
      typeof v === "number" && Number.isFinite(v) ? (v / first) * 100 : null
    );
  }

  function monthEndCloseByMonth(eodRowsAsc) {
    const byMonth = new Map();
    for (const r of eodRowsAsc) {
      const m = isoMonth(r.date);
      const c = toNum(r.close);
      if (!m || c == null) continue;
      byMonth.set(m, c);
    }
    return byMonth;
  }

  function monthEndCloseFromPoints(pointsAsc) {
    const byMonth = new Map();
    for (const p of pointsAsc) {
      const ymd = p && p[0];
      const val =
        typeof p?.[1] === "number" ? p[1] : Number(p?.[1]);
      if (!ymd || !Number.isFinite(val)) continue;
      const m = isoMonthFromYmd(ymd);
      if (!m) continue;
      byMonth.set(m, val); // overwrite => month-end
    }
    return byMonth;
  }

  function monthLabelPretty(yyyyMm) {
    const m = String(yyyyMm || "").match(/^(\d{4})-(\d{2})$/);
    if (!m) return yyyyMm;
    const year = m[1];
    const month = parseInt(m[2], 10);
    const names = [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
    ];
    return `${names[month - 1] || m[2]} ${year}`;
  }

  async function createQuickChartShortUrl(cfg, version) {
    const payload = {
      chart: cfg,
      width: 640,
      height: 320,
      format: "png",
      backgroundColor: "white",
    };
    if (version) payload.version = version;

    const res = await fetchWithTimeout(
      "https://quickchart.io/chart/create",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      },
      15000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`quickchart create failed: ${res.status} ${txt}`);
    }

    const j = await res.json().catch(() => null);
    if (!j || !j.url) throw new Error("quickchart create returned no url");
    return j.url;
  }

  async function buildEtfMonthlyOverlayChart(tickers, labels) {
    const n = Math.min(tickers.length, labels.length);
    const useTickers = tickers.slice(0, n);
    const useLabels = labels.slice(0, n);

    const to = new Date();
    const from5 = new Date(to.getTime());
    from5.setUTCFullYear(from5.getUTCFullYear() - 5);

    const fromISO5 = isoDate(from5);
    const toISO5 = isoDate(to);

    const seriesByMonth = [];
    for (const t of useTickers) {
      const rows = await fetchEodSeries(t, fromISO5, toISO5);
      rows.sort((a, b) => String(a.date).localeCompare(String(b.date)));
      seriesByMonth.push(monthEndCloseByMonth(rows));
    }

    const months = Array.from(
      new Set(seriesByMonth.flatMap((m) => Array.from(m.keys())))
    )
      .sort()
      .slice(-60);

    const prettyLabels = months.map(monthLabelPretty);

    const datasets = months.length
      ? seriesByMonth.map((m, i) => {
          const vals = months.map((k) => m.get(k) ?? null);
          return {
            label: `${useLabels[i]} (${useTickers[i]})`,
            data: rebaseTo100(vals),
            fill: false,
            borderWidth: 2,
            pointRadius: 0,
          };
        })
      : [];

    const cfg = {
      type: "line",
      data: { labels: prettyLabels, datasets },
      options: {
        responsive: true,
        title: { display: false},
        legend: { display: true },
        scales: {
          xAxes: [
            {
              ticks: {
                maxRotation: 0,
                minRotation: 0,
                autoSkip: true,
                maxTicksLimit: 10,
              },
            },
          ],
          yAxes: [{ ticks: {} }],
        },
      },
    };

    return await createQuickChartShortUrl(cfg, "2.9.4");
  }

  async function buildMarkets10yChart(tickers, labels) {
    const n = Math.min(tickers.length, labels.length);
    const useTickers = tickers.slice(0, n);
    const useLabels = labels.slice(0, n);

    const to = new Date();
    const from10 = new Date(to.getTime());
    from10.setUTCFullYear(from10.getUTCFullYear() - 10);

    const fromISO10 = isoDate(from10);
    const toISO10 = isoDate(to);

    const seriesByMonth = [];
    for (const t of useTickers) {
      const rows = await fetchEodSeries(t, fromISO10, toISO10);
      rows.sort((a, b) => String(a.date).localeCompare(String(b.date)));
      seriesByMonth.push(monthEndCloseByMonth(rows));
    }

    const months = Array.from(
      new Set(seriesByMonth.flatMap((m) => Array.from(m.keys())))
    )
      .sort()
      .slice(-120);

    const prettyLabels = months.map(monthLabelPretty);

    const datasets = months.length
      ? seriesByMonth.map((m, i) => {
          const vals = months.map((k) => m.get(k) ?? null);
          return {
            label: `${useLabels[i]} (${useTickers[i]})`,
            data: rebaseTo100(vals),
            fill: false,
            borderWidth: 2,
            pointRadius: 0,
          };
        })
      : [];

    const cfg = {
      type: "line",
      data: { labels: prettyLabels, datasets },
      options: {
        responsive: true,
        title: { display: false) },
        legend: { display: true },
        scales: {
          xAxes: [
            {
              ticks: {
                maxRotation: 0,
                minRotation: 0,
                autoSkip: true,
                maxTicksLimit: 6,
              },
            },
          ],
          yAxes: [{ ticks: {} }],
        },
      },
    };

    return await createQuickChartShortUrl(cfg, "2.9.4");
  }

  async function buildCommoditiesOverlayFromUpstash(keys, labels, titleText) {
    const n = Math.min(keys.length, labels.length);
    const useKeys = keys.slice(0, n);
    const useLabels = labels.slice(0, n);

    const seriesByMonth = [];
    for (const k of useKeys) {
      const raw = await redisGet(k);
      if (!raw) {
        seriesByMonth.push(new Map());
        continue;
      }

      let parsed = null;
      try {
        parsed = JSON.parse(raw);
      } catch {
        parsed = null;
      }

      const pts = Array.isArray(parsed?.points) ? parsed.points : [];
      pts.sort((a, b) => String(a?.[0]).localeCompare(String(b?.[0])));
      seriesByMonth.push(monthEndCloseFromPoints(pts));
    }

    const months = Array.from(
      new Set(seriesByMonth.flatMap((m) => Array.from(m.keys())))
    )
      .sort()
      .slice(-60); // your data is ~6 months now; will scale later

    const prettyLabels = months.map(monthLabelPretty);

    const datasets = months.length
      ? seriesByMonth.map((m, i) => {
          const vals = months.map((k) => m.get(k) ?? null);
          return {
            label: useLabels[i], // clean legend
            data: rebaseTo100(vals),
            fill: false,
            borderWidth: 2,
            pointRadius: 0,
          };
        })
      : [];

    const cfg = {
      type: "line",
      data: { labels: prettyLabels, datasets },
      options: {
        responsive: true,
        title: { display: false },
        legend: { display: true },
        scales: {
          xAxes: [
            {
              ticks: {
                maxRotation: 0,
                minRotation: 0,
                autoSkip: true,
                maxTicksLimit: 10,
              },
            },
          ],
          yAxes: [{ ticks: {} }],
        },
      },
    };

    return await createQuickChartShortUrl(cfg, "2.9.4");
  }

  // --- Chart inputs ---
  const defaultEtfTickers = ["OZR.AU", "QFN.AU", "ATEC.AU"];
  const defaultEtfLabels = ["Resources", "Financials", "Tech"];

  const etfTickers = String(process.env.WEEK_AHEAD_ETF_TICKERS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const etfLabels = String(process.env.WEEK_AHEAD_ETF_LABELS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const finalEtfTickers = etfTickers.length === 3 ? etfTickers : defaultEtfTickers;
  const finalEtfLabels = etfLabels.length === 3 ? etfLabels : defaultEtfLabels;

  const defaultMarketsTickers = ["STW.AU", "NDQ.AU", "EWU.US"];
  const defaultMarketsLabels = [
    "Australia (ASX 200)",
    "US (Nasdaq 100)",
    "UK (Equities)",
  ];

  const marketsTickers = String(process.env.WEEK_AHEAD_MARKETS_TICKERS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const marketsLabels = String(process.env.WEEK_AHEAD_MARKETS_LABELS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  function alignTickersAndLabels(tickers, labels, fallbackTickers, fallbackLabels) {
    const t = Array.isArray(tickers) ? tickers : [];
    const l = Array.isArray(labels) ? labels : [];
    if (t.length >= 2 && l.length >= 2) {
      const n = Math.min(t.length, l.length);
      return { tickers: t.slice(0, n), labels: l.slice(0, n) };
    }
    return { tickers: fallbackTickers, labels: fallbackLabels };
  }

  const alignedMarkets = alignTickersAndLabels(
    marketsTickers,
    marketsLabels,
    defaultMarketsTickers,
    defaultMarketsLabels
  );

  // Commodities (from Upstash history:* keys)
  const commodityHistoryKeys = [
    "history:metal:daily:XAU",
    "history:metal:daily:XAG",
    "history:metal:daily:IRON",
    "history:metal:daily:LITH-CAR",
    "history:metal:daily:NI",
    "history:metal:daily:URANIUM",
  ];

  const commodityLabels = ["Gold", "Silver", "Iron Ore", "Lithium", "Nickel", "Uranium"];

  let charts = {
    enabled: !disableCharts,
    markets10y: {
      title: "Major markets (10y)",
      tickers: alignedMarkets.tickers,
      labels: alignedMarkets.labels,
      url: null,
    },
    etfMonthly: {
      title: "Sector ETFs (5y)",
      tickers: finalEtfTickers,
      labels: finalEtfLabels,
      url: null,
    },
    commodities: {
      title: "Key commodities (6m)",
      keys: commodityHistoryKeys,
      labels: commodityLabels,
      url: null,
    },
    macroAnnual: {
      title: "Where is Australia now? (annual macro)",
      url: null,
      disabled: true, // ✅ TEMP: disabled
      note: "Temporarily disabled",
    },
  };

  if (!disableCharts) {
    try {
      charts.markets10y.url = await buildMarkets10yChart(
        alignedMarkets.tickers,
        alignedMarkets.labels
      );
    } catch (e) {
      charts.markets10y.error =
        e && e.message ? e.message : "Markets chart failed";
    }

    try {
      charts.etfMonthly.url = await buildEtfMonthlyOverlayChart(
        finalEtfTickers,
        finalEtfLabels
      );
    } catch (e) {
      charts.etfMonthly.error = e && e.message ? e.message : "ETF chart failed";
    }

    try {
      charts.commodities.url = await buildCommoditiesOverlayFromUpstash(
        commodityHistoryKeys,
        commodityLabels,
        "Key commodities (rebased to 100)"
      );
    } catch (e) {
      charts.commodities.error =
        e && e.message ? e.message : "Commodities chart failed";
    }

    // ✅ Macro annual intentionally skipped while disabled
  }

  const payload = {
    meta: {
      id: "week_ahead_v1",
      region: "au",
      timezone: "Australia/Brisbane",
      generatedAtAEST: getAestDate(new Date()).toISOString().replace("Z", "+10:00"),
    },
    week: { weekStartAEST: weekStart, weekEndAEST: weekEnd, label: weekLabel },
    macro,
    sectors: {
      title: "Sector trends (6M / 3M / 1M)",
      sort: "3m_desc",
      proxies: SECTOR_PROXIES,
      results: sectorRows,
    },
    charts,
  };

  await redisSet(payloadKey, JSON.stringify(payload), 60 * 60 * 24 * 10);

  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ key: payloadKey, payload }),
  };
};
