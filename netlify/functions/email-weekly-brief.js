// netlify/functions/email-weekly-brief.js
// Scheduled function: sends "The Week That Was" email on Saturday morning AEST.

const fetch = (...args) => global.fetch(...args);

// Optional AI weekly note function (safe if missing)
let matesWeeklyNoteFn = null;
try {
  matesWeeklyNoteFn = require("./matesWeeklyNote");
} catch (e) {
  console.warn("matesWeeklyNote function not found; weekly note will be empty");
}

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  const RESEND_API_KEY = process.env.RESEND_API_KEY;
  const EMAIL_FROM = process.env.EMAIL_FROM || "hello@matesinvest.com";

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }
  if (!RESEND_API_KEY) {
    console.error("RESEND_API_KEY missing");
    return { statusCode: 500, body: "Resend not configured" };
  }

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

  function getAestDate(baseDate = new Date()) {
    // Australia/Brisbane: UTC+10, no DST
    const AEST_OFFSET_MINUTES = 10 * 60;
    const aest = new Date(baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
    return aest;
  }

  function formatMoney(n) {
    if (typeof n !== "number" || !Number.isFinite(n)) return null;
    try {
      return n.toLocaleString("en-AU", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    } catch {
      return n.toFixed(2);
    }
  }

  async function getSubscribers() {
    const key = "email:subscribers";
    const url = `${UPSTASH_URL}/smembers/` + encodeURIComponent(key);

    const res = await fetchWithTimeout(
      url,
      {
        headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      },
      8000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("smembers subscribers failed", res.status, txt);
      return [];
    }

    const j = await res.json().catch(() => null);
    if (!j || !Array.isArray(j.result)) return [];
    return j.result.filter((e) => typeof e === "string" && e.includes("@"));
  }

  async function sendEmail(toList, subject, html) {
    const res = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        from: `MatesInvest <${EMAIL_FROM}>`,
        to: toList,
        subject,
        html,
      }),
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.error("Resend send failed", res.status, txt);
      throw new Error("Failed to send weekly email");
    }
  }

  // Last N *market* days in AEST (skip Sat/Sun).
  function getLastNMarketDaysAest(n = 5) {
    const todayAest = getAestDate();
    const dates = [];
    const d = new Date(todayAest);

    // Expected to run Saturday AEST: step back until we have 5 weekdays
    while (dates.length < n) {
      d.setDate(d.getDate() - 1);
      const day = d.getDay(); // 0=Sun, 6=Sat
      if (day === 0 || day === 6) continue;

      const iso = d.toISOString().slice(0, 10); // YYYY-MM-DD (UTC date, but consistent with your keys)
      dates.push(iso);
    }

    // Currently newest -> oldest; flip to oldest -> newest
    return dates.reverse();
  }

  function formatWeekRangeForSubject(datesAsc) {
    if (!datesAsc.length) return "";
    const first = new Date(datesAsc[0] + "T00:00:00Z");
    const last = new Date(datesAsc[datesAsc.length - 1] + "T00:00:00Z");

    const fmtShort = (dt) =>
      dt.toLocaleDateString("en-AU", { day: "2-digit", month: "short" });
    const fmtLong = (dt) =>
      dt.toLocaleDateString("en-AU", {
        day: "2-digit",
        month: "short",
        year: "numeric",
      });

    return `${fmtShort(first)} – ${fmtLong(last)}`;
  }

  // -------------------------------
  // Fetch raw daily data from Upstash
  // -------------------------------
  async function getAsxDailySnapshots(datesAsc) {
    const snapshots = [];

    for (const date of datesAsc) {
      const key = `asx200:daily:${date}`;
      const url = `${UPSTASH_URL}/get/${encodeURIComponent(key)}`;

      const res = await fetchWithTimeout(
        url,
        {
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );

      if (!res.ok) {
        console.warn("asx200 daily fetch failed", date, res.status);
        continue;
      }

      const j = await res.json().catch(() => null);
      if (!j || !j.result) continue;

      try {
        const rows = JSON.parse(j.result);
        if (Array.isArray(rows)) {
          snapshots.push({ date, rows });
        }
      } catch (e) {
        console.warn("Failed to parse asx200 daily", date, e.message);
      }
    }

    return snapshots;
  }

  async function getMetalsDailySnapshots(datesAsc) {
    const snapshots = [];

    for (const date of datesAsc) {
      const key = `metals:${date}`;
      const url = `${UPSTASH_URL}/get/${encodeURIComponent(key)}`;

      const res = await fetchWithTimeout(
        url,
        {
          headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
        },
        8000
      );

      if (!res.ok) {
        console.warn("metals daily fetch failed", date, res.status);
        continue;
      }

      const j = await res.json().catch(() => null);
      if (!j || !j.result) continue;

      try {
        const payload = JSON.parse(j.result);
        snapshots.push({ date, payload });
      } catch (e) {
        console.warn("Failed to parse metals daily", date, e.message);
      }
    }

    return snapshots;
  }

  // -------------------------------
  // Aggregation: sectors + commodities
  // -------------------------------
  function buildWeeklyAggregates(asxDaily, metalsDaily) {
    // ----- Sector performance -----
    const sectorPerf = new Map();

    for (const snap of asxDaily) {
      const { rows } = snap;
      for (const row of rows) {
        const pct =
          typeof row.pctChange === "number" ? row.pctChange : 0;

        const sectorRaw =
          row.sector ||
          row.gicsSector ||
          row.industry ||
          row.industryGroup ||
          "Other";

        const sector = String(sectorRaw).trim() || "Other";

        const prev =
          sectorPerf.get(sector) || {
            sector,
            sumPct: 0,
            daysSeen: 0,
          };

        prev.sumPct += pct;
        prev.daysSeen += 1;

        sectorPerf.set(sector, prev);
      }
    }

    const allSectors = Array.from(sectorPerf.values()).map((s) => {
      const avgPct = s.daysSeen > 0 ? s.sumPct / s.daysSeen : 0;
      return { ...s, avgPct };
    });

    const weeklyTopSectors = allSectors
      .slice()
      .sort((a, b) => b.avgPct - a.avgPct)
      .slice(0, 5);

    const weeklyBottomSectors = allSectors
      .slice()
      .sort((a, b) => a.avgPct - b.avgPct)
      .slice(0, 5);

    // ----- Commodities -----
    const historyBySymbol = {};

    for (const snap of metalsDaily) {
      const { date, payload } = snap;
      const symbols = (payload && payload.symbols) || {};
      for (const sym of Object.keys(symbols)) {
        const m = symbols[sym];
        if (!m) continue;

        historyBySymbol[sym] ||= [];
        historyBySymbol[sym].push({
          date,
          priceAUD:
            typeof m.priceAUD === "number" ? m.priceAUD : null,
        });
      }
    }

    const metalsWeekly = {};
    for (const [sym, points] of Object.entries(historyBySymbol)) {
      const valid = points
        .filter((p) => typeof p.priceAUD === "number")
        .sort((a, b) => a.date.localeCompare(b.date));
      if (!valid.length) continue;

      const first = valid[0];
      const last = valid[valid.length - 1];
      const weeklyPct = first.priceAUD
        ? ((last.priceAUD - first.priceAUD) / first.priceAUD) * 100
        : 0;

      metalsWeekly[sym] = {
        firstDate: first.date,
        lastDate: last.date,
        firstPriceAUD: first.priceAUD,
        lastPriceAUD: last.priceAUD,
        weeklyPct,
        series: valid,
      };
    }

    return { weeklyTopSectors, weeklyBottomSectors, metalsWeekly };
  }

  // -------------------------------
  // AI weekly note (optional)
  // -------------------------------
  async function getWeeklyNote(aggregates) {
    if (!matesWeeklyNoteFn) return null;

    try {
      const resp = await matesWeeklyNoteFn.handler(
        {
          body: JSON.stringify({ aggregates }),
        },
        {}
      );

      if (!resp || resp.statusCode !== 200) {
        console.warn("matesWeeklyNote handler failed", resp);
        return null;
      }
      const data = JSON.parse(resp.body || "{}");
      return data.note || null;
    } catch (err) {
      console.error("Error fetching weekly note:", err && err.message);
      return null;
    }
  }

  // -------------------------------
  // HTML builder
  // -------------------------------
  function buildWeeklyEmailHtml(aggregates, weeklyNote, datesAsc) {
    const { weeklyTopSectors, weeklyBottomSectors, metalsWeekly } =
      aggregates;

    const aestNow = getAestDate();
    const niceDate = aestNow.toLocaleDateString("en-AU", {
      weekday: "long",
      day: "numeric",
      month: "long",
      year: "numeric",
    });

    const rangeStr = formatWeekRangeForSubject(datesAsc);

    const friendlyCommodity = {
      XAU: "Gold",
      XAG: "Silver",
      IRON: "Iron Ore 62% Fe",
      "LITH-CAR": "Lithium Carbonate",
      NI: "Nickel",
      URANIUM: "Uranium",
    };

    const sectorRows = (list) =>
      list
        .map((s) => {
          const pct = s.avgPct;
          const pctStr =
            typeof pct === "number" ? pct.toFixed(2) + "%" : "—";
          const isUp = typeof pct === "number" && pct > 0;
          const isDown = typeof pct === "number" && pct < 0;
          const color = isUp
            ? "#16a34a"
            : isDown
            ? "#dc2626"
            : "#64748b";
          const arrow = isUp ? "▲" : isDown ? "▼" : "";
          return `
            <tr>
              <td style="padding:8px 6px;font-weight:600;font-size:13px;color:#0b1220;">${s.sector}</td>
              <td style="padding:8px 6px;font-size:13px;text-align:right;color:${color};white-space:nowrap;">
                ${pctStr !== "—" ? `${arrow} ${pctStr}` : pctStr}
              </td>
            </tr>
          `;
        })
        .join("");

    const topRowsHtml =
      weeklyTopSectors && weeklyTopSectors.length
        ? sectorRows(weeklyTopSectors)
        : "";
    const bottomRowsHtml =
      weeklyBottomSectors && weeklyBottomSectors.length
        ? sectorRows(weeklyBottomSectors)
        : "";

    const metalsRows = Object.keys(metalsWeekly)
      .map((sym) => {
        const m = metalsWeekly[sym];
        const label = friendlyCommodity[sym] || sym;

        const lastPrice =
          typeof m.lastPriceAUD === "number"
            ? "$" + formatMoney(m.lastPriceAUD)
            : "—";
        const weeklyPct =
          typeof m.weeklyPct === "number"
            ? m.weeklyPct.toFixed(2) + "%"
            : "—";
        const isUp =
          typeof m.weeklyPct === "number" && m.weeklyPct > 0;
        const isDown =
          typeof m.weeklyPct === "number" && m.weeklyPct < 0;
        const color = isUp
          ? "#16a34a"
          : isDown
          ? "#dc2626"
          : "#64748b";
        const arrow = isUp ? "▲" : isDown ? "▼" : "";

        return `
          <tr>
            <td style="padding:8px 6px;font-size:13px;color:#0b1220;">
              ${label}
              <span style="color:#94a3b8;">(${sym})</span>
            </td>
            <td style="padding:8px 6px;font-size:13px;text-align:right;color:#0b1220;">${lastPrice}</td>
            <td style="padding:8px 6px;font-size:13px;text-align:right;color:${color};white-space:nowrap;">
              ${weeklyPct !== "—" ? `${arrow} ${weeklyPct}` : weeklyPct}
            </td>
          </tr>
        `;
      })
      .join("");

    return `
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>MatesMorning – The Week That Was</title>
</head>
<body style="margin:0;padding:0;background-color:#f5f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#f5f7fb;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background-color:#ffffff;border-radius:18px;overflow:hidden;border:1px solid #e2e8f0;box-shadow:0 10px 30px rgba(15,23,42,0.10);">

          <!-- Header -->
          <tr>
            <td style="padding:18px 20px 10px 20px;border-bottom:1px solid #e2e8f0;background:radial-gradient(circle at top left,#e2ebff 0,#f5f7fb 60%);">
              <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:10px;">
                <div>
                  <div style="font-size:12px;color:#64748b;margin-bottom:4px;">
                    <span style="
                      display:inline-block;
                      padding:2px 9px;
                      border-radius:999px;
                      background:#e7f7ff;
                      border:1px solid #c5e5ff;
                      color:#083a59;
                      font-size:11px;
                      font-weight:600;
                    ">
                      MatesInvest · MatesMorning
                    </span>
                  </div>
                  <h1 style="margin:2px 0 2px 0;font-size:19px;color:#002040;">
                    The Week That Was – ASX
                  </h1>
                  <div style="font-size:13px;color:#64748b;">
                    ${rangeStr} · Sent ${niceDate}
                  </div>
                </div>
                <div style="text-align:right;font-size:11px;color:#94a3b8;line-height:1.4;max-width:160px;">
                  Built for Australian retail investors.<br/>
                  Weekly snapshot, not financial advice.
                </div>
              </div>
            </td>
          </tr>

          <!-- Weekly note -->
          ${
            weeklyNote
              ? `
          <tr>
            <td style="padding:14px 20px 4px 20px;">
              <h2 style="margin:0 0 4px 0;font-size:14px;color:#002040;">Weekly Wrap</h2>
              <div style="
                background:#f9fbff;
                border:1px solid #dbeafe;
                padding:10px 14px;
                border-radius:12px;
                font-size:13px;
                line-height:1.45;
                color:#0b1220;
              ">
                ${weeklyNote.replace(/\n/g, "<br/>")}
              </div>
              <div style="margin-top:6px;font-size:11px;color:#94a3b8;">
                Based on the last 5 trading days · Not financial advice
              </div>
            </td>
          </tr>
          `
              : ""
          }

          <!-- Top sectors -->
          ${
            topRowsHtml
              ? `
          <tr>
            <td style="padding:14px 20px 6px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Top sectors – week up</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 6px 4px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Sector</th>
                    <th align="right" style="padding:6px 10px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Avg week move</th>
                  </tr>
                </thead>
                <tbody>
                  ${topRowsHtml}
                </tbody>
              </table>
            </td>
          </tr>
          `
              : ""
          }

          <!-- Weakest sectors -->
          ${
            bottomRowsHtml
              ? `
          <tr>
            <td style="padding:10px 20px 6px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Weakest sectors – week down</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 6px 4px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Sector</th>
                    <th align="right" style="padding:6px 10px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Avg week move</th>
                  </tr>
                </thead>
                <tbody>
                  ${bottomRowsHtml}
                </tbody>
              </table>
            </td>
          </tr>
          `
              : ""
          }

          <!-- Commodities -->
          ${
            metalsRows
              ? `
          <tr>
            <td style="padding:10px 20px 14px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Key Commodities – week move</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 6px 4px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Commodity</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Last price (AUD)</th>
                    <th align="right" style="padding:6px 10px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Week</th>
                  </tr>
                </thead>
                <tbody>
                  ${metalsRows}
                </tbody>
              </table>
              <div style="margin-top:6px;font-size:11px;color:#94a3b8;">
                Weekly moves based on daily closing snapshots · Not live prices · Not financial advice.
              </div>
            </td>
          </tr>
          `
              : ""
          }

          <!-- Invite a mate -->
          <tr>
            <td style="padding:18px 20px 8px 20px;">
              <div style="background:#f9fbff;border:1px solid #dbeafe;padding:14px;border-radius:12px;">
                <h3 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Send to a mate</h3>
                <p style="margin:0 0 10px 0;font-size:12px;color:#64748b;line-height:1.4;">
                  Know someone who’d enjoy a weekly ASX recap?
                  Forward this email or send them this link to subscribe:
                </p>
                <a href="https://matesinvest.com/mates-summaries#subscribe"
                   style="
                     display:inline-block;
                     padding:8px 14px;
                     background:#00BFFF;
                     color:#ffffff;
                     text-decoration:none;
                     border-radius:999px;
                     font-size:13px;
                     font-weight:600;
                   ">
                  Subscribe to MatesMorning
                </a>
              </div>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:12px 20px 18px 20px;border-top:1px solid #e2e8f0;background-color:#ffffff;">
              <p style="margin:0 0 6px 0;font-size:12px;color:#64748b;">
                View the live version and full AI summaries on
                <a href="https://matesinvest.com/mates-summaries" style="color:#00BFFF;text-decoration:none;font-weight:600;">
                  MatesFeed
                </a>.
              </p>
              <p style="margin:0;font-size:11px;color:#94a3b8;">
                This email is general information only and is not financial advice.
              </p>
            </td>
          </tr>

        </table>

        <div style="max-width:640px;margin-top:8px;font-size:10px;color:#94a3b8;">
          You’re receiving this because you subscribed to the MatesInvest daily / weekly briefing.
        </div>
      </td>
    </tr>
  </table>
</body>
</html>
    `;
  }

  // -------------------------------
  // Main
  // -------------------------------
  try {
    const datesAsc = getLastNMarketDaysAest(5);

    const [subscribers, asxDaily, metalsDaily] = await Promise.all([
      getSubscribers(),
      getAsxDailySnapshots(datesAsc),
      getMetalsDailySnapshots(datesAsc),
    ]);

    if (!subscribers.length) {
      console.log("No subscribers – skipping weekly send");
      return { statusCode: 200, body: "No subscribers" };
    }

    if (!asxDaily.length && !metalsDaily.length) {
      console.log("No weekly data – skipping");
      return { statusCode: 200, body: "No weekly data" };
    }

    const aggregates = buildWeeklyAggregates(asxDaily, metalsDaily);
    const weeklyNote = await getWeeklyNote(aggregates);

    const rangeStr = formatWeekRangeForSubject(datesAsc);
    const subject = `MatesMorning – The Week That Was (${rangeStr})`;

    const html = buildWeeklyEmailHtml(aggregates, weeklyNote, datesAsc);

    await sendEmail(subscribers, subject, html);

    console.log("Sent weekly brief to subscribers", subscribers.length);
    return {
      statusCode: 200,
      body: `Sent weekly brief to ${subscribers.length} subscribers`,
    };
  } catch (err) {
    console.error(
      "email-weekly-brief error",
      err && (err.stack || err.message)
    );
    return {
      statusCode: 500,
      body: "Internal error",
    };
  }
};
