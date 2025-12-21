// netlify/functions/email-week-ahead.js
// Scheduled function: sends the Monday "Week Ahead" email to all subscribers.

const fetch = (...args) => global.fetch(...args);

const weekAheadFn = require("./week-ahead");

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

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
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

  // Australia/Brisbane: UTC+10, no DST
  const AEST_OFFSET_MINUTES = 10 * 60;

  function getAestDate(baseDate = new Date()) {
    return new Date(baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
  }

  function formatAestForSubject(date) {
    const d = getAestDate(date);
    return d.toLocaleDateString("en-AU", {
      weekday: "short",
      day: "2-digit",
      month: "short",
      year: "numeric",
    });
  }

  async function getSubscribers() {
    const key = "email:subscribers3";
    const url = `${UPSTASH_URL}/smembers/` + encodeURIComponent(key);

    const res = await fetchWithTimeout(
      url,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
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

  async function redisGet(key) {
    const url = `${UPSTASH_URL}/get/` + encodeURIComponent(key);
    const res = await fetchWithTimeout(
      url,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      5000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisGet failed", key, res.status, txt);
      return null;
    }

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
      5000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisSet failed", key, res.status, txt);
    }
  }

  async function sendEmail(to, subject, html) {
    const toList = Array.isArray(to) ? to : [to];

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
      throw new Error("Failed to send email");
    }
  }

  function formatPct(n) {
    if (typeof n !== "number" || !Number.isFinite(n)) return "—";
    return `${n.toFixed(2)}%`;
  }

  function formatMoney(n) {
    if (typeof n !== "number" || !Number.isFinite(n)) return "—";
    try {
      return n.toLocaleString("en-AU", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    } catch {
      return n.toFixed(2);
    }
  }

  function escapeHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function pctColor(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return "#64748b";
    if (v > 0) return "#16a34a";
    if (v < 0) return "#dc2626";
    return "#64748b";
  }

  function renderChartCard({ title, url, alt }) {
    if (!url) {
      return `
        <div style="background:#f9fbff;border:1px solid #dbeafe;padding:12px 14px;border-radius:12px;font-size:13px;color:#64748b;">
          Chart unavailable.
        </div>
      `;
    }

    return `
      <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
        <div style="padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e2e8f0;font-size:12px;color:#334155;font-weight:700;">
          ${escapeHtml(title || "Chart")}
        </div>
        <img src="${url}" alt="${escapeHtml(alt || title || "Chart")}" style="display:block;width:100%;height:auto;"/>
      </div>
    `;
  }

  function buildEmailHtml(payload) {
    const week = payload.week || {};
    const macro = payload.macro || { bullets: [] };
    const sectors = payload.sectors || { results: [] };
    const charts = payload.charts || {}; // NEW: charts live under payload.charts

    const macroBullets = Array.isArray(macro.bullets) ? macro.bullets : [];
    const macroHtml =
      macroBullets.length > 0
        ? `<ul style="margin:8px 0 0 18px;padding:0;color:#0b1220;font-size:13px;line-height:1.5;">
            ${macroBullets
              .map((b) => `<li style="margin:6px 0;">${escapeHtml(b)}</li>`)
              .join("")}
          </ul>`
        : `<div style="margin-top:8px;font-size:13px;color:#64748b;line-height:1.45;">
            No major Australian macro releases scheduled.
          </div>`;

    const rows = Array.isArray(sectors.results) ? sectors.results : [];
    const sectorRowsHtml = rows
      .map((r) => {
        const m6 = r?.returnsPct?.m6;
        const m3 = r?.returnsPct?.m3;
        const m1 = r?.returnsPct?.m1;

        return `
          <tr>
            <td style="padding:8px 10px;font-size:13px;color:#0b1220;font-weight:600;">
              ${escapeHtml(r.label || r.key || "")}
              <span style="color:#94a3b8;font-weight:500;">(${escapeHtml(
                r.ticker || ""
              )})</span>
            </td>
            <td style="padding:8px 10px;font-size:13px;text-align:right;color:#0b1220;">
              ${typeof r.close === "number" ? "$" + formatMoney(r.close) : "—"}
            </td>
            <td style="padding:8px 10px;font-size:13px;text-align:right;color:${pctColor(
              m6
            )};white-space:nowrap;font-weight:600;">
              ${formatPct(m6)}
            </td>
            <td style="padding:8px 10px;font-size:13px;text-align:right;color:${pctColor(
              m3
            )};white-space:nowrap;font-weight:600;">
              ${formatPct(m3)}
            </td>
            <td style="padding:8px 10px;font-size:13px;text-align:right;color:${pctColor(
              m1
            )};white-space:nowrap;">
              ${formatPct(m1)}
            </td>
          </tr>
        `;
      })
      .join("");

// NEW: 10y markets chart (ASX200 + US majors)
const indicesUrl = charts?.markets10y?.url || null;
const indicesTitle =
  charts?.markets10y?.title || "Major markets (10y, rebased)";
const indicesAlt = "ASX200 vs US indices chart";


    // Existing charts
    const etfChartUrl = charts?.etfMonthly?.url || null;
    const etfTitle =
      charts?.etfMonthly?.title || "Sector ETFs (monthly, rebased)";
    const etfAlt = "Sector ETFs chart";

    const macroChartUrl = charts?.macroAnnual?.url || null;
    const macroTitle =
      charts?.macroAnnual?.title || "Where is Australia now? (annual macro)";
    const macroAlt = "Australia macro chart";

    return `
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>MatesMorning – Week Ahead</title>
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
                    Week Ahead
                  </h1>
                  <div style="font-size:13px;color:#64748b;">
                    ${escapeHtml(week.label || "")}
                  </div>
                </div>
                <div style="text-align:right;font-size:11px;color:#94a3b8;line-height:1.4;max-width:180px;">
                  Built for Australian retail investors.<br/>
                  Short, plain-English, not financial advice.
                </div>
              </div>
            </td>
          </tr>

          <!-- Section 1 -->
          <tr>
            <td style="padding:14px 20px 10px 20px;">
              <h2 style="margin:0 0 4px 0;font-size:14px;color:#002040;">
               ${escapeHtml(macro.title || "Important AU macro this week")}
              </h2>
              <div style="background:#f9fbff;border:1px solid #dbeafe;padding:12px 14px;border-radius:12px;">
                ${macroHtml}
              </div>
            </td>
          </tr>

          <!-- Section 2 -->
          <tr>
            <td style="padding:6px 20px 12px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">
               ${escapeHtml(sectors.title || "Sector trends (6M / 3M / 1M)")}
              </h2>

              <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
                style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">
                      Sector proxy
                    </th>
                    <th align="right" style="padding:6px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">
                      Close
                    </th>
                    <th align="right" style="padding:6px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">
                      6M
                    </th>
                    <th align="right" style="padding:6px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">
                      3M
                    </th>
                    <th align="right" style="padding:6px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">
                      1M
                    </th>
                  </tr>
                </thead>
                <tbody>
                  ${sectorRowsHtml || ""}
                </tbody>
              </table>

              <div style="margin-top:6px;font-size:11px;color:#94a3b8;">
                Proxies are ETFs. Returns are approximate and based on end-of-day pricing.
              </div>
            </td>
          </tr>

          <!-- Section 3 -->
          <tr>
            <td style="padding:6px 20px 16px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">
                Macro Visualisations
              </h2>

              ${renderChartCard({ title: indicesTitle, url: indicesUrl, alt: indicesAlt })}

              <div style="height:10px;"></div>

              ${renderChartCard({ title: etfTitle, url: etfChartUrl, alt: etfAlt })}

              <div style="height:10px;"></div>

              ${renderChartCard({ title: macroTitle, url: macroChartUrl, alt: macroAlt })}

              <div style="margin-top:8px;font-size:11px;color:#94a3b8;">
                Macro series can lag official releases. Not financial advice.
              </div>
            </td>
          </tr>

          <!-- CTA -->
          <tr>
            <td style="padding:18px 20px 8px 20px;">
              <div style="background:#f9fbff;border:1px solid #dbeafe;padding:14px;border-radius:12px;">
                <h3 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Invite a mate</h3>
                <p style="margin:0 0 10px 0;font-size:12px;color:#64748b;line-height:1.4;">
                  Know someone who’d enjoy the Week Ahead? Send them this link:
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

          <tr>
            <td style="padding:12px 20px 18px 20px;border-top:1px solid #e2e8f0;background-color:#ffffff;">
              <p style="margin:0;font-size:11px;color:#94a3b8;">
                This email is general information only and is not financial advice.
              </p>
            </td>
          </tr>

        </table>

        <div style="max-width:640px;margin-top:8px;font-size:10px;color:#94a3b8;">
          You’re receiving this because you subscribed to the MatesInvest emails.
        </div>
      </td>
    </tr>
  </table>
</body>
</html>
    `;
  }

  try {
    // Generate week-ahead payload
    const waResponse = await weekAheadFn.handler({}, {});
    if (!waResponse || waResponse.statusCode !== 200) {
      console.error("week-ahead handler failed", waResponse);
      return { statusCode: 500, body: "Failed to generate week-ahead payload" };
    }

    let wa;
    try {
      wa = JSON.parse(waResponse.body || "{}");
    } catch (e) {
      console.error("Failed to parse week-ahead response", e && e.message);
      return { statusCode: 500, body: "Invalid week-ahead payload" };
    }

    const payload = wa.payload || null;
    const weekStart = payload?.week?.weekStartAEST || null;

    if (!payload || !weekStart) {
      console.error("Missing payload.week.weekStartAEST", wa);
      return { statusCode: 500, body: "Week-ahead payload missing weekStart" };
    }

    const subjectDate = formatAestForSubject(new Date());
    const subject = `MatesMorning – Week Ahead (${payload.week.label || subjectDate})`;
    const html = buildEmailHtml(payload);

    // Preview mode
    const previewTo = String(process.env.WEEK_AHEAD_EMAIL_PREVIEW_TO || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const disableSend =
      String(process.env.WEEK_AHEAD_EMAIL_DISABLE_SEND || "").trim() === "1";

    if (previewTo.length) {
      if (disableSend) {
        return {
          statusCode: 200,
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            mode: "preview-disabled",
            subject,
            previewTo,
            weekStart,
          }),
        };
      }

      await sendEmail(previewTo, subject, html);
      return {
        statusCode: 200,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          mode: "preview",
          sentTo: previewTo.length,
          weekStart,
        }),
      };
    }

    // Subscriber blast
    const subscribers = await getSubscribers();
    if (!subscribers.length) return { statusCode: 200, body: "No subscribers" };

    const sendKeyPrefix = `email:weekAhead:${weekStart}`;
    const perRecipientTtlSeconds = 60 * 60 * 24 * 14;

    let sentCount = 0;

    for (const email of subscribers) {
      const personKey = `${sendKeyPrefix}:${email}`;
      const already = await redisGet(personKey);
      if (already) continue;

      if (disableSend) continue;

      try {
        await sendEmail(email, subject, html);
        sentCount += 1;
        await redisSet(personKey, "sent", perRecipientTtlSeconds);
        await sleep(300);
      } catch (err) {
        console.error("Failed sending to", email, err && err.message);
      }
    }

    return {
      statusCode: 200,
      body: `Sent week-ahead to ${sentCount} subscribers`,
    };
  } catch (err) {
    console.error("email-week-ahead error", err && (err.stack || err.message));
    return { statusCode: 500, body: "Internal error" };
  }
};
