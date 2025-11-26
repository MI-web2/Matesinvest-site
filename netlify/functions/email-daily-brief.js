// netlify/functions/email-daily-brief.js
// Scheduled function: sends the Morning Brief email to all subscribers.

const fetch = (...args) => global.fetch(...args);

// Import existing functions so we reuse their logic
const morningBriefFn = require("./morning-brief");
const matesMorningNoteFn = require("./matesMorningNote");

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

  // --- Helpers ---
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
    const key = "email:subscribers";
    const url = `${UPSTASH_URL}/smembers/` + encodeURIComponent(key);

    const res = await fetchWithTimeout(
      url,
      {
        headers: {
          Authorization: `Bearer ${UPSTASH_TOKEN}`,
        },
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
      throw new Error("Failed to send email");
    }
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

  // Fetch the Mates Morning Note via the existing function
  async function getMorningNote() {
    try {
      const resp = await matesMorningNoteFn.handler({}, {});
      if (!resp || resp.statusCode !== 200) {
        console.warn("matesMorningNote handler failed", resp);
        return null;
      }
      const data = JSON.parse(resp.body || "{}");
      return data.note || null;
    } catch (err) {
      console.error("Error fetching morning note:", err && err.message);
      return null;
    }
  }

  // Build HTML email from morning-brief payload + morning note
  function buildEmailHtml(payload, morningNote) {
    const aestNow = getAestDate();
    const niceDate = aestNow.toLocaleDateString("en-AU", {
      weekday: "long",
      day: "numeric",
      month: "long",
      year: "numeric",
    });

    const top = Array.isArray(payload.topPerformers)
      ? payload.topPerformers
      : [];

    const metalsObj = payload.metals || payload.symbols || {};
    const friendly = {
      XAU: "Gold",
      XAG: "Silver",
      IRON: "Iron Ore 62% Fe",
      "LITH-CAR": "Lithium Carbonate",
      NI: "Nickel",
      URANIUM: "Uranium",
    };

    const topRows = top
      .map((tp) => {
        const sym = tp.symbol || tp.code || "";
        const name = tp.name || "";
        const last =
          typeof tp.lastClose === "number"
            ? "$" + formatMoney(tp.lastClose)
            : "—";
        const pct =
          typeof tp.pctGain === "number"
            ? tp.pctGain.toFixed(2) + "%"
            : tp.pctGain
            ? String(tp.pctGain)
            : "—";
        const isUp =
          typeof tp.pctGain === "number" && tp.pctGain > 0;
        const color = isUp ? "#16a34a" : "#dc2626";
        const arrow = isUp ? "▲" : "▼";
        return `
          <tr>
            <td style="padding:8px 6px;font-weight:600;font-size:13px;">${sym}</td>
            <td style="padding:8px 6px;font-size:13px;color:#4b5563;">${name}</td>
            <td style="padding:8px 6px;font-size:13px;text-align:right;">${last}</td>
            <td style="padding:8px 6px;font-size:13px;text-align:right;color:${color};white-space:nowrap;">
              ${pct !== "—" ? `${arrow} ${pct}` : pct}
            </td>
          </tr>
        `;
      })
      .join("");

    const metalsRows = Object.keys(metalsObj)
      .map((sym) => {
        const m = metalsObj[sym] || {};
        const label = friendly[sym] || sym;
        const unit = m.unit || (sym === "IRON" ? "tonne" : "unit");
        const price =
          typeof m.priceAUD === "number"
            ? "$" + formatMoney(m.priceAUD) + ` / ${unit}`
            : "Unavailable";
        const pct =
          typeof m.pctChange === "number"
            ? m.pctChange.toFixed(2) + "%"
            : "—";
        const isUp =
          typeof m.pctChange === "number" && m.pctChange > 0;
        const isDown =
          typeof m.pctChange === "number" && m.pctChange < 0;
        const color = isUp
          ? "#16a34a"
          : isDown
          ? "#dc2626"
          : "#6b7280";
        const arrow = isUp ? "▲" : isDown ? "▼" : "";
        return `
          <tr>
            <td style="padding:8px 6px;font-size:13px;">${label} <span style="color:#9ca3af;">(${sym})</span></td>
            <td style="padding:8px 6px;font-size:13px;text-align:right;">${price}</td>
            <td style="padding:8px 6px;font-size:13px;text-align:right;color:${color};white-space:nowrap;">
              ${pct !== "—" ? `${arrow} ${pct}` : pct}
            </td>
          </tr>
        `;
      })
      .join("");

    const sourceNote =
      payload._debug && payload._debug.metalsDataSource
        ? `Metals source: ${payload._debug.metalsDataSource}`
        : "Metals snapshot – not live prices";

    // Simple, single-column HTML email
    return `
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>MatesMorning – ASX Briefing</title>
</head>
<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#f3f4f6;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background-color:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb;">

          <!-- Header -->
          <tr>
            <td style="padding:18px 20px 6px 20px;border-bottom:1px solid #e5e7eb;">
              <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;">
                MatesInvest · Daily
              </div>
              <h1 style="margin:4px 0 2px 0;font-size:19px;color:#111827;">MatesMorning – ASX Briefing</h1>
              <div style="font-size:13px;color:#6b7280;">${niceDate}</div>
            </td>
          </tr>

          <!-- Mates Morning Note -->
          ${
            morningNote
              ? `
          <tr>
            <td style="padding:16px 20px 6px 20px;">
              <h2 style="margin:0 0 4px 0;font-size:14px;color:#111827;">Mates Morning Note</h2>
              <div style="
                background:#f9fafb;
                border:1px solid #e5e7eb;
                padding:10px 14px;
                border-radius:8px;
                font-size:13px;
                line-height:1.45;
                color:#111827;
              ">
                ${morningNote.replace(/\n/g, "<br/>")}
              </div>
              <div style="margin-top:6px;font-size:11px;color:#9ca3af;">
                Updated 6:00am AEST · Not financial advice
              </div>
            </td>
          </tr>
          `
              : ""
          }

          <!-- Top performers -->
          ${
            top.length
              ? `
          <tr>
            <td style="padding:14px 20px 6px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#111827;">Yesterday's Top Performers</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                <thead>
                  <tr>
                    <th align="left" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Code</th>
                    <th align="left" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Name</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Close</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Move</th>
                  </tr>
                </thead>
                <tbody>
                  ${topRows}
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
            <td style="padding:10px 20px 12px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#111827;">Key Commodities</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                <thead>
                  <tr>
                    <th align="left" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Commodity</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Price (AUD)</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">1D</th>
                  </tr>
                </thead>
                <tbody>
                  ${metalsRows}
                </tbody>
              </table>
              <div style="margin-top:6px;font-size:11px;color:#9ca3af;">${sourceNote}. Not financial advice.</div>
            </td>
          </tr>
          `
              : ""
          }

          <!-- Footer -->
          <tr>
            <td style="padding:12px 20px 18px 20px;border-top:1px solid #e5e7eb;">
              <p style="margin:0 0 6px 0;font-size:12px;color:#6b7280;">
                View the live version and full AI summaries on
                <a href="https://matesinvest.com/mates-summaries" style="color:#0284c7;text-decoration:none;">MatesFeed</a>.
              </p>
              <p style="margin:0;font-size:11px;color:#9ca3af;">
                This email is general information only and is not financial advice.
              </p>
            </td>
          </tr>
        </table>

        <div style="max-width:640px;margin-top:8px;font-size:10px;color:#9ca3af;">
          You’re receiving this because you subscribed to the MatesInvest daily briefing.
        </div>
      </td>
    </tr>
  </table>
</body>
</html>
    `;
  }

  try {
    // 1) Get the morning brief payload by calling the existing handler
    const mbResponse = await morningBriefFn.handler(
      {
        queryStringParameters: { region: "au" },
      },
      {}
    );

    if (!mbResponse || mbResponse.statusCode !== 200) {
      console.error("morning-brief handler failed", mbResponse);
      return {
        statusCode: 500,
        body: "Failed to generate morning brief",
      };
    }

    let payload;
    try {
      payload = JSON.parse(mbResponse.body);
    } catch (e) {
      console.error("Failed to parse morning-brief payload", e && e.message);
      return { statusCode: 500, body: "Invalid morning brief payload" };
    }

    // 2) Get subscriber list
    const subscribers = await getSubscribers();
    if (!subscribers.length) {
      console.log("No subscribers – skipping send");
      return {
        statusCode: 200,
        body: "No subscribers",
      };
    }

    // 3) Get the Mates Morning Note
    const morningNote = await getMorningNote();

    // 4) Build subject + HTML
    const subjectDate = formatAestForSubject(new Date());
    const subject = `MatesMorning – ASX Briefing for ${subjectDate}`;
    const html = buildEmailHtml(payload, morningNote);

    // 5) Send via Resend
    await sendEmail(subscribers, subject, html);

    console.log("Sent daily brief to subscribers", subscribers.length);

    return {
      statusCode: 200,
      body: `Sent to ${subscribers.length} subscribers`,
    };
  } catch (err) {
    console.error(
      "email-daily-brief error",
      err && (err.stack || err.message)
    );
    return {
      statusCode: 500,
      body: "Internal error",
    };
  }
};
