// netlify/functions/email-daily-brief-quiz-background.js
// Background sender: sends the Morning Brief email to all subscribers
// with an added CTA to the "How You Think" quiz.
// Triggered by email-daily-brief-quiz "kicker" scheduled function.

const fetch = (...args) => global.fetch(...args);

// Import existing functions so we reuse their logic
const morningBriefFn = require("./morning-brief");
const matesMorningNoteFn = require("./matesMorningNote");

exports.handler = async function (event) {
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
    return new Promise((resolve) => setTimeout(resolve, ms));
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

  async function redisGet(key) {
    const url = `${UPSTASH_URL}/get/` + encodeURIComponent(key);
    const res = await fetchWithTimeout(
      url,
      {
        headers: {
          Authorization: `Bearer ${UPSTASH_TOKEN}`,
        },
      },
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
    if (ttlSeconds && Number.isFinite(ttlSeconds)) {
      url += `?EX=${ttlSeconds}`;
    }

    const res = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${UPSTASH_TOKEN}`,
        },
      },
      5000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("redisSet failed", key, res.status, txt);
    }
  }

  // Send email to one or multiple recipients
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
    const aestNow = getAestDate(new Date());

    const niceDate = aestNow.toLocaleDateString("en-AU", {
      weekday: "long",
      day: "numeric",
      month: "long",
      year: "numeric",
    });

    // -----------------------------
    // ✅ Holiday banner (AEST dates)
    // -----------------------------
    const yyyy = aestNow.getFullYear();
    const mm = String(aestNow.getMonth() + 1).padStart(2, "0");
    const dd = String(aestNow.getDate()).padStart(2, "0");
    const ymd = `${yyyy}-${mm}-${dd}`;

    // Add/remove dates as needed
    const holidayBannerDates = new Set([
      "2025-12-25", // Christmas Day
      "2025-12-26", // Boxing Day
      "2026-01-01", // New Year's Day
    ]);

    const holidayBannerHtml = holidayBannerDates.has(ymd)
      ? `
          <tr>
            <td style="padding:12px 20px 0 20px;">
              <div style="
                background:#fff7ed;
                border:1px solid #fed7aa;
                padding:12px 14px;
                border-radius:12px;
              ">
                <div style="font-size:13px;font-weight:700;margin:0 0 4px 0;color:#7c2d12;">
                  🎄 Happy Holidays from MatesInvest
                </div>
                <div style="font-size:12px;line-height:1.45;color:#64748b;">
                  Hope you and your family have a great break.
                </div>
              </div>
            </td>
          </tr>
      `
      : "";

    const top = Array.isArray(payload.topPerformers) ? payload.topPerformers : [];

    const metalsObj = payload.metals || payload.symbols || {};
    const friendlyMetals = {
      XAU: "Gold",
      XAG: "Silver",
      IRON: "Iron Ore 62% Fe",
      "LITH-CAR": "Lithium Carbonate",
      NI: "Nickel",
      URANIUM: "Uranium",
    };

    const cryptoObj =
      payload.crypto && typeof payload.crypto === "object" ? payload.crypto : {};
    const cryptoOrder = ["BTC", "ETH", "SOL", "ADA"];
    const friendlyCrypto = {
      BTC: "Bitcoin",
      ETH: "Ethereum",
      SOL: "Solana",
      ADA: "Cardano",
    };

    const topRows = top
      .map((tp) => {
        const sym = tp.symbol || tp.code || "";
        const name = tp.name || "";
        const last =
          typeof tp.lastClose === "number" ? "$" + formatMoney(tp.lastClose) : "—";
        const pct =
          typeof tp.pctGain === "number"
            ? tp.pctGain.toFixed(2) + "%"
            : tp.pctGain
            ? String(tp.pctGain)
            : "—";
        const isUp = typeof tp.pctGain === "number" && tp.pctGain > 0;
        const isDown = typeof tp.pctGain === "number" && tp.pctGain < 0;
        const color = isUp ? "#16a34a" : isDown ? "#dc2626" : "#64748b";
        const arrow = isUp ? "▲" : isDown ? "▼" : "";
        return `
        <tr>
          <td style="padding:8px 6px;font-weight:600;font-size:13px;color:#0b1220;">${sym}</td>
          <td style="padding:8px 6px;font-size:13px;color:#64748b;">${name}</td>
          <td style="padding:8px 6px;font-size:13px;text-align:right;color:#0b1220;">${last}</td>
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
        const label = friendlyMetals[sym] || sym;
        const unit = m.unit || (sym === "IRON" ? "tonne" : "unit");
        const price =
          typeof m.priceAUD === "number"
            ? "$" + formatMoney(m.priceAUD) + ` / ${unit}`
            : "Unavailable";
        const pct = typeof m.pctChange === "number" ? m.pctChange.toFixed(2) + "%" : "—";
        const isUp = typeof m.pctChange === "number" && m.pctChange > 0;
        const isDown = typeof m.pctChange === "number" && m.pctChange < 0;
        const color = isUp ? "#16a34a" : isDown ? "#dc2626" : "#64748b";
        const arrow = isUp ? "▲" : isDown ? "▼" : "";
        return `
        <tr>
          <td style="padding:8px 6px;font-size:13px;color:#0b1220;">
            ${label}
            <span style="color:#94a3b8;">(${sym})</span>
          </td>
          <td style="padding:8px 6px;font-size:13px;text-align:right;color:#0b1220;">${price}</td>
          <td style="padding:8px 6px;font-size:13px;text-align:right;color:${color};white-space:nowrap;">
            ${pct !== "—" ? `${arrow} ${pct}` : pct}
          </td>
        </tr>
      `;
      })
      .join("");

    const cryptoRows = cryptoOrder
      .filter((sym) => cryptoObj[sym])
      .map((sym) => {
        const c = cryptoObj[sym] || {};
        const label = friendlyCrypto[sym] || sym;
        const unit = (c.unit || "coin").toString().trim() || "coin";

        const price =
          typeof c.priceAUD === "number"
            ? "$" + formatMoney(c.priceAUD) + ` / ${unit}`
            : "Unavailable";

        const pctVal =
          typeof c.pctChange === "number" && Number.isFinite(c.pctChange)
            ? c.pctChange
            : null;
        const pct = pctVal !== null ? pctVal.toFixed(2) + "%" : "—";

        const isUp = pctVal !== null && pctVal > 0;
        const isDown = pctVal !== null && pctVal < 0;

        const color = isUp ? "#16a34a" : isDown ? "#dc2626" : "#64748b";
        const arrow = isUp ? "▲" : isDown ? "▼" : "";

        return `
        <tr>
          <td style="padding:8px 6px;font-size:13px;color:#0b1220;">
            ${label}
            <span style="color:#94a3b8;">(${sym})</span>
          </td>
          <td style="padding:8px 6px;font-size:13px;text-align:right;color:#0b1220;">${price}</td>
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

    const quizUrl = "https://matesinvest.com/how-you-think";

    return `
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>MatesMorning – ASX Briefing</title>
</head>
<body style="margin:0;padding:0;background-color:#f5f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#f5f7fb;padding:24px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background-color:#ffffff;border-radius:18px;overflow:hidden;border:1px solid #e2e8f0;box-shadow:0 10px 30px rgba(15,23,42,0.10);">

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
                    ASX Morning Briefing
                  </h1>
                  <div style="font-size:13px;color:#64748b;">
                    ${niceDate}
                  </div>
                </div>
                <div style="text-align:right;font-size:11px;color:#94a3b8;line-height:1.4;max-width:160px;">
                  Built for Australian retail investors.<br/>
                  Short, plain-English, not financial advice.
                </div>
              </div>
            </td>
          </tr>

          ${holidayBannerHtml}

          ${
            morningNote
              ? `
          <tr>
            <td style="padding:14px 20px 4px 20px;">
              <h2 style="margin:0 0 4px 0;font-size:14px;color:#002040;">Mates Morning Note</h2>
              <div style="
                background:#f9fbff;
                border:1px solid #dbeafe;
                padding:10px 14px;
                border-radius:12px;
                font-size:13px;
                line-height:1.45;
                color:#0b1220;
              ">
                ${morningNote.replace(/\n/g, "<br/>")}
              </div>
              <div style="margin-top:6px;font-size:11px;color:#94a3b8;">
                Updated 6:00am AEST · Not financial advice
              </div>
            </td>
          </tr>
          `
              : ""
          }

          ${
            top.length
              ? `
          <tr>
            <td style="padding:14px 20px 6px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Yesterday's Top Performers</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 6px 4px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Code</th>
                    <th align="left" style="padding:6px 6px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Name</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Close</th>
                    <th align="right" style="padding:6px 10px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Move</th>
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

          ${
            metalsRows
              ? `
          <tr>
            <td style="padding:10px 20px 8px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Key Commodities</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 6px 4px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Commodity</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Price (AUD)</th>
                    <th align="right" style="padding:6px 10px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">1D</th>
                  </tr>
                </thead>
                <tbody>
                  ${metalsRows}
                </tbody>
              </table>
              <div style="margin-top:6px;font-size:11px;color:#94a3b8;">
                ${sourceNote}. Not financial advice.
              </div>
            </td>
          </tr>
          `
              : ""
          }

          ${
            cryptoRows
              ? `
          <tr>
            <td style="padding:4px 20px 14px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Crypto snapshot</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;background:#f9fafb;">
                <thead>
                  <tr style="background:#edf2ff;">
                    <th align="left" style="padding:6px 6px 4px 10px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Asset</th>
                    <th align="right" style="padding:6px 6px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Price (AUD)</th>
                    <th align="right" style="padding:6px 10px 4px 6px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">1D</th>
                  </tr>
                </thead>
                <tbody>
                  ${cryptoRows}
                </tbody>
              </table>
            </td>
          </tr>
          `
              : ""
          }

          <tr>
            <td style="padding:8px 20px 8px 20px;">
              <div style="background:#f0f9ff;border:1px solid #bae6fd;padding:14px;border-radius:12px;">
                <h3 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Quick one: what kind of investor are you?</h3>
                <p style="margin:0 0 10px 0;font-size:12px;color:#64748b;line-height:1.4;">
                  Take our 30 second “How you think” quiz — it helps you understand your investing style in plain English.
                </p>
                <a href="${quizUrl}"
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
                  Take the quiz
                </a>
                <div style="margin-top:8px;font-size:11px;color:#94a3b8;">
                  Link: <a href="${quizUrl}" style="color:#00BFFF;text-decoration:none;font-weight:600;">matesinvest.com/how-you-think</a>
                </div>
              </div>
            </td>
          </tr>

          <tr>
            <td style="padding:18px 20px 8px 20px;">
              <div style="background:#f9fbff;border:1px solid #dbeafe;padding:14px;border-radius:12px;">
                <h3 style="margin:0 0 6px 0;font-size:14px;color:#002040;">Invite a mate</h3>
                <p style="margin:0 0 10px 0;font-size:12px;color:#64748b;line-height:1.4;">
                  Know someone who would enjoy the MatesMorning Daily Briefing?
                  Send them this link to subscribe:
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
    let region = "au";
    try {
      if (event && event.body) {
        const parsed = JSON.parse(event.body);
        if (parsed && typeof parsed.region === "string") region = parsed.region;
      }
    } catch (_) {}

    const aestNowForKey = getAestDate(new Date());
    const yyyy = aestNowForKey.getFullYear();
    const mm = String(aestNowForKey.getMonth() + 1).padStart(2, "0");
    const dd = String(aestNowForKey.getDate()).padStart(2, "0");
    const sendKeyPrefix = `email:daily:${yyyy}-${mm}-${dd}`;
    const perRecipientTtlSeconds = 60 * 60 * 72; // 72h

    const mbResponse = await morningBriefFn.handler(
      { queryStringParameters: { region } },
      {}
    );

    if (!mbResponse || mbResponse.statusCode !== 200) {
      console.error("morning-brief handler failed", mbResponse);
      return { statusCode: 500, body: "Failed to generate morning brief" };
    }

    let payload;
    try {
      payload = JSON.parse(mbResponse.body);
    } catch (e) {
      console.error("Failed to parse morning-brief payload", e && e.message);
      return { statusCode: 500, body: "Invalid morning-brief payload" };
    }

    const subscribers = await getSubscribers();
    if (!subscribers.length) {
      console.log("No subscribers – skipping send");
      return { statusCode: 200, body: "No subscribers" };
    }

    const morningNote = await getMorningNote();

    const subjectDate = formatAestForSubject(new Date());
    const subject = `MatesMorning – ASX Briefing for ${subjectDate}`;
    const html = buildEmailHtml(payload, morningNote);

    let sentCount = 0;

    for (const email of subscribers) {
      const personKey = `${sendKeyPrefix}:${email}`;
      const already = await redisGet(personKey);
      if (already) continue;

      try {
        await sendEmail(email, subject, html);
        sentCount += 1;

        await redisSet(personKey, "sent", perRecipientTtlSeconds);

        await sleep(300);
      } catch (err) {
        console.error("Failed sending to", email, err && err.message);
      }
    }

    console.log(
      `Daily quiz brief ${sendKeyPrefix} – sent to ${sentCount} subscribers`
    );
    return { statusCode: 200, body: `Sent to ${sentCount} subscribers` };
  } catch (err) {
    console.error(
      "email-daily-brief-quiz-background error",
      err && (err.stack || err.message)
    );
    return { statusCode: 500, body: "Internal error" };
  }
};
