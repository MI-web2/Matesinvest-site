// netlify/functions/email-daily-brief.js
// Scheduled function: sends the Morning Brief email to all subscribers.

const fetch = (...args) => global.fetch(...args);

// Import existing functions so we reuse their logic
const morningBriefFn = require("./morning-brief");
const matesMorningNoteFn = require("./matesMorningNote");

exports.handler = async function (event, context) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  const RESEND_API_KEY = process.env.RESEND_API_KEY;
  const EMAIL_FROM = process.env.EMAIL_FROM || "hello@matesinvest.com";
  const FUNCTION_SECRET = process.env.FUNCTION_SECRET;
  const TEST_RECIPIENTS = process.env.TEST_RECIPIENTS || "";

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }
  if (!RESEND_API_KEY) {
    console.error("RESEND_API_KEY missing");
    return { statusCode: 500, body: "Resend not configured" };
  }

  // Optional test mode for manual runs (does not affect scheduled cron)
  const qs = (event && event.queryStringParameters) || {};
  const isTestRun =
    qs.mode === "test" &&
    qs.secret &&
    FUNCTION_SECRET &&
    qs.secret === FUNCTION_SECRET;

  if (isTestRun) {
    console.log("Running email-daily-brief in TEST MODE");
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

  // 🔧 UPDATED: send a single email (to one or a small list of recipients)
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

  function changeToBadge(change) {
    if (change == null) return "";
    const value = Number(change);
    if (!Number.isFinite(value)) return String(change);
    const prefix = value > 0 ? "+" : "";
    return `${prefix}${value.toFixed(2)}%`;
  }

  function getChangeBadgeColor(change) {
    if (change == null) return "#64748b"; // neutral
    const value = Number(change);
    if (!Number.isFinite(value)) return "#64748b";
    if (value > 0) return "#16a34a"; // green
    if (value < 0) return "#dc2626"; // red
    return "#64748b";
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

    const top = Array.isArray(payload.topPerformers)
      ? payload.topPerformers
      : [];
    const bottom = Array.isArray(payload.bottomPerformers)
      ? payload.bottomPerformers
      : [];
    const sectors = Array.isArray(payload.sectorMoves)
      ? payload.sectorMoves
      : [];
    const indexMoves = payload.indexMoves || {};
    const metals = payload.metals || {};
    const crypto = payload.crypto || {};

    const friendlyCrypto = {
      BTC: "Bitcoin",
      ETH: "Ethereum",
      SOL: "Solana",
      DOGE: "Dogecoin",
      ADA: "Cardano",
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
        const badgeColor = getChangeBadgeColor(tp.pctGain);

        return `
        <tr>
          <td style="padding:4px 8px;font-size:12px;color:#0b1220;">
            <strong>${sym}</strong>
            <span style="color:#64748b;"> · ${name}</span>
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${last}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;">
            <span style="
              display:inline-block;
              padding:2px 6px;
              border-radius:999px;
              background:${badgeColor}1A;
              color:${badgeColor};
              font-size:11px;
            ">
              ${pct}
            </span>
          </td>
        </tr>
      `;
      })
      .join("");

    const bottomRows = bottom
      .map((bp) => {
        const sym = bp.symbol || bp.code || "";
        const name = bp.name || "";
        const last =
          typeof bp.lastClose === "number"
            ? "$" + formatMoney(bp.lastClose)
            : "—";
        const pct =
          typeof bp.pctLoss === "number"
            ? bp.pctLoss.toFixed(2) + "%"
            : bp.pctLoss
            ? String(bp.pctLoss)
            : "—";
        const badgeColor = getChangeBadgeColor(bp.pctLoss);

        return `
        <tr>
          <td style="padding:4px 8px;font-size:12px;color:#0b1220;">
            <strong>${sym}</strong>
            <span style="color:#64748b;"> · ${name}</span>
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${last}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;">
            <span style="
              display:inline-block;
              padding:2px 6px;
              border-radius:999px;
              background:${badgeColor}1A;
              color:${badgeColor};
              font-size:11px;
            ">
              ${pct}
            </span>
          </td>
        </tr>
      `;
      })
      .join("");

    const sectorRows = sectors
      .map((s) => {
        const name = s.name || s.sector || "";
        const pct =
          typeof s.change === "number"
            ? s.change.toFixed(2) + "%"
            : s.change
            ? String(s.change)
            : "—";
        const badgeColor = getChangeBadgeColor(s.change);

        return `
        <tr>
          <td style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${name}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;">
            <span style="
              display:inline-block;
              padding:2px 6px;
              border-radius:999px;
              background:${badgeColor}1A;
              color:${badgeColor};
              font-size:11px;
            ">
              ${pct}
            </span>
          </td>
        </tr>
      `;
      })
      .join("");

    const metalsOrder = ["XAU", "XAG", "IRON", "CU", "AL", "NI"];
    const friendlyMetals = {
      XAU: "Gold",
      XAG: "Silver",
      IRON: "Iron ore",
      CU: "Copper",
      AL: "Aluminium",
      NI: "Nickel",
    };

    const metalsRows = metalsOrder
      .filter((sym) => metals[sym])
      .map((sym) => {
        const m = metals[sym] || {};
        const label = friendlyMetals[sym] || sym;
        const price =
          typeof m.priceAUD === "number"
            ? "$" + formatMoney(m.priceAUD)
            : "Unavailable";
        const pctVal =
          typeof m.pctChange === "number" && Number.isFinite(m.pctChange)
            ? m.pctChange
            : null;
        const pct =
          pctVal !== null ? pctVal.toFixed(2) + "%" : "—";
        const isUp = pctVal !== null && pctVal > 0;
        const isDown = pctVal !== null && pctVal < 0;
        const color = isUp
          ? "#16a34a"
          : isDown
          ? "#dc2626"
          : "#64748b";

        return `
        <tr>
          <td style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${label}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${price}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;">
            <span style="
              display:inline-block;
              padding:2px 6px;
              border-radius:999px;
              background:${color}1A;
              color:${color};
              font-size:11px;
            ">
              ${pct}
            </span>
          </td>
        </tr>
      `;
      })
      .join("");

    const cryptoOrder = ["BTC", "ETH", "SOL", "DOGE", "ADA"];
    const cryptoObj = crypto || {};

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
        const pct =
          pctVal !== null ? pctVal.toFixed(2) + "%" : "—";

        const isUp = pctVal !== null && pctVal > 0;
        const isDown = pctVal !== null && pctVal < 0;

        const color = isUp
          ? "#16a34a"
          : isDown
          ? "#dc2626"
          : "#64748b";

        return `
        <tr>
          <td style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${label}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;color:#0b1220;">
            ${price}
          </td>
          <td align="right" style="padding:4px 8px;font-size:12px;">
            <span style="
              display:inline-block;
              padding:2px 6px;
              border-radius:999px;
              background:${color}1A;
              color:${color};
              font-size:11px;
            ">
              ${pct}
            </span>
          </td>
        </tr>
      `;
      })
      .join("");

    const indexSummaryParts = [];
    if (indexMoves["^AXJO"]) {
      const axjo = indexMoves["^AXJO"];
      indexSummaryParts.push(
        `ASX 200 ${
          typeof axjo.change === "number" && axjo.change >= 0 ? "up" : "down"
        } ${changeToBadge(axjo.change)}`
      );
    }
    if (indexMoves["^AXKO"]) {
      const small = indexMoves["^AXKO"];
      indexSummaryParts.push(
        `Small Ords ${
          typeof small.change === "number" && small.change >= 0
            ? "up"
            : "down"
        } ${changeToBadge(small.change)}`
      );
    }
    if (indexMoves["^NDX"]) {
      const ndx = indexMoves["^NDX"];
      indexSummaryParts.push(
        `Nasdaq ${
          typeof ndx.change === "number" && ndx.change >= 0 ? "up" : "down"
        } ${changeToBadge(ndx.change)}`
      );
    }
    const indexSummary =
      indexSummaryParts.length > 0
        ? indexSummaryParts.join(" · ")
        : "Key indices summary unavailable";

    return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>MatesMorning – ASX Morning Briefing</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
    <tr>
      <td align="center" style="padding:20px 12px;">
        <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="max-width:640px;background:#0f172a;border-radius:18px;overflow:hidden;border:1px solid #1e293b;">
          <tr>
            <td style="padding:18px 20px 12px 20px;border-bottom:1px solid #1f2937;background:radial-gradient(circle at top,#1d4ed8,#020617);">
              <div style="font-size:20px;font-weight:700;color:#eff6ff;letter-spacing:0.02em;">
                MatesMorning
              </div>
              <div style="margin-top:4px;font-size:12px;color:#cbd5f5;">
                ${niceDate} · ASX snapshot in plain English
              </div>
            </td>
          </tr>

          <tr>
            <td style="padding:14px 20px 10px 20px;background:#020617;">
              <div style="font-size:13px;color:#e5e7eb;line-height:1.5;">
                <p style="margin:0 0 6px 0;">
                  Good morning mate – here’s what moved the market and what’s on the radar before the open.
                </p>
                <p style="margin:0;font-size:12px;color:#9ca3af;">
                  ${indexSummary}
                </p>
              </div>
            </td>
          </tr>

          ${
            morningNote
              ? `
          <tr>
            <td style="padding:14px 20px 4px 20px;">
              <h2 style="margin:0 0 4px 0;font-size:14px;color:#e5e7eb;">Mates Morning Note</h2>
              <div style="
                background:#020617;
                border:1px solid #1f2937;
                padding:10px 14px;
                border-radius:12px;
                font-size:13px;
                line-height:1.45;
                color:#e5e7eb;
              ">
              ${morningNote.replace(/\n/g, "<br/>")}
              </div>
              <div style="margin-top:6px;font-size:11px;color:#6b7280;">
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
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#e5e7eb;">Top movers</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-radius:12px;overflow:hidden;border:1px solid #1f2937;background:#020617;">
                <thead>
                  <tr style="background:#030712;">
                    <th align="left" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Company</th>
                    <th align="right" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Last</th>
                    <th align="right" style="padding:6px 10px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Move</th>
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
            bottom.length
              ? `
          <tr>
            <td style="padding:4px 20px 12px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#e5e7eb;">Biggest falls</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-radius:12px;overflow:hidden;border:1px solid #1f2937;background:#020617;">
                <thead>
                  <tr style="background:#030712;">
                    <th align="left" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Company</th>
                    <th align="right" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Last</th>
                    <th align="right" style="padding:6px 10px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Move</th>
                  </tr>
                </thead>
                <tbody>
                  ${bottomRows}
                </tbody>
              </table>
            </td>
          </tr>
          `
              : ""
          }

          ${
            sectorRows
              ? `
          <tr>
            <td style="padding:4px 20px 12px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#e5e7eb;">Sector moves</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-radius:12px;overflow:hidden;border:1px solid #1f2937;background:#020617;">
                <thead>
                  <tr style="background:#030712;">
                    <th align="left" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Sector</th>
                    <th align="right" style="padding:6px 10px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Move</th>
                  </tr>
                </thead>
                <tbody>
                  ${sectorRows}
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
            <td style="padding:4px 20px 12px 20px;">
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#e5e7eb;">Key commodities</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-radius:12px;overflow:hidden;border:1px solid #1f2937;background:#020617;">
                <thead>
                  <tr style="background:#030712;">
                    <th align="left" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Asset</th>
                    <th align="right" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Price (AUD)</th>
                    <th align="right" style="padding:6px 10px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">1D</th>
                  </tr>
                </thead>
                <tbody>
                  ${metalsRows}
                </tbody>
              </table>
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
              <h2 style="margin:0 0 6px 0;font-size:14px;color:#e5e7eb;">Crypto snapshot</h2>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-radius:12px;overflow:hidden;border:1px solid #1f2937;background:#020617;">
                <thead>
                  <tr style="background:#030712;">
                    <th align="left" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Asset</th>
                    <th align="right" style="padding:6px 8px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">Price (AUD)</th>
                    <th align="right" style="padding:6px 10px 4px 8px;font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:0.08em;">1D</th>
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
            <td style="padding:18px 20px 8px 20px;">
              <div style="background:#020617;border:1px solid #1f2937;padding:14px;border-radius:12px;">
                <h3 style="margin:0 0 6px 0;font-size:14px;color:#e5e7eb;">Invite a mate</h3>
                <p style="margin:0 0 10px 0;font-size:12px;color:#9ca3af;line-height:1.4;">
                  Know someone who would enjoy the MatesMorning Daily Briefing?
                  Send them this link to subscribe:
                </p>

                <a href="https://matesinvest.com/mates-summaries#subscribe"
                   style="display:inline-block;padding:8px 12px;border-radius:999px;background:#2563eb;color:#eff6ff;font-size:12px;font-weight:600;text-decoration:none;">
                  Share the signup link
                </a>
              </div>
            </td>
          </tr>

          <tr>
            <td style="padding:0 20px 18px 20px;font-size:11px;color:#6b7280;">
              <p style="margin:0 0 4px 0;">
                This email is general in nature and not financial advice. Consider your own circumstances or talk to a licensed adviser.
              </p>
              <p style="margin:0;">
                Want to unsubscribe? Reply to this email or contact <a href="mailto:hello@matesinvest.com" style="color:#93c5fd;text-decoration:none;">hello@matesinvest.com</a>.
              </p>
            </td>
          </tr>
        </table>

        <div style="max-width:640px;margin-top:8px;font-size:10px;color:#6b7280;">
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
    // Idempotency key so retries don't send duplicates
    const aestNowForKey = getAestDate(new Date());
    const yyyy = aestNowForKey.getFullYear();
    const mm = String(aestNowForKey.getMonth() + 1).padStart(2, "0");
    const dd = String(aestNowForKey.getDate()).padStart(2, "0");
    const keyPrefix = isTestRun ? "email:daily:test" : "email:daily";
    const sendKey = `${keyPrefix}:${yyyy}-${mm}-${dd}`;

    const alreadySent = await redisGet(sendKey);
    if (alreadySent && !isTestRun) {
      console.log("Daily brief already sent for", sendKey, "- skipping");
      return {
        statusCode: 200,
        body: `Already sent daily brief for ${yyyy}-${mm}-${dd}`,
      };
    }

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
    let subscribers = await getSubscribers();

    // In TEST MODE, override subscriber list with TEST_RECIPIENTS env var
    if (isTestRun) {
      const testList = TEST_RECIPIENTS.split(",")
        .map((e) => e.trim())
        .filter(Boolean);

      if (testList.length) {
        console.log(
          "TEST MODE: overriding subscribers with",
          testList.length,
          "test recipient(s)"
        );
        subscribers = testList;
      } else {
        console.warn("TEST MODE: no TEST_RECIPIENTS configured, aborting");
        return {
          statusCode: 200,
          body: "Test mode: no TEST_RECIPIENTS configured",
        };
      }
    }

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

    // Mark as sent so future retries today won't resend (production runs only)
    await redisSet(sendKey, "sent", 60 * 60 * 36); // ~36 hours

    // 🔧 5) Send individually to each subscriber, throttled for Resend (2 req/sec)
    let sentCount = 0;
    for (const email of subscribers) {
      try {
        await sendEmail(email, subject, html); // one recipient at a time
        sentCount++;

        // Respect Resend rate limit: max 2 req/sec
        await sleep(1200); // ~1.6 requests per second
      } catch (err) {
        console.error("Failed sending to", email, err && err.message);
      }
    }

    console.log("Sent daily brief to subscribers", sentCount);

    return {
      statusCode: 200,
      body: `Sent to ${sentCount} subscribers`,
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
