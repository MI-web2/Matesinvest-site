// netlify/functions/email-analytics-summary.js
// Scheduled: emails yesterday + MTD + YTD analytics summary via Resend.
// Requires Upstash + Resend env vars.

const fetch = (...args) => global.fetch(...args);

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const RESEND_API_KEY = process.env.RESEND_API_KEY;
const EMAIL_FROM = process.env.EMAIL_FROM || "hello@matesinvest.com";

// Comma-separated list: "luke@...,dale@..."
const ANALYTICS_EMAIL_TO = process.env.ANALYTICS_EMAIL_TO || "";

function assertEnv() {
  const missing = [];
  if (!UPSTASH_URL) missing.push("UPSTASH_REDIS_REST_URL");
  if (!UPSTASH_TOKEN) missing.push("UPSTASH_REDIS_REST_TOKEN");
  if (!RESEND_API_KEY) missing.push("RESEND_API_KEY");
  if (!ANALYTICS_EMAIL_TO) missing.push("ANALYTICS_EMAIL_TO");
  if (missing.length) throw new Error("Missing env vars: " + missing.join(", "));
}

// AEST = UTC+10 (no DST handling). Matches your tracking function approach.
function toAESTDate(ts = Date.now()) {
  const d = new Date(ts + 10 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

function addDaysYYYYMMDD(day, delta) {
  const [y, m, d] = day.split("-").map(Number);
  const utc = Date.UTC(y, m - 1, d);
  const next = new Date(utc + delta * 24 * 60 * 60 * 1000);
  return next.toISOString().slice(0, 10);
}

function startOfMonth(day) {
  return day.slice(0, 8) + "01";
}

function startOfYear(day) {
  return day.slice(0, 5) + "01-01";
}

async function upstashPipeline(commands) {
  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });
  if (!res.ok) throw new Error(`Upstash pipeline error: ${res.status} ${await res.text()}`);
  return res.json(); // [{result: ...}, ...]
}

function hgetallArrayToObject(arr) {
  // Upstash returns [field, value, field, value...]
  const out = {};
  if (!Array.isArray(arr)) return out;
  for (let i = 0; i < arr.length; i += 2) {
    out[arr[i]] = Number(arr[i + 1] || 0);
  }
  return out;
}

function sumCounters(objs) {
  const total = { visits: 0, unique_users: 0, new_users: 0, returning_users: 0 };
  for (const o of objs) {
    total.visits += o.visits || 0;
    total.unique_users += o.unique_users || 0;
    total.new_users += o.new_users || 0;
    total.returning_users += o.returning_users || 0;
  }
  return total;
}

function pct(n, d) {
  if (!d) return "0%";
  return ((n / d) * 100).toFixed(1) + "%";
}

async function resendSend({ to, subject, html }) {
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ from: EMAIL_FROM, to, subject, html }),
  });
  if (!res.ok) throw new Error(`Resend error: ${res.status} ${await res.text()}`);
  return res.json();
}

exports.handler = async function () {
  try {
    assertEnv();

    const todayAEST = toAESTDate(Date.now());
    const yesterday = addDaysYYYYMMDD(todayAEST, -1);

    const mtdStart = startOfMonth(yesterday);
    const ytdStart = startOfYear(yesterday);

    // Build date lists
    function dateRange(start, endInclusive) {
      const out = [];
      let d = start;
      while (d <= endInclusive) {
        out.push(d);
        d = addDaysYYYYMMDD(d, 1);
      }
      return out;
    }

    const mtdDays = dateRange(mtdStart, yesterday);
    const ytdDays = dateRange(ytdStart, yesterday);

    // Fetch HGETALL for each day in one pipeline per range
    const dayKey = (day) => `mates:analytics:day:${day}`;

    const yCmd = [["HGETALL", dayKey(yesterday)]];
    const mCmd = mtdDays.map((d) => ["HGETALL", dayKey(d)]);
    const ytdCmd = ytdDays.map((d) => ["HGETALL", dayKey(d)]);

    const [yRes, mRes, ytdRes] = await Promise.all([
      upstashPipeline(yCmd),
      upstashPipeline(mCmd),
      upstashPipeline(ytdCmd),
    ]);

    const yObj = hgetallArrayToObject(yRes?.[0]?.result);
    const mObjs = (mRes || []).map((r) => hgetallArrayToObject(r.result));
    const ytdObjs = (ytdRes || []).map((r) => hgetallArrayToObject(r.result));

    const mtd = sumCounters(mObjs);
    const ytd = sumCounters(ytdObjs);

    const yReturningShare = pct(yObj.returning_users || 0, yObj.visits || 0);
    const mReturningShare = pct(mtd.returning_users || 0, mtd.visits || 0);
    const ytdReturningShare = pct(ytd.returning_users || 0, ytd.visits || 0);

    const to = ANALYTICS_EMAIL_TO.split(",").map((s) => s.trim()).filter(Boolean);

    const subject = `MatesInvest Daily Analytics — ${yesterday} (AEST)`;

    const html = `
      <div style="font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; line-height:1.4;">
        <h2 style="margin:0 0 12px;">MatesInvest Daily Analytics</h2>
        <p style="margin:0 0 16px;color:#444;">Date: <b>${yesterday}</b> (AEST)</p>

        <table style="border-collapse:collapse;width:100%;max-width:640px;">
          <tr>
            <th style="text-align:left;padding:8px;border-bottom:1px solid #eee;">Period</th>
            <th style="text-align:right;padding:8px;border-bottom:1px solid #eee;">Visits</th>
            <th style="text-align:right;padding:8px;border-bottom:1px solid #eee;">Unique</th>
            <th style="text-align:right;padding:8px;border-bottom:1px solid #eee;">New</th>
            <th style="text-align:right;padding:8px;border-bottom:1px solid #eee;">Returning</th>
            <th style="text-align:right;padding:8px;border-bottom:1px solid #eee;">Returning %</th>
          </tr>

          <tr>
            <td style="padding:8px;border-bottom:1px solid #f5f5f5;"><b>Yesterday</b></td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${yObj.visits || 0}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${yObj.unique_users || 0}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${yObj.new_users || 0}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${yObj.returning_users || 0}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${yReturningShare}</td>
          </tr>

          <tr>
            <td style="padding:8px;border-bottom:1px solid #f5f5f5;"><b>Month-to-date</b></td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${mtd.visits}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${mtd.unique_users}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${mtd.new_users}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${mtd.returning_users}</td>
            <td style="text-align:right;padding:8px;border-bottom:1px solid #f5f5f5;">${mReturningShare}</td>
          </tr>

          <tr>
            <td style="padding:8px;"><b>Year-to-date</b></td>
            <td style="text-align:right;padding:8px;">${ytd.visits}</td>
            <td style="text-align:right;padding:8px;">${ytd.unique_users}</td>
            <td style="text-align:right;padding:8px;">${ytd.new_users}</td>
            <td style="text-align:right;padding:8px;">${ytd.returning_users}</td>
            <td style="text-align:right;padding:8px;">${ytdReturningShare}</td>
          </tr>
        </table>

        <p style="margin:16px 0 0;color:#666;font-size:12px;">
          Source: Upstash keys mates:analytics:day:YYYY-MM-DD (AEST day boundary).
        </p>
      </div>
    `;

    await resendSend({ to, subject, html });

    return { statusCode: 200, body: "Sent analytics summary." };
  } catch (err) {
    console.error("email-analytics-summary error:", err);
    return { statusCode: 500, body: "Error sending analytics summary." };
  }
};
