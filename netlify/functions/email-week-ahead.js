// netlify/functions/email-week-ahead.js
// Scheduled "kicker": triggers the long-running background sender.

const fetchFn = (...args) => global.fetch(...args);

exports.handler = async function () {
  try {
    const SITE_URL =
      process.env.URL ||
      process.env.DEPLOY_PRIME_URL ||
      process.env.DEPLOY_URL;

    const SECRET = String(process.env.INTERNAL_CRON_SECRET || "").trim();

    if (!SITE_URL) {
      console.error("Missing Netlify URL env (URL/DEPLOY_PRIME_URL/DEPLOY_URL)");
      return { statusCode: 500, body: "Missing site URL env" };
    }
    if (!SECRET) {
      console.error("Missing INTERNAL_CRON_SECRET env");
      return { statusCode: 500, body: "Missing INTERNAL_CRON_SECRET" };
    }

    const endpoint = `${SITE_URL.replace(/\/$/, "")}/.netlify/functions/email-week-ahead-background`;

    const res = await fetchFn(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-internal-cron-secret": SECRET,
      },
      body: JSON.stringify({ trigger: "scheduled-kicker" }),
    });

    const txt = await res.text().catch(() => "");

    if (!res.ok) {
      console.error("Failed to trigger background sender", res.status, txt);
      return { statusCode: 500, body: `Trigger failed: ${res.status} ${txt}` };
    }

    // Background function usually returns 202 quickly.
    return { statusCode: 200, body: "Triggered week-ahead background sender" };
  } catch (err) {
    console.error("email-week-ahead kicker error", err && (err.stack || err.message));
    return { statusCode: 500, body: "Internal error" };
  }
};
