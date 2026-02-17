// netlify/functions/email-week-ahead.js
// Scheduled "kicker": triggers the long-running background sender.

const fetchFn = (...args) => global.fetch(...args);

exports.handler = async function () {
  async function fetchWithTimeout(url, opts = {}, timeout = 8000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      console.log(`fetchWithTimeout: Attempting to fetch ${url} with timeout ${timeout}ms`);
      const res = await fetchFn(url, { ...opts, signal: controller.signal });
      clearTimeout(id);
      console.log(`fetchWithTimeout: Success for ${url}, status: ${res.status}`);
      return res;
    } catch (err) {
      clearTimeout(id);
      console.error(`fetchWithTimeout: Failed for ${url}`, err.message, err.cause);
      throw err;
    }
  }

  async function fetchWithRetry(url, opts = {}, timeout = 8000, maxRetries = 3) {
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await fetchWithTimeout(url, opts, timeout);
      } catch (err) {
        lastError = err;
        console.warn(`Fetch attempt ${attempt}/${maxRetries} failed for ${url}:`, err.message);
        if (attempt < maxRetries) {
          // Exponential backoff: wait 1s, 2s, 4s...
          const delay = Math.pow(2, attempt - 1) * 1000;
          console.log(`Retrying in ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    throw lastError;
  }

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

    console.log(`Site URL resolved to: ${SITE_URL}`);

    // Ensure URL starts with http:// or https://
    const normalizedUrl = SITE_URL.startsWith('http') ? SITE_URL : `https://${SITE_URL}`;
    const endpoint = `${normalizedUrl.replace(/\/$/, "")}/.netlify/functions/email-week-ahead-background`;

    console.log(`Background function URL: ${endpoint}`);

    const res = await fetchWithRetry(
      endpoint,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-internal-cron-secret": SECRET,
        },
        body: JSON.stringify({ trigger: "scheduled-kicker" }),
      },
      10000, // Increased timeout to 10 seconds
      2 // Retry once if it fails
    );

    const txt = await res.text().catch(() => "");

    if (!res.ok) {
      console.error("Failed to trigger background sender", res.status, txt);
      return { statusCode: 500, body: `Trigger failed: ${res.status} ${txt}` };
    }

    // Background function usually returns 202 quickly.
    return { statusCode: 200, body: "Triggered week-ahead background sender" };
  } catch (err) {
    console.error("email-week-ahead kicker error", err.name, err.message);
    if (err.stack) {
      console.error("Stack trace:", err.stack);
    }
    if (err.cause) {
      console.error("Error cause:", err.cause);
    }
    return { statusCode: 500, body: "Internal error" };
  }
};
