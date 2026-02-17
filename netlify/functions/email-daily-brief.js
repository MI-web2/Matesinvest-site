// netlify/functions/email-daily-brief.js
// Scheduled "kicker" function: triggers the long-running background sender.
// This should stay scheduled in netlify.toml.

const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }

  // --- Helpers ---
  async function fetchWithTimeout(url, opts = {}, timeout = 8000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      console.log(`fetchWithTimeout: Attempting to fetch ${url} with timeout ${timeout}ms`);
      const res = await fetch(url, { ...opts, signal: controller.signal });
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

  function getAestDate(baseDate = new Date()) {
    // Australia/Brisbane: UTC+10, no DST
    const AEST_OFFSET_MINUTES = 10 * 60;
    return new Date(baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
  }

  async function redisGet(key) {
    const url = `${UPSTASH_URL}/get/` + encodeURIComponent(key);
    const res = await fetchWithRetry(
      url,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      5000,
      3
    );
    if (!res.ok) return null;
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
    const res = await fetchWithRetry(
      url,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      },
      5000,
      3
    );
    return res.ok;
  }

  // Build a "today" lock (AEST) to prevent duplicate kicks if Netlify retries
  const aestNow = getAestDate(new Date());
  const yyyy = aestNow.getFullYear();
  const mm = String(aestNow.getMonth() + 1).padStart(2, "0");
  const dd = String(aestNow.getDate()).padStart(2, "0");
  const kickKey = `email:kick:daily:${yyyy}-${mm}-${dd}`;

  try {
    console.log(`Checking Redis for kick key: ${kickKey}`);
    const alreadyKicked = await redisGet(kickKey);
    if (alreadyKicked) {
      console.log("Daily brief kicker already ran for", `${yyyy}-${mm}-${dd}`);
      return { statusCode: 200, body: "Already kicked today" };
    }

    // Set the kick lock for a few hours (covers retries)
    console.log(`Setting Redis kick lock for key: ${kickKey}`);
    const setSuccess = await redisSet(kickKey, "1", 60 * 60 * 6);
    if (!setSuccess) {
      console.warn("Failed to set Redis lock, continuing anyway");
    }

    // Trigger background sender (non-blocking style)
    const siteUrl =
      process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL;
    if (!siteUrl) {
      console.error("No site URL env found (URL/DEPLOY_PRIME_URL/DEPLOY_URL)");
      return { statusCode: 500, body: "Missing site URL env" };
    }

    console.log(`Site URL resolved to: ${siteUrl}`);

    // Ensure siteUrl starts with http:// or https:// (case-insensitive check)
    const normalizedUrl = siteUrl.toLowerCase().startsWith('http') ? siteUrl : `https://${siteUrl}`;
    const bgUrl = `${normalizedUrl}/.netlify/functions/email-daily-brief-background`;
    
    console.log(`Background function URL: ${bgUrl}`);

    // Fire-and-forget-ish: we still await the request, but we keep timeout short.
    // Use retry logic for better resilience
    const resp = await fetchWithRetry(
      bgUrl,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ region: "au", kickedAt: new Date().toISOString() }),
      },
      10000, // Increased timeout to 10 seconds
      2 // Retry once if it fails
    );

    const txt = await resp.text().catch(() => "");
    console.log("Triggered background daily brief:", resp.status, txt);

    // Even if background returns non-200, we don't want Netlify re-trying forever.
    return { statusCode: 200, body: `Kicked background: ${resp.status}` };
  } catch (err) {
    console.error("email-daily-brief kicker error", err.name, err.message);
    if (err.stack) {
      console.error("Stack trace:", err.stack);
    }
    if (err.cause) {
      console.error("Error cause:", err.cause);
    }
    // Return 200 to avoid endless retries; background job is the real work.
    return { statusCode: 200, body: "Kicker errored (see logs)" };
  }
};
