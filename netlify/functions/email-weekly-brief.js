// netlify/functions/email-weekly-brief.js
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
    return new Date(baseDate.getTime() + AEST_OFFSET_MINUTES * 60 * 1000);
  }

  async function redisGet(key) {
    const url = `${UPSTASH_URL}/get/` + encodeURIComponent(key);
    const res = await fetchWithTimeout(
      url,
      { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } },
      5000
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
    const res = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      },
      5000
    );
    return res.ok;
  }

  // Build a "today" lock (AEST) to prevent duplicate kicks if Netlify retries
  const aestNow = getAestDate(new Date());
  const yyyy = aestNow.getFullYear();
  const mm = String(aestNow.getMonth() + 1).padStart(2, "0");
  const dd = String(aestNow.getDate()).padStart(2, "0");
  const kickKey = `email:kick:weekly:${yyyy}-${mm}-${dd}`;

  try {
    const alreadyKicked = await redisGet(kickKey);
    if (alreadyKicked) {
      console.log("Weekly brief kicker already ran for", `${yyyy}-${mm}-${dd}`);
      return { statusCode: 200, body: "Already kicked today" };
    }

    // Set the kick lock for a few hours (covers retries)
    await redisSet(kickKey, "1", 60 * 60 * 6);

    // Trigger background sender (non-blocking style)
    const siteUrl =
      process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL;
    if (!siteUrl) {
      console.error("No site URL env found (URL/DEPLOY_PRIME_URL/DEPLOY_URL)");
      return { statusCode: 500, body: "Missing site URL env" };
    }

    const bgUrl = `${siteUrl}/.netlify/functions/email-weekly-brief-background`;

    // Fire-and-forget-ish: we still await the request, but we keep timeout short.
    const resp = await fetchWithTimeout(
      bgUrl,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ region: "au", kickedAt: new Date().toISOString() }),
      },
      8000
    );

    const txt = await resp.text().catch(() => "");
    console.log("Triggered background weekly brief:", resp.status, txt);

    // Even if background returns non-200, we don't want Netlify re-trying forever.
    return { statusCode: 200, body: `Kicked background: ${resp.status}` };
  } catch (err) {
    console.error("email-weekly-brief kicker error", err && (err.stack || err.message));
    // Return 200 to avoid endless retries; background job is the real work.
    return { statusCode: 200, body: "Kicker errored (see logs)" };
  }
};
