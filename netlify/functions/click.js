// netlify/functions/click.js
// Tracks email link clicks by MI user id, then redirects.
//
// Usage:
//   /.netlify/functions/click?u=MI0000282&to=/discover.html&c=daily-email
// or absolute to= url is allowed (we'll restrict by allowlist)

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  const qs = event.queryStringParameters || {};
  const u = (qs.u || "").trim();
  const to = (qs.to || "/").trim();
  const campaign = (qs.c || "email").trim();

  // Basic safety: only allow relative paths on your domain
  if (!to.startsWith("/")) {
    return { statusCode: 400, body: "Invalid 'to' (must be relative path)" };
  }

  // Optional: validate MI id format
  const okId = /^MI\d{7}$/.test(u);

  // Record click best-effort (never block redirect)
  async function pipeline(cmds) {
    try {
      await fetch(`${UPSTASH_URL}/pipeline`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${UPSTASH_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(cmds),
      });
    } catch (e) {
      // ignore
    }
  }

  if (UPSTASH_URL && UPSTASH_TOKEN && okId) {
    const now = new Date();
    const day = now.toISOString().slice(0, 10); // UTC day is fine for analytics

    // What we store:
    // - total clicks per user
    // - total clicks per campaign
    // - daily click counts (so you can chart later)
    await pipeline([
      ["INCR", `clicks:user:${u}:total`],
      ["INCR", `clicks:user:${u}:campaign:${campaign}:total`],
      ["INCR", `clicks:day:${day}:total`],
      ["INCR", `clicks:day:${day}:campaign:${campaign}:total`],
      ["INCR", `clicks:day:${day}:user:${u}`],
      ["SADD", `clicks:users:${day}`, u],
    ]);
  }

  return {
    statusCode: 302,
    headers: {
      Location: to,
      "Cache-Control": "no-store",
    },
    body: "",
  };
};
