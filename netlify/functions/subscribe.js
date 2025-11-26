// netlify/functions/subscribe.js
// Simple subscription endpoint for Daily Morning Brief
// Usage (frontend):
//   POST /.netlify/functions/subscribe  { "email": "user@example.com" }
//   or:  /.netlify/functions/subscribe?email=user@example.com

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Upstash not configured" }),
    };
  }

  // Only allow POST + simple CORS
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
      body: "",
    };
  }

  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method not allowed" }),
    };
  }

  let email =
    (event.queryStringParameters &&
      event.queryStringParameters.email) ||
    null;

  if (!email && event.body) {
    try {
      const parsed = JSON.parse(event.body);
      if (parsed && typeof parsed.email === "string") {
        email = parsed.email;
      }
    } catch {
      // ignore body parse error
    }
  }

  if (!email || typeof email !== "string") {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Email is required" }),
    };
  }

  email = email.trim().toLowerCase();

  // Very light email validation
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Invalid email format" }),
    };
  }

  async function fetchWithTimeout(url, opts = {}, timeout = 7000) {
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

  const key = "email:subscribers";

  try {
    // SADD email:subscribers <email>
    const url =
      `${UPSTASH_URL}/sadd/` +
      `${encodeURIComponent(key)}/` +
      `${encodeURIComponent(email)}`;

    const res = await fetchWithTimeout(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
      },
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn("subscribe sadd failed", res.status, txt);
      return {
        statusCode: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Failed to save subscription" }),
      };
    }

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ ok: true }),
    };
  } catch (err) {
    console.error("subscribe error", err && err.message);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Internal error" }),
    };
  }
};
