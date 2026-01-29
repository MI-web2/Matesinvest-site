// netlify/functions/lead-capture.js
//
// Secure server-to-server lead capture endpoint for agencies (CJ&Co).
// Uses the SAME sequential ID system as subscribe.js.
//
// Behaviour:
// - Always add to app waitlist: email:subscribers-App
// - Only add to daily email list if daily_updates === true
// - Assign stable MI000000X ID using existing Redis counter + mappings

const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  const LEAD_API_KEY = process.env.LEAD_API_KEY;

  if (!UPSTASH_URL || !UPSTASH_TOKEN || !LEAD_API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Server not configured" }),
    };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }

  // Auth
  const auth = event.headers.authorization || event.headers.Authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (token !== LEAD_API_KEY) {
    return { statusCode: 401, body: "Unauthorized" };
  }

  let body;
  try {
    body = JSON.parse(event.body || "{}");
  } catch {
    return { statusCode: 400, body: "Invalid JSON" };
  }

  let { email, name, daily_updates, source, campaign, adset, ad } = body;

  if (!email || typeof email !== "string") {
    return { statusCode: 400, body: "Email required" };
  }

  email = email.trim().toLowerCase();
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return { statusCode: 400, body: "Invalid email" };
  }

  daily_updates = !!daily_updates;

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

  async function sadd(keyName) {
    const url =
      `${UPSTASH_URL}/sadd/` +
      `${encodeURIComponent(keyName)}/` +
      `${encodeURIComponent(email)}`;

    return fetchWithTimeout(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
      },
    });
  }

  function fmtId(n) {
    return `MI${String(n).padStart(7, "0")}`;
  }

  // === SAME ID LOGIC AS subscribe.js ===
  async function ensureMemberId() {
    const pipelineUrl = `${UPSTASH_URL}/pipeline`;

    // 1) Check existing
    const r1 = await fetchWithTimeout(pipelineUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["GET", `email:id:${email}`]]),
    });

    const j1 = await r1.json().catch(() => null);
    const existing = j1?.[0]?.result;
    if (existing) return existing;

    // 2) Allocate next ID
    const r2 = await fetchWithTimeout(pipelineUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["INCR", "user:id:counter"]]),
    });

    const j2 = await r2.json().catch(() => null);
    const n = j2?.[0]?.result;
    if (!n) return null;

    const newId = fmtId(n);

    // 3) Claim email → id
    const r3 = await fetchWithTimeout(pipelineUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["SETNX", `email:id:${email}`, newId]]),
    });

    const j3 = await r3.json().catch(() => null);
    if (j3?.[0]?.result === 1) {
      // write reverse mapping
      await fetchWithTimeout(pipelineUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${UPSTASH_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify([["SET", `id:email:${newId}`, email]]),
      });

      return newId;
    }

    // fallback
    const r4 = await fetchWithTimeout(pipelineUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["GET", `email:id:${email}`]]),
    });

    const j4 = await r4.json().catch(() => null);
    return j4?.[0]?.result || null;
  }

  try {
    // 1) Always add to waitlist
    await sadd("email:subscribers-App");

    // 2) Optional daily emails
    if (daily_updates) {
      await sadd("email:subscribers");
    }

    // 3) Assign SAME member ID
    const id = await ensureMemberId();

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ok: true, ...(id ? { id } : {}) }),
    };
  } catch (err) {
    console.error("lead-capture error", err?.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal error" }),
    };
  }
};
