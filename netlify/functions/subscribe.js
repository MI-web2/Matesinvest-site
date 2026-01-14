// netlify/functions/subscribe.js
// Subscription endpoint
//
// Backwards-compatible behaviour (unchanged):
// - If no `source` (or not app-related): defaults to existing list: email:subscribers
// - If app-related source: always add to email:subscribers-App
//   and optionally to email:subscribers if daily_updates === true
//
// NEW:
// - Assigns a stable sequential ID for each email: MI0000001...
// - Stores mappings:
//    email:id:{email} -> MI0000001
//    id:email:{MI0000001} -> email
// - Uses Redis counter:
//    user:id:counter (INCR) for new IDs
//
// Usage:
//  POST /.netlify/functions/subscribe
//    { "email": "user@example.com", "source": "meta-social-coming-soon", "daily_updates": true }
//
//  OR GET for testing:
//    /.netlify/functions/subscribe?email=user@example.com

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

  // CORS / preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
      body: "",
    };
  }

  let email =
    (event.queryStringParameters && event.queryStringParameters.email) || null;

  // Parse body (POST)
  let source = null;
  let dailyUpdates = false;

  if (!email && event.body && event.httpMethod === "POST") {
    try {
      const parsed = JSON.parse(event.body);

      if (parsed && typeof parsed.email === "string") {
        email = parsed.email;
      }
      if (parsed && typeof parsed.source === "string") {
        source = parsed.source;
      }
      if (parsed && typeof parsed.daily_updates === "boolean") {
        dailyUpdates = parsed.daily_updates;
      }
    } catch {
      // ignore body parse error
    }
  } else if (event.body && event.httpMethod === "POST") {
    // Email came via query param, but still allow source/daily_updates in body if present
    try {
      const parsed = JSON.parse(event.body);
      if (parsed && typeof parsed.source === "string") {
        source = parsed.source;
      }
      if (parsed && typeof parsed.daily_updates === "boolean") {
        dailyUpdates = parsed.daily_updates;
      }
    } catch {
      // ignore
    }
  }

  if (!email || typeof email !== "string") {
    return {
      statusCode: 400,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ error: "Email is required" }),
    };
  }

  email = email.trim().toLowerCase();

  // Light email validation
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return {
      statusCode: 400,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
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

  // Keys
  const dailyKey = "email:subscribers";
  const appKey = "email:subscribers-App";

  // Treat these sources as "app coming soon" signups
  const isAppSignup =
    source === "meta-social-coming-soon" ||
    source === "app-early-access" ||
    source === "social-investing";

  async function sadd(keyName) {
    const url =
      `${UPSTASH_URL}/sadd/` +
      `${encodeURIComponent(keyName)}/` +
      `${encodeURIComponent(email)}`;

    const res = await fetchWithTimeout(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
      },
    });

    return res;
  }

  function fmtId(n) {
    return `MI${String(n).padStart(7, "0")}`;
  }

  // NEW: ensure ID exists for this email, return it.
  // - If email already has an ID, re-use it.
  // - Else allocate the next sequential ID using INCR (safe under concurrency).
async function ensureMemberId() {
  const pipelineUrl = `${UPSTASH_URL}/pipeline`;

  // Step 1: GET existing id
  const r1 = await fetchWithTimeout(pipelineUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([["GET", `email:id:${email}`]]),
  });

  if (!r1.ok) {
    const txt = await r1.text().catch(() => "");
    console.warn("ensureMemberId: pipeline GET failed", r1.status, txt);
    return null;
  }

  const j1 = await r1.json().catch(() => null);
  const existing = j1 && j1[0] && j1[0].result ? j1[0].result : null;
  if (existing) return existing;

  // Step 2: Allocate next ID number
  const r2 = await fetchWithTimeout(pipelineUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([["INCR", "user:id:counter"]]),
  });

  if (!r2.ok) {
    const txt = await r2.text().catch(() => "");
    console.warn("ensureMemberId: pipeline INCR failed", r2.status, txt);
    return null;
  }

  const j2 = await r2.json().catch(() => null);
  const n =
    j2 && j2[0] && typeof j2[0].result === "number" ? j2[0].result : null;
  if (!n) return null;

  const newId = fmtId(n);

  // Step 3: Try to claim email -> id
  const r3 = await fetchWithTimeout(pipelineUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([["SETNX", `email:id:${email}`, newId]]),
  });

  if (!r3.ok) {
    const txt = await r3.text().catch(() => "");
    console.warn("ensureMemberId: pipeline SETNX failed", r3.status, txt);
    return null;
  }

  const j3 = await r3.json().catch(() => null);
  const setnx =
    j3 && j3[0] && typeof j3[0].result === "number" ? j3[0].result : null;

  if (setnx === 1) {
    // We won the claim; now write reverse mapping
    const r4 = await fetchWithTimeout(pipelineUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${UPSTASH_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify([["SET", `id:email:${newId}`, email]]),
    });

    if (!r4.ok) {
      // Not fatal, but good to log
      const txt = await r4.text().catch(() => "");
      console.warn("ensureMemberId: pipeline SET reverse failed", r4.status, txt);
    }

    return newId;
  }

  // Another request beat us; fetch canonical id and return it.
  const r5 = await fetchWithTimeout(pipelineUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([["GET", `email:id:${email}`]]),
  });

  if (!r5.ok) return null;
  const j5 = await r5.json().catch(() => null);
  const canonical = j5 && j5[0] && j5[0].result ? j5[0].result : null;
  return canonical || null;
}

  try {
    // Backwards compatibility:
    // If not an app signup, preserve existing behaviour: add to daily list.
    if (!isAppSignup) {
      const r = await sadd(dailyKey);
      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        console.warn("subscribe sadd failed (daily default)", r.status, txt);
        return {
          statusCode: 500,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
          body: JSON.stringify({ error: "Failed to save subscription" }),
        };
      }

      // NEW: assign ID (best-effort; never blocks)
      const id = await ensureMemberId();

      return {
        statusCode: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ ok: true, ...(id ? { id } : {}) }),
      };
    }

    // App signup path:
    // 1) Always add to app waitlist
    const r1 = await sadd(appKey);
    if (!r1.ok) {
      const txt = await r1.text().catch(() => "");
      console.warn("subscribe sadd failed (app)", r1.status, txt);
      return {
        statusCode: 500,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify({ error: "Failed to save subscription" }),
      };
    }

    // 2) Only add to daily updates if they opted in
    if (dailyUpdates) {
      const r2 = await sadd(dailyKey);
      if (!r2.ok) {
        const txt = await r2.text().catch(() => "");
        console.warn("subscribe sadd failed (daily opt-in)", r2.status, txt);
        return {
          statusCode: 500,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
          body: JSON.stringify({ error: "Failed to save subscription" }),
        };
      }
    }

    // NEW: assign ID (best-effort; never blocks)
    const id = await ensureMemberId();

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ ok: true, ...(id ? { id } : {}) }),
    };
  } catch (err) {
    console.error("subscribe error", err && err.message);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({ error: "Internal error" }),
    };
  }
};
