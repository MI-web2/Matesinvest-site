// netlify/functions/quizEvent.js
const fetch = (...args) => global.fetch(...args);

exports.handler = async function (event) {
  // CORS (keep it simple)
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json"
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true }) };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  }

  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: "Upstash not configured" }) };
  }

  let payload;
  try {
    payload = JSON.parse(event.body || "{}");
  } catch (e) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) };
  }

  // Minimal validation (avoid storing junk)
  const quizId = String(payload.quiz_id || "");
  const primary = String(payload?.result?.primary || "");
  const secondary = payload?.result?.secondary ? String(payload.result.secondary) : "";
  const sessionId = String(payload.session_id || "");

  if (!quizId || !primary || !sessionId) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Missing required fields" }) };
  }

  // No PII: do not store IP, UA, email, etc.
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const dayKey = `${yyyy}-${mm}-${dd}`;

  // Counters + a rolling list for sampling/debug
  const keyBase = `quiz:${quizId}`;
  const keyDay = `${keyBase}:day:${dayKey}`;
  const keyRecent = `${keyBase}:recent`; // list

  const utm = payload.utm || {};
  const safeEvent = {
    t: now.toISOString(),
    sid: sessionId.slice(0, 48),
    p: primary,
    s: secondary || null,
    src: utm.source || null,
    med: utm.medium || null,
    camp: utm.campaign || null
  };

  const urlIncrTotal = `${UPSTASH_URL}/incr/${encodeURIComponent(keyBase + ":count")}`;
  const urlIncrDay = `${UPSTASH_URL}/incr/${encodeURIComponent(keyDay + ":count")}`;
  const urlIncrPrimary = `${UPSTASH_URL}/incr/${encodeURIComponent(keyDay + ":bucket:" + primary)}`;
  const urlLPush = `${UPSTASH_URL}/lpush/${encodeURIComponent(keyRecent)}/${encodeURIComponent(JSON.stringify(safeEvent))}`;
  const urlLTrim = `${UPSTASH_URL}/ltrim/${encodeURIComponent(keyRecent)}/0/199`; // keep last 200

  const headersUpstash = { Authorization: `Bearer ${UPSTASH_TOKEN}` };

  try {
    await Promise.all([
      fetch(urlIncrTotal, { headers: headersUpstash }),
      fetch(urlIncrDay, { headers: headersUpstash }),
      fetch(urlIncrPrimary, { headers: headersUpstash }),
      fetch(urlLPush, { headers: headersUpstash }),
      fetch(urlLTrim, { headers: headersUpstash })
    ]);
  } catch (e) {
    // Don’t break UX if analytics fails
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, logged: false }) };
  }

  return { statusCode: 200, headers, body: JSON.stringify({ ok: true, logged: true }) };
};
