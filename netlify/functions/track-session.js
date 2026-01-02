// netlify/functions/track-session.js
// Tracks session analytics: session_count, session_seconds_total, engaged_sessions into Upstash Redis.

const fetch = (...args) => global.fetch(...args);

function getAESTDateString(ts = Date.now()) {
  // AEST = UTC+10 (no DST handling).
  const d = new Date(ts + 10 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

async function redisPipeline(commands) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!UPSTASH_URL || !UPSTASH_TOKEN) throw new Error("Upstash not configured");

  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upstash error ${res.status}: ${text}`);
  }

  return res.json();
}

function ok(bodyObj) {
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify(bodyObj),
  };
}

function bad(statusCode, msg) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify({ error: msg }),
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
      },
      body: "",
    };
  }

  if (event.httpMethod !== "POST") return bad(405, "Use POST");

  let payload;
  try {
    payload = JSON.parse(event.body || "{}");
  } catch {
    return bad(400, "Invalid JSON");
  }

  const uid = (payload.uid || "").trim();
  const pathRaw = (payload.path || "/").trim();
  const path = (pathRaw || "/").slice(0, 200);
  const sessionId = (payload.sessionId || "").trim();
  const sessionSeconds = Number(payload.sessionSeconds || 0);
  const isEngaged = Boolean(payload.isEngaged);
  const ts = Number(payload.ts || Date.now());

  if (!uid || uid.length < 8 || uid.length > 80) {
    return bad(400, "Missing/invalid uid");
  }

  if (!sessionId || sessionId.length < 8) {
    return bad(400, "Missing/invalid sessionId");
  }

  if (sessionSeconds < 0 || sessionSeconds > 86400) {
    return bad(400, "Invalid sessionSeconds (must be 0-86400)");
  }

  const day = getAESTDateString(ts);

  // Keys for session tracking
  const dayHash = `mates:analytics:day:${day}`;
  const dayPathStats = `mates:analytics:day:${day}:pathstats`;
  const daySessionsSet = `mates:analytics:day:${day}:sessions`; // Set of unique session IDs
  const dayPathSessionsSet = `mates:analytics:day:${day}:pathsessions:${encodeURIComponent(path)}`;

  try {
    // Pipeline: track session stats
    const commands = [
      // Add to global session set (for unique session count)
      ["SADD", daySessionsSet, sessionId],
      
      // Add to per-path session set
      ["SADD", dayPathSessionsSet, sessionId],
      
      // Increment total session seconds (global)
      ["HINCRBY", dayHash, "session_seconds_total", Math.floor(sessionSeconds)],
      
      // Increment per-path session seconds
      ["HINCRBY", dayPathStats, `${path || "/"}|session_seconds_total`, Math.floor(sessionSeconds)],
    ];

    const pipelineRes = await redisPipeline(commands);
    
    const isNewSession = pipelineRes?.[0]?.result === 1; // SADD returns 1 if new
    const isNewPathSession = pipelineRes?.[1]?.result === 1;

    // Counter increments for session-level stats
    const counterCmds = [];

    // Increment session_count only for new sessions
    if (isNewSession) {
      counterCmds.push(["HINCRBY", dayHash, "session_count", 1]);
    }

    if (isNewPathSession) {
      counterCmds.push(["HINCRBY", dayPathStats, `${path || "/"}|session_count`, 1]);
    }

    // Increment engaged_sessions only if this session meets engagement criteria
    if (isEngaged) {
      if (isNewSession) {
        counterCmds.push(["HINCRBY", dayHash, "engaged_sessions", 1]);
      }
      if (isNewPathSession) {
        counterCmds.push(["HINCRBY", dayPathStats, `${path || "/"}|engaged_sessions`, 1]);
      }
    }

    if (counterCmds.length) await redisPipeline(counterCmds);

    return ok({
      day,
      uid,
      path,
      sessionId,
      sessionSeconds,
      isEngaged,
      is_new_session: isNewSession,
      is_new_path_session: isNewPathSession,
    });
  } catch (err) {
    console.error("track-session error:", err);
    return bad(500, "Server error");
  }
};
