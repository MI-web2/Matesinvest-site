// netlify/functions/track-visit.js
// Tracks new vs returning users + daily uniques into Upstash Redis.

const fetch = (...args) => global.fetch(...args);

function getAESTDateString(ts = Date.now()) {
  // AEST = UTC+10 (no DST handling). Good enough for now.
  // If you want AEDT support later, we can improve this.
  const d = new Date(ts + 10 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

async function redisCmd(cmdArray) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!UPSTASH_URL || !UPSTASH_TOKEN) throw new Error("Upstash not configured");

  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify([cmdArray]),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upstash error ${res.status}: ${text}`);
  }

  const json = await res.json();
  // pipeline returns [{ result: ... }]
  return json?.[0]?.result;
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
  const path = (payload.path || "/").trim().slice(0, 200);
  const ts = Number(payload.ts || Date.now());

  if (!uid || uid.length < 8 || uid.length > 80) return bad(400, "Missing/invalid uid");

  const day = getAESTDateString(ts);

  const firstSeenKey = `mates:analytics:user:first_seen:${uid}`;
  const lastSeenKey = `mates:analytics:user:last_seen:${uid}`;

  const dayHash = `mates:analytics:day:${day}`;
  const dayUids = `mates:analytics:day:${day}:uids`;
  const dayPaths = `mates:analytics:day:${day}:paths`;

  try {
    // 1) Determine if this user is new globally (SETNX first_seen)
    // SETNX returns 1 if set, 0 if already existed
    const setnx = await redisCmd(["SETNX", firstSeenKey, String(ts)]);

    const isNewUser = setnx === 1;

    // 2) Add uid to daily set; SADD returns 1 if added (first time today)
    // 3) Increment counters
    const commands = [
      ["SADD", dayUids, uid],
      ["HINCRBY", dayHash, "visits", 1],
      // optional per-path
      ["HINCRBY", dayPaths, path || "/", 1],
      // update last_seen
      ["SET", lastSeenKey, String(ts)],
    ];

    const pipelineRes = await redisPipeline(commands);

    const saddResult = pipelineRes?.[0]?.result; // 1 if first time today, 0 otherwise
    const firstTimeToday = saddResult === 1;

    const counterCmds = [];

    if (isNewUser) counterCmds.push(["HINCRBY", dayHash, "new_users", 1]);
    else counterCmds.push(["HINCRBY", dayHash, "returning_users", 1]);

    if (firstTimeToday) counterCmds.push(["HINCRBY", dayHash, "unique_users", 1]);

    if (counterCmds.length) await redisPipeline(counterCmds);

    return ok({
      day,
      uid,
      path,
      is_new_user: isNewUser,
      first_time_today: firstTimeToday,
    });
  } catch (err) {
    console.error("track-visit error:", err);
    return bad(500, "Server error");
  }
};
