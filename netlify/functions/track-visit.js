// netlify/functions/track-visit.js
// Tracks new vs returning users + daily uniques + per-page repeat stats into Upstash Redis.

const fetch = (...args) => global.fetch(...args);

function getAESTDateString(ts = Date.now()) {
  // AEST = UTC+10 (no DST handling).
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
  const pathRaw = (payload.path || "/").trim();
  const path = (pathRaw || "/").slice(0, 200);
  const ts = Number(payload.ts || Date.now());

  if (!uid || uid.length < 8 || uid.length > 80) {
    return bad(400, "Missing/invalid uid");
  }

  const day = getAESTDateString(ts);

  // Global user keys
  const firstSeenKey = `mates:analytics:user:first_seen:${uid}`;
  const lastSeenKey = `mates:analytics:user:last_seen:${uid}`;

  // Day keys
  const dayHash = `mates:analytics:day:${day}`;
  const dayUids = `mates:analytics:day:${day}:uids`;
  const dayPaths = `mates:analytics:day:${day}:paths`; // simple per-path visit counts (already used)

  // New per-path repeat stats keys
  const dayPathStats = `mates:analytics:day:${day}:pathstats`; // hash: `${path}|visits`, `${path}|unique_users`, etc.
  const dayPathUids = `mates:analytics:day:${day}:pathuids:${encodeURIComponent(path)}`; // set of uids seen on this path today

  try {
    // 1) Determine if user is new globally (SETNX)
    const setnx = await redisCmd(["SETNX", firstSeenKey, String(ts)]);
    const isNewUser = setnx === 1;

    // 2) Main pipeline: record visit + sets
    // Order matters because we read back SADD results by index.
    const commands = [
      // 0: daily unique users set
      ["SADD", dayUids, uid],

      // 1: day totals
      ["HINCRBY", dayHash, "visits", 1],

      // 2: old per-path visit counts (nice quick “top pages”)
      ["HINCRBY", dayPaths, path || "/", 1],

      // 3: new per-path visit totals
      ["HINCRBY", dayPathStats, `${path || "/"}|visits`, 1],

      // 4: per-path unique set (detect first time on this path today)
      ["SADD", dayPathUids, uid],

      // 5: update last_seen
      ["SET", lastSeenKey, String(ts)],
    ];

    const pipelineRes = await redisPipeline(commands);

    const firstTimeToday = pipelineRes?.[0]?.result === 1;      // SADD dayUids
    const firstTimeOnThisPathToday = pipelineRes?.[4]?.result === 1; // SADD dayPathUids

    // 3) Counter increments (separate pipeline)
    const counterCmds = [];

    // Overall new vs returning
    if (isNewUser) counterCmds.push(["HINCRBY", dayHash, "new_users", 1]);
    else counterCmds.push(["HINCRBY", dayHash, "returning_users", 1]);

    // Overall uniques (per day)
    if (firstTimeToday) counterCmds.push(["HINCRBY", dayHash, "unique_users", 1]);

    // Per-path new vs returning
    if (isNewUser) counterCmds.push(["HINCRBY", dayPathStats, `${path || "/"}|new_users`, 1]);
    else counterCmds.push(["HINCRBY", dayPathStats, `${path || "/"}|returning_users`, 1]);

    // Per-path uniques (per day)
    if (firstTimeOnThisPathToday) {
      counterCmds.push(["HINCRBY", dayPathStats, `${path || "/"}|unique_users`, 1]);
    }

    if (counterCmds.length) await redisPipeline(counterCmds);

    return ok({
      day,
      uid,
      path,
      is_new_user: isNewUser,
      first_time_today: firstTimeToday,
      first_time_on_path_today: firstTimeOnThisPathToday,
    });
  } catch (err) {
    console.error("track-visit error:", err);
    return bad(500, "Server error");
  }
};
