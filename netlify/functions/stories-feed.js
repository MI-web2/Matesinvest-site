// netlify/functions/stories-feed.js
// Returns all APPROVED stories for MatesBook.

const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    // All approved IDs
    const resIds = await redisCommand(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      "SMEMBERS",
      "mates:stories:approved"
    );
    const ids = (resIds && resIds.result) || [];

    if (!ids.length) {
      return {
        statusCode: 200,
        body: JSON.stringify({ stories: [] }),
      };
    }

    // Fetch each story (HGET id data). Count will be small so 1-by-1 is fine.
    const stories = [];
    for (const id of ids) {
      const r = await redisCommand(UPSTASH_URL, UPSTASH_TOKEN, "HGET", id, "data");
      const raw = r && r.result;
      if (!raw) continue;
      try {
        const story = JSON.parse(raw);
        stories.push(story);
      } catch {
        // skip bad json
      }
    }

    // Sort newest first
    stories.sort((a, b) => {
      const da = new Date(a.createdAt || 0);
      const db = new Date(b.createdAt || 0);
      return db - da;
    });

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stories }),
    };
  } catch (err) {
    console.error("stories-feed error", err && (err.stack || err.message));
    return { statusCode: 500, body: "Internal error" };
  }
};

async function redisCommand(UPSTASH_URL, UPSTASH_TOKEN, ...command) {
  const res = await fetch(UPSTASH_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${UPSTASH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ command }),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    console.error("Upstash command failed", command[0], res.status, txt);
    throw new Error("Upstash command failed");
  }
  return res.json().catch(() => null);
}
