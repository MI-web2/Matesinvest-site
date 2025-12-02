// netlify/functions/stories-feed.js
const fetch = (...args) => global.fetch(...args);

exports.handler = async function () {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Upstash not configured");
    return { statusCode: 500, body: "Upstash not configured" };
  }

  try {
    // SMEMBERS mates:stories:approved
    const idsRes = await upstashPath(
      UPSTASH_URL,
      UPSTASH_TOKEN,
      `/smembers/${encodeURIComponent("mates:stories:approved")}`,
      "GET"
    );
    const ids = (idsRes && idsRes.result) || [];

    if (!ids.length) {
      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stories: [] }),
      };
    }

    const stories = [];
    for (const id of ids) {
      const r = await upstashPath(
        UPSTASH_URL,
        UPSTASH_TOKEN,
        `/hget/${encodeURIComponent(id)}/${encodeURIComponent("data")}`,
        "GET"
      );
      const raw = r && r.result;
      if (!raw) continue;
      try {
        stories.push(JSON.parse(raw));
      } catch {
        // skip bad json
      }
    }

    // newest first
    stories.sort((a, b) => {
      const da = new Date(a.createdAt || a.date || 0);
      const db = new Date(b.createdAt || b.date || 0);
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

async function upstashPath(baseUrl, token, path, method = "GET") {
  const url = `${baseUrl}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    console.error("Upstash path failed", method, path, res.status, txt);
    throw new Error("Upstash command failed");
  }
  return res.json().catch(() => null);
}
